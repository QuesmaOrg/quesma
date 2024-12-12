// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package quesma

import (
	"context"
	"crypto/tls"
	"errors"
	"net/http"
	"quesma/ab_testing"
	"quesma/clickhouse"
	"quesma/elasticsearch"
	"quesma/frontend_connectors"
	"quesma/ingest"
	"quesma/logger"
	"quesma/queryparser"
	"quesma/quesma/async_search_storage"
	"quesma/quesma/config"
	"quesma/quesma/ui"
	"quesma/schema"
	"quesma/table_resolver"
	"quesma/telemetry"
	"quesma/util"
	quesma_api "quesma_v2/core"
	"strconv"
	"sync/atomic"
	"time"
)

const concurrentClientsLimitV2 = 100 // FIXME this should be configurable

type simultaneousClientsLimiterV2 struct {
	counter atomic.Int64
	handler http.Handler
	limit   int64
}

func newSimultaneousClientsLimiterV2(handler http.Handler, limit int64) *simultaneousClientsLimiterV2 {
	return &simultaneousClientsLimiterV2{
		handler: handler,
		limit:   limit,
	}
}

func (c *simultaneousClientsLimiterV2) ServeHTTP(w http.ResponseWriter, r *http.Request) {

	current := c.counter.Load()
	// this is hard limit, we should not allow to go over it
	if current >= c.limit {
		logger.ErrorWithCtx(r.Context()).Msgf("Too many requests. current: %d, limit: %d", current, c.limit)
		http.Error(w, "Too many requests", http.StatusTooManyRequests)
		return
	}

	c.counter.Add(1)
	defer c.counter.Add(-1)
	c.handler.ServeHTTP(w, r)
}

type dualWriteHttpProxyV2 struct {
	routingHttpServer   *http.Server
	indexManagement     elasticsearch.IndexManagement
	logManager          *clickhouse.LogManager
	publicPort          util.Port
	asyncQueriesEvictor *async_search_storage.AsyncQueriesEvictor
	queryRunner         *QueryRunner
	schemaRegistry      schema.Registry
	schemaLoader        clickhouse.TableDiscovery
}

func (q *dualWriteHttpProxyV2) Stop(ctx context.Context) {
	q.Close(ctx)
}

func newDualWriteProxyV2(schemaLoader clickhouse.TableDiscovery, logManager *clickhouse.LogManager, indexManager elasticsearch.IndexManagement, registry schema.Registry, config *config.QuesmaConfiguration, quesmaManagementConsole *ui.QuesmaManagementConsole, agent telemetry.PhoneHomeAgent, ingestProcessor *ingest.IngestProcessor, resolver table_resolver.TableResolver, abResultsRepository ab_testing.Sender) *dualWriteHttpProxyV2 {
	queryProcessor := NewQueryRunner(logManager, config, indexManager, quesmaManagementConsole, registry, abResultsRepository, resolver, schemaLoader)

	// not sure how we should configure our query translator ???
	// is this a config option??

	queryProcessor.DateMathRenderer = queryparser.DateMathExpressionFormatLiteral

	// tests should not be run with optimization enabled by default
	queryProcessor.EnableQueryOptimization(config)

	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	client := &http.Client{
		Transport: tr,
		Timeout:   time.Minute, // should be more configurable, 30s is Kibana default timeout
	}

	routerInstance := frontend_connectors.RouterV2{PhoneHomeAgent: agent,
		Config: config, QuesmaManagementConsole: quesmaManagementConsole,
		HttpClient: client, RequestPreprocessors: quesma_api.ProcessorChain{}}
	routerInstance.
		RegisterPreprocessor(quesma_api.NewTraceIdPreprocessor())
	agent.FailedRequestsCollector(func() int64 {
		return routerInstance.FailedRequests.Load()
	})

	ingestRouter := ConfigureIngestRouterV2(config, ingestProcessor, agent, resolver)
	searchRouter := ConfigureSearchRouterV2(config, registry, logManager, quesmaManagementConsole, queryProcessor, resolver)

	elasticHttpIngestFrontendConnector := NewElasticHttpIngestFrontendConnector(":"+strconv.Itoa(int(config.PublicTcpPort)),
		&routerInstance, logManager, registry, agent)
	elasticHttpIngestFrontendConnector.AddRouter(ingestRouter)

	elasticHttpQueryFrontendConnector := NewElasticHttpQueryFrontendConnector(":"+strconv.Itoa(int(config.PublicTcpPort)),
		&routerInstance, logManager, registry, agent)
	elasticHttpQueryFrontendConnector.AddRouter(searchRouter)

	quesmaBuilder := quesma_api.NewQuesma()
	ingestPipeline := quesma_api.NewPipeline()
	ingestPipeline.AddFrontendConnector(elasticHttpIngestFrontendConnector)

	queryPipeline := quesma_api.NewPipeline()
	queryPipeline.AddFrontendConnector(elasticHttpQueryFrontendConnector)
	quesmaBuilder.AddPipeline(ingestPipeline)
	quesmaBuilder.AddPipeline(queryPipeline)
	_, err := quesmaBuilder.Build()
	if err != nil {
		logger.Fatal().Msgf("Error building Quesma: %v", err)
	}

	var limitedHandler http.Handler
	if config.DisableAuth {
		limitedHandler = newSimultaneousClientsLimiterV2(elasticHttpIngestFrontendConnector, concurrentClientsLimitV2)
	} else {
		limitedHandler = newSimultaneousClientsLimiterV2(NewAuthMiddleware(elasticHttpIngestFrontendConnector, config.Elasticsearch), concurrentClientsLimitV2)
	}

	return &dualWriteHttpProxyV2{
		schemaRegistry: registry,
		schemaLoader:   schemaLoader,
		routingHttpServer: &http.Server{
			Addr:    ":" + strconv.Itoa(int(config.PublicTcpPort)),
			Handler: limitedHandler,
		},
		indexManagement: indexManager,
		logManager:      logManager,
		publicPort:      config.PublicTcpPort,
		asyncQueriesEvictor: async_search_storage.NewAsyncQueriesEvictor(
			queryProcessor.AsyncRequestStorage.(async_search_storage.AsyncSearchStorageInMemory),
			queryProcessor.AsyncQueriesContexts.(async_search_storage.AsyncQueryContextStorageInMemory),
		),
		queryRunner: queryProcessor,
	}
}

func (q *dualWriteHttpProxyV2) Close(ctx context.Context) {
	if q.logManager != nil {
		defer q.logManager.Close()
	}
	if q.queryRunner != nil {
		q.queryRunner.Close()
	}
	if q.asyncQueriesEvictor != nil {
		q.asyncQueriesEvictor.Close()
	}
	if err := q.routingHttpServer.Shutdown(ctx); err != nil {
		logger.Fatal().Msgf("Error during server shutdown: %v", err)
	}
}

func (q *dualWriteHttpProxyV2) Ingest() {
	q.schemaLoader.ReloadTableDefinitions()
	q.logManager.Start()
	q.indexManagement.Start()
	go q.asyncQueriesEvictor.AsyncQueriesGC()
	go func() {
		if err := q.routingHttpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Fatal().Msgf("Error starting http server: %v", err)
		}
		logger.Info().Msgf("Accepting HTTP at :%d", q.publicPort)
	}()
}
