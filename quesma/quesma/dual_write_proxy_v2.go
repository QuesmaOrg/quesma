// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package quesma

import (
	"context"
	"github.com/QuesmaOrg/quesma/quesma/ab_testing"
	"github.com/QuesmaOrg/quesma/quesma/backend_connectors"
	"github.com/QuesmaOrg/quesma/quesma/clickhouse"
	"github.com/QuesmaOrg/quesma/quesma/elasticsearch"
	"github.com/QuesmaOrg/quesma/quesma/frontend_connectors"
	"github.com/QuesmaOrg/quesma/quesma/ingest"
	"github.com/QuesmaOrg/quesma/quesma/logger"
	"github.com/QuesmaOrg/quesma/quesma/queryparser"
	"github.com/QuesmaOrg/quesma/quesma/quesma/async_search_storage"
	"github.com/QuesmaOrg/quesma/quesma/quesma/config"
	"github.com/QuesmaOrg/quesma/quesma/schema"
	"github.com/QuesmaOrg/quesma/quesma/table_resolver"
	"github.com/QuesmaOrg/quesma/quesma/util"
	"net/http"
	quesma_api "quesma_v2/core"
	"strconv"
	"sync/atomic"
)

const concurrentClientsLimitV2 = 100 // FIXME this should be configurable

type simultaneousClientsLimiterV2 struct {
	counter atomic.Int64
	limit   int64
}

func newSimultaneousClientsLimiterV2(limit int64) *simultaneousClientsLimiterV2 {
	return &simultaneousClientsLimiterV2{
		limit: limit,
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
}

type dualWriteHttpProxyV2 struct {
	quesmaV2            quesma_api.QuesmaBuilder
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

func newDualWriteProxyV2(dependencies quesma_api.Dependencies, schemaLoader clickhouse.TableDiscovery, logManager *clickhouse.LogManager, indexManager elasticsearch.IndexManagement, registry schema.Registry, config *config.QuesmaConfiguration, ingestProcessor *ingest.IngestProcessor, resolver table_resolver.TableResolver, abResultsRepository ab_testing.Sender) *dualWriteHttpProxyV2 {

	queryProcessor := NewQueryRunner(logManager, config, indexManager, dependencies.DebugInfoCollector(), registry, abResultsRepository, resolver, schemaLoader)

	// not sure how we should configure our query translator ???
	// is this a config option??

	queryProcessor.DateMathRenderer = queryparser.DateMathExpressionFormatLiteral

	// tests should not be run with optimization enabled by default
	queryProcessor.EnableQueryOptimization(config)
	esConn := backend_connectors.NewElasticsearchBackendConnector(config.Elasticsearch)

	ingestRouter := ConfigureIngestRouterV2(config, dependencies, ingestProcessor, resolver, esConn)
	searchRouter := ConfigureSearchRouterV2(config, dependencies, registry, logManager, queryProcessor, resolver)

	elasticHttpIngestFrontendConnector := frontend_connectors.NewElasticHttpIngestFrontendConnector(":"+strconv.Itoa(int(config.PublicTcpPort)),
		logManager, registry, config, ingestRouter)

	elasticHttpQueryFrontendConnector := frontend_connectors.NewElasticHttpQueryFrontendConnector(":"+strconv.Itoa(int(config.PublicTcpPort)),
		logManager, registry, config, searchRouter)

	quesmaBuilder := quesma_api.NewQuesma(dependencies)
	ingestPipeline := quesma_api.NewPipeline()
	ingestPipeline.AddFrontendConnector(elasticHttpIngestFrontendConnector)

	queryPipeline := quesma_api.NewPipeline()
	queryPipeline.AddFrontendConnector(elasticHttpQueryFrontendConnector)
	quesmaBuilder.AddPipeline(queryPipeline)
	quesmaBuilder.AddPipeline(ingestPipeline)

	quesmaV2, err := quesmaBuilder.Build()
	if err != nil {
		logger.Fatal().Msgf("Error building Quesma: %v", err)
	}
	if config.DisableAuth {
		elasticHttpIngestFrontendConnector.AddMiddleware(newSimultaneousClientsLimiterV2(concurrentClientsLimitV2))
		elasticHttpQueryFrontendConnector.AddMiddleware(newSimultaneousClientsLimiterV2(concurrentClientsLimitV2))
	} else {
		elasticHttpQueryFrontendConnector.AddMiddleware(newSimultaneousClientsLimiterV2(concurrentClientsLimitV2))
		elasticHttpQueryFrontendConnector.AddMiddleware(NewAuthMiddlewareV2(config.Elasticsearch))
		elasticHttpIngestFrontendConnector.AddMiddleware(newSimultaneousClientsLimiterV2(concurrentClientsLimitV2))
		elasticHttpIngestFrontendConnector.AddMiddleware(NewAuthMiddlewareV2(config.Elasticsearch))
	}

	return &dualWriteHttpProxyV2{
		schemaRegistry:  registry,
		schemaLoader:    schemaLoader,
		quesmaV2:        quesmaV2,
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
	q.quesmaV2.Stop(ctx)
}

func (q *dualWriteHttpProxyV2) Ingest() {
	q.schemaLoader.ReloadTableDefinitions()
	q.logManager.Start()
	q.indexManagement.Start()
	go q.asyncQueriesEvictor.AsyncQueriesGC()
	q.quesmaV2.Start()
}
