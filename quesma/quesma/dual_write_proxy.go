// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package quesma

import (
	"context"
	"crypto/tls"
	"errors"
	"net/http"
	"quesma/clickhouse"
	"quesma/elasticsearch"
	"quesma/logger"
	"quesma/network"
	"quesma/quesma/config"
	"quesma/quesma/mux"
	"quesma/quesma/recovery"
	"quesma/quesma/ui"
	"quesma/schema"
	"quesma/telemetry"
	"strconv"
	"sync/atomic"
	"time"
)

type simultaneousClientsLimiter struct {
	counter atomic.Int64
	handler http.Handler
	limit   int64

	//authCache sync.Map
}

func newSimultaneousClientsLimiter(handler http.Handler, limit int64) *simultaneousClientsLimiter {
	return &simultaneousClientsLimiter{
		handler: handler,
		limit:   limit,
	}
}

func (c *simultaneousClientsLimiter) ServeHTTP(w http.ResponseWriter, r *http.Request) {

	current := c.counter.Load()
	// this is hard limit, we should not allow to go over it
	if current >= c.limit {
		logger.ErrorWithCtx(r.Context()).Msgf("Too many requests. current: %d, limit: %d", current, c.limit)
		http.Error(w, "Too many requests", http.StatusTooManyRequests)
		return
	}

	c.counter.Add(1)
	defer c.counter.Add(-1)

	//authHeader := r.Header.Get("Authorization")
	//if authHeader == "" {
	//	w.Header().Set("WWW-Authenticate", `Basic realm="restricted"`)
	//	http.Error(w, "Unauthorized", http.StatusUnauthorized)
	//	return
	//}
	//authParts := strings.SplitN(authHeader, " ", 2)
	//if len(authParts) != 2 || authParts[0] != "Basic" {
	//	logger.Warn().Msg("Something borked with auth hdr")
	//	http.Error(w, "Unauthorized", http.StatusUnauthorized)
	//	return
	//}
	//decodedUserAndPass, _ := base64.StdEncoding.DecodeString(authParts[1])
	//pair := strings.SplitN(string(decodedUserAndPass), ":", 2)
	//if len(pair) != 2 {
	//	http.Error(w, "Unauthorized", http.StatusUnauthorized)
	//	return
	//}
	//if _, ok := c.authCache.Load(authHeader); !ok {
	//	//check auth
	//	//if auth is ok, store it in cache
	//	c.authCache.Store(authHeader, struct{}{})
	//	//if auth is not ok, return unauthorized
	//} else {
	//
	//}
	//auth := r.Header.Get("Authorization")
	//if auth == "" {
	//	w.Header().Set("WWW-Authenticate", `Basic realm="restricted"`)
	//	http.Error(w, "Unauthorized", http.StatusUnauthorized)
	//	return
	//}
	//
	//authParts := strings.SplitN(auth, " ", 2)
	//if len(authParts) != 2 || authParts[0] != "Basic" {
	//	http.Error(w, "Unauthorized", http.StatusUnauthorized)
	//	return
	//}
	//
	//payload, _ := base64.StdEncoding.DecodeString(authParts[1])
	//pair := strings.SplitN(string(payload), ":", 2)
	//logger.Info().Msgf("PRZEMYSLAW - [%s] called out as [%s] with pass [%s]", r.URL, pair[0], pair[1])
	//if len(pair) != 2 {
	//	http.Error(w, "Unauthorized", http.StatusUnauthorized)
	//	return
	//}
	//if !((pair[0] != "elastic" && pair[1] != "pwpw") ||
	//	(pair[0] != "q1" && pair[1] != "q2") ||
	//	(pair[0] != "kibana" && pair[1] != "kibanana")) {
	//	http.Error(w, "Unauthorized", http.StatusUnauthorized)
	//	return
	//}

	c.handler.ServeHTTP(w, r)
}

type dualWriteHttpProxy struct {
	routingHttpServer   *http.Server
	elasticRouter       *mux.PathRouter
	indexManagement     elasticsearch.IndexManagement
	logManager          *clickhouse.LogManager
	publicPort          network.Port
	asyncQueriesEvictor *AsyncQueriesEvictor
	queryRunner         *QueryRunner
	schemaRegistry      schema.Registry
	schemaLoader        clickhouse.TableDiscovery
}

func (q *dualWriteHttpProxy) Stop(ctx context.Context) {
	q.Close(ctx)
}

func newDualWriteProxy(schemaLoader clickhouse.TableDiscovery, logManager *clickhouse.LogManager, indexManager elasticsearch.IndexManagement, registry schema.Registry, config *config.QuesmaConfiguration, pathRouter *mux.PathRouter, quesmaManagementConsole *ui.QuesmaManagementConsole, agent telemetry.PhoneHomeAgent, queryRunner *QueryRunner) *dualWriteHttpProxy {

	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	client := &http.Client{
		Transport: tr,
		Timeout:   time.Minute, // should be more configurable, 30s is Kibana default timeout
	}
	routerInstance := router{phoneHomeAgent: agent, config: config, quesmaManagementConsole: quesmaManagementConsole, httpClient: client, requestPreprocessors: processorChain{}}
	routerInstance.
		registerPreprocessor(NewTraceIdPreprocessor())

	agent.FailedRequestsCollector(func() int64 {
		return routerInstance.failedRequests.Load()
	})

	handler := http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		defer recovery.LogPanic()
		reqBody, err := peekBody(req)
		if err != nil {
			http.Error(w, "Error reading request body", http.StatusInternalServerError)
			return
		}

		ua := req.Header.Get("User-Agent")
		agent.UserAgentCounters().Add(ua, 1)

		routerInstance.reroute(req.Context(), w, req, reqBody, pathRouter, logManager)
	})
	var limitedHandler http.Handler
	if config.DisableAuth {
		limitedHandler = newSimultaneousClientsLimiter(handler, 100) // FIXME this should be configurable
	} else {
		limitedHandler = newSimultaneousClientsLimiter(NewAuthMiddleware(handler, config.Elasticsearch), 100)
	}

	return &dualWriteHttpProxy{
		elasticRouter:  pathRouter,
		schemaRegistry: registry,
		schemaLoader:   schemaLoader,
		routingHttpServer: &http.Server{
			Addr:    ":" + strconv.Itoa(int(config.PublicTcpPort)),
			Handler: limitedHandler,
		},
		indexManagement:     indexManager,
		logManager:          logManager,
		publicPort:          config.PublicTcpPort,
		asyncQueriesEvictor: NewAsyncQueriesEvictor(queryRunner.AsyncRequestStorage, queryRunner.AsyncQueriesContexts),
		queryRunner:         queryRunner,
	}
}

func (q *dualWriteHttpProxy) Close(ctx context.Context) {
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

func (q *dualWriteHttpProxy) Ingest() {
	q.schemaLoader.ReloadTableDefinitions()
	q.logManager.Start()
	q.indexManagement.Start()
	go q.asyncQueriesEvictor.asyncQueriesGC()
	go func() {
		if err := q.routingHttpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Fatal().Msgf("Error starting http server: %v", err)
		}
		logger.Info().Msgf("Accepting HTTP at :%d", q.publicPort)
	}()
}
