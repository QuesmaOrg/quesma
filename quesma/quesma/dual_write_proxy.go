package quesma

import (
	"context"
	"crypto/tls"
	"errors"
	"mitmproxy/quesma/clickhouse"
	"mitmproxy/quesma/elasticsearch"
	"mitmproxy/quesma/logger"
	"mitmproxy/quesma/network"
	"mitmproxy/quesma/quesma/config"
	"mitmproxy/quesma/quesma/mux"
	"mitmproxy/quesma/quesma/recovery"
	"mitmproxy/quesma/quesma/ui"
	"mitmproxy/quesma/telemetry"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"
)

type simultaneousClientsLimiter struct {
	counter   atomic.Int64
	handler   http.Handler
	softLimit int64
	hardLimit int64
}

func newSimultaneousClientsLimiter(handler http.Handler, softLimit, hardLimit int64) *simultaneousClientsLimiter {
	return &simultaneousClientsLimiter{
		handler:   handler,
		hardLimit: hardLimit,
		softLimit: softLimit,
	}
}

func (c *simultaneousClientsLimiter) ServeHTTP(w http.ResponseWriter, r *http.Request) {

	current := c.counter.Load()
	// this is hard limit, we should not allow to go over it
	if current >= c.hardLimit {
		logger.ErrorWithCtx(r.Context()).Msgf("Too many requests. current: %d, hard limit: %d", current, c.hardLimit)
		http.Error(w, "Too many requests", http.StatusTooManyRequests)
		return
	}

	if current >= c.softLimit {
		var tries = []int64{1, 2, 5}
		var shouldPass bool
		for _, wait := range tries {
			logger.DebugWithCtx(r.Context()).Msgf("Too many requests. current: %d, soft limit: %d, waiting %d s", current, c.softLimit, wait)
			time.Sleep(time.Duration(wait) * time.Second)

			current = c.counter.Load()
			if current < c.softLimit {
				shouldPass = true
				break
			}
		}

		if !shouldPass {
			logger.ErrorWithCtx(r.Context()).Msgf("Too many requests. Waiting didn't help. current: %d, soft limit: %d", current, c.softLimit)
			http.Error(w, "Too many requests", http.StatusTooManyRequests)
			return
		} else {
			logger.DebugWithCtx(r.Context()).Msgf("Too many requests. current: %d, soft limit: %d, waiting did help", current, c.softLimit)
		}
	}

	c.counter.Add(1)
	defer c.counter.Add(-1)

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
}

func (q *dualWriteHttpProxy) Stop(ctx context.Context) {
	q.Close(ctx)
}

func newDualWriteProxy(logManager *clickhouse.LogManager, indexManager elasticsearch.IndexManagement, config config.QuesmaConfiguration, pathRouter *mux.PathRouter, quesmaManagementConsole *ui.QuesmaManagementConsole, agent telemetry.PhoneHomeAgent, queryRunner *QueryRunner) *dualWriteHttpProxy {
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	client := &http.Client{
		Transport: tr,
		Timeout:   time.Minute, // should be more configurable, 30s is Kibana default timeout
	}
	routerInstance := router{phoneHomeAgent: agent, config: config, quesmaManagementConsole: quesmaManagementConsole, httpClient: client}

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

		routerInstance.reroute(withTracing(req), w, req, reqBody, pathRouter, logManager)
	})

	limitedHandler := newSimultaneousClientsLimiter(handler, 50, 100) // FIXME this should be configurable

	return &dualWriteHttpProxy{
		elasticRouter: pathRouter,
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
