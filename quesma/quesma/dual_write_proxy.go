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
	"mitmproxy/quesma/tracing"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"
)

type simultaneousClientsLimiter struct {
	counter atomic.Int64
	handler http.Handler
	limit   int64
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

	handler := http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		defer recovery.LogPanic()
		reqBody, err := peekBody(req)
		if err != nil {
			http.Error(w, "Error reading request body", http.StatusInternalServerError)
			return
		}

		ua := req.Header.Get("User-Agent")
		agent.UserAgentCounters().Add(ua, 1)

		rid := tracing.GetRequestId()
		req.Header.Add("RequestId", rid)
		ctx := req.Context()
		ctx = context.WithValue(ctx, tracing.RequestIdCtxKey, rid)
		ctx = context.WithValue(ctx, tracing.RequestPath, req.URL.Path)
		streamHandlerBypass := NewStreamHandlerBypass(ctx, config, w, pathRouter)
		bypassed := bypass(ctx, NewBypassChain(streamHandlerBypass), HttpRequest{Body: string(reqBody), Path: req.URL.Path, Method: req.Method, Headers: req.Header})

		if !bypassed {
			routerInstance.reroute(withTracing(req), w, req, reqBody, pathRouter, logManager)
		}
	})

	limitedHandler := newSimultaneousClientsLimiter(handler, 50) // FIXME this should be configurable

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

func bypass(ctx context.Context, bypassChain *BypassChain, r HttpRequest) bool {
	bypassed := false
	for _, processor := range bypassChain.Bypasses {
		if processor.Applies(r) {
			bypassed = true
			_, _ = processor.Execute(ctx, r, r.Headers)
		}
	}
	return bypassed
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
