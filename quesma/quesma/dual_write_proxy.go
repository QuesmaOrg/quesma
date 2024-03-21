package quesma

import (
	"context"
	"errors"
	"mitmproxy/quesma/clickhouse"
	"mitmproxy/quesma/logger"
	"mitmproxy/quesma/network"
	"mitmproxy/quesma/quesma/config"
	"mitmproxy/quesma/quesma/mux"
	"mitmproxy/quesma/quesma/recovery"
	"mitmproxy/quesma/quesma/ui"
	"mitmproxy/quesma/telemetry"
	"net/http"
	"strconv"
)

type dualWriteHttpProxy struct {
	routingHttpServer *http.Server
	elasticRouter     *mux.PathRouter
	logManager        *clickhouse.LogManager
	publicPort        network.Port
}

func (q *dualWriteHttpProxy) Stop(ctx context.Context) {
	q.Close(ctx)
}

func newDualWriteProxy(logManager *clickhouse.LogManager, config config.QuesmaConfiguration, router *mux.PathRouter, quesmaManagementConsole *ui.QuesmaManagementConsole, agent telemetry.PhoneHomeAgent) *dualWriteHttpProxy {
	return &dualWriteHttpProxy{
		elasticRouter: router,
		routingHttpServer: &http.Server{
			Addr: ":" + strconv.Itoa(int(config.PublicTcpPort)),
			Handler: http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				defer recovery.LogPanic()
				reqBody, err := peekBody(req)
				if err != nil {
					http.Error(w, "Error reading request body", http.StatusInternalServerError)
					return
				}

				ua := req.Header.Get("User-Agent")
				agent.UserAgentCounters().Add(ua, 1)

				reroute(withTracing(req), w, req, reqBody, router, config, quesmaManagementConsole, agent)
			}),
		},
		logManager: logManager,
		publicPort: config.PublicTcpPort,
	}
}

func (q *dualWriteHttpProxy) Close(ctx context.Context) {
	if q.logManager != nil {
		defer q.logManager.Close()
	}
	if err := q.routingHttpServer.Shutdown(ctx); err != nil {
		logger.Fatal().Msgf("Error during server shutdown: %v", err)
	}
}

func (q *dualWriteHttpProxy) Ingest() {
	q.logManager.Start()
	go func() {
		if err := q.routingHttpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Fatal().Msgf("Error starting http server: %v", err)
		}
		logger.Info().Msgf("Accepting HTTP at :%d", q.publicPort)
	}()
}
