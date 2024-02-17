package quesma

import (
	"bytes"
	"context"
	"errors"
	"io"
	"mitmproxy/quesma/clickhouse"
	"mitmproxy/quesma/logger"
	"mitmproxy/quesma/network"
	"mitmproxy/quesma/proxy"
	"mitmproxy/quesma/quesma/config"
	"mitmproxy/quesma/quesma/gzip"
	"mitmproxy/quesma/quesma/mux"
	"mitmproxy/quesma/quesma/recovery"
	"mitmproxy/quesma/quesma/routes"
	"mitmproxy/quesma/quesma/ui"
	"mitmproxy/quesma/tracing"
	"net/http"
	"strconv"
	"strings"
)

type (
	Quesma struct {
		processor               RequestProcessor
		publicTcpPort           network.Port
		quesmaManagementConsole *ui.QuesmaManagementConsole
		config                  config.QuesmaConfiguration
	}
	RequestProcessor interface {
		Ingest()
		Stop(ctx context.Context)
	}
	dualWriteHttpProxy struct {
		routingHttpServer *http.Server
		elasticRouter     *mux.PathRouter
		logManager        *clickhouse.LogManager
		publicPort        network.Port
	}
)

func (q *dualWriteHttpProxy) Stop(ctx context.Context) {
	q.Close(ctx)
}

func responseFromElastic(ctx context.Context, elkResponse *http.Response, w http.ResponseWriter) {
	id := ctx.Value(tracing.RequestIdCtxKey).(string)
	logger.Debug().Str(logger.RID, id).Msg("responding from Elasticsearch")
	if _, err := io.Copy(w, elkResponse.Body); err != nil {
		http.Error(w, "Error copying response body", http.StatusInternalServerError)
		return
	}
	elkResponse.Body.Close()
}

func responseFromQuesma(ctx context.Context, unzipped []byte, w http.ResponseWriter, elkResponse *http.Response) {
	id := ctx.Value(tracing.RequestIdCtxKey).(string)
	logger.Debug().Str(logger.RID, id).Msg("responding from Quesma")
	if gzip.IsGzipped(elkResponse) {
		zipped, err := gzip.Zip(unzipped)
		if err == nil {
			w.Header().Add("Content-Length", strconv.Itoa(len(zipped)))
			_, _ = io.Copy(w, bytes.NewBuffer(zipped))
		}
	} else {
		w.Header().Add("Content-Length", strconv.Itoa(len(unzipped)))
		_, _ = io.Copy(w, bytes.NewBuffer(unzipped))
	}
}

func sendElkResponseToQuesmaConsole(ctx context.Context, uri string, elkResponse *http.Response, console *ui.QuesmaManagementConsole) {
	reader := elkResponse.Body
	body, err := io.ReadAll(reader)
	id := ctx.Value(tracing.RequestIdCtxKey).(string)
	if err != nil {
		logger.Fatal().Str(logger.RID, id).Msg(err.Error())
	}
	elkResponse.Body = io.NopCloser(bytes.NewBuffer(body))

	if routes.IsIndexSearchPath(uri) || routes.IsIndexAsyncSearchPath(uri) {
		if gzip.IsGzipped(elkResponse) {
			body, err = gzip.UnZip(body)
			if err != nil {
				logger.Error().Str(logger.RID, id).Msgf("Error unzipping: %v", err)
			}
		}
		console.PushPrimaryInfo(&ui.QueryDebugPrimarySource{Id: id, QueryResp: body})
	}
}

func NewQuesmaTcpProxy(config config.QuesmaConfiguration, logChan <-chan string, inspect bool) *Quesma {
	quesmaManagementConsole := ui.NewQuesmaManagementConsole(config, logChan)
	return &Quesma{
		processor:               proxy.NewTcpProxy(config.PublicTcpPort, config.ElasticsearchUrl.Host, inspect),
		publicTcpPort:           config.PublicTcpPort,
		quesmaManagementConsole: quesmaManagementConsole,
		config:                  config,
	}
}

func NewHttpProxy(logManager *clickhouse.LogManager, config config.QuesmaConfiguration, logChan <-chan string) *Quesma {
	quesmaManagementConsole := ui.NewQuesmaManagementConsole(config, logChan)
	router := configureRouter(config, logManager, quesmaManagementConsole)
	return &Quesma{
		processor:               newDualWriteProxy(logManager, config, router, quesmaManagementConsole),
		publicTcpPort:           config.PublicTcpPort,
		quesmaManagementConsole: quesmaManagementConsole,
		config:                  config,
	}
}

func newDualWriteProxy(logManager *clickhouse.LogManager, config config.QuesmaConfiguration, router *mux.PathRouter, quesmaManagementConsole *ui.QuesmaManagementConsole) *dualWriteHttpProxy {
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

				reroute(withTracing(req), w, req, reqBody, router, config, quesmaManagementConsole)
			}),
		},
		logManager: logManager,
		publicPort: config.PublicTcpPort,
	}
}

func reroute(ctx context.Context, w http.ResponseWriter, req *http.Request, reqBody []byte, router *mux.PathRouter, config config.QuesmaConfiguration, quesmaManagementConsole *ui.QuesmaManagementConsole) {
	if isElasticInternalRequest(req) {
		response := sendHttpRequest(ctx, config.ElasticsearchUrl.String(), req, reqBody)
		copyHeaders(w, response)
		copyStatusCode(w, response)
		responseFromElastic(ctx, response, w)
	} else {
		elkResponseChan := make(chan *http.Response)
		go func() {
			elkResponseChan <- sendHttpRequest(ctx, config.ElasticsearchUrl.String(), req, reqBody)
		}()
		quesmaResponse, matched, err := router.Execute(ctx, req.URL.Path, string(reqBody), req.Method)
		elkResponse := <-elkResponseChan
		copyHeaders(w, elkResponse)
		copyStatusCode(w, elkResponse)
		sendElkResponseToQuesmaConsole(ctx, req.RequestURI, elkResponse, quesmaManagementConsole)
		copyBody(w, req, matched, err, ctx, quesmaResponse, elkResponse, config)
	}
}

func peekBody(r *http.Request) ([]byte, error) {
	reqBody, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}
	r.Body = io.NopCloser(bytes.NewBuffer(reqBody))
	return reqBody, nil
}

func isElasticInternalRequest(r *http.Request) bool {
	return strings.HasPrefix(r.URL.Path, "/.")
}

func isEnabled(path string, configuration config.QuesmaConfiguration) bool {
	if indexConfig, found := configuration.GetIndexConfig(strings.Split(path, "/")[1]); found {
		return indexConfig.Enabled
	} else {
		return false
	}
}

func copyBody(w http.ResponseWriter, r *http.Request, matched bool, err error, ctx context.Context, quesmaResponse string, elkResponse *http.Response, configuration config.QuesmaConfiguration) {
	if matched && isEnabled(r.URL.Path, configuration) && (routes.IsIndexAsyncSearchPath(r.URL.Path) || routes.IsIndexSearchPath(r.URL.Path)) && err == nil {
		logger.Debug().Ctx(ctx).Msg("responding from quesma")
		unzipped := []byte(quesmaResponse)
		if string(unzipped) != "" {
			responseFromQuesma(ctx, unzipped, w, elkResponse)
		} else {
			logger.Error().Ctx(ctx).Msg("Empty response from Quesma, responding from Elastic")
			responseFromElastic(ctx, elkResponse, w)
		}
	} else {
		if err != nil {
			logger.Error().Ctx(ctx).Msgf("Error processing request: %v, responding from Elastic", err)
		}
		responseFromElastic(ctx, elkResponse, w)
	}
}

func copyStatusCode(w http.ResponseWriter, elkResponse *http.Response) {
	w.WriteHeader(elkResponse.StatusCode)
}

func copyHeaders(w http.ResponseWriter, elkResponse *http.Response) {
	for key, values := range elkResponse.Header {
		for _, value := range values {
			if key != "Content-Length" {
				if w.Header().Get(key) == "" {
					w.Header().Add(key, value)
				}
			}
		}
	}
}

func withTracing(r *http.Request) context.Context {
	rid := tracing.GetRequestId()
	r.Header.Add("RequestId", rid)
	return context.WithValue(r.Context(), tracing.RequestIdCtxKey, rid)
}

func (q *Quesma) Close(ctx context.Context) {
	q.processor.Stop(ctx)
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
	go q.listen()
}

func (q *Quesma) Start() {
	defer recovery.LogPanic()
	logger.Info().Msgf("starting quesma in the mode: %v", q.config.Mode)
	go q.processor.Ingest()
	go q.quesmaManagementConsole.Run()
}

func sendHttpRequest(ctx context.Context, address string, originalReq *http.Request, originalReqBody []byte) *http.Response {
	id := ctx.Value(tracing.RequestIdCtxKey).(string)
	req, err := http.NewRequestWithContext(ctx, originalReq.Method, address+originalReq.URL.String(), bytes.NewBuffer(originalReqBody))
	if err != nil {
		logger.Error().Str(logger.RID, id).Msgf("Error creating request: %v", err)
		return nil
	}

	req.Header = originalReq.Header
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		logger.Error().Str(logger.RID, id).Msgf("Error sending request: %v", err)
		return nil
	}

	return resp
}

func (q *dualWriteHttpProxy) listen() {
	if err := q.routingHttpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Fatal().Msgf("Error starting http server: %v", err)
	}
	logger.Info().Msgf("Accepting HTTP at :%d", q.publicPort)
}
