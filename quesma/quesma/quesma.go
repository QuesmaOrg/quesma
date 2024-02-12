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
	"mitmproxy/quesma/quesma/ui"
	"mitmproxy/quesma/tracing"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

type (
	Quesma struct {
		processor               RequestProcessor
		publicTcpPort           network.Port
		targetUrl               *url.URL
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

	if isSearch(uri) {
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
	targetUrl := parseURL(config.TargetElasticsearchAddr)
	return &Quesma{
		processor:               proxy.NewTcpProxy(config.PublicTcpPort, targetUrl, inspect),
		targetUrl:               targetUrl,
		publicTcpPort:           config.PublicTcpPort,
		quesmaManagementConsole: quesmaManagementConsole,
		config:                  config,
	}
}

func NewHttpProxy(logManager *clickhouse.LogManager, config config.QuesmaConfiguration, logChan <-chan string) *Quesma {
	return New(logManager, config, logChan)
}

func NewHttpClickhouseAdapter(logManager *clickhouse.LogManager, config config.QuesmaConfiguration, logChan <-chan string) *Quesma {
	return New(logManager, config, logChan)
}

func New(logManager *clickhouse.LogManager, config config.QuesmaConfiguration, logChan <-chan string) *Quesma {

	quesmaManagementConsole := ui.NewQuesmaManagementConsole(config, logChan)
	router := configureRouter(config, logManager, quesmaManagementConsole)
	q := &Quesma{
		processor: &dualWriteHttpProxy{
			elasticRouter: router,
			routingHttpServer: &http.Server{
				Addr: ":" + strconv.Itoa(int(config.PublicTcpPort)),
				Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					defer recovery.LogPanic()
					reqBody, err := io.ReadAll(r.Body)
					if err != nil {
						http.Error(w, "Error reading request body", http.StatusInternalServerError)
						return
					}

					ctx := withTracing(r)
					elkResponse := sendHttpRequest(ctx, "http://"+config.TargetElasticsearchAddr, r, reqBody)
					copyHeaders(w, elkResponse)
					copyStatusCode(w, elkResponse)

					// TODO check with Quesma config
					if strings.HasPrefix(r.URL.Path, "/.") && !strings.HasPrefix(r.URL.Path, "/logs") && !strings.HasPrefix(r.URL.Path, "/device") {
						logger.Debug().Ctx(ctx).Msgf("unrecognized index '%s', ignoring...", strings.Split(r.RequestURI, "/")[1])
						responseFromElastic(ctx, elkResponse, w)
					} else {
						quesmaResponse, matched, err := router.Execute(ctx, r.URL.Path, string(reqBody), r.Method)
						sendElkResponseToQuesmaConsole(ctx, r.RequestURI, elkResponse, quesmaManagementConsole)
						copyBody(w, r, matched, err, ctx, quesmaResponse, elkResponse)
					}
				}),
			},
			logManager: logManager,
			publicPort: config.PublicTcpPort,
		},
		targetUrl:               parseURL(config.TargetElasticsearchAddr),
		publicTcpPort:           config.PublicTcpPort,
		quesmaManagementConsole: quesmaManagementConsole,
		config:                  config,
	}

	return q
}

func copyBody(w http.ResponseWriter, r *http.Request, matched bool, err error, ctx context.Context, quesmaResponse string, elkResponse *http.Response) {
	if matched && isSearch(r.URL.Path) && err == nil {
		logger.Debug().Ctx(ctx).Msg("responding from quesma")
		unzipped := []byte(quesmaResponse)
		if string(unzipped) != "" {
			responseFromQuesma(ctx, unzipped, w, elkResponse)
		} else {
			responseFromElastic(ctx, elkResponse, w)
		}
	} else {
		if !matched {
			logger.Debug().Ctx(ctx).Msgf("Handler not found for URI, routing to Elastic: %s", r.URL.Path)
		}
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

func isSearch(path string) bool {
	return strings.Contains(path, "/_search") || strings.Contains(path, "/_async_search")
}

func withTracing(r *http.Request) context.Context {
	rid := tracing.GetRequestId()
	r.Header.Add("RequestId", rid)
	return context.WithValue(r.Context(), tracing.RequestIdCtxKey, rid)
}

func parseURL(urlStr string) *url.URL {
	parsed, err := url.Parse(urlStr)
	if err != nil {
		logger.Fatal().Msgf("Error parsing target url: %v", err)
	}
	return parsed
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
