package quesma

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"mitmproxy/quesma/clickhouse"
	"mitmproxy/quesma/logger"
	"mitmproxy/quesma/network"
	"mitmproxy/quesma/proxy"
	"mitmproxy/quesma/quesma/config"
	"mitmproxy/quesma/quesma/gzip"
	"mitmproxy/quesma/quesma/recovery"
	"mitmproxy/quesma/quesma/ui"
	"mitmproxy/quesma/tcp"
	"mitmproxy/quesma/tracing"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
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
		processingHttpServer *http.Server
		routingHttpServer    *http.Server
		logManager           *clickhouse.LogManager
		internalHttpPort     string
		publicPort           string
	}
)

func (p *dualWriteHttpProxy) Stop(ctx context.Context) {
	p.Close(ctx)
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

	if strings.Contains(uri, "/_search") || strings.Contains(uri, "/_async_search") {
		if gzip.IsGzipped(elkResponse) {
			body, err = gzip.UnZip(body)
			if err != nil {
				logger.Error().Str(logger.RID, id).Msgf("Error unzipping: %v", err)
			}
		}
		console.PushPrimaryInfo(&ui.QueryDebugPrimarySource{Id: id, QueryResp: body})
	}
}

func NewQuesmaTcpProxy(target string, tcpPort string, config config.QuesmaConfiguration, logChan <-chan string, inspect bool) *Quesma {
	quesmaManagementConsole := ui.NewQuesmaManagementConsole(config, logChan)
	port := parsePort(tcpPort)
	targetUrl := parseURL(target)
	return &Quesma{
		processor:               proxy.NewTcpProxy(port, targetUrl, inspect),
		targetUrl:               targetUrl,
		publicTcpPort:           port,
		quesmaManagementConsole: quesmaManagementConsole,
		config:                  config,
	}
}

func NewHttpProxy(logManager *clickhouse.LogManager, target string, tcpPort string, httpPort string, config config.QuesmaConfiguration, logChan <-chan string) *Quesma {
	return New(logManager, target, tcpPort, httpPort, config, logChan)
}

func NewHttpClickhouseAdapter(logManager *clickhouse.LogManager, target string, tcpPort string,
	httpPort string, config config.QuesmaConfiguration, logChan <-chan string) *Quesma {
	return New(logManager, target, tcpPort, httpPort, config, logChan)
}

func New(logManager *clickhouse.LogManager, target string, tcpPort string, httpPort string, config config.QuesmaConfiguration, logChan <-chan string) *Quesma {
	quesmaManagementConsole := ui.NewQuesmaManagementConsole(config, logChan)
	q := &Quesma{
		processor: &dualWriteHttpProxy{
			processingHttpServer: &http.Server{
				Addr:    ":" + httpPort,
				Handler: configureRouting(config, logManager, quesmaManagementConsole),
			},
			routingHttpServer: &http.Server{
				Addr: ":" + tcpPort,
				Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					defer recovery.LogPanic()
					reqBody, err := io.ReadAll(r.Body)
					if err != nil {
						http.Error(w, "Error reading request body", http.StatusInternalServerError)
						return
					}

					ctx := withTracing(r)
					elkResponse := sendHttpRequest(ctx, "http://"+target, r, reqBody)
					quesmaResponse := sendHttpRequest(ctx, "http://localhost:"+httpPort, r, reqBody)

					if elkResponse == nil {
						logger.Panic().Msg("elkResponse is nil")
						panic("elkResponse is nil") // probably unnecessary, as previous call should do the job. Suppresses linter.
					}

					if quesmaResponse == nil {
						logger.Panic().Msg("quesmaResponse is nil")
						panic("quesmaResponse is nil") // probably unnecessary, as previous call should do the job. Suppresses linter.
					}

					sendElkResponseToQuesmaConsole(ctx, r.RequestURI, elkResponse, quesmaManagementConsole)

					for key, values := range elkResponse.Header {
						for _, value := range values {
							if key != "Content-Length" {
								if w.Header().Get(key) == "" {
									w.Header().Add(key, value)
								}
							}
						}
					}

					w.WriteHeader(elkResponse.StatusCode)

					if quesmaResponse.StatusCode == 200 && (strings.Contains(r.RequestURI, "/_search") || strings.Contains(r.RequestURI, "/_async_search")) {
						logger.Debug().Ctx(ctx).Msg("responding from quesma")
						unzipped, err := io.ReadAll(quesmaResponse.Body)
						if err == nil {
							// Sometimes when query is invalid, quesma returns empty response,
							// and we have to handle this case.
							// When this happens, we want to return response from elk (for now), look else branch.
							if string(unzipped) != "" {
								responseFromQuesma(ctx, unzipped, w, elkResponse)
							} else {
								responseFromElastic(ctx, elkResponse, w)
							}
						}
					} else {
						responseFromElastic(ctx, elkResponse, w)
					}
				}),
			},
			logManager:       logManager,
			internalHttpPort: httpPort,
			publicPort:       tcpPort,
		},
		targetUrl:               parseURL(target),
		publicTcpPort:           parsePort(tcpPort),
		quesmaManagementConsole: quesmaManagementConsole,
		config:                  config,
	}

	return q
}

func withTracing(r *http.Request) context.Context {
	rid := tracing.GetRequestId()
	r.Header.Add("RequestId", rid)
	return context.WithValue(r.Context(), tracing.RequestIdCtxKey, rid)
}

func parsePort(port string) network.Port {
	tcpPortInt, err := strconv.Atoi(port)
	if err != nil {
		logger.Fatal().Msgf("Error parsing tcp port %s: %v", port, err)
	}
	if tcpPortInt < 0 || tcpPortInt > 65535 {
		logger.Fatal().Msgf("Invalid port: %s", port)
	}
	return network.Port(tcpPortInt)
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

func (p *dualWriteHttpProxy) Close(ctx context.Context) {
	if p.logManager != nil {
		defer p.logManager.Close()
	}
	if err := p.processingHttpServer.Shutdown(ctx); err != nil {
		logger.Fatal().Msgf("Error during server shutdown: %v", err)
	}
}

func (p *dualWriteHttpProxy) Ingest() {
	go p.listenRoutingHTTP()
	go p.listenHTTP()
}

func (p *dualWriteHttpProxy) listenRoutingHTTP() {
	if err := p.routingHttpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Fatal().Msgf("Error starting http server: %v", err)
	}
}

func (p *dualWriteHttpProxy) listenHTTP() {
	if err := p.processingHttpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Fatal().Msgf("Error starting http server: %v", err)
	}
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

func (q *dualWriteHttpProxy) listen() (net.Listener, error) {
	go q.listenHTTP()
	logger.Info().Msgf("listening TCP at %s", q.publicPort)
	listener, err := net.Listen("tcp", ":"+q.publicPort)
	if err != nil {
		return nil, err
	}
	return listener, nil
}

func (q *Quesma) handleRequest(in net.Conn) {
	defer recovery.LogPanic()
	defer in.Close()
	httpPort := ""
	internalHttpServerConnection, err := net.Dial("tcp", ":"+httpPort)
	logger.Info().Msgf("internalHttpServerConnection: %s", httpPort)
	if err != nil {
		logger.Error().Msgf("error dialing secondary addr %v", err)
		return
	}

	defer internalHttpServerConnection.Close()

	var copyCompletionBarrier sync.WaitGroup
	copyCompletionBarrier.Add(2)
	switch q.config.Mode {
	case config.Proxy, config.ProxyInspect:
		log.Println("TCP proxy to Elasticsearch")
		elkConnection, err := q.connectElasticsearch()
		if err != nil {
			logger.Panic().Msg(err.Error())
		}
		defer elkConnection.Close()
		go tcp.CopyAndSignal(&copyCompletionBarrier, elkConnection, in)
		go tcp.CopyAndSignal(&copyCompletionBarrier, in, elkConnection)
		if q.config.Mode == config.ProxyInspect {
			config.SetTrafficAnalysis(true)
		}
	case config.DualWriteQueryElastic:
		logger.Info().Msg("writing to Elasticsearch and mirroring to Clickhouse")
		elkConnection, err := q.connectElasticsearch()
		if err != nil {
			logger.Panic().Msg(err.Error())
		}
		defer elkConnection.Close()
		go tcp.CopyAndSignal(&copyCompletionBarrier, io.MultiWriter(elkConnection, internalHttpServerConnection), in)
		go tcp.CopyAndSignal(&copyCompletionBarrier, in, elkConnection)
	case config.DualWriteQueryClickhouse:
		logger.Panic().Msg("DualWriteQueryClickhouse not yet available")
	case config.DualWriteQueryClickhouseVerify:
		logger.Panic().Msg("DualWriteQueryClickhouseVerify not yet available")
	case config.DualWriteQueryClickhouseFallback:
		logger.Panic().Msg("DualWriteQueryClickhouseFallback not yet available")
	case config.ClickHouse:
		logger.Info().Msg("handling Clickhouse only")
		go tcp.CopyAndSignal(&copyCompletionBarrier, internalHttpServerConnection, in)
		go tcp.CopyAndSignal(&copyCompletionBarrier, in, internalHttpServerConnection)
	default:
		logger.Panic().Msg("unknown operation mode")
	}

	copyCompletionBarrier.Wait()

	logger.Info().Msgf("Connection complete %v", in.RemoteAddr())
}

func (q *Quesma) connectElasticsearch() (net.Conn, error) {
	elkConnection, err := net.Dial("tcp", q.targetUrl.RequestURI())
	logger.Info().Msg("elkConnection:" + q.targetUrl.RequestURI())
	if err != nil {
		logger.Error().Msgf("error dialing primary addr %v", err)
		return nil, fmt.Errorf("error dialing primary elasticsearch addr: %w", err)
	}
	return elkConnection, nil
}
