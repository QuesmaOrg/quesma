package quesma

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"mitmproxy/quesma/clickhouse"
	"mitmproxy/quesma/quesma/config"
	"mitmproxy/quesma/quesma/gzip"
	"mitmproxy/quesma/quesma/recovery"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

const (
	TcpProxyPort = "8888"
	RemoteUrl    = "http://" + "localhost:" + TcpProxyPort + "/"
)

var trafficAnalysis atomic.Bool

type RequestId struct{}

type (
	Quesma struct {
		processor               RequestProcessor
		publicTcpPort           Port
		targetUrl               *url.URL
		quesmaManagementConsole *QuesmaManagementConsole
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
		responseDecorator    *http.Server
		tcpProxyPort         string
		requestId            int64
	}
	Port uint16
)

func (p *dualWriteHttpProxy) Stop(ctx context.Context) {
	p.Close(ctx)
}

func responseFromElastic(ctx context.Context, elkResponse *http.Response, w http.ResponseWriter, rId int) {
	_ = ctx
	log.Printf("rId: %d, responding from elk\n", rId)
	_, err := io.Copy(w, elkResponse.Body)
	if err != nil {
		http.Error(w, "Error copying response body", http.StatusInternalServerError)
		return
	}
	elkResponse.Body.Close()
}

func responseFromQuesma(ctx context.Context, unzipped []byte, w http.ResponseWriter, rId int) {
	_ = ctx
	log.Printf("rId: %d, responding from quesma\n", rId)
	// Response from clickhouse is always unzipped
	// so we have to zip it before sending to client
	zipped, err := gzip.Zip(unzipped)
	if err == nil {
		_, _ = io.Copy(w, bytes.NewBuffer(zipped))
	}
}

func sendElkResponseToQuesmaConsole(ctx context.Context, uri string, elkResponse *http.Response, console *QuesmaManagementConsole) {
	reader := elkResponse.Body
	body, err := io.ReadAll(reader)
	if err != nil {
		log.Fatal(err)
	}
	elkResponse.Body = io.NopCloser(bytes.NewBuffer(body))

	if strings.Contains(uri, "/_search") || strings.Contains(uri, "/_async_search") {
		isGzipped := strings.Contains(elkResponse.Header.Get("Content-Encoding"), "gzip")
		if isGzipped {
			body, err = gzip.UnZip(body)
			if err != nil {
				log.Println("Error unzipping:", err)
			}
		}
		console.PushPrimaryInfo(&QueryDebugPrimarySource{ctx.Value(RequestId{}).(string), body})
	}
}

func NewTcpProxy(logManager *clickhouse.LogManager, target string, tcpPort string, httpPort string, config config.QuesmaConfiguration) *Quesma {
	return New(logManager, target, tcpPort, httpPort, config)
}

func NewHttpProxy(logManager *clickhouse.LogManager, target string, tcpPort string, httpPort string, config config.QuesmaConfiguration) *Quesma {
	return New(logManager, target, tcpPort, httpPort, config)
}

func NewHttpClickhouseAdapter(logManager *clickhouse.LogManager, target string, tcpPort string, httpPort string, config config.QuesmaConfiguration) *Quesma {
	return New(logManager, target, tcpPort, httpPort, config)
}

func New(logManager *clickhouse.LogManager, target string, tcpPort string, httpPort string, config config.QuesmaConfiguration) *Quesma {
	quesmaManagementConsole := NewQuesmaManagementConsole()
	q := &Quesma{
		processor: &dualWriteHttpProxy{
			processingHttpServer: &http.Server{
				Addr:    ":" + httpPort,
				Handler: configureRouting(config, logManager, quesmaManagementConsole),
			},
			routingHttpServer: &http.Server{
				Addr: ":" + TcpProxyPort,
				Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					defer recovery.LogPanic()
					ctx := context.WithValue(r.Context(), RequestId{}, r.Header.Get("RequestId"))

					reqBody, err := io.ReadAll(r.Body)
					if err != nil {
						http.Error(w, "Error reading request body", http.StatusInternalServerError)
						return
					}

					rId := rand.Intn(1000000)

					log.Printf("rId: %d, URI: %s\n", rId, r.RequestURI)

					elkResponse := sendHttpRequest(ctx, "http://"+target, r, reqBody)
					quesmaResponse := sendHttpRequest(ctx, "http://localhost:"+httpPort, r, reqBody)

					log.Printf("r.RequestURI: %+v\n", r.RequestURI)

					if elkResponse == nil {
						panic("elkResponse is nil")
					}

					if quesmaResponse == nil {
						panic("quesmaResponse is nil")
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
						log.Printf("rId: %d, responding from quesma\n", rId)
						unzipped, err := io.ReadAll(quesmaResponse.Body)
						if err == nil {
							// Sometimes when query is invalid, quesma returns empty response,
							// and we have to handle this case.
							// When this happens, we want to return response from elk (for now), look else branch.
							if string(unzipped) != "" {
								responseFromQuesma(ctx, unzipped, w, rId)
							} else {
								responseFromElastic(ctx, elkResponse, w, rId)
							}
						}
					} else {
						responseFromElastic(ctx, elkResponse, w, rId)
					}
				}),
			},
			logManager:        logManager,
			internalHttpPort:  httpPort,
			tcpProxyPort:      TcpProxyPort,
			requestId:         0,
			responseDecorator: NewResponseDecorator(tcpPort, 0, quesmaManagementConsole),
		},
		targetUrl:               parseURL(target),
		publicTcpPort:           parsePort(tcpPort),
		quesmaManagementConsole: quesmaManagementConsole,
		config:                  config,
	}

	return q
}

func parsePort(port string) Port {
	tcpPortInt, err := strconv.Atoi(port)
	if err != nil {
		log.Fatalf("Error parsing tcp port: %s", err)
	}
	if tcpPortInt < 0 || tcpPortInt > 65535 {
		log.Fatalf("Invalid port: %s", port)
	}
	return Port(tcpPortInt)
}

func parseURL(urlStr string) *url.URL {
	parsed, err := url.Parse(urlStr)
	if err != nil {
		log.Fatalf("Error parsing target url: %s", err)
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
		log.Fatal("Error during server shutdown:", err)
	}
}

func (p *dualWriteHttpProxy) Ingest() {
	go p.listenRoutingHTTP()
	go p.listenHTTP()
	go p.listenResponseDecorator()
}

func (p *dualWriteHttpProxy) listenRoutingHTTP() {
	if err := p.routingHttpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal("Error starting http server:", err)
	}
}

func (p *dualWriteHttpProxy) listenHTTP() {
	if err := p.processingHttpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal("Error starting http server:", err)
	}
}

func (p *dualWriteHttpProxy) listenResponseDecorator() {
	if err := p.responseDecorator.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal("Error starting response decorator server:", err)
	}
}

func (q *Quesma) Start() {
	defer recovery.LogPanic()
	log.Println("starting quesma in the mode:", q.config.Mode)
	go q.processor.Ingest()
	go q.quesmaManagementConsole.Run()
}

func SetTrafficAnalysis(val bool) {
	trafficAnalysis.Store(val)
}

func sendHttpRequest(ctx context.Context, address string, originalReq *http.Request, originalReqBody []byte) *http.Response {
	req, err := http.NewRequestWithContext(ctx, originalReq.Method, address+originalReq.URL.String(), bytes.NewBuffer(originalReqBody))
	if err != nil {
		fmt.Println("Error creating request:", err)
		return nil
	}

	req.Header = originalReq.Header
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		fmt.Println("Error sending request:", err)
		return nil
	}
	return resp
}

func (q *dualWriteHttpProxy) listen() (net.Listener, error) {
	go q.listenHTTP()
	go q.listenResponseDecorator()
	fmt.Printf("listening TCP at %s\n", q.tcpProxyPort)
	listener, err := net.Listen("tcp", ":"+q.tcpProxyPort)
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
	log.Println("internalHttpServerConnection:" + httpPort)
	if err != nil {
		log.Println("error dialing secondary addr", err)
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
			panic(err)
		}
		defer elkConnection.Close()
		go copyAndSignal(&copyCompletionBarrier, elkConnection, in)
		go copyAndSignal(&copyCompletionBarrier, in, elkConnection)
		if q.config.Mode == config.ProxyInspect {
			SetTrafficAnalysis(true)
		}
	case config.DualWriteQueryElastic:
		log.Println("writing to Elasticsearch and mirroring to Clickhouse")
		elkConnection, err := q.connectElasticsearch()
		if err != nil {
			panic(err)
		}
		defer elkConnection.Close()
		go copyAndSignal(&copyCompletionBarrier, io.MultiWriter(elkConnection, internalHttpServerConnection), in)
		go copyAndSignal(&copyCompletionBarrier, in, elkConnection)
	case config.DualWriteQueryClickhouse:
		panic("DualWriteQueryClickhouse not yet available")
	case config.DualWriteQueryClickhouseVerify:
		panic("DualWriteQueryClickhouseVerify not yet available")
	case config.DualWriteQueryClickhouseFallback:
		panic("DualWriteQueryClickhouseFallback not yet available")
	case config.ClickHouse:
		log.Println("handling Clickhouse only")
		go copyAndSignal(&copyCompletionBarrier, internalHttpServerConnection, in)
		go copyAndSignal(&copyCompletionBarrier, in, internalHttpServerConnection)
	default:
		panic("unknown operation mode")
	}

	copyCompletionBarrier.Wait()

	log.Println("Connection complete", in.RemoteAddr())
}

func (q *Quesma) connectElasticsearch() (net.Conn, error) {
	elkConnection, err := net.Dial("tcp", q.targetUrl.RequestURI())
	log.Println("elkConnection:" + q.targetUrl.RequestURI())
	if err != nil {
		log.Println("error dialing primary addr", err)
		return nil, fmt.Errorf("error dialing primary elasticsearch addr: %w", err)
	}
	return elkConnection, nil
}
