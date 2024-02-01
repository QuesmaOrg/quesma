package quesma

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"mitmproxy/quesma/clickhouse"
	"mitmproxy/quesma/model"
	"mitmproxy/quesma/quesma/config"
	"mitmproxy/quesma/quesma/gzip"
	"mitmproxy/quesma/quesma/recovery"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
)

const (
	TcpProxyPort = "8888"
	RemoteUrl    = "http://" + "localhost:" + TcpProxyPort + "/"
)

var trafficAnalysis atomic.Bool

type (
	Quesma struct {
		processingHttpServer *http.Server
		routingHttpServer    *http.Server
		logManager           *clickhouse.LogManager
		targetUrl            string
		publicTcpPort        string
		internalHttpPort     string
		responseDecorator    *http.Server
		tcpProxyPort         string
		requestId            int64
		queryDebugger        *QueryDebugger
		config               config.QuesmaConfiguration
	}
)

func responseFromElastic(elkResponse *http.Response, w http.ResponseWriter, rId int) {
	log.Printf("rId: %d, responding from elk\n", rId)
	_, err := io.Copy(w, elkResponse.Body)
	if err != nil {
		http.Error(w, "Error copying response body", http.StatusInternalServerError)
		return
	}
	elkResponse.Body.Close()
}

func responseFromQuesma(unzipped []byte, w http.ResponseWriter, rId int) {
	log.Printf("rId: %d, responding from quesma\n", rId)

	var response model.Response

	err := json.Unmarshal(unzipped, &response)
	if err != nil {
		return
	}
	replaced, err := json.Marshal(response)

	if err != nil {
		return
	}
	zipped, err := gzip.Zip(replaced)
	if err == nil {
		_, _ = io.Copy(w, bytes.NewBuffer(zipped))
	}
}

func New(logManager *clickhouse.LogManager, target string, tcpPort string, httpPort string, config config.QuesmaConfiguration) *Quesma {
	queryDebugger := NewQueryDebugger()
	q := &Quesma{
		logManager:       logManager,
		targetUrl:        target,
		publicTcpPort:    tcpPort,
		internalHttpPort: httpPort,
		processingHttpServer: &http.Server{
			Addr:    ":" + httpPort,
			Handler: configureRouting(config, logManager, queryDebugger),
		},
		routingHttpServer: &http.Server{
			Addr: ":" + TcpProxyPort,
			Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				//defer recovery.LogPanic()
				reqBody, err := io.ReadAll(r.Body)
				if err != nil {
					http.Error(w, "Error reading request body", http.StatusInternalServerError)
					return
				}

				rId := rand.Intn(1000000)

				log.Printf("rId: %d, URI: %s\n", rId, r.RequestURI)

				elkResponse := sendHttpRequest("http://"+target, r, reqBody)
				quesmaResponse := sendHttpRequest("http://localhost:"+httpPort, r, reqBody)

				log.Printf("r.RequestURI: %+v\n", r.RequestURI)

				if elkResponse == nil {
					panic("elkResponse is nil")
				}

				if quesmaResponse == nil {
					panic("quesmaResponse is nil")
				}

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
							responseFromQuesma(unzipped, w, rId)
						} else {
							responseFromElastic(elkResponse, w, rId)
						}
					}
				} else {
					responseFromElastic(elkResponse, w, rId)
				}
			}),
		},
		requestId:     0,
		tcpProxyPort:  TcpProxyPort,
		queryDebugger: queryDebugger,
		config:        config,
	}

	q.responseDecorator = NewResponseDecorator(tcpPort, q.requestId, q.queryDebugger)

	return q
}

func (q *Quesma) Close(ctx context.Context) {
	if q.logManager != nil {
		defer q.logManager.Close()
	}
	if err := q.processingHttpServer.Shutdown(ctx); err != nil {
		log.Fatal("Error during server shutdown:", err)
	}
}

func (q *Quesma) listenRoutingHTTP() {
	if err := q.routingHttpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal("Error starting http server:", err)
	}
}

func (q *Quesma) listenHTTP() {
	if err := q.processingHttpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal("Error starting http server:", err)
	}
}

func (q *Quesma) listenResponseDecorator() {
	if err := q.responseDecorator.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal("Error starting response decorator server:", err)
	}
}

func (q *Quesma) Start() {
	defer recovery.LogPanic()
	go q.listenRoutingHTTP()
	go q.listenHTTP()
	go q.listenResponseDecorator()
	go q.queryDebugger.Run()
}

func SetTrafficAnalysis(val bool) {
	trafficAnalysis.Store(val)
}

func sendHttpRequest(address string, originalReq *http.Request, originalReqBody []byte) *http.Response {
	req, err := http.NewRequest(originalReq.Method, address+originalReq.URL.String(), bytes.NewBuffer(originalReqBody))
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

func (q *Quesma) listen() (net.Listener, error) {
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
	elkConnection, err := net.Dial("tcp", q.targetUrl)
	log.Println("elkConnection:" + q.targetUrl)
	if err != nil {
		log.Println("error dialing primary addr", err)
		return nil, fmt.Errorf("error dialing primary elasticsearch addr: %w", err)
	}
	return elkConnection, nil
}
