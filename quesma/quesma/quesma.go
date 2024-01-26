package quesma

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"mitmproxy/quesma/clickhouse"
	"mitmproxy/quesma/quesma/config"
	"mitmproxy/quesma/quesma/recovery"
	"net"
	"net/http"
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
		server            *http.Server
		logManager        *clickhouse.LogManager
		targetUrl         string
		tcpPort           string
		httpPort          string
		responseDecorator *http.Server
		tcpProxyPort      string
		requestId         int64
		queryDebugger     *QueryDebugger
		config            config.QuesmaConfiguration
	}
)

func New(logManager *clickhouse.LogManager, target string, tcpPort string, httpPort string, config config.QuesmaConfiguration) *Quesma {
	queryDebugger := NewQueryDebugger()
	q := &Quesma{
		logManager: logManager,
		targetUrl:  target,
		tcpPort:    tcpPort,
		httpPort:   httpPort,
		server: &http.Server{
			Addr:    ":" + httpPort,
			Handler: configureRouting(config, logManager, queryDebugger),
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
	if err := q.server.Shutdown(ctx); err != nil {
		log.Fatal("Error during server shutdown:", err)
	}
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

	internalHttpServerConnection, err := net.Dial("tcp", ":"+q.httpPort)
	log.Println("internalHttpServerConnection:" + q.httpPort)
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

func (q *Quesma) listenHTTP() {
	if err := q.server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
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
	listener, err := q.listen()
	if err != nil {
		log.Println(err)
		return
	}

	go func() {
		for {
			in, err := listener.Accept()
			log.Println("New connection from: ", in.RemoteAddr())
			if err != nil {
				log.Println("error accepting connection", err)
				continue
			}
			go q.handleRequest(in)
		}
	}()
	go q.queryDebugger.Run()
}

func SetTrafficAnalysis(val bool) {
	trafficAnalysis.Store(val)
}
