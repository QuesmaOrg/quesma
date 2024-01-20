package quesma

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"mitmproxy/quesma/clickhouse"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
)

const (
	TcpProxyPort = "8888"
	RemoteUrl    = "http://" + "localhost:" + TcpProxyPort + "/"
)

type Quesma struct {
	server            *http.Server
	logManager        *clickhouse.LogManager
	targetUrl         string
	tcpPort           string
	httpPort          string
	responseDecorator *http.Server
	tcpProxyPort      string
	requestId         int64
	responseMatcher   *ResponseMatcher
	queryDebugger     *QueryDebugger
}

func New(logManager *clickhouse.LogManager, target string, tcpPort string, httpPort string) *Quesma {
	responseMatcher := NewResponseMatcher()
	queryDebugger := NewQueryDebugger()
	q := &Quesma{
		logManager: logManager,
		targetUrl:  target,
		tcpPort:    tcpPort,
		httpPort:   httpPort,
		server: &http.Server{
			Addr:    ":" + httpPort,
			Handler: configureRouting(logManager, responseMatcher, queryDebugger),
		},
		requestId:       0,
		tcpProxyPort:    TcpProxyPort,
		responseMatcher: responseMatcher,
		queryDebugger:   queryDebugger,
	}

	q.responseDecorator = NewResponseDecorator(tcpPort, q.requestId, q.responseMatcher, q.queryDebugger)

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
	defer quesmaRecover()
	defer in.Close()
	elkConnection, err := net.Dial("tcp", q.targetUrl)
	log.Println("elkConnection:" + q.targetUrl)
	if err != nil {
		log.Println("error dialing primary addr", err)
		return
	}

	internalHttpServerConnection, err := net.Dial("tcp", ":"+q.httpPort)
	log.Println("internalHttpServerConnection:" + q.httpPort)
	if err != nil {
		log.Println("error dialing secondary addr", err)
		return
	}

	defer elkConnection.Close()
	defer internalHttpServerConnection.Close()

	var copyCompletionBarrier sync.WaitGroup
	copyCompletionBarrier.Add(2)
	go copyAndSignal(&copyCompletionBarrier, io.MultiWriter(elkConnection, internalHttpServerConnection), in)
	go copyAndSignal(&copyCompletionBarrier, in, elkConnection)
	copyCompletionBarrier.Wait()

	log.Println("Connection complete", in.RemoteAddr())
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
	defer quesmaRecover()
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
	go q.responseMatcher.Compare()
	go q.queryDebugger.GenerateReport()
}
func dualWriteBulk(url string, body string, lm *clickhouse.LogManager) {
	fmt.Printf("%s  --> clickhouse, body(shortened): %s\n", url, body[:70])
	jsons := strings.Split(body, "\n")
	for i, singleJson := range jsons {
		if len(singleJson) == 0 {
			continue
		}
		tName := url
		if len(jsons) > 1 {
			tName += "_" + strconv.Itoa(i+1)
		}
		err := lm.ProcessInsertQuery(tName, singleJson)
		if err != nil {
			log.Fatal(err)
		}
	}
}

func dualWrite(url string, body string, lm *clickhouse.LogManager) {
	fmt.Printf("%s  --> clickhouse, body(shortened): %s\n", url, body[:70])
	if len(body) == 0 {
		return
	}
	tName := url
	err := lm.ProcessInsertQuery(tName, body)
	if err != nil {
		log.Fatal(err)
	}
}

func handleQuery(url string, body []byte, lm *clickhouse.LogManager,
	responseMatcher *ResponseMatcher,
	queryDebugger *QueryDebugger,
	requestId string) {
	if strings.Contains(url, "/_search?pretty") {
		var translatedQueryBody []byte
		queryTranslator := &ClickhouseQueryTranslator{clickhouseLM: lm}
		queryTranslator.Write(body)
		// TODO query clickhouse
		// get response
		// and translate
		var responseBody []byte
		responseTranslator := &ClickhouseResultReader{clickhouseLM: lm}
		responseTranslator.Read(responseBody)
		responseBody = []byte("clickhouse")
		var rawResults []byte
		responseMatcher.Push(&QResponse{requestId, responseBody})

		queryDebugger.PushSecondaryInfo(&QueryDebugSecondarySource{
			id:                     requestId,
			incomingQueryBody:      body,
			queryBodyTranslated:    translatedQueryBody,
			queryRawResults:        rawResults,
			queryTranslatedResults: responseBody,
		})
		return
	}
}
