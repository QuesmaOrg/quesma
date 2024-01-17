package quesma

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"mitmproxy/quesma/clickhouse"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

const TCP_PROXY_PORT = "8888"
const REMOTE_URL = "http://" + "localhost:" + TCP_PROXY_PORT + "/"

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
}

func New(logManager *clickhouse.LogManager, target string, tcpPort string, httpPort string) *Quesma {
	responseMatcher := NewResponseMatcher()
	q := &Quesma{
		logManager: logManager,
		targetUrl:  target,
		tcpPort:    tcpPort,
		httpPort:   httpPort,
		server: &http.Server{
			Addr: ":" + httpPort,
			Handler: http.HandlerFunc(func(writer http.ResponseWriter, r *http.Request) {
				body, err := io.ReadAll(r.Body)
				if err != nil {
					log.Fatal(err)
				}
				r.Body = io.NopCloser(bytes.NewBuffer(body))
				if r.Method == "POST" {
					go dualWrite(r.RequestURI, string(body), logManager)
				} else {
					id := r.Header.Get("RequestId")
					go handleQuery(r.RequestURI, body, logManager, responseMatcher, id)
				}
			}),
		},
		requestId:       0,
		tcpProxyPort:    TCP_PROXY_PORT,
		responseMatcher: responseMatcher,
	}

	q.responseDecorator = NewResponseDecorator(tcpPort, q.requestId, q.responseMatcher)
	return q
}

func unzip(gzippedData []byte) ([]byte, error) {
	// Create a reader for the gzipped data
	reader := bytes.NewReader(gzippedData)

	// Create a gzip reader
	gzipReader, err := gzip.NewReader(reader)
	if err != nil {
		return nil, err
	}
	defer gzipReader.Close()

	// Read the unzipped data
	unzippedData, err := io.ReadAll(gzipReader)
	if err != nil {
		return nil, err
	}

	return unzippedData, nil
}

func NewResponseDecorator(tcpPort string, requestId int64, matcher *ResponseMatcher) *http.Server {
	remote, err := url.Parse(REMOTE_URL)
	if err != nil {
		log.Fatal("Cannot parse target url:", err)
	}
	return &http.Server{
		Addr: ":" + tcpPort,
		Handler: http.HandlerFunc(func(writer http.ResponseWriter, req *http.Request) {
			req.Host = remote.Host
			req.URL.Host = remote.Host
			req.URL.Scheme = "http"
			log.Println(req.URL.Host)

			id := atomic.AddInt64(&requestId, 1)
			req.Header.Add("RequestId", strconv.FormatInt(id, 10))

			// httputil.ReverseProxy does not serve
			// our purpose which is copy RequestID header from request to response.
			// The only thing http.ReverseProxy can do is to rewrite returned request OR response
			// therefore we have to pass id via closure
			proxy := httputil.NewSingleHostReverseProxy(remote)
			proxy.ModifyResponse = func(resp *http.Response) error {
				reader := resp.Body
				body, err := io.ReadAll(reader)
				if err != nil {
					log.Fatal(err)
				}
				resp.Body = io.NopCloser(bytes.NewBuffer(body))

				if strings.Contains(req.RequestURI, "/_search?pretty") {
					isGzipped := strings.Contains(resp.Header.Get("Content-Encoding"), "gzip")
					if isGzipped {
						unzippedBuffer, err := unzip(body)
						if err != nil {
							log.Println("Error unzipping:", err)
							return err
						}
						matcher.Push(&QResponse{req.Header.Get("RequestId"), unzippedBuffer})
					} else {
						matcher.Push(&QResponse{req.Header.Get("RequestId"), body})
					}
				}
				return nil
			}
			proxy.ServeHTTP(writer, req)
		}),
	}
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
		log.Fatal("Error starting server:", err)
	}
}

func (q *Quesma) listenResponseDecorator() {
	if err := q.responseDecorator.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal("Error starting server:", err)
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
}

func dualWrite(url string, body string, lm *clickhouse.LogManager) {
	// to make logs more readable by truncating very long request bodies
	firstNChars := func(s string, n int) string {
		if len(s) > n {
			return s[:n]
		}
		return s
	}
	if strings.Contains(url, "bulk") || strings.Contains(url, "/_doc") {
		fmt.Printf("%s  --> clickhouse, body: %s\n", url, firstNChars(body, 34))
		jsons := strings.Split(body, "\n")
		for i, singleJson := range jsons {
			if len(singleJson) == 0 {
				continue
			}
			tableName := url
			if len(jsons) > 1 {
				tableName += "_" + strconv.Itoa(i+1)
			}
			// very unnecessary trying to create tables with every request
			// We can improve this later if needed
			err := lm.CreateTable(tableName, singleJson, clickhouse.DefaultCHConfig())
			if err != nil {
				log.Fatal(err)
			}
			err = lm.Insert(tableName, singleJson, clickhouse.DefaultCHConfig())
			if err != nil {
				_, _ = fmt.Fprintln(os.Stderr, err)
			}
		}
	} else {
		fmt.Printf("%s --> pass-through\n", url)
	}
}

func handleQuery(url string, body []byte, lm *clickhouse.LogManager, responseMatcher *ResponseMatcher, requestId string) {
	if strings.Contains(url, "/_search?pretty") {
		responseMatcher.Push(&QResponse{requestId, body})
		return
	}
}
