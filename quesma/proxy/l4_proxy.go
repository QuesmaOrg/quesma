package proxy

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/gorilla/mux"
	"io"
	"log"
	"mitmproxy/quesma/clickhouse"
	"mitmproxy/quesma/network"
	"mitmproxy/quesma/stats"
	"mitmproxy/quesma/util"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"time"
)

const internalHttpPort = "8081"

type TcpProxy struct {
	From                 network.Port
	To                   *url.URL
	inspect              bool
	inspectHttpServer    *http.Server
	ready                chan struct{}
	acceptingConnections atomic.Bool
}

func NewTcpProxy(From network.Port, To *url.URL, inspect bool) *TcpProxy {
	return &TcpProxy{
		From:              From,
		To:                To,
		inspect:           inspect,
		inspectHttpServer: resolveHttpServer(inspect),
		ready:             make(chan struct{}),
	}
}

func resolveHttpServer(inspect bool) *http.Server {
	if inspect {
		return &http.Server{
			Addr:    ":" + internalHttpPort,
			Handler: configureRouting(),
		}
	}
	return nil
}

func configureRouting() *mux.Router {
	router := mux.NewRouter()
	router.Path("/").Methods("GET").HandlerFunc(func(writer http.ResponseWriter, r *http.Request) {
		writer.WriteHeader(http.StatusOK)
	})
	router.PathPrefix("/{index}/_doc").Methods("POST").HandlerFunc(util.BodyHandler(func(body []byte, writer http.ResponseWriter, r *http.Request) {
		stats.GlobalStatistics.Process(mux.Vars(r)["index"], string(body), clickhouse.NestedSeparator)
	}))
	router.PathPrefix("/{index}/_bulk").Methods("POST").HandlerFunc(util.BodyHandler(func(body []byte, writer http.ResponseWriter, r *http.Request) {
		stats.GlobalStatistics.Process(mux.Vars(r)["index"], string(body), clickhouse.NestedSeparator)
	}))
	router.PathPrefix("/_bulk").Methods("POST").HandlerFunc(util.BodyHandler(func(body []byte, writer http.ResponseWriter, r *http.Request) {
		forEachInBulk(string(body), func(index string, document string) {
			stats.GlobalStatistics.Process(index, document, clickhouse.NestedSeparator)
		})
	}))
	return router
}

func (t *TcpProxy) Ingest() {
	if t.inspect {
		go func() {
			log.Println("Starting inspect HTTP server on port", internalHttpPort)
			if err := t.inspectHttpServer.ListenAndServe(); err != nil {
				log.Fatal("Error starting inspect HTTP server:", err)
			}
		}()
	}
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", t.From))
	if err != nil {
		log.Fatal("Error listening to port:", err)
	}
	defer func(l net.Listener) {
		if err := l.Close(); err != nil {
			log.Printf("Error closing the listener: %s\n", err)
		}
	}(listener)

	close(t.ready)
	t.acceptingConnections.Store(true)

	if t.inspect {
		log.Printf("Listening on port %d and forwarding to %s, inspecting traffic\n", t.From, t.To.String())
	} else {
		log.Printf("Listening on port %d and forwarding to %s\n", t.From, t.To.String())
	}

	for t.acceptingConnections.Load() {
		fromConn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err)
			continue
		}

		destConn, err := net.Dial("tcp", t.To.String())
		if err != nil {
			fmt.Println("Error connecting to remote server:", err)
			closeConnection(fromConn)
			continue
		}

		go t.handle(fromConn, destConn)
	}
}

func (t *TcpProxy) WaitUntilReady() {
	if t.inspect {
		serverReady := false
		for !serverReady {
			resp, err := http.Get("http://localhost:" + internalHttpPort + "/")
			if err == nil && resp.StatusCode == http.StatusOK {
				serverReady = true
			} else if err != nil {
				log.Println("Error waiting for server to be ready:", err)
			}
			_ = resp.Body.Close()
			time.Sleep(100 * time.Millisecond)
		}
	}

	<-t.ready
}

func (t *TcpProxy) Stop(context.Context) {
	// TODO: handle the case where the proxy blocks on listener.Accept()
	t.acceptingConnections.Store(false)
	if t.inspect {
		log.Println("Shutting down inspect HTTP server")
		_ = t.inspectHttpServer.Close()
	}
}

func (t *TcpProxy) handle(fromConn, destConn net.Conn) {
	log.Printf("Handling incoming connection from [%s] to [%s]\n", fromConn.RemoteAddr(), destConn.RemoteAddr())
	defer closeConnection(fromConn)
	defer closeConnection(destConn)

	if t.inspect {
		httpConn, err := net.Dial("tcp", ":"+internalHttpPort)
		if err != nil {
			log.Println("error dialing internal HTTP addr, TCP proxying...", err)
			go t.copyData(fromConn, destConn)
		} else {
			go t.copyData(fromConn, io.MultiWriter(destConn, httpConn))
		}
		t.copyData(destConn, fromConn)
	} else {
		go t.copyData(fromConn, destConn)
		t.copyData(destConn, fromConn)
	}
}

func (t *TcpProxy) copyData(src io.Reader, dest io.Writer) {
	if _, err := io.Copy(dest, src); err != nil {
		fmt.Println("Error copying data:", err)
	}
}

func closeConnection(connection net.Conn) {
	if err := connection.Close(); err != nil {
		log.Printf("Error closing connection: %s\n", err)
	}
}

func forEachInBulk(body string, f func(index string, document string)) {
	jsons := strings.Split(body, "\n")
	for i := 0; i+1 < len(jsons); i += 2 {
		action := jsons[i]
		document := jsons[i+1]

		var jsonData map[string]interface{}
		err := json.Unmarshal([]byte(action), &jsonData)
		if err != nil {
			fmt.Println("Invalid action JSON in _bulk:", err, action)
			continue
		}
		if jsonData["create"] != nil {
			createObj, ok := jsonData["create"].(map[string]interface{})
			if !ok {
				fmt.Println("Invalid create JSON in _bulk:", action)
				continue
			}
			indexName, ok := createObj["_index"].(string)
			if !ok {
				if len(indexName) == 0 {
					fmt.Println("Invalid create JSON in _bulk, no _index name:", action)
					continue
				}
			}

			f(indexName, document)
		}
	}
}
