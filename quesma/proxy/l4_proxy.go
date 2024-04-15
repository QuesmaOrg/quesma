package proxy

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"mitmproxy/quesma/clickhouse"
	"mitmproxy/quesma/elasticsearch"
	"mitmproxy/quesma/logger"
	"mitmproxy/quesma/network"
	"mitmproxy/quesma/quesma/config"
	"mitmproxy/quesma/stats"
	"mitmproxy/quesma/util"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"time"
)

const (
	internalHttpPort = "8082"
	bulkCreate       = "create"
)

type TcpProxy struct {
	From                 network.Port
	To                   string
	inspect              bool
	inspectHttpServer    *http.Server
	ready                chan struct{}
	acceptingConnections atomic.Bool
}

func NewTcpProxy(From network.Port, To string, inspect bool) *TcpProxy {
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

func configureRouting() *http.ServeMux {
	router := http.NewServeMux()
	configuration := config.QuesmaConfiguration{IndexConfig: map[string]config.IndexConfiguration{"_all": {Name: "_all", Enabled: true}}}
	router.HandleFunc("POST /{index}/_doc", util.BodyHandler(func(body []byte, writer http.ResponseWriter, r *http.Request) {
		index := r.PathValue("index")
		if !elasticsearch.IsInternalIndex(index) {
			stats.GlobalStatistics.Process(configuration, index, string(body), clickhouse.NestedSeparator)
		}
	}))

	router.HandleFunc("POST /{index}/_bulk", util.BodyHandler(func(body []byte, writer http.ResponseWriter, r *http.Request) {
		index := r.PathValue("index")
		if !elasticsearch.IsInternalIndex(index) {
			stats.GlobalStatistics.Process(configuration, index, string(body), clickhouse.NestedSeparator)
		}
	}))

	router.HandleFunc("POST /_bulk", util.BodyHandler(func(body []byte, writer http.ResponseWriter, r *http.Request) {
		forEachInBulk(string(body), func(index string, document string) {
			if !elasticsearch.IsInternalIndex(index) {
				stats.GlobalStatistics.Process(configuration, index, document, clickhouse.NestedSeparator)
			}
		})
	}))
	router.HandleFunc("GET /", func(writer http.ResponseWriter, r *http.Request) {
		writer.WriteHeader(http.StatusOK)
	})
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
		logger.Fatal().Msgf("Error listening to port: %v", err)
	}
	defer func(l net.Listener) {
		if err := l.Close(); err != nil {
			logger.Error().Msgf("Error closing the listener: %s", err)
		}
	}(listener)

	close(t.ready)
	t.acceptingConnections.Store(true)

	if t.inspect {
		logger.Info().Msgf("Listening on port %d and forwarding to %s, inspecting traffic\n", t.From, t.To)
	} else {
		logger.Info().Msgf("Listening on port %d and forwarding to %s", t.From, t.To)
	}

	for t.acceptingConnections.Load() {
		fromConn, err := listener.Accept()
		if err != nil {
			logger.Error().Msgf("Error accepting connection: %v", err)
			continue
		}

		destConn, err := net.Dial("tcp", t.To)
		if err != nil {
			logger.Error().Msgf("Error connecting to remote server: %v", err)
			closeConnection(fromConn)
			continue
		}

		go t.handle(fromConn, destConn)
	}
}

func (t *TcpProxy) WaitUntilReady(timeout time.Duration) error {
	if t.inspect {
		var resp *http.Response
		var err error
		serverReady := false
		startTime := time.Now()
		for !serverReady && time.Since(startTime) < timeout {
			resp, err = http.Get("http://localhost:" + internalHttpPort + "/")
			if err == nil && resp.StatusCode == http.StatusOK {
				_ = resp.Body.Close()
				serverReady = true
			} else if err != nil {
				logger.Error().Msgf("Error waiting for server to be ready: %v", err)
			}
			time.Sleep(100 * time.Millisecond)
		}

		if !serverReady {
			return fmt.Errorf("server not ready after %v: %v", timeout, err)
		}
	}

	<-t.ready
	return nil
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
		logger.Error().Msgf("Error copying data: %v", err)
	}
}

func closeConnection(connection net.Conn) {
	if err := connection.Close(); err != nil {
		logger.Error().Msgf("Error closing connection: %v", err)
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
			logger.Error().Msgf("Invalid action JSON in _bulk: %v %s", err, action)
			continue
		}
		createObj, ok := jsonData[bulkCreate]
		if ok {
			createJson, ok := createObj.(map[string]interface{})
			if !ok {
				logger.Error().Msgf("Invalid create JSON in _bulk: %s", action)
				continue
			}
			indexName, ok := createJson["_index"].(string)
			if !ok {
				if len(indexName) == 0 {
					logger.Error().Msgf("Invalid create JSON in _bulk, no _index name: %s", action)
					continue
				}
			}

			f(indexName, document)
		} else {
			logger.Debug().Msgf("Unsupported actions in _bulk: %s", action)
		}
	}
}
