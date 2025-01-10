// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package proxy

import (
	"context"
	"fmt"
	"github.com/QuesmaOrg/quesma/quesma/clickhouse"
	"github.com/QuesmaOrg/quesma/quesma/elasticsearch"
	"github.com/QuesmaOrg/quesma/quesma/logger"
	"github.com/QuesmaOrg/quesma/quesma/quesma/config"
	"github.com/QuesmaOrg/quesma/quesma/quesma/types"
	"github.com/QuesmaOrg/quesma/quesma/stats"
	"github.com/QuesmaOrg/quesma/quesma/util"
	"io"
	"log"
	"net"
	"net/http"
	"sync/atomic"
	"time"
)

const (
	internalHttpPort = "8082"
	bulkCreate       = "create"
)

type TcpProxy struct {
	From                 util.Port
	To                   string
	inspect              bool
	inspectHttpServer    *http.Server
	ready                chan struct{}
	acceptingConnections atomic.Bool
}

func NewTcpProxy(From util.Port, To string, inspect bool) *TcpProxy {
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
	router.HandleFunc("POST /{index}/_doc", util.BodyHandler(func(body []byte, writer http.ResponseWriter, r *http.Request) {
		index := r.PathValue("index")

		parsedBody := types.ParseRequestBody(string(body))
		var jsonBody types.JSON
		switch b := parsedBody.(type) {
		case types.JSON:
			jsonBody = b
		default:
			logger.Error().Msgf("Invalid JSON body: %v", parsedBody)
			return
		}

		if !elasticsearch.IsInternalIndex(index) {
			stats.GlobalStatistics.Process(false, index, jsonBody, clickhouse.NestedSeparator)
		}
	}))

	router.HandleFunc("POST /{index}/_bulk", util.BodyHandler(func(body []byte, writer http.ResponseWriter, r *http.Request) {
		index := r.PathValue("index")

		parsedBody := types.ParseRequestBody(string(body))
		var jsonBody types.JSON
		switch b := parsedBody.(type) {
		case types.JSON:
			jsonBody = b
		default:
			logger.Error().Msgf("Invalid JSON body: %v", parsedBody)
			return
		}

		if !elasticsearch.IsInternalIndex(index) {
			stats.GlobalStatistics.Process(false, index, jsonBody, clickhouse.NestedSeparator)
		}
	}))

	router.HandleFunc("POST /_bulk", util.BodyHandler(func(body []byte, writer http.ResponseWriter, r *http.Request) {

		parsedBody := types.ParseRequestBody(string(body))
		var ndjson types.NDJSON
		switch b := parsedBody.(type) {
		case types.NDJSON:
			ndjson = b
		default:
			logger.Error().Msgf("Invalid JSON body: %v", parsedBody)
			return
		}

		err := ndjson.BulkForEach(func(entryNumber int, operation types.BulkOperation, _, document types.JSON) error {

			index := operation.GetIndex()
			if index == "" {
				logger.Error().Msg("No index in operation")
				return nil
			}

			if !elasticsearch.IsInternalIndex(index) {
				stats.GlobalStatistics.Process(false, index, document, clickhouse.NestedSeparator)
			}
			return nil
		})

		if err != nil {
			logger.Error().Msgf("Error processing _bulk: %v", err)
		}

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
