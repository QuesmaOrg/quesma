// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package main

import (
	"context"
	"github.com/stretchr/testify/assert"
	"net/http"
	"os"
	"os/signal"
	"quesma/backend_connectors"
	"quesma/frontend_connectors"
	"quesma/processors"
	"quesma/quesma/config"
	quesma_api "quesma_v2/core"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// just to make sure that the buildIngestOnlyQuesma is used
func Test_Main(m *testing.T) {
	_ = buildIngestOnlyQuesma()
}

func emitRequests(stop chan os.Signal) {
	go func() {
		time.Sleep(1 * time.Second)
		requestBody := []byte(`{"query": {"match_all": {}}}`)
		sendRequest("http://localhost:8888/_bulk", requestBody)
		sendRequest("http://localhost:8888/_doc", requestBody)
		sendRequest("http://localhost:8888/_search", requestBody)
		sendRequest("http://localhost:8888/_search", requestBody)
		signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
		close(stop)
	}()
}

func Test_backendConnectorValidation(t *testing.T) {
	var tcpProcessor quesma_api.Processor = processors.NewPostgresToMySqlProcessor()
	var postgressPipeline quesma_api.PipelineBuilder = quesma_api.NewPipeline()
	postgressPipeline.AddProcessor(tcpProcessor)
	var quesmaBuilder quesma_api.QuesmaBuilder = quesma_api.NewQuesma(quesma_api.EmptyDependencies())

	const endpoint = "root:password@tcp(127.0.0.1:3306)/test"
	var mySqlBackendConnector quesma_api.BackendConnector = &backend_connectors.MySqlBackendConnector{
		Endpoint: endpoint,
	}
	postgressPipeline.AddBackendConnector(mySqlBackendConnector)
	quesmaBuilder.AddPipeline(postgressPipeline)
	_, err := quesmaBuilder.Build()
	assert.NoError(t, err)
}

var fallbackCalled int32 = 0

func fallback(_ context.Context, _ *quesma_api.Request, _ http.ResponseWriter) (*quesma_api.Result, error) {
	metadata := quesma_api.MakeNewMetadata()
	atomic.AddInt32(&fallbackCalled, 1)
	resp := []byte("unknown\n")
	return &quesma_api.Result{Meta: metadata, GenericResult: resp}, nil
}

func ab_testing_scenario() quesma_api.QuesmaBuilder {
	var quesmaBuilder quesma_api.QuesmaBuilder = quesma_api.NewQuesma(quesma_api.EmptyDependencies())

	cfg := &config.QuesmaConfiguration{
		DisableAuth: true,
		Elasticsearch: config.ElasticsearchConfiguration{
			Url:      &config.Url{Host: "localhost:9200", Scheme: "http"},
			User:     "",
			Password: "",
		},
	}

	ingestFrontendConnector := frontend_connectors.NewBasicHTTPFrontendConnector(":8888", cfg)
	ingestHTTPRouter := quesma_api.NewPathRouter()
	ingestHTTPRouter.AddRoute("/_bulk", bulk)
	ingestHTTPRouter.AddRoute("/_doc", doc)
	ingestFrontendConnector.AddRouter(ingestHTTPRouter)
	var ingestPipeline quesma_api.PipelineBuilder = quesma_api.NewPipeline()
	ingestPipeline.AddFrontendConnector(ingestFrontendConnector)
	var abIngestTestProcessor quesma_api.Processor = processors.NewABTestProcessor("ABIngestTestProcessor", false)

	var ingestProcessor quesma_api.Processor = NewIngestProcessor()
	var innerIngestProcessor1 quesma_api.Processor = NewInnerIngestProcessor1()
	ingestProcessor.AddProcessor(innerIngestProcessor1)
	var innerIngestProcessor2 quesma_api.Processor = NewInnerIngestProcessor2()
	ingestProcessor.AddProcessor(innerIngestProcessor2)

	ingestPipeline.AddProcessor(ingestProcessor)
	ingestPipeline.AddProcessor(abIngestTestProcessor)

	queryFrontendConnector := frontend_connectors.NewBasicHTTPFrontendConnector(":8888", cfg)
	queryHTTPRouter := quesma_api.NewPathRouter()
	queryHTTPRouter.AddRoute("/_search", search)
	queryFrontendConnector.AddRouter(queryHTTPRouter)
	var queryPipeline quesma_api.PipelineBuilder = quesma_api.NewPipeline()
	queryPipeline.AddFrontendConnector(queryFrontendConnector)
	var queryProcessor quesma_api.Processor = NewQueryProcessor()
	var innerQueryProcessor1 quesma_api.Processor = NewInnerQueryProcessor1()
	queryProcessor.AddProcessor(innerQueryProcessor1)
	var innerQueryProcessor2 quesma_api.Processor = NewInnerQueryProcessor2()
	queryProcessor.AddProcessor(innerQueryProcessor2)
	var abQueryTestProcessor quesma_api.Processor = processors.NewABTestProcessor("ABQueryTestProcessor", true)

	queryPipeline.AddProcessor(queryProcessor)
	queryPipeline.AddProcessor(abQueryTestProcessor)
	quesmaBuilder.AddPipeline(ingestPipeline)
	quesmaBuilder.AddPipeline(queryPipeline)

	quesma, _ := quesmaBuilder.Build()
	return quesma
}

func fallbackScenario() quesma_api.QuesmaBuilder {
	var quesmaBuilder quesma_api.QuesmaBuilder = quesma_api.NewQuesma(quesma_api.EmptyDependencies())

	cfg := &config.QuesmaConfiguration{
		DisableAuth: true,
		Elasticsearch: config.ElasticsearchConfiguration{
			Url:      &config.Url{Host: "localhost:9200", Scheme: "http"},
			User:     "",
			Password: "",
		},
	}
	ingestFrontendConnector := frontend_connectors.NewBasicHTTPFrontendConnector(":8888", cfg)

	ingestHTTPRouter := quesma_api.NewPathRouter()
	var fallback quesma_api.HTTPFrontendHandler = fallback
	ingestHTTPRouter.AddFallbackHandler(fallback)
	ingestFrontendConnector.AddRouter(ingestHTTPRouter)
	var ingestPipeline quesma_api.PipelineBuilder = quesma_api.NewPipeline()
	ingestPipeline.AddFrontendConnector(ingestFrontendConnector)
	quesmaBuilder.AddPipeline(ingestPipeline)

	return quesmaBuilder
}

func Test_fallbackScenario(t *testing.T) {
	qBuilder := fallbackScenario()
	q1, _ := qBuilder.Build()
	q1.Start()
	stop := make(chan os.Signal, 1)
	emitRequests(stop)
	<-stop
	q1.Stop(context.Background())
	atomic.LoadInt32(&fallbackCalled)
	assert.Equal(t, int32(4), fallbackCalled)
}

func Test_scenario1(t *testing.T) {
	q1 := ab_testing_scenario()
	q1.Start()
	stop := make(chan os.Signal, 1)
	emitRequests(stop)
	<-stop
	q1.Stop(context.Background())
}

var middleWareCalled int32 = 0

type Middleware struct {
	emitError bool
}

func (m *Middleware) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	atomic.AddInt32(&middleWareCalled, 1)
	if m.emitError {
		http.Error(w, "middleware", http.StatusInternalServerError)
	}
}

type Middleware2 struct {
}

func (m *Middleware2) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	atomic.AddInt32(&middleWareCalled, 1)
	w.WriteHeader(200)
}

func Test_middleware(t *testing.T) {
	var quesmaBuilder quesma_api.QuesmaBuilder = quesma_api.NewQuesma(quesma_api.EmptyDependencies())

	cfg := &config.QuesmaConfiguration{
		DisableAuth: true,
		Elasticsearch: config.ElasticsearchConfiguration{
			Url:      &config.Url{Host: "localhost:9200", Scheme: "http"},
			User:     "",
			Password: "",
		},
	}
	{
		frontendConnector := frontend_connectors.NewBasicHTTPFrontendConnector(":8888", cfg)
		HTTPRouter := quesma_api.NewPathRouter()
		var fallback quesma_api.HTTPFrontendHandler = fallback
		HTTPRouter.AddFallbackHandler(fallback)
		frontendConnector.AddRouter(HTTPRouter)
		frontendConnector.AddMiddleware(&Middleware{emitError: true})
		frontendConnector.AddMiddleware(&Middleware2{})

		var pipeline quesma_api.PipelineBuilder = quesma_api.NewPipeline()
		pipeline.AddFrontendConnector(frontendConnector)
		var ingestProcessor quesma_api.Processor = NewIngestProcessor()
		pipeline.AddProcessor(ingestProcessor)
		quesmaBuilder.AddPipeline(pipeline)

		quesmaBuilder.Build()
		quesmaBuilder.Start()
		stop := make(chan os.Signal, 1)
		emitRequests(stop)
		<-stop
		quesmaBuilder.Stop(context.Background())
		atomic.LoadInt32(&middleWareCalled)
		assert.Equal(t, int32(4), middleWareCalled)
	}
	middleWareCalled = 0
	{
		frontendConnector := frontend_connectors.NewBasicHTTPFrontendConnector(":8888", cfg)
		HTTPRouter := quesma_api.NewPathRouter()
		var fallback quesma_api.HTTPFrontendHandler = fallback
		HTTPRouter.AddFallbackHandler(fallback)
		frontendConnector.AddRouter(HTTPRouter)
		frontendConnector.AddMiddleware(&Middleware{emitError: false})
		frontendConnector.AddMiddleware(&Middleware2{})

		var pipeline quesma_api.PipelineBuilder = quesma_api.NewPipeline()
		pipeline.AddFrontendConnector(frontendConnector)
		var ingestProcessor quesma_api.Processor = NewIngestProcessor()
		pipeline.AddProcessor(ingestProcessor)
		quesmaBuilder.AddPipeline(pipeline)

		quesmaBuilder.Build()
		quesmaBuilder.Start()
		stop := make(chan os.Signal, 1)
		emitRequests(stop)
		<-stop
		quesmaBuilder.Stop(context.Background())
		atomic.LoadInt32(&middleWareCalled)
		assert.Equal(t, int32(8), middleWareCalled)
	}
}
