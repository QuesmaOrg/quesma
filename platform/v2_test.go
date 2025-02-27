// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package main

import (
	"context"
	"github.com/QuesmaOrg/quesma/platform/backend_connectors"
	"github.com/QuesmaOrg/quesma/platform/config"
	"github.com/QuesmaOrg/quesma/platform/frontend_connectors"
	"github.com/QuesmaOrg/quesma/platform/processors"
	quesma_api "github.com/QuesmaOrg/quesma/platform/v2/core"
	"github.com/stretchr/testify/assert"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

func emitRequests(stop chan os.Signal, t *testing.T, testData []struct {
	url              string
	expectedResponse string
}) {
	go func() {
		time.Sleep(1 * time.Second)
		requestBody := []byte(`{"query": {"match_all": {}}}`)
		var resp string
		var err error
		for _, test := range testData {
			resp, err = sendRequest(test.url, requestBody)
			assert.NoError(t, err)
			assert.Contains(t, test.expectedResponse, resp)
		}
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
	ingestHTTPRouter.AddRoute("/_bulk", bulkHandler)
	ingestHTTPRouter.AddRoute("/_doc", docHandler)
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
	queryHTTPRouter.AddRoute("/_search", searchHandler)
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

func full_workflow_scenario() quesma_api.QuesmaBuilder {
	var quesmaBuilder quesma_api.QuesmaBuilder = quesma_api.NewQuesma(quesma_api.EmptyDependencies())

	cfg := &config.QuesmaConfiguration{
		DisableAuth: true,
		Elasticsearch: config.ElasticsearchConfiguration{
			Url:      &config.Url{Host: "localhost:9200", Scheme: "http"},
			User:     "",
			Password: "",
		},
	}

	queryFrontendConnector := frontend_connectors.NewBasicHTTPFrontendConnector(":8888", cfg)
	queryHTTPRouter := quesma_api.NewPathRouter()
	queryHTTPRouter.AddRoute("/_search", searchHandler2)
	queryHTTPRouter.AddFallbackHandler(fallback)
	queryFrontendConnector.AddRouter(queryHTTPRouter)
	var queryPipeline quesma_api.PipelineBuilder = quesma_api.NewPipeline()
	queryPipeline.AddFrontendConnector(queryFrontendConnector)
	var queryProcessor quesma_api.Processor = NewQueryComplexProcessor()

	queryPipeline.AddProcessor(queryProcessor)
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
	testData := []struct {
		url              string
		expectedResponse string
	}{
		{"http://localhost:8888/_bulk", "unknown\n"},
		{"http://localhost:8888/_doc", "unknown\n"},
		{"http://localhost:8888/_search", "unknown\n"},
		{"http://localhost:8888/_search", "unknown\n"},
	}
	emitRequests(stop, t, testData)
	<-stop
	q1.Stop(context.Background())
	atomic.LoadInt32(&fallbackCalled)
	assert.Equal(t, int32(4), fallbackCalled)
}

func Test_scenario1(t *testing.T) {
	q1 := ab_testing_scenario()
	q1.Start()
	stop := make(chan os.Signal, 1)
	testData := []struct {
		url              string
		expectedResponse string
	}{
		{"http://localhost:8888/_bulk", `bulk->IngestProcessor->InnerIngestProcessor1->0ABIngestTestProcessor
bulk->IngestProcessor->InnerIngestProcessor2->0ABIngestTestProcessor
`},
		{"http://localhost:8888/_doc", `doc->IngestProcessor->InnerIngestProcessor1->0ABIngestTestProcessor
doc->IngestProcessor->InnerIngestProcessor2->0ABIngestTestProcessor
`},
		{"http://localhost:8888/_search", "ABTestProcessor processor: Responses are equal\n"},
		{"http://localhost:8888/_search", "ABTestProcessor processor: Responses are not equal\n"},
	}
	emitRequests(stop, t, testData)
	<-stop
	q1.Stop(context.Background())
}

var middlewareCallCount int32 = 0

type Middleware struct {
	emitError bool
}

func (m *Middleware) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	atomic.AddInt32(&middlewareCallCount, 1)
	if m.emitError {
		http.Error(w, "middleware", http.StatusInternalServerError)
	}
}

type Middleware2 struct {
}

func (m *Middleware2) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	atomic.AddInt32(&middlewareCallCount, 1)
	w.WriteHeader(200)
}

func createMiddleWareScenario(emitError bool, cfg *config.QuesmaConfiguration) quesma_api.QuesmaBuilder {
	var quesmaBuilder quesma_api.QuesmaBuilder = quesma_api.NewQuesma(quesma_api.EmptyDependencies())

	frontendConnector := frontend_connectors.NewBasicHTTPFrontendConnector(":8888", cfg)
	HTTPRouter := quesma_api.NewPathRouter()
	var fallback quesma_api.HTTPFrontendHandler = fallback
	HTTPRouter.AddFallbackHandler(fallback)
	frontendConnector.AddRouter(HTTPRouter)
	frontendConnector.AddMiddleware(&Middleware{emitError: emitError})
	frontendConnector.AddMiddleware(&Middleware2{})

	var pipeline quesma_api.PipelineBuilder = quesma_api.NewPipeline()
	pipeline.AddFrontendConnector(frontendConnector)
	var ingestProcessor quesma_api.Processor = NewIngestProcessor()
	pipeline.AddProcessor(ingestProcessor)
	quesmaBuilder.AddPipeline(pipeline)
	return quesmaBuilder
}

func Test_middleware(t *testing.T) {

	cfg := &config.QuesmaConfiguration{
		DisableAuth: true,
		Elasticsearch: config.ElasticsearchConfiguration{
			Url:      &config.Url{Host: "localhost:9200", Scheme: "http"},
			User:     "",
			Password: "",
		},
	}
	{
		quesmaBuilder := createMiddleWareScenario(true, cfg)
		quesmaBuilder.Build()
		quesmaBuilder.Start()
		stop := make(chan os.Signal, 1)
		testData := []struct {
			url              string
			expectedResponse string
		}{
			{"http://localhost:8888/_bulk", "middleware\n"},
			{"http://localhost:8888/_doc", "middleware\n"},
			{"http://localhost:8888/_search", "middleware\n"},
			{"http://localhost:8888/_search", "middleware\n"},
		}
		emitRequests(stop, t, testData)

		<-stop
		quesmaBuilder.Stop(context.Background())
		atomic.LoadInt32(&middlewareCallCount)
		assert.Equal(t, int32(4), middlewareCallCount)
	}
	atomic.StoreInt32(&middlewareCallCount, 0)
	{
		quesmaBuilder := createMiddleWareScenario(false, cfg)
		quesmaBuilder.Build()
		quesmaBuilder.Start()
		stop := make(chan os.Signal, 1)
		testData := []struct {
			url              string
			expectedResponse string
		}{
			{"http://localhost:8888/_bulk", "middleware\n"},
			{"http://localhost:8888/_doc", "middleware\n"},
			{"http://localhost:8888/_search", "middleware\n"},
			{"http://localhost:8888/_search", "middleware\n"},
		}
		emitRequests(stop, t, testData)
		<-stop
		quesmaBuilder.Stop(context.Background())
		atomic.LoadInt32(&middlewareCallCount)
		assert.Equal(t, int32(8), middlewareCallCount)
	}
}

func Test_QuesmaBuild(t *testing.T) {
	cfg := &config.QuesmaConfiguration{
		DisableAuth: true,
		Elasticsearch: config.ElasticsearchConfiguration{
			Url:      &config.Url{Host: "localhost:9200", Scheme: "http"},
			User:     "",
			Password: "",
		},
	}
	{
		// Two pipelines with different endpoints
		var quesmaBuilder quesma_api.QuesmaBuilder = quesma_api.NewQuesma(quesma_api.EmptyDependencies())
		firstFrontendConnector := frontend_connectors.NewBasicHTTPFrontendConnector(":8888", cfg)
		firstHTTPRouter := quesma_api.NewPathRouter()
		firstHTTPRouter.AddRoute("/_bulk", bulkHandler)
		firstFrontendConnector.AddRouter(firstHTTPRouter)
		var firstPipeline quesma_api.PipelineBuilder = quesma_api.NewPipeline()
		firstPipeline.AddFrontendConnector(firstFrontendConnector)

		secondFrontendConnector := frontend_connectors.NewBasicHTTPFrontendConnector(":8889", cfg)
		secondHTTPRouter := quesma_api.NewPathRouter()
		secondHTTPRouter.AddRoute("/_search", searchHandler)
		secondFrontendConnector.AddRouter(secondHTTPRouter)
		var secondPipeline quesma_api.PipelineBuilder = quesma_api.NewPipeline()
		secondPipeline.AddFrontendConnector(secondFrontendConnector)

		quesmaBuilder.AddPipeline(firstPipeline)
		quesmaBuilder.AddPipeline(secondPipeline)
		quesma, err := quesmaBuilder.Build()
		assert.NotNil(t, quesma)
		assert.Equal(t, 2, len(quesma.GetPipelines()))
		assert.Equal(t, 1, len(quesma.GetPipelines()[0].GetFrontendConnectors()))
		assert.Equal(t, 1, len(quesma.GetPipelines()[1].GetFrontendConnectors()))
		assert.Equal(t, 1, len(quesma.GetPipelines()[0].GetFrontendConnectors()[0].(quesma_api.HTTPFrontendConnector).GetRouter().GetHandlers()))
		assert.Equal(t, 1, len(quesma.GetPipelines()[1].GetFrontendConnectors()[0].(quesma_api.HTTPFrontendConnector).GetRouter().GetHandlers()))
		assert.NotEqual(t, quesma.GetPipelines()[1].GetFrontendConnectors()[0], quesma.GetPipelines()[0].GetFrontendConnectors()[0])

		assert.NoError(t, err)

	}
	{
		// Two pipelines with the same endpoint
		var quesmaBuilder quesma_api.QuesmaBuilder = quesma_api.NewQuesma(quesma_api.EmptyDependencies())
		firstFrontendConnector := frontend_connectors.NewBasicHTTPFrontendConnector(":8888", cfg)
		firstHTTPRouter := quesma_api.NewPathRouter()
		firstHTTPRouter.AddRoute("/_bulk", bulkHandler)
		firstFrontendConnector.AddRouter(firstHTTPRouter)
		var firstPipeline quesma_api.PipelineBuilder = quesma_api.NewPipeline()
		firstPipeline.AddFrontendConnector(firstFrontendConnector)

		secondFrontendConnector := frontend_connectors.NewBasicHTTPFrontendConnector(":8888", cfg)
		secondHTTPRouter := quesma_api.NewPathRouter()
		secondHTTPRouter.AddRoute("/_search", searchHandler)
		secondFrontendConnector.AddRouter(secondHTTPRouter)
		var secondPipeline quesma_api.PipelineBuilder = quesma_api.NewPipeline()
		secondPipeline.AddFrontendConnector(secondFrontendConnector)

		quesmaBuilder.AddPipeline(firstPipeline)
		quesmaBuilder.AddPipeline(secondPipeline)
		quesma, err := quesmaBuilder.Build()
		assert.NotNil(t, quesma)
		assert.Equal(t, 2, len(quesma.GetPipelines()))
		assert.Equal(t, 1, len(quesma.GetPipelines()[0].GetFrontendConnectors()))
		assert.Equal(t, 1, len(quesma.GetPipelines()[1].GetFrontendConnectors()))
		assert.Equal(t, 2, len(quesma.GetPipelines()[0].GetFrontendConnectors()[0].(quesma_api.HTTPFrontendConnector).GetRouter().GetHandlers()))
		assert.Equal(t, 2, len(quesma.GetPipelines()[1].GetFrontendConnectors()[0].(quesma_api.HTTPFrontendConnector).GetRouter().GetHandlers()))
		assert.Equal(t, quesma.GetPipelines()[1].GetFrontendConnectors()[0], quesma.GetPipelines()[0].GetFrontendConnectors()[0])
		assert.NoError(t, err)
	}
	{
		// One pipeline with the same endpoint
		var quesmaBuilder quesma_api.QuesmaBuilder = quesma_api.NewQuesma(quesma_api.EmptyDependencies())
		firstFrontendConnector := frontend_connectors.NewBasicHTTPFrontendConnector(":8888", cfg)
		firstHTTPRouter := quesma_api.NewPathRouter()
		firstHTTPRouter.AddRoute("/_bulk", bulkHandler)
		firstFrontendConnector.AddRouter(firstHTTPRouter)
		var firstPipeline quesma_api.PipelineBuilder = quesma_api.NewPipeline()
		firstPipeline.AddFrontendConnector(firstFrontendConnector)

		secondFrontendConnector := frontend_connectors.NewBasicHTTPFrontendConnector(":8888", cfg)
		secondHTTPRouter := quesma_api.NewPathRouter()
		secondHTTPRouter.AddRoute("/_search", searchHandler)
		secondFrontendConnector.AddRouter(secondHTTPRouter)
		firstPipeline.AddFrontendConnector(secondFrontendConnector)

		quesmaBuilder.AddPipeline(firstPipeline)
		quesma, err := quesmaBuilder.Build()
		assert.NotNil(t, quesma)
		assert.Equal(t, 1, len(quesma.GetPipelines()))
		assert.Equal(t, 2, len(quesma.GetPipelines()[0].GetFrontendConnectors()))
		assert.Equal(t, 2, len(quesma.GetPipelines()[0].GetFrontendConnectors()[0].(quesma_api.HTTPFrontendConnector).GetRouter().GetHandlers()))
		assert.Equal(t, 2, len(quesma.GetPipelines()[0].GetFrontendConnectors()[1].(quesma_api.HTTPFrontendConnector).GetRouter().GetHandlers()))
		assert.Equal(t, quesma.GetPipelines()[0].GetFrontendConnectors()[0], quesma.GetPipelines()[0].GetFrontendConnectors()[1])

		assert.NoError(t, err)
	}
}

func Test_complex_scenario1(t *testing.T) {
	q1 := full_workflow_scenario()
	q1.Start()
	stop := make(chan os.Signal, 1)
	testData := []struct {
		url              string
		expectedResponse string
	}{
		{"http://localhost:8888/_search", "qqq->"},
	}
	emitRequests(stop, t, testData)
	<-stop
	q1.Stop(context.Background())
}
