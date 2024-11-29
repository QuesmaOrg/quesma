// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package v2

import (
	"context"
	"github.com/stretchr/testify/assert"
	"os"
	"os/signal"
	"quesma_v2/backend_connectors"
	quesma_api "quesma_v2/core"
	"quesma_v2/frontend_connectors"
	"quesma_v2/processors"
	"syscall"
	"testing"
	"time"
)

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
	var quesmaBuilder quesma_api.QuesmaBuilder = quesma_api.NewQuesma()
	const endpoint = "root:password@tcp(127.0.0.1:3306)/test"
	var mySqlBackendConnector quesma_api.BackendConnector = &backend_connectors.MySqlBackendConnector{
		Endpoint: endpoint,
	}
	postgressPipeline.AddBackendConnector(mySqlBackendConnector)
	quesmaBuilder.AddPipeline(postgressPipeline)
	_, err := quesmaBuilder.Build()
	assert.NoError(t, err)
}

func ab_testing_scenario() quesma_api.QuesmaBuilder {
	var quesmaBuilder quesma_api.QuesmaBuilder = quesma_api.NewQuesma()

	ingestFrontendConnector := frontend_connectors.NewBasicHTTPFrontendConnector(":8888")
	ingestHTTPRouter := frontend_connectors.NewHTTPRouter()
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

	queryFrontendConnector := frontend_connectors.NewBasicHTTPFrontendConnector(":8888")
	queryHTTPRouter := frontend_connectors.NewHTTPRouter()
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

func Test_scenario1(t *testing.T) {
	q1 := ab_testing_scenario()
	q1.Start()
	stop := make(chan os.Signal, 1)
	emitRequests(stop)
	<-stop
	q1.Stop(context.Background())
}
