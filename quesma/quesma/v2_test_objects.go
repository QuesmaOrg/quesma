// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package quesma

import (
	"context"
	"github.com/QuesmaOrg/quesma/quesma/frontend_connectors"
	"github.com/QuesmaOrg/quesma/quesma/logger"
	"github.com/QuesmaOrg/quesma/quesma/processors"
	quesma_api "github.com/QuesmaOrg/quesma/quesma/v2/core"
	"net/http"
	"sync/atomic"
)

var responses = [][]byte{
	[]byte(`{
  "took": 5,
  "timed_out": false,
  "_shards": {
    "total": 1,
    "successful": 1,
    "skipped": 0,
    "failed": 0
  },
  "hits": {
    "total": {
      "value": 1,
      "relation": "eq"
    },
    "max_score": 1.0,
    "hits": [
      {
        "_index": "blog",
        "_type": "_doc",
        "_id": "1",
        "_score": 1.0,
        "_source": {
          "title": "Second Post",
          "author": "John Doe",
          "content": "This is the second blog post.",
          "published_at": "2024-11-20"
        }
      }
    ]
  }
}`),
	[]byte(`
{
  "took": 5,
  "timed_out": false,
  "_shards": {
    "total": 1,
    "successful": 1,
    "skipped": 0,
    "failed": 0
  },
  "hits": {
    "total": {
      "value": 1,
      "relation": "eq"
    },
    "max_score": 1.0,
    "hits": [
      {
        "_index": "blog",
        "_type": "_doc",
        "_id": "1",
        "_score": 1.0,
        "_source": {
          "title": "First Post",
          "author": "John Doe",
          "content": "This is the first blog post.",
          "published_at": "2024-11-01"
        }
      }
    ]
  }
}`),
}

func bulkHandler(_ context.Context, request *quesma_api.Request, _ http.ResponseWriter) (*quesma_api.Result, error) {
	_, err := frontend_connectors.ReadRequestBody(request.OriginalRequest)
	if err != nil {
		return nil, err
	}
	metadata := quesma_api.MakeNewMetadata()
	metadata["level"] = 0
	resp := []byte("bulk->")
	atomic.AddInt64(&correlationId, 1)
	quesma_api.SetCorrelationId(metadata, correlationId)
	return &quesma_api.Result{Meta: metadata, GenericResult: resp, StatusCode: 200}, nil
}

func docHandler(_ context.Context, request *quesma_api.Request, _ http.ResponseWriter) (*quesma_api.Result, error) {
	_, err := frontend_connectors.ReadRequestBody(request.OriginalRequest)
	if err != nil {
		return nil, err
	}
	metadata := quesma_api.MakeNewMetadata()
	metadata["level"] = 0
	atomic.AddInt64(&correlationId, 1)
	quesma_api.SetCorrelationId(metadata, correlationId)
	resp := []byte("doc->")

	return &quesma_api.Result{Meta: metadata, GenericResult: resp, StatusCode: 200}, nil
}

var correlationId int64 = 0

func searchHandler(_ context.Context, request *quesma_api.Request, _ http.ResponseWriter) (*quesma_api.Result, error) {
	metadata := quesma_api.MakeNewMetadata()
	metadata["level"] = 0
	atomic.AddInt64(&correlationId, 1)
	quesma_api.SetCorrelationId(metadata, correlationId)
	return &quesma_api.Result{Meta: metadata, GenericResult: request.OriginalRequest, StatusCode: 200}, nil
}

type IngestProcessor struct {
	processors.BaseProcessor
	logger quesma_api.QuesmaLogger
}

func NewIngestProcessor() *IngestProcessor {
	return &IngestProcessor{BaseProcessor: processors.NewBaseProcessor()}
}

func (p *IngestProcessor) SetDependencies(deps quesma_api.Dependencies) {
	p.logger = deps.Logger()
}

func (p *IngestProcessor) InstanceName() string {
	return "IngestProcessor" // TODO return name from config
}

func (p *IngestProcessor) GetId() string {
	return "IngestProcessor"
}

func (p *IngestProcessor) Handle(metadata map[string]interface{}, message ...any) (map[string]interface{}, any, error) {

	p.logger.Info().Msgf("IngestProcessor: handling message %T", message)

	var data []byte
	for _, m := range message {
		var err error
		data, err = quesma_api.CheckedCast[[]byte](m)
		if err != nil {
			panic("IngestProcessor: invalid message type")
		}

		data = append(data, []byte(p.GetId())...)
		data = append(data, []byte("->")...)
	}
	return metadata, data, nil
}

type InnerQueryProcessor2 struct {
	processors.BaseProcessor
	reqNum int
}

func NewInnerQueryProcessor2() *InnerQueryProcessor2 {
	return &InnerQueryProcessor2{
		BaseProcessor: processors.NewBaseProcessor(),
		reqNum:        0,
	}
}

func (p *InnerQueryProcessor2) InstanceName() string {
	return "InnerQueryProcessor2"
}

func (p *InnerQueryProcessor2) GetId() string {
	return "InnerQueryProcessor2"
}

func (p *InnerQueryProcessor2) Handle(metadata map[string]interface{}, message ...any) (map[string]interface{}, any, error) {
	if len(message) != 1 {
		panic("InnerQueryProcessor2: expect only one message")
	}
	request, err := quesma_api.CheckedCast[*http.Request](message[0])
	if err != nil {
		panic("QueryProcessor: invalid message type")
	}

	_, err = frontend_connectors.ReadRequestBody(request)
	if err != nil {
		return nil, nil, err
	}
	// Simulate a search response
	resp := make([]byte, 0)
	resp = append(resp, responses[0]...)
	return metadata, resp, nil
}

type InnerQueryProcessor1 struct {
	processors.BaseProcessor
	reqNum int
}

func NewInnerQueryProcessor1() *InnerQueryProcessor1 {
	return &InnerQueryProcessor1{
		BaseProcessor: processors.NewBaseProcessor(),
		reqNum:        0,
	}
}

func (p *InnerQueryProcessor1) InstanceName() string {
	return "InnerQueryProcessor1"
}

func (p *InnerQueryProcessor1) GetId() string {
	return "InnerQueryProcessor1"
}

func (p *InnerQueryProcessor1) Handle(metadata map[string]interface{}, message ...any) (map[string]interface{}, any, error) {
	if len(message) != 1 {
		panic("InnerQueryProcessor1: expect only one message")
	}
	request, err := quesma_api.CheckedCast[*http.Request](message[0])
	if err != nil {
		panic("QueryProcessor: invalid message type")
	}
	_, err = frontend_connectors.ReadRequestBody(request)
	if err != nil {
		return nil, nil, err
	}
	// Simulate a search response
	responseIndex := p.reqNum % 2
	p.reqNum++
	resp := make([]byte, 0)
	resp = append(resp, responses[responseIndex]...)
	return metadata, resp, nil
}

type InnerIngestProcessor2 struct {
	processors.BaseProcessor
}

func NewInnerIngestProcessor2() *InnerIngestProcessor2 {
	return &InnerIngestProcessor2{
		BaseProcessor: processors.NewBaseProcessor(),
	}
}

func (p *InnerIngestProcessor2) InstanceName() string {
	return "InnerIngestProcessor2"
}

func (p *InnerIngestProcessor2) GetId() string {
	return "InnerIngestProcessor2"
}

func (p *InnerIngestProcessor2) Handle(metadata map[string]interface{}, message ...any) (map[string]interface{}, any, error) {
	var data []byte
	for _, m := range message {
		var err error
		data, err = quesma_api.CheckedCast[[]byte](m)
		if err != nil {
			panic("InnerIngestProcessor2: invalid message type")
		}
		data = append(data, []byte(p.GetId())...)
		data = append(data, []byte("->")...)
	}
	return metadata, data, nil
}

type InnerIngestProcessor1 struct {
	processors.BaseProcessor
}

func NewInnerIngestProcessor1() *InnerIngestProcessor1 {
	return &InnerIngestProcessor1{
		BaseProcessor: processors.NewBaseProcessor(),
	}
}

func (p *InnerIngestProcessor1) InstanceName() string {
	return "InnerIngestProcessor1"
}

func (p *InnerIngestProcessor1) GetId() string {
	return "InnerIngestProcessor1"
}

func (p *InnerIngestProcessor1) Handle(metadata map[string]interface{}, message ...any) (map[string]interface{}, any, error) {
	var data []byte
	for _, m := range message {
		var err error
		data, err = quesma_api.CheckedCast[[]byte](m)
		if err != nil {
			panic("InnerIngestProcessor1: invalid message type")
		}
		data = append(data, []byte(p.GetId())...)
		data = append(data, []byte("->")...)
	}
	return metadata, data, nil
}

type QueryProcessor struct {
	processors.BaseProcessor
}

func NewQueryProcessor() *QueryProcessor {
	return &QueryProcessor{
		BaseProcessor: processors.NewBaseProcessor(),
	}
}

func (p *QueryProcessor) InstanceName() string {
	return "QueryProcessor" // TODO return name from config
}

func (p *QueryProcessor) GetId() string {
	return "QueryProcessor"
}

func (p *QueryProcessor) Handle(metadata map[string]interface{}, message ...any) (map[string]interface{}, any, error) {
	if len(message) != 1 {
		panic("QueryProcessor: expect only one message")
	}
	request, err := quesma_api.CheckedCast[*http.Request](message[0])
	if err != nil {
		panic("QueryProcessor: invalid message type")
	}
	return metadata, request, nil
}

type QueryComplexProcessor struct {
	processors.BaseProcessor
}

func NewQueryComplexProcessor() *QueryComplexProcessor {
	return &QueryComplexProcessor{
		BaseProcessor: processors.NewBaseProcessor(),
	}
}

func (p *QueryComplexProcessor) InstanceName() string {
	return "QueryProcessor" // TODO return name from config
}

func (p *QueryComplexProcessor) GetId() string {
	return "QueryProcessor"
}

/*
func (p *QueryComplexProcessor) Handle(metadata map[string]interface{}, message ...any) (map[string]interface{}, any, error) {
	if len(message) != 1 {
		panic("QueryProcessor: expect only one message")
	}
	_, err := quesma_api.CheckedCast[*http.Request](message[0])
	if err != nil {
		panic("QueryProcessor: invalid message type")
	}
	resp := []byte("qqq->")
	return metadata, resp, nil
}
*/

type QueryTransformationPipeline struct {
	*processors.BasicQueryTransformationPipeline
}

func NewQueryTransformationPipeline() *QueryTransformationPipeline {
	return &QueryTransformationPipeline{
		BasicQueryTransformationPipeline: processors.NewBasicQueryTransformationPipeline(),
	}
}

func (p *QueryTransformationPipeline) ParseQuery(message any) (*quesma_api.ExecutionPlan, error) {
	logger.Debug().Msg("SimpleQueryTransformationPipeline: ParseQuery")
	plan := &quesma_api.ExecutionPlan{}
	query := "SELECT * FROM users"
	plan.Queries = append(plan.Queries, &quesma_api.Query{Query: query})
	return plan, nil
}

func (p *QueryTransformationPipeline) TransformResults(results [][]quesma_api.QueryResultRow) [][]quesma_api.QueryResultRow {
	logger.Debug().Msg("SimpleQueryTransformationPipeline: TransformResults")
	return results
}
