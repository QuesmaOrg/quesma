// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package main

import (
	"net/http"
	"quesma/frontend_connectors"
	"quesma/processors"
	quesma_api "quesma_v2/core"
	"strconv"
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

func bulk(request *http.Request) (map[string]interface{}, any, error) {
	_, err := frontend_connectors.ReadRequestBody(request)
	if err != nil {
		return nil, nil, err
	}
	metadata := quesma_api.MakeNewMetadata()
	metadata["level"] = 0
	resp := []byte("bulk\n")
	atomic.AddInt64(&correlationId, 1)
	quesma_api.SetCorrelationId(metadata, correlationId)
	return metadata, resp, nil
}

func doc(request *http.Request) (map[string]interface{}, any, error) {
	_, err := frontend_connectors.ReadRequestBody(request)
	if err != nil {
		return nil, nil, err
	}
	metadata := quesma_api.MakeNewMetadata()
	metadata["level"] = 0
	atomic.AddInt64(&correlationId, 1)
	quesma_api.SetCorrelationId(metadata, correlationId)
	resp := []byte("doc\n")

	return metadata, resp, nil
}

var correlationId int64 = 0

func search(request *http.Request) (map[string]interface{}, any, error) {
	metadata := quesma_api.MakeNewMetadata()
	metadata["level"] = 0
	atomic.AddInt64(&correlationId, 1)
	quesma_api.SetCorrelationId(metadata, correlationId)
	return metadata, request, nil
}

type IngestProcessor struct {
	processors.BaseProcessor
}

func NewIngestProcessor() *IngestProcessor {
	return &IngestProcessor{BaseProcessor: processors.NewBaseProcessor()}
}

func (p *IngestProcessor) GetId() string {
	return "IngestProcessor"
}

func (p *IngestProcessor) Handle(metadata map[string]interface{}, message ...any) (map[string]interface{}, any, error) {
	var data []byte
	for _, m := range message {
		var err error
		data, err = quesma_api.CheckedCast[[]byte](m)
		if err != nil {
			panic("IngestProcessor: invalid message type")
		}

		level := metadata["level"].(int)
		data = append(data, strconv.Itoa(level)...)
		data = append(data, []byte(p.GetId())...)
		data = append(data, []byte("\n")...)
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
		level := metadata["level"].(int)
		data = append(data, strconv.Itoa(level)...)
		data = append(data, []byte(p.GetId())...)
		data = append(data, []byte("\n")...)
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
		level := metadata["level"].(int)
		data = append(data, strconv.Itoa(level)...)
		data = append(data, []byte(p.GetId())...)
		data = append(data, []byte("\n")...)
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
