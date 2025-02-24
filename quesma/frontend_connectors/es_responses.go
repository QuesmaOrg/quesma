// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package frontend_connectors

import (
	"context"
	"errors"
	"github.com/QuesmaOrg/quesma/quesma/elasticsearch"
	"github.com/QuesmaOrg/quesma/quesma/end_user_errors"
	"github.com/QuesmaOrg/quesma/quesma/logger"
	"github.com/QuesmaOrg/quesma/quesma/parsers/elastic_query_dsl"
	"github.com/QuesmaOrg/quesma/quesma/quesma/functionality/bulk"
	quesma_api "github.com/QuesmaOrg/quesma/quesma/v2/core"
	"github.com/goccy/go-json"
	"net/http"
	"sync"
)

func elasticsearchCountResult(body int64, statusCode int) (*quesma_api.Result, error) {
	var result = countResult{
		Shards: struct {
			Failed     int `json:"failed"`
			Skipped    int `json:"skipped"`
			Successful int `json:"successful"`
			Total      int `json:"total"`
		}{
			Failed:     0,
			Skipped:    0,
			Successful: 1,
			Total:      1,
		},
		Count: body,
	}
	serialized, err := json.Marshal(result)
	if err != nil {
		return nil, err
	}
	return &quesma_api.Result{Body: string(serialized), Meta: map[string]any{
		"Content-Type":            "application/json",
		"X-Quesma-Headers-Source": "Quesma",
	}, StatusCode: statusCode,
		GenericResult: serialized}, nil
}

type countResult struct {
	Shards struct {
		Failed     int `json:"failed"`
		Skipped    int `json:"skipped"`
		Successful int `json:"successful"`
		Total      int `json:"total"`
	} `json:"_shards"`
	Count int64 `json:"count"`
}

// ElasticsearchQueryResult is a low-effort way to export widely used func without too much refactoring
func ElasticsearchQueryResult(body string, statusCode int) *quesma_api.Result {
	return elasticsearchQueryResult(body, statusCode)
}

func elasticsearchQueryResult(body string, statusCode int) *quesma_api.Result {
	return &quesma_api.Result{Body: body, Meta: map[string]any{
		// TODO copy paste from the original request
		"X-Quesma-Headers-Source": "Quesma",
	}, StatusCode: statusCode,
		GenericResult: []byte(body)}
}

var ingestWarning sync.Once

func noIngestEnabledButThereIngestRequest() {
	logger.Error().Msgf("Ingest is disabled by configuration, but the request is trying to ingest data. ")
}

func bulkInsertResult(ctx context.Context, ops []bulk.BulkItem, err error) (*quesma_api.Result, error) {

	if err != nil {
		var msg string
		var reason string
		var httpCode int

		var endUserError *end_user_errors.EndUserError
		if errors.As(err, &endUserError) {
			msg = string(elastic_query_dsl.InternalQuesmaError(endUserError.EndUserErrorMessage()))
			reason = endUserError.Reason()
			httpCode = http.StatusInternalServerError

			if endUserError.ErrorType().Number == end_user_errors.ErrNoIngest.Number {
				// agents have no mercy, they will try again, and again
				// we should log this error once
				ingestWarning.Do(noIngestEnabledButThereIngestRequest)
			}

		} else {
			msg = string(elastic_query_dsl.BadRequestParseError(err))
			reason = err.Error()
			httpCode = http.StatusBadRequest
		}

		// ingest can be noisy, so we can enable debug logs here
		var logEveryIngestError bool

		if logEveryIngestError {
			logger.ErrorWithCtxAndReason(ctx, reason).Msgf("Bulk insert error: %v", err)
		}

		return &quesma_api.Result{
			Body:          msg,
			StatusCode:    httpCode,
			GenericResult: []byte(msg),
		}, nil
	}

	if err != nil {
		return &quesma_api.Result{
			Body:          string(elastic_query_dsl.BadRequestParseError(err)),
			StatusCode:    http.StatusBadRequest,
			GenericResult: elastic_query_dsl.BadRequestParseError(err),
		}, nil
	}

	body, err := json.Marshal(bulk.BulkResponse{
		Errors: false,
		Items:  ops,
		Took:   42,
	})
	if err != nil {
		return nil, err
	}

	return elasticsearchInsertResult(string(body), http.StatusOK), nil
}

// ElasticsearchInsertResult is a low-effort way to export widely used func without too much refactoring
func ElasticsearchInsertResult(body string, statusCode int) *quesma_api.Result {
	return elasticsearchInsertResult(body, statusCode)
}

func elasticsearchInsertResult(body string, statusCode int) *quesma_api.Result {
	return &quesma_api.Result{Body: body, Meta: map[string]any{
		// TODO copy paste from the original request
		ContentTypeHeaderKey:      "application/json",
		"X-Quesma-Headers-Source": "Quesma",
	}, StatusCode: statusCode,
		GenericResult: []byte(body)}
}

func resolveIndexResult(sources elasticsearch.Sources) (*quesma_api.Result, error) {
	if len(sources.Aliases) == 0 && len(sources.DataStreams) == 0 && len(sources.Indices) == 0 {
		return &quesma_api.Result{StatusCode: http.StatusNotFound}, nil
	}

	body, err := json.Marshal(sources)
	if err != nil {
		return nil, err
	}

	return &quesma_api.Result{
		Body:          string(body),
		Meta:          map[string]any{},
		StatusCode:    http.StatusOK,
		GenericResult: body}, nil
}

func indexDocResult(bulkItem bulk.BulkItem) (*quesma_api.Result, error) {
	body, err := json.Marshal(bulkItem.Index)
	if err != nil {
		return nil, err
	}
	return elasticsearchInsertResult(string(body), http.StatusOK), nil
}

func putIndexResult(index string) (*quesma_api.Result, error) {
	result := putIndexResponse{
		Acknowledged:       true,
		ShardsAcknowledged: true,
		Index:              index,
	}
	serialized, err := json.Marshal(result)
	if err != nil {
		return nil, err
	}

	return &quesma_api.Result{StatusCode: http.StatusOK, Body: string(serialized), GenericResult: serialized}, nil
}

func getIndexMappingResult(index string, mappings map[string]any) (*quesma_api.Result, error) {
	result := map[string]any{
		index: map[string]any{
			"mappings": mappings,
		},
	}
	serialized, err := json.Marshal(result)
	if err != nil {
		return nil, err
	}

	return &quesma_api.Result{StatusCode: http.StatusOK, Body: string(serialized), GenericResult: serialized}, nil
}

func getIndexResult(index string, mappings map[string]any) (*quesma_api.Result, error) {
	// For now return the same as getIndexMappingResult,
	// but "GET /:index" can also contain "settings" and "aliases" (in the future)
	return getIndexMappingResult(index, mappings)
}

type (
	putIndexResponse struct {
		Acknowledged       bool   `json:"acknowledged"`
		ShardsAcknowledged bool   `json:"shards_acknowledged"`
		Index              string `json:"index"`
	}
)
