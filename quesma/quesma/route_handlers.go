// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package quesma

import (
	"context"
	"errors"
	"net/http"
	"quesma/clickhouse"
	"quesma/elasticsearch"
	"quesma/queryparser"
	"quesma/quesma/config"
	quesma_errors "quesma/quesma/errors"
	"quesma/quesma/functionality/field_capabilities"
	"quesma/quesma/functionality/resolve"
	"quesma/quesma/types"
	"quesma/schema"
	quesma_api "quesma_v2/core"
	"quesma_v2/core/tracing"
)

func HandleDeletingAsyncSearchById(queryRunner QueryRunnerIFace, asyncSearchId string) (*quesma_api.Result, error) {
	responseBody, err := queryRunner.DeleteAsyncSearch(asyncSearchId)
	if err != nil {
		return nil, err
	}
	return elasticsearchQueryResult(string(responseBody), http.StatusOK), nil
}

func HandleGettingAsyncSearchById(ctx context.Context, asyncSearchId string, queryRunner QueryRunnerIFace) (*quesma_api.Result, error) {
	ctx = context.WithValue(ctx, tracing.AsyncIdCtxKey, asyncSearchId)
	responseBody, err := queryRunner.HandlePartialAsyncSearch(ctx, asyncSearchId)
	if err != nil {
		return nil, err
	}
	return elasticsearchQueryResult(string(responseBody), http.StatusOK), nil
}

func HandleAsyncSearchStatus(ctx context.Context, asyncSearchId string, queryRunner QueryRunnerIFace) (*quesma_api.Result, error) {
	responseBody, err := queryRunner.HandleAsyncSearchStatus(ctx, asyncSearchId)
	if err != nil {
		return nil, err
	}
	return elasticsearchQueryResult(string(responseBody), http.StatusOK), nil
}

func HandleIndexSearch(ctx context.Context, indexPattern string, query types.JSON, queryRunner QueryRunnerIFace) (*quesma_api.Result, error) {
	responseBody, err := queryRunner.HandleSearch(ctx, indexPattern, query)
	if err != nil {
		if errors.Is(quesma_errors.ErrIndexNotExists(), err) {
			return &quesma_api.Result{StatusCode: http.StatusNotFound, GenericResult: make([]byte, 0)}, nil
		} else if errors.Is(err, quesma_errors.ErrCouldNotParseRequest()) {
			return &quesma_api.Result{
				Body:          string(queryparser.BadRequestParseError(err)),
				StatusCode:    http.StatusBadRequest,
				GenericResult: queryparser.BadRequestParseError(err),
			}, nil
		} else {
			return nil, err
		}
	}
	return elasticsearchQueryResult(string(responseBody), http.StatusOK), nil
}

func HandleIndexAsyncSearch(ctx context.Context, indexPattern string, query types.JSON, waitForResultsMs int, keepOnCompletion bool, queryRunner QueryRunnerIFace) (*quesma_api.Result, error) {
	responseBody, err := queryRunner.HandleAsyncSearch(ctx, indexPattern, query, waitForResultsMs, keepOnCompletion)
	if err != nil {
		if errors.Is(quesma_errors.ErrIndexNotExists(), err) {
			return &quesma_api.Result{StatusCode: http.StatusNotFound, GenericResult: make([]byte, 0)}, nil
		} else if errors.Is(err, quesma_errors.ErrCouldNotParseRequest()) {
			return &quesma_api.Result{
				Body:          string(queryparser.BadRequestParseError(err)),
				StatusCode:    http.StatusBadRequest,
				GenericResult: queryparser.BadRequestParseError(err),
			}, nil
		} else {
			return nil, err
		}
	}
	return elasticsearchQueryResult(string(responseBody), http.StatusOK), nil
}

func HandleResolveIndex(_ context.Context, indexPattern string, sr schema.Registry, esConfig config.ElasticsearchConfiguration) (*quesma_api.Result, error) {
	ir := elasticsearch.NewIndexResolver(esConfig)
	sources, err := resolve.HandleResolve(indexPattern, sr, ir)
	if err != nil {
		return nil, err
	}
	return resolveIndexResult(sources)
}

func HandleIndexCount(ctx context.Context, indexPattern string, queryRunner QueryRunnerIFace) (*quesma_api.Result, error) {
	cnt, err := queryRunner.HandleCount(ctx, indexPattern)
	if err != nil {
		if errors.Is(quesma_errors.ErrIndexNotExists(), err) {
			return &quesma_api.Result{StatusCode: http.StatusNotFound, GenericResult: make([]byte, 0)}, nil
		} else {
			return nil, err
		}
	}

	if cnt == -1 {
		return &quesma_api.Result{StatusCode: http.StatusNotFound, GenericResult: make([]byte, 0)}, nil
	} else {
		return elasticsearchCountResult(cnt, http.StatusOK)
	}
}

func HandleFieldCaps(ctx context.Context, indexPattern string, allowNoIndices, ignoreUnavailable bool, cfg map[string]config.IndexConfiguration, sr schema.Registry, lm clickhouse.LogManagerIFace) (*quesma_api.Result, error) {
	responseBody, err := field_capabilities.HandleFieldCaps(ctx, cfg, sr, indexPattern, lm)
	if err != nil {
		if errors.Is(quesma_errors.ErrIndexNotExists(), err) {
			if allowNoIndices || ignoreUnavailable { // TODO I think this is no longer applicable? :|
				return elasticsearchQueryResult(string(field_capabilities.EmptyFieldCapsResponse()), http.StatusOK), nil
			}
			return &quesma_api.Result{StatusCode: http.StatusNotFound, GenericResult: make([]byte, 0)}, nil
		} else {
			return nil, err
		}
	}
	return elasticsearchQueryResult(string(responseBody), http.StatusOK), nil
}
