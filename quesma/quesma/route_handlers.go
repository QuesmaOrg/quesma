// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package quesma

import (
	"context"
	"errors"
	"net/http"
	"quesma/clickhouse"
	"quesma/elasticsearch"
	"quesma/logger"
	"quesma/queryparser"
	"quesma/quesma/config"
	quesma_errors "quesma/quesma/errors"
	"quesma/quesma/functionality/field_capabilities"
	"quesma/quesma/functionality/resolve"
	"quesma/quesma/functionality/terms_enum"
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

func HandlePutIndex(index string, reqBody types.JSON, sr schema.Registry) (*quesma_api.Result, error) {
	if len(reqBody) == 0 {
		logger.Warn().Msgf("empty body in PUT /%s request, Quesma is not doing anything", index)
		return putIndexResult(index)
	}

	mappings, ok := reqBody["mappings"]
	if !ok {
		logger.Warn().Msgf("no mappings found in PUT /%s request, ignoring that request. Full content: %s", index, reqBody)
		return putIndexResult(index)
	}
	columns := elasticsearch.ParseMappings("", mappings.(map[string]interface{}))

	sr.UpdateDynamicConfiguration(schema.IndexName(index), schema.Table{Columns: columns})

	return putIndexResult(index)
}

func HandleGetIndex(sr schema.Registry, index string) (*quesma_api.Result, error) {
	foundSchema, found := sr.FindSchema(schema.IndexName(index))
	if !found {
		return &quesma_api.Result{StatusCode: http.StatusNotFound, GenericResult: make([]byte, 0)}, nil
	}

	hierarchicalSchema := schema.SchemaToHierarchicalSchema(&foundSchema)
	mappings := elasticsearch.GenerateMappings(hierarchicalSchema)

	return getIndexResult(index, mappings)
}

func HandleTermsEnum(ctx context.Context, indexPattern string, body types.JSON, lm clickhouse.LogManagerIFace, sr schema.Registry, dependencies quesma_api.Dependencies) (*quesma_api.Result, error) {
	if responseBody, err := terms_enum.HandleTermsEnum(ctx, indexPattern, body, lm, sr, dependencies.DebugInfoCollector()); err != nil {
		return nil, err
	} else {
		return elasticsearchQueryResult(string(responseBody), http.StatusOK), nil
	}
}

func HandleClusterHealth() (*quesma_api.Result, error) {
	return ElasticsearchQueryResult(`{"cluster_name": "quesma"}`, http.StatusOK), nil
}

func HandleIndexRefresh() (*quesma_api.Result, error) {
	return ElasticsearchInsertResult(`{"_shards":{"total":1,"successful":1,"failed":0}}`, http.StatusOK), nil
}

func HandleGetIndexMapping(sr schema.Registry, index string) (*quesma_api.Result, error) {
	foundSchema, found := sr.FindSchema(schema.IndexName(index))
	if !found {
		return &quesma_api.Result{StatusCode: http.StatusNotFound, GenericResult: make([]byte, 0)}, nil
	}

	hierarchicalSchema := schema.SchemaToHierarchicalSchema(&foundSchema)
	mappings := elasticsearch.GenerateMappings(hierarchicalSchema)

	return getIndexMappingResult(index, mappings)
}
