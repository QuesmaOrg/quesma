// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package quesma

import (
	"context"
	"encoding/json"
	"errors"
	"quesma/clickhouse"
	"quesma/elasticsearch"
	"quesma/logger"
	"quesma/queryparser"
	"quesma/quesma/config"
	"quesma/quesma/errors"
	"quesma/quesma/functionality/bulk"
	"quesma/quesma/functionality/doc"
	"quesma/quesma/functionality/field_capabilities"
	"quesma/quesma/functionality/resolve"
	"quesma/quesma/functionality/terms_enum"
	"quesma/quesma/mux"
	"quesma/quesma/routes"
	"quesma/quesma/types"
	"quesma/quesma/ui"
	"quesma/schema"
	"quesma/telemetry"
	"quesma/tracing"
	"regexp"
	"strings"
	"time"
)

const (
	httpOk = 200
)

func configureRouter(cfg config.QuesmaConfiguration, sr schema.Registry, lm *clickhouse.LogManager, console *ui.QuesmaManagementConsole, phoneHomeAgent telemetry.PhoneHomeAgent, queryRunner *QueryRunner) *mux.PathRouter {

	// some syntactic sugar
	method := mux.IsHTTPMethod
	and := mux.And

	router := mux.NewPathRouter()
	router.Register(routes.ClusterHealthPath, method("GET"), func(_ context.Context, req *mux.Request) (*mux.Result, error) {
		return elasticsearchQueryResult(`{"cluster_name": "quesma"}`, httpOk), nil
	})

	router.Register(routes.BulkPath, and(method("POST"), matchedAgainstBulkBody(cfg)), func(ctx context.Context, req *mux.Request) (*mux.Result, error) {

		body, err := types.ExpectNDJSON(req.ParsedBody)
		if err != nil {
			return nil, err
		}

		results, err := bulk.Write(ctx, nil, body, lm, cfg, phoneHomeAgent)
		return bulkInsertResult(results, err), nil
	})

	router.Register(routes.IndexRefreshPath, and(method("POST"), matchedExact(cfg)), func(ctx context.Context, req *mux.Request) (*mux.Result, error) {
		return elasticsearchInsertResult(`{"_shards":{"total":1,"successful":1,"failed":0}}`, httpOk), nil
	})

	router.Register(routes.IndexDocPath, and(method("POST"), matchedExact(cfg)), func(ctx context.Context, req *mux.Request) (*mux.Result, error) {

		body, err := types.ExpectJSON(req.ParsedBody)
		if err != nil {
			return nil, err
		}

		err = doc.Write(ctx, req.Params["index"], body, lm, cfg)
		return indexDocResult(req.Params["index"], httpOk), err
	})

	router.Register(routes.IndexBulkPath, and(method("POST", "PUT"), matchedExact(cfg)), func(ctx context.Context, req *mux.Request) (*mux.Result, error) {
		index := req.Params["index"]

		body, err := types.ExpectNDJSON(req.ParsedBody)
		if err != nil {
			return nil, err
		}

		results, err := bulk.Write(ctx, &index, body, lm, cfg, phoneHomeAgent)
		return bulkInsertResult(results, err), nil
	})

	router.Register(routes.ResolveIndexPath, method("GET"), func(ctx context.Context, req *mux.Request) (*mux.Result, error) {
		sources, err := resolve.HandleResolve(req.Params["index"], sr, cfg)
		if err != nil {
			return nil, err
		}
		return resolveIndexResult(sources), nil
	})

	router.Register(routes.IndexCountPath, and(method("GET"), matchedAgainstPattern(cfg)), func(ctx context.Context, req *mux.Request) (*mux.Result, error) {
		cnt, err := queryRunner.handleCount(ctx, req.Params["index"])
		if err != nil {
			if errors.Is(quesma_errors.ErrIndexNotExists(), err) {
				return &mux.Result{StatusCode: 404}, nil
			} else {
				return nil, err
			}
		}

		if cnt == -1 {
			return &mux.Result{StatusCode: 404}, nil
		} else {
			return elasticsearchCountResult(cnt, httpOk), nil
		}
	})

	// TODO: This endpoint is currently disabled (mux.Never()) as it's pretty much used only by internal Kibana requests,
	// it's error-prone to detect them in matchAgainstKibanaInternal() and Quesma can't handle well the cases of wildcard
	// matching many indices either way.
	router.Register(routes.GlobalSearchPath, and(mux.Never(), method("GET", "POST"), matchAgainstKibanaInternal()), func(ctx context.Context, req *mux.Request) (*mux.Result, error) {

		body, err := types.ExpectJSON(req.ParsedBody)
		if err != nil {
			return nil, err
		}

		// TODO we should pass JSON here instead of []byte
		responseBody, err := queryRunner.handleSearch(ctx, "*", body)
		if err != nil {
			if errors.Is(quesma_errors.ErrIndexNotExists(), err) {
				return &mux.Result{StatusCode: 404}, nil
			} else {
				return nil, err
			}
		}
		return elasticsearchQueryResult(string(responseBody), httpOk), nil
	})

	router.Register(routes.IndexSearchPath, and(method("GET", "POST"), matchedAgainstPattern(cfg)), func(ctx context.Context, req *mux.Request) (*mux.Result, error) {

		body, err := types.ExpectJSON(req.ParsedBody)
		if err != nil {
			return nil, err
		}

		responseBody, err := queryRunner.handleSearch(ctx, req.Params["index"], body)
		if err != nil {
			if errors.Is(quesma_errors.ErrIndexNotExists(), err) {
				return &mux.Result{StatusCode: 404}, nil
			} else if errors.Is(err, quesma_errors.ErrCouldNotParseRequest()) {
				return &mux.Result{
					Body:       string(queryparser.BadRequestParseError(err)),
					StatusCode: 400,
				}, nil
			} else {
				return nil, err
			}
		}
		return elasticsearchQueryResult(string(responseBody), httpOk), nil
	})
	router.Register(routes.IndexAsyncSearchPath, and(method("POST"), matchedAgainstPattern(cfg)), func(ctx context.Context, req *mux.Request) (*mux.Result, error) {
		waitForResultsMs := 1000 // Defaults to 1 second as in docs
		if v, ok := req.Params["wait_for_completion_timeout"]; ok {
			if w, err := time.ParseDuration(v); err == nil {
				waitForResultsMs = int(w.Milliseconds())
			} else {
				logger.Warn().Msgf("Can't parse wait_for_completion_timeout value: %s", v)
			}
		}
		keepOnCompletion := false
		if v, ok := req.Params["keep_on_completion"]; ok {
			if v == "true" {
				keepOnCompletion = true
			}
		}

		body, err := types.ExpectJSON(req.ParsedBody)
		if err != nil {
			return nil, err
		}

		responseBody, err := queryRunner.handleAsyncSearch(ctx, req.Params["index"], body, waitForResultsMs, keepOnCompletion)
		if err != nil {
			if errors.Is(quesma_errors.ErrIndexNotExists(), err) {
				return &mux.Result{StatusCode: 404}, nil
			} else if errors.Is(err, quesma_errors.ErrCouldNotParseRequest()) {
				return &mux.Result{
					Body:       string(queryparser.BadRequestParseError(err)),
					StatusCode: 400,
				}, nil
			} else {
				return nil, err
			}
		}
		return elasticsearchQueryResult(string(responseBody), httpOk), nil
	})

	router.Register(routes.AsyncSearchIdPath, and(method("GET"), matchedAgainstAsyncId()), func(ctx context.Context, req *mux.Request) (*mux.Result, error) {
		ctx = context.WithValue(ctx, tracing.AsyncIdCtxKey, req.Params["id"])
		responseBody, err := queryRunner.handlePartialAsyncSearch(ctx, req.Params["id"])
		if err != nil {
			return nil, err
		}
		return elasticsearchQueryResult(string(responseBody), httpOk), nil
	})

	router.Register(routes.AsyncSearchIdPath, and(method("DELETE"), matchedAgainstAsyncId()), func(ctx context.Context, req *mux.Request) (*mux.Result, error) {
		responseBody, err := queryRunner.deleteAsyncSeach(req.Params["id"])
		if err != nil {
			return nil, err
		}
		return elasticsearchQueryResult(string(responseBody), httpOk), nil
	})

	router.Register(routes.FieldCapsPath, and(method("GET", "POST"), matchedAgainstPattern(cfg)), func(ctx context.Context, req *mux.Request) (*mux.Result, error) {

		responseBody, err := field_capabilities.HandleFieldCaps(ctx, cfg, sr, req.Params["index"], lm)
		if err != nil {
			if errors.Is(quesma_errors.ErrIndexNotExists(), err) {
				if req.QueryParams.Get("allow_no_indices") == "true" || req.QueryParams.Get("ignore_unavailable") == "true" {
					return elasticsearchQueryResult(string(field_capabilities.EmptyFieldCapsResponse()), httpOk), nil
				}
				return &mux.Result{StatusCode: 404}, nil
			} else {
				return nil, err
			}
		}
		return elasticsearchQueryResult(string(responseBody), httpOk), nil
	})
	router.Register(routes.TermsEnumPath, and(method("POST"), matchedAgainstPattern(cfg)), func(ctx context.Context, req *mux.Request) (*mux.Result, error) {
		if strings.Contains(req.Params["index"], ",") {
			return nil, errors.New("multi index terms enum is not yet supported")
		} else {

			var body types.JSON
			switch b := req.ParsedBody.(type) {
			case types.JSON:
				body = b
			default:
				return nil, errors.New("invalid request body, expecting JSON")
			}

			if responseBody, err := terms_enum.HandleTermsEnum(ctx, req.Params["index"], body, lm, console); err != nil {
				return nil, err
			} else {
				return elasticsearchQueryResult(string(responseBody), httpOk), nil
			}
		}
	})

	router.Register(routes.EQLSearch, and(method("GET", "POST"), matchedAgainstPattern(cfg)), func(ctx context.Context, req *mux.Request) (*mux.Result, error) {
		body, err := types.ExpectJSON(req.ParsedBody)
		if err != nil {
			return nil, err
		}

		responseBody, err := queryRunner.handleEQLSearch(ctx, req.Params["index"], body)
		if err != nil {
			if errors.Is(quesma_errors.ErrIndexNotExists(), err) {
				return &mux.Result{StatusCode: 404}, nil
			} else {
				return nil, err
			}
		}
		return elasticsearchQueryResult(string(responseBody), httpOk), nil
	})

	router.Register(routes.IndexPath, and(method("PUT"), matchedAgainstPattern(cfg)), func(ctx context.Context, req *mux.Request) (*mux.Result, error) {
		index := req.Params["index"]

		body, err := types.ExpectJSON(req.ParsedBody)
		if err != nil {
			return nil, err
		}

		mappings, ok := body["mappings"]
		if !ok {
			logger.Warn().Msgf("no mappings found in PUT /%s request, ignoring that request. Full content: %s", index, req.Body)
			return putIndexResult(index), nil
		}
		columns := elasticsearch.ParseMappings("", mappings.(map[string]interface{}))

		sr.UpdateDynamicConfiguration(schema.TableName(index), schema.Table{Columns: columns})

		return putIndexResult(index), nil
	})

	return router
}

// check whether exact index name is enabled
func matchedExact(config config.QuesmaConfiguration) mux.RequestMatcher {
	return mux.RequestMatcherFunc(func(req *mux.Request) bool {
		if elasticsearch.IsInternalIndex(req.Params["index"]) {
			logger.Debug().Msgf("index %s is an internal Elasticsearch index, skipping", req.Params["index"])
			return false
		}
		indexConfig, exists := config.IndexConfig[req.Params["index"]]
		return exists && indexConfig.Enabled
	})
}

func elasticsearchCountResult(body int64, statusCode int) *mux.Result {
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
		panic(err)
	}
	return &mux.Result{Body: string(serialized), Meta: map[string]string{
		"Content-Type":            "application/json",
		"X-Quesma-Headers-Source": "Quesma",
	}, StatusCode: statusCode}
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

func elasticsearchQueryResult(body string, statusCode int) *mux.Result {
	return &mux.Result{Body: body, Meta: map[string]string{
		// TODO copy paste from the original request
		"X-Quesma-Headers-Source": "Quesma",
	}, StatusCode: statusCode}
}

func bulkInsertResult(ops []bulk.BulkItem, err error) *mux.Result {
	if err != nil {
		return &mux.Result{
			Body:       string(queryparser.BadRequestParseError(err)),
			StatusCode: 400,
		}
	}

	body, err := json.Marshal(bulk.BulkResponse{
		Errors: false,
		Items:  ops,
		Took:   42,
	})
	if err != nil {
		panic(err)
	}

	return elasticsearchInsertResult(string(body), httpOk)
}

func elasticsearchInsertResult(body string, statusCode int) *mux.Result {
	return &mux.Result{Body: body, Meta: map[string]string{
		// TODO copy paste from the original request
		contentTypeHeaderKey:      "application/json",
		"X-Quesma-Headers-Source": "Quesma",
	}, StatusCode: statusCode}
}

func resolveIndexResult(sources elasticsearch.Sources) *mux.Result {
	if len(sources.Aliases) == 0 && len(sources.DataStreams) == 0 && len(sources.Indices) == 0 {
		return &mux.Result{StatusCode: 404}
	}

	body, err := json.Marshal(sources)
	if err != nil {
		panic(err)
	}

	return &mux.Result{
		Body:       string(body),
		Meta:       map[string]string{},
		StatusCode: httpOk}
}

func indexDocResult(index string, statusCode int) *mux.Result {
	body, err := json.Marshal(indexDocResponse{
		Id:          "fakeId",
		Index:       index,
		PrimaryTerm: 1,
		SeqNo:       0,
		Shards: shardsResponse{
			Failed:     0,
			Successful: 1,
			Total:      1,
		},
		Version: 1,
		Result:  "created",
	})
	if err != nil {
		panic(err)
	}
	return elasticsearchInsertResult(string(body), statusCode)
}

func putIndexResult(index string) *mux.Result {
	result := putIndexResponse{
		Acknowledged:       true,
		ShardsAcknowledged: true,
		Index:              index,
	}
	serialized, err := json.Marshal(result)
	if err != nil {
		panic(err)
	}

	return &mux.Result{StatusCode: httpOk, Body: string(serialized)}
}

type (
	indexDocResponse struct {
		Id          string         `json:"_id"`
		Index       string         `json:"_index"`
		PrimaryTerm int            `json:"_primary_term"`
		SeqNo       int            `json:"_seq_no"`
		Shards      shardsResponse `json:"_shards"`
		Version     int            `json:"_version"`
		Result      string         `json:"result"`
	}
	shardsResponse struct {
		Failed     int `json:"failed"`
		Successful int `json:"successful"`
		Total      int `json:"total"`
	}
	putIndexResponse struct {
		Acknowledged       bool   `json:"acknowledged"`
		ShardsAcknowledged bool   `json:"shards_acknowledged"`
		Index              string `json:"index"`
	}
)

var indexNamePattern = regexp.MustCompile(`"_index"\s*:\s*"([^"]+)"`)

func extractIndexName(input string) string {
	results := indexNamePattern.FindStringSubmatch(input)

	if len(results) < 2 {
		return ""
	}

	return results[1]
}
