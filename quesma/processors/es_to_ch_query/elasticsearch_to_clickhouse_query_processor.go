// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package es_to_ch_query

import (
	"context"
	"fmt"
	"github.com/QuesmaOrg/quesma/quesma/backend_connectors"
	"github.com/QuesmaOrg/quesma/quesma/elasticsearch"
	"github.com/QuesmaOrg/quesma/quesma/frontend_connectors"
	"github.com/QuesmaOrg/quesma/quesma/logger"
	"github.com/QuesmaOrg/quesma/quesma/parsers/elastic_query_dsl"
	"github.com/QuesmaOrg/quesma/quesma/processors"
	"github.com/QuesmaOrg/quesma/quesma/processors/es_to_ch_common"
	"github.com/QuesmaOrg/quesma/quesma/quesma/config"
	"github.com/QuesmaOrg/quesma/quesma/quesma/types"
	"github.com/QuesmaOrg/quesma/quesma/util"
	quesma_api "github.com/QuesmaOrg/quesma/quesma/v2/core"
	"github.com/QuesmaOrg/quesma/quesma/v2/core/tracing"
	"io"
	"net/http"
	"strings"
	"time"
)

type ElasticsearchToClickHouseQueryProcessor struct {
	processors.BaseProcessor
	config             config.QuesmaProcessorConfig
	queryRunner        *frontend_connectors.QueryRunner
	legacyDependencies *es_to_ch_common.LegacyQuesmaDependencies
}

func NewElasticsearchToClickHouseQueryProcessor(conf config.QuesmaProcessorConfig, legacyDependencies *es_to_ch_common.LegacyQuesmaDependencies) *ElasticsearchToClickHouseQueryProcessor {
	return &ElasticsearchToClickHouseQueryProcessor{
		BaseProcessor:      processors.NewBaseProcessor(),
		config:             conf,
		legacyDependencies: legacyDependencies,
	}
}

func (p *ElasticsearchToClickHouseQueryProcessor) InstanceName() string {
	return "ElasticsearchToClickHouseQueryProcessor"
}

func (p *ElasticsearchToClickHouseQueryProcessor) Init() error {
	queryRunner := p.prepareTemporaryQueryProcessor()
	p.queryRunner = queryRunner
	return nil
}

func (p *ElasticsearchToClickHouseQueryProcessor) getElasticsearchBackendConnector() (*backend_connectors.ElasticsearchBackendConnector, error) {
	esBackendConnector := p.GetBackendConnector(quesma_api.ElasticsearchBackend)
	if esBackendConnector == nil {
		return nil, fmt.Errorf("backend connector for Elasticsearch not found")
	}
	esBackendConnectorCasted, ok := esBackendConnector.(*backend_connectors.ElasticsearchBackendConnector) // OKAY JUST FOR NOW
	if !ok {
		return nil, fmt.Errorf("failed to cast Elasticsearch backend connector")
	}
	return esBackendConnectorCasted, nil
}

func (p *ElasticsearchToClickHouseQueryProcessor) GetId() string {
	return "elasticsearch_to_clickhouse_ingest"
}

// prepareTemporaryQueryProcessor creates a temporary ingest processor which is a new version of the ingest processor,
// which uses `quesma_api.BackendConnector` instead of `*sql.DB` for the database connection.
func (p *ElasticsearchToClickHouseQueryProcessor) prepareTemporaryQueryProcessor() *frontend_connectors.QueryRunner {

	queryRunner := frontend_connectors.NewQueryRunner(
		p.legacyDependencies.LogManager,
		p.legacyDependencies.OldQuesmaConfig,
		p.legacyDependencies.UIConsole,
		p.legacyDependencies.SchemaRegistry,
		p.legacyDependencies.AbTestingController.GetSender(),
		p.legacyDependencies.TableResolver,
		p.legacyDependencies.TableDiscovery,
	)
	queryRunner.DateMathRenderer = elastic_query_dsl.DateMathExpressionFormatLiteral

	return queryRunner
}

func (p *ElasticsearchToClickHouseQueryProcessor) Handle(metadata map[string]interface{}, message ...any) (map[string]interface{}, any, error) {
	var data []byte

	for _, m := range message {
		req, err := quesma_api.CheckedCast[*http.Request](m)
		if err != nil {
			fmt.Printf("Another cast failed: invalid message type: %v", err)
			return nil, data, err
		}

		pathPattern, indexPattern, id := "", "", ""
		if val, ok := metadata[es_to_ch_common.PathPattern]; ok {
			pathPattern = val.(string)
		}
		indexPattern = es_to_ch_common.GetParamFromRequestURI(req, pathPattern, "index")
		id = es_to_ch_common.GetParamFromRequestURI(req, pathPattern, "id")

		routerOrderedToBypass := metadata[es_to_ch_common.Bypass] == true
		if !routerOrderedToBypass && pathPattern == "" {
			logger.Error().Msgf("PathPattern not found in metadata but request will be processed, url=%s", req.URL)
		}
		indexNotInConfig := findQueryTarget(indexPattern, p.config) != config.ClickhouseTarget
		if routerOrderedToBypass {
			logger.Info().Msgf("BYPASSING %s  per frontend connector decision", req.URL)
			return p.routeToElasticsearch(metadata, req)
		}
		logger.Info().Msgf("Maybe processing %s", req.URL)
		ctx := context.Background()
		switch metadata[es_to_ch_common.PathPattern] {
		case es_to_ch_common.ClusterHealthPath:
			res, err := frontend_connectors.HandleClusterHealth()
			metadata[es_to_ch_common.RealSourceHeader] = es_to_ch_common.RealSourceQuesma
			return metadata, res, err
		case es_to_ch_common.IndexRefreshPath:
			res, err := frontend_connectors.HandleIndexRefresh()
			metadata[es_to_ch_common.RealSourceHeader] = es_to_ch_common.RealSourceQuesma
			return metadata, res, err
		case es_to_ch_common.IndexMappingPath:
			if indexNotInConfig {
				return p.routeToElasticsearch(metadata, req)
			}
			res, err := frontend_connectors.HandleGetIndexMapping(p.queryRunner.GetSchemaRegistry(), indexPattern)
			if err != nil {
				return metadata, nil, err
			}
			metadata[es_to_ch_common.RealSourceHeader] = es_to_ch_common.RealSourceQuesma
			return metadata, res, nil
		case es_to_ch_common.TermsEnumPath:
			if indexNotInConfig {
				return p.routeToElasticsearch(metadata, req)
			}
			query, err := GetQueryFromRequest(req)
			if err != nil {
				return metadata, nil, err
			}
			res, err := frontend_connectors.HandleTermsEnum(ctx, indexPattern, query, p.queryRunner.GetLogManager(), p.queryRunner.GetSchemaRegistry(), p.legacyDependencies)
			return metadata, res, err
		case es_to_ch_common.IndexPath:
			if indexNotInConfig {
				return p.routeToElasticsearch(metadata, req)
			}
			metadata[es_to_ch_common.RealSourceHeader] = es_to_ch_common.RealSourceClickHouse
			res, err := frontend_connectors.HandleGetIndex(p.queryRunner.GetSchemaRegistry(), indexPattern)
			return metadata, res, err
		case es_to_ch_common.IndexSearchPath:
			if indexNotInConfig {
				return p.routeToElasticsearch(metadata, req)
			}
			query, err := GetQueryFromRequest(req)
			if err != nil {
				return metadata, nil, err
			}
			res, _ := frontend_connectors.HandleIndexSearch(ctx, indexPattern, query, p.queryRunner)
			metadata[es_to_ch_common.RealSourceHeader] = es_to_ch_common.RealSourceClickHouse
			return metadata, res, nil
		case es_to_ch_common.IndexAsyncSearchPath:
			if indexNotInConfig {
				return p.routeToElasticsearch(metadata, req)
			}
			query, err := GetQueryFromRequest(req)
			if err != nil {
				return metadata, nil, err
			}
			queryParams := req.URL.Query()
			waitForResultsMs := 1000
			keepOnCompletion := false
			if v := queryParams.Get("wait_for_completion_timeout"); v != "" {
				if w, err := time.ParseDuration(v); err == nil {
					waitForResultsMs = int(w.Milliseconds())
				} else {
					logger.Warn().Msgf("Can't parse wait_for_completion_timeout value: %s", v)
				}
			}
			if v := queryParams.Get("keep_on_completion"); v != "" {
				if v == "true" {
					keepOnCompletion = true
				}
			}
			metadata[es_to_ch_common.RealSourceHeader] = es_to_ch_common.RealSourceClickHouse
			res, _ := frontend_connectors.HandleIndexAsyncSearch(ctx, indexPattern, query, waitForResultsMs, keepOnCompletion, p.queryRunner)
			return metadata, res, nil
		case es_to_ch_common.AsyncSearchIdPath:
			if !strings.Contains(id, tracing.AsyncIdPrefix) {
				return p.routeToElasticsearch(metadata, req)
			}
			metadata[es_to_ch_common.RealSourceHeader] = es_to_ch_common.RealSourceClickHouse
			var res *quesma_api.Result
			switch req.Method {
			case "GET":
				res, _ = frontend_connectors.HandleGettingAsyncSearchById(ctx, id, p.queryRunner)
			case "DELETE":
				res, _ = frontend_connectors.HandleDeletingAsyncSearchById(p.queryRunner, id)
			}
			if res == nil {
				return metadata, nil, fmt.Errorf("failed to handle async search id")
			}
			return metadata, res, nil
		case es_to_ch_common.AsyncSearchStatusPath:
			if !strings.Contains(id, tracing.AsyncIdPrefix) {
				return p.routeToElasticsearch(metadata, req)
			}
			res, _ := frontend_connectors.HandleAsyncSearchStatus(ctx, id, p.queryRunner)
			metadata[es_to_ch_common.RealSourceHeader] = es_to_ch_common.RealSourceClickHouse
			return metadata, res, nil
		case es_to_ch_common.ResolveIndexPath:
			esConn, err := p.getElasticsearchBackendConnector()
			if err != nil {
				return nil, nil, err
			}
			res, _ := frontend_connectors.HandleResolveIndex(ctx, indexPattern, p.queryRunner.GetSchemaRegistry(), esConn.GetConfig())
			metadata[es_to_ch_common.RealSourceHeader] = es_to_ch_common.RealSourceMixed
			return metadata, res, nil
		case es_to_ch_common.IndexCountPath:
			if indexNotInConfig {
				return p.routeToElasticsearch(metadata, req)
			}
			metadata[es_to_ch_common.RealSourceHeader] = es_to_ch_common.RealSourceClickHouse
			res, _ := frontend_connectors.HandleIndexCount(ctx, indexPattern, p.queryRunner)
			return metadata, res, nil
		case es_to_ch_common.FieldCapsPath:
			if !elasticsearch.IsIndexPattern(indexPattern) && indexNotInConfig { // TODO this is a bit of a hack, you can see patterns in this endpoint but or now that's how it is
				return p.routeToElasticsearch(metadata, req)
			}
			metadata[es_to_ch_common.RealSourceHeader] = es_to_ch_common.RealSourceClickHouse
			res, _ := frontend_connectors.HandleFieldCaps(ctx, indexPattern,
				req.URL.Query().Get("allow_no_indices") == "true",
				req.URL.Query().Get("ignore_unavailable") == "true",
				p.config.IndexConfig, p.queryRunner.GetSchemaRegistry(), p.queryRunner.GetLogManager())
			return metadata, res, nil
		default:
			return nil, data, fmt.Errorf("invalid processor action")
		}

	}
	return metadata, data, nil
}

func (p *ElasticsearchToClickHouseQueryProcessor) routeToElasticsearch(metadata map[string]interface{}, req *http.Request) (map[string]interface{}, *quesma_api.Result, error) {
	metadata[es_to_ch_common.RealSourceHeader] = es_to_ch_common.RealSourceElasticsearch
	esConn, err := p.getElasticsearchBackendConnector()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to fetch Elasticsearch backend connector")
	}
	resp, err := esConn.Send(req)
	if err != nil {
		return metadata, nil, fmt.Errorf("failed sending request to Elastic")
	}
	respBody, err := util.ReadResponseBody(resp)
	if err != nil {
		return metadata, nil, fmt.Errorf("failed to read response body from Elastic")
	}
	return metadata, &quesma_api.Result{
		Body:          string(respBody),
		Meta:          metadata,
		StatusCode:    resp.StatusCode,
		GenericResult: respBody,
	}, nil
}

func (p *ElasticsearchToClickHouseQueryProcessor) GetSupportedBackendConnectors() []quesma_api.BackendConnectorType {
	return []quesma_api.BackendConnectorType{quesma_api.ClickHouseSQLBackend, quesma_api.ElasticsearchBackend}
}

func findQueryTarget(index string, processorConfig config.QuesmaProcessorConfig) string {
	_, found := processorConfig.IndexConfig[index]
	if !found {
		return processorConfig.DefaultTargetConnectorType
	} else { // per legacy syntax, if present means it's a clickhouse target
		return config.ClickhouseTarget
	}
}

func GetQueryFromRequest(req *http.Request) (types.JSON, error) {
	reqBody, err := io.ReadAll(req.Body)
	if err != nil {
		return nil, err
	}
	reqBodyParsed := types.ParseRequestBody(string(reqBody))
	bodyJson, err := types.ExpectJSON(reqBodyParsed)
	if err != nil {
		return nil, err
	}
	return bodyJson, nil
}
