// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package es_to_ch_query

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"quesma/backend_connectors"
	"quesma/clickhouse"
	"quesma/common_table"
	"quesma/elasticsearch"
	"quesma/frontend_connectors"
	"quesma/logger"
	"quesma/persistence"
	"quesma/processors"
	"quesma/queryparser"
	quesm "quesma/quesma"
	"quesma/quesma/config"
	"quesma/quesma/types"
	"quesma/schema"
	"quesma/table_resolver"
	"quesma/telemetry"
	"quesma_v2/core"
	"quesma_v2/core/tracing"
	"strings"
	"time"
)

const ( // taken from `router.go`
	GlobalSearchPath     = "/_search" // TODO decide what to do here
	IndexSearchPath      = "/:index/_search"
	IndexAsyncSearchPath = "/:index/_async_search"
	IndexCountPath       = "/:index/_count"
	//IndexDocPath              = "/:index/_doc" // irrelevant for this processor
	IndexRefreshPath = "/:index/_refresh"
	//IndexBulkPath             = "/:index/_bulk" // irrelevant for this processor
	IndexMappingPath = "/:index/_mapping"
	FieldCapsPath    = "/:index/_field_caps"
	TermsEnumPath    = "/:index/_terms_enum"
	//EQLSearch                 = "/:index/_eql/search" // irrelevant for this processor
	ResolveIndexPath  = "/_resolve/index/:index"
	ClusterHealthPath = "/_cluster/health"
	//BulkPath                  = "/_bulk"	// irrelevant for this processor
	//AsyncSearchIdPrefix       = "/_async_search/" // TODO -> it's not used as route so just skip
	AsyncSearchIdPath         = "/_async_search/:id"
	AsyncSearchStatusPath     = "/_async_search/status/:id"
	KibanaInternalPrefix      = "/.kibana_"
	IndexPath                 = "/:index"
	ExecutePainlessScriptPath = "/_scripts/painless/_execute" // This path is used on the Kibana side to evaluate painless scripts when adding a new scripted field.

	// Quesma internal paths

	QuesmaTableResolverPath = "/:index/_quesma_table_resolver" // irrelevant for this processor we might ditch this concept entirely or rethink
	/*
		section on metadata/headers below
	*/
	//SearchIndexTargetKey = "search_index_target"
	IndexPattern = "index_pattern"
	PathPattern  = "path_pattern"
	Id           = "id"

	// RealSourceHeader is a header to determine what exactly processor has calld
	RealSourceHeader        = "X-Backend-Called"
	RealSourceClickHouse    = "CLICKHOUSE"
	RealSourceElasticsearch = "ELASTICSEARCH"
	RealSourceQuesma        = "NONE"  // if response is just processor's own rendered content, no DB is called
	RealSourceMixed         = "MIXED" // e.g. in case of _resolve API
)

type ElasticsearchToClickHouseQueryProcessor struct {
	processors.BaseProcessor
	config      config.QuesmaProcessorConfig
	queryRunner *quesm.QueryRunner
}

func NewElasticsearchToClickHouseQueryProcessor(conf config.QuesmaProcessorConfig) *ElasticsearchToClickHouseQueryProcessor {
	return &ElasticsearchToClickHouseQueryProcessor{
		BaseProcessor: processors.NewBaseProcessor(),
		config:        conf,
	}
}

func (p *ElasticsearchToClickHouseQueryProcessor) InstanceName() string {
	return "ElasticsearchToClickHouseQueryProcessor"
}

func (p *ElasticsearchToClickHouseQueryProcessor) Init() error {
	chBackendConnector := p.GetBackendConnector(quesma_api.ClickHouseSQLBackend)
	if chBackendConnector == nil {
		return fmt.Errorf("backend connector for ClickHouse not found")
	}
	esBackendConnector, err := p.getElasticsearchBackendConnector()
	if err != nil {
		return err
	}

	queryRunner := p.prepareTemporaryQueryProcessor(chBackendConnector, esBackendConnector)
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
func (p *ElasticsearchToClickHouseQueryProcessor) prepareTemporaryQueryProcessor(_ quesma_api.BackendConnector, esBackendConn *backend_connectors.ElasticsearchBackendConnector) *quesm.QueryRunner {

	oldQuesmaConfig := &config.QuesmaConfiguration{
		IndexConfig: p.config.IndexConfig,
		ClickHouse:  config.RelationalDbConfiguration{Url: (*config.Url)(&url.URL{Scheme: "clickhouse", Host: "localhost:9000"})},
		// Elasticsearch section is here only for the phone home agent not to blow up
		Elasticsearch: config.ElasticsearchConfiguration{Url: (*config.Url)(&url.URL{Scheme: "http", Host: "localhost:9200"})},
	}

	connectionPool := clickhouse.InitDBConnectionPool(oldQuesmaConfig)

	phoneHomeAgent := telemetry.NewPhoneHomeAgent(oldQuesmaConfig, connectionPool, "dummy-id")
	phoneHomeAgent.Start()

	virtualTableStorage := persistence.NewElasticJSONDatabase(esBackendConn.GetConfig(), common_table.VirtualTableElasticIndexName)
	tableDisco := clickhouse.NewTableDiscovery(oldQuesmaConfig, connectionPool, virtualTableStorage)
	schemaRegistry := schema.NewSchemaRegistry(clickhouse.TableDiscoveryTableProviderAdapter{TableDiscovery: tableDisco}, oldQuesmaConfig, clickhouse.SchemaTypeAdapter{})

	logManager := clickhouse.NewEmptyLogManager(oldQuesmaConfig, connectionPool, phoneHomeAgent, tableDisco)
	logManager.Start()

	dummyTableResolver := table_resolver.NewDummyTableResolver()

	queryRunner := quesm.NewQueryRunner(logManager, oldQuesmaConfig, nil, nil, schemaRegistry, nil, dummyTableResolver, tableDisco)
	queryRunner.DateMathRenderer = queryparser.DateMathExpressionFormatLiteral

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

		indexPattern, id := "", ""
		if val, ok := metadata[IndexPattern]; ok {
			indexPattern = val.(string)
		}
		if val, ok := metadata[Id]; ok {
			id = val.(string)
		}

		routerOrderedToBypass := metadata[frontend_connectors.Bypass] == true
		indexNotInConfig := findQueryTarget(indexPattern, p.config) != config.ClickhouseTarget
		if routerOrderedToBypass {
			logger.Info().Msgf("%s - BYPASSED per frontend connector decision", req.URL)
			return p.routeToElasticsearch(metadata, req)
		}
		logger.Info().Msgf("Maybe processing %s", req.URL)
		ctx := context.Background()
		switch metadata[PathPattern] { // TODO well, this IS http routing TBH
		case ClusterHealthPath:
			res, err := quesm.HandleClusterHealth()
			metadata[RealSourceHeader] = RealSourceQuesma
			return metadata, res, err
		case IndexRefreshPath:
			res, err := quesm.HandleIndexRefresh()
			metadata[RealSourceHeader] = RealSourceQuesma
			return metadata, res, err
		case IndexMappingPath:
			res, err := quesm.HandleGetIndexMapping(p.queryRunner.GetSchemaRegistry(), indexPattern)
			if err != nil {
				return metadata, nil, err
			}
			metadata[RealSourceHeader] = RealSourceQuesma
			return metadata, res, nil
		case TermsEnumPath:
			if indexNotInConfig {
				return p.routeToElasticsearch(metadata, req)
			}
			query, err := GetQueryFromRequest(req)
			if err != nil {
				return metadata, nil, err
			}
			res, err := quesm.HandleTermsEnum(ctx, indexPattern, query, p.queryRunner.GetLogManager(), p.queryRunner.GetSchemaRegistry(), nil) // TODO dependencies are nil for now
			return metadata, res, err
		case IndexPath:
			logger.Warn().Msgf("PROBLEMATIC INDEXPATH CALLED FOR %s", indexPattern)
			if indexNotInConfig {
				return p.routeToElasticsearch(metadata, req)
			}
			metadata[RealSourceHeader] = RealSourceClickHouse
			res, err := quesm.HandleGetIndex(p.queryRunner.GetSchemaRegistry(), indexPattern)
			return metadata, res, err
		case IndexSearchPath:
			if indexNotInConfig {
				return p.routeToElasticsearch(metadata, req)
			}
			query, err := GetQueryFromRequest(req)
			if err != nil {
				return metadata, nil, err
			}
			res, _ := quesm.HandleIndexSearch(ctx, indexPattern, query, p.queryRunner)
			metadata[RealSourceHeader] = RealSourceClickHouse
			return metadata, res, nil
		case IndexAsyncSearchPath:
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
			metadata[RealSourceHeader] = RealSourceClickHouse
			res, _ := quesm.HandleIndexAsyncSearch(ctx, indexPattern, query, waitForResultsMs, keepOnCompletion, p.queryRunner)
			return metadata, res, nil
		case AsyncSearchIdPath:
			if !strings.Contains(id, tracing.AsyncIdPrefix) {
				return p.routeToElasticsearch(metadata, req)
			}
			metadata[RealSourceHeader] = RealSourceClickHouse
			var res *quesma_api.Result
			switch req.Method {
			case "GET":
				res, _ = quesm.HandleGettingAsyncSearchById(ctx, id, p.queryRunner)
			case "DELETE":
				res, _ = quesm.HandleDeletingAsyncSearchById(p.queryRunner, id)
			}
			if res == nil {
				return metadata, nil, fmt.Errorf("failed to handle async search id")
			}
			return metadata, res, nil
		case AsyncSearchStatusPath:
			if !strings.Contains(id, tracing.AsyncIdPrefix) {
				return p.routeToElasticsearch(metadata, req)
			}
			res, _ := quesm.HandleAsyncSearchStatus(ctx, id, p.queryRunner)
			metadata[RealSourceHeader] = RealSourceClickHouse
			return metadata, res, nil
		case ResolveIndexPath:
			esConn, err := p.getElasticsearchBackendConnector()
			if err != nil {
				return nil, nil, err
			}
			res, _ := quesm.HandleResolveIndex(ctx, indexPattern, p.queryRunner.GetSchemaRegistry(), esConn.GetConfig())
			metadata[RealSourceHeader] = RealSourceMixed
			return metadata, res, nil
		case IndexCountPath:
			if indexNotInConfig {
				return p.routeToElasticsearch(metadata, req)
			}
			metadata[RealSourceHeader] = RealSourceClickHouse
			res, _ := quesm.HandleIndexCount(ctx, indexPattern, p.queryRunner)
			return metadata, res, nil
		case FieldCapsPath:
			if !elasticsearch.IsIndexPattern(indexPattern) && indexNotInConfig { // TODO this is a bit of a hack, you can see patterns in this endpoint but or now that's how it is
				return p.routeToElasticsearch(metadata, req)
			}
			metadata[RealSourceHeader] = RealSourceClickHouse
			res, _ := quesm.HandleFieldCaps(ctx, indexPattern,
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
	metadata[RealSourceHeader] = RealSourceElasticsearch
	esConn, err := p.getElasticsearchBackendConnector()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to fetch Elasticsearch backend connector")
	}
	resp := esConn.Send(req)
	respBody, err := ReadResponseBody(resp)
	if err != nil {
		return metadata, nil, fmt.Errorf("failed to read response body from Elastic")
	}
	return metadata, &quesma_api.Result{
		Body:          string(respBody),
		Meta:          metadata,
		StatusCode:    http.StatusOK,
		GenericResult: respBody,
	}, nil
}

func (p *ElasticsearchToClickHouseQueryProcessor) GetSupportedBackendConnectors() []quesma_api.BackendConnectorType {
	return []quesma_api.BackendConnectorType{quesma_api.ClickHouseSQLBackend, quesma_api.ElasticsearchBackend}
}

func findQueryTarget(index string, processorConfig config.QuesmaProcessorConfig) string {
	var defaultTargetFromConfig string
	wildcardConfig, ok := processorConfig.IndexConfig["*"]
	if !ok {
		logger.Warn().Msgf("No wildcard index config found in processor config!!")
		return config.ClickhouseTarget
	}
	if len(wildcardConfig.QueryTarget) == 0 {
		logger.Warn().Msgf("wildcard index has no target!!")
		return config.ClickhouseTarget
	}
	defaultTargetFromConfig = wildcardConfig.QueryTarget[0]
	_, found := processorConfig.IndexConfig[index]
	if !found {
		return defaultTargetFromConfig
	} else { // per legacy syntax, if present means it's a clickhouse target
		return config.ClickhouseTarget
	}
}

func ReadResponseBody(resp *http.Response) ([]byte, error) {
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	resp.Body = io.NopCloser(bytes.NewBuffer(respBody))
	return respBody, nil
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
