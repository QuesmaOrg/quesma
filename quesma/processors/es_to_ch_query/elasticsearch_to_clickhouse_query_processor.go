// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package es_to_ch_query

import (
	"context"
	"fmt"
	"net/http"
	"quesma/backend_connectors"
	"quesma/clickhouse"
	"quesma/common_table"
	"quesma/frontend_connectors"
	"quesma/persistence"
	"quesma/processors"
	"quesma/queryparser"
	quesm "quesma/quesma"
	"quesma/quesma/config"
	"quesma/quesma/types"
	"quesma/schema"
	"quesma_v2/core"
	"strings"
)

const ( // taken from `router.go`
	IndexSearchPath       = "/:index/_search"
	IndexAsyncSearchPath  = "/:index/_async_search"
	IndexCountPath        = "/:index/_count"
	IndexRefreshPath      = "/:index/_refresh"
	IndexMappingPath      = "/:index/_mapping"
	FieldCapsPath         = "/:index/_field_caps"
	TermsEnumPath         = "/:index/_terms_enum"
	EQLSearch             = "/:index/_eql/search"
	ResolveIndexPath      = "/_resolve/index/:index"
	ClusterHealthPath     = "/_cluster/health"
	BulkPath              = "/_bulk"
	AsyncSearchIdPrefix   = "/_async_search/"
	AsyncSearchIdPath     = "/_async_search/:id"
	AsyncSearchStatusPath = "/_async_search/status/:id"
	/*
		section on metadata/headers below
	*/
	//SearchIndexTargetKey = "search_index_target"
	IndexPattern = "index_pattern"
	PathPattern  = "path_pattern"
	Id           = "id"
)

type ElasticsearchToClickHouseQueryProcessor struct {
	processors.BaseProcessor
	config      config.QuesmaProcessorConfig
	queryRunner *quesm.QueryRunner2
}

func NewElasticsearchToClickHouseQueryProcessor(conf config.QuesmaProcessorConfig) *ElasticsearchToClickHouseQueryProcessor {
	return &ElasticsearchToClickHouseQueryProcessor{
		BaseProcessor: processors.NewBaseProcessor(),
		config:        conf,
	}
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
func (p *ElasticsearchToClickHouseQueryProcessor) prepareTemporaryQueryProcessor(chBackendConn quesma_api.BackendConnector, esBackendConn *backend_connectors.ElasticsearchBackendConnector) *quesm.QueryRunner2 {

	oldQuesmaConfig := &config.QuesmaConfiguration{
		IndexConfig: p.config.IndexConfig,
	}

	virtualTableStorage := persistence.NewElasticJSONDatabase(esBackendConn.GetConfig(), common_table.VirtualTableElasticIndexName)
	tableDisco := clickhouse.NewTableDiscovery2(oldQuesmaConfig, chBackendConn, virtualTableStorage)
	schemaRegistry := schema.NewSchemaRegistry(clickhouse.TableDiscoveryTableProviderAdapter{TableDiscovery: tableDisco}, oldQuesmaConfig, clickhouse.SchemaTypeAdapter{})

	logManager := clickhouse.NewEmptyLogManager2(oldQuesmaConfig, chBackendConn, nil, tableDisco)
	logManager.Start()
	queryRunner := quesm.NewQueryRunner2(logManager, oldQuesmaConfig, nil, nil, schemaRegistry, nil, tableDisco)
	queryRunner.DateMathRenderer = queryparser.DateMathExpressionFormatLiteral

	return queryRunner
}

func (p *ElasticsearchToClickHouseQueryProcessor) Handle(metadata map[string]interface{}, message ...any) (map[string]interface{}, any, error) {
	var data []byte
	//indexNameFromIncomingReq := metadata[SearchIndexTargetKey].(string)
	//if indexNameFromIncomingReq == "" {
	//	fmt.Printf("Missing index name in metadata") // SHOULD NEVER HAPPEN AND NOT BE VERIFIED HERE I GUESS
	//	return nil, data, nil
	//}

	for _, m := range message {
		req, err := quesma_api.CheckedCast[*http.Request](m)
		if err != nil {
			fmt.Printf("Another cast failed: invalid message type: %v", err)
			return nil, data, err
		}

		quesmaReq := ToQuesmaRequest(req)

		if findQueryTarget(quesmaReq.Params["index"], p.config) != config.ClickhouseTarget {
			return nil, data, fmt.Errorf("WOULD FORWARD TO ELASTICSEARCH")
		}

		switch metadata[PathPattern] { // TODO well, this IS http routing TBH
		case IndexSearchPath:
			res, _ := quesm.HandleIndexSearch(context.Background(), quesmaReq, p.queryRunner)
			return metadata, res, nil
		case IndexAsyncSearchPath:
			res, _ := quesm.HandleIndexAsyncSearch(context.Background(), quesmaReq, nil, p.queryRunner)
			return metadata, res, nil
		case AsyncSearchIdPath:
			fmt.Printf("ID OF ASYNC SEARCH %d", metadata[Id])
			return nil, nil, fmt.Errorf("not implemented")
		case AsyncSearchStatusPath:
			fmt.Printf("ID OF ASYNC SEARCH %d", metadata[Id])
			res, _ := quesm.HandleAsyncSearchStatus(context.Background(), quesmaReq, nil, p.queryRunner)
			return metadata, res, nil
		case ResolveIndexPath:
			esConn, err := p.getElasticsearchBackendConnector()
			if err != nil {
				return nil, nil, err
			}
			res, _ := quesm.HandleResolveIndex(context.Background(), quesmaReq, nil, p.queryRunner.GetSchemaRegistry(), esConn.GetConfig())
			return metadata, res, nil
		case IndexCountPath:
			res, _ := quesm.HandleIndexCount(context.Background(), quesmaReq, nil, p.queryRunner)
			return metadata, res, nil
		case FieldCapsPath:
			res, _ := quesm.HandleFieldCaps(context.Background(), quesmaReq, nil, p.config.IndexConfig, p.queryRunner.GetSchemaRegistry(), p.queryRunner.GetLogManager())
			return metadata, res, nil
		default:
			return nil, data, fmt.Errorf("invalid processor action")
		}

	}
	return metadata, data, nil
}

func (p *ElasticsearchToClickHouseQueryProcessor) GetSupportedBackendConnectors() []quesma_api.BackendConnectorType {
	return []quesma_api.BackendConnectorType{quesma_api.ClickHouseSQLBackend, quesma_api.ElasticsearchBackend}
}

func findQueryTarget(index string, processorConfig config.QuesmaProcessorConfig) string {
	//unsafe, but config validation should have caught this
	defaultTarget := processorConfig.IndexConfig["*"].QueryTarget[0]
	_, found := processorConfig.IndexConfig[index]
	if !found {
		return defaultTarget
	} else { // per legacy syntax, if present means it's a clickhouse target
		return config.ClickhouseTarget
	}
}

func ToQuesmaRequest(req *http.Request) *quesma_api.Request {
	if reqBody, err := frontend_connectors.PeekBodyV2(req); err != nil {
		println("FAILED CREATING QUESMA REQUEST")
		return nil
	} else {
		bodyAsString := string(reqBody)

		return &quesma_api.Request{
			Method: req.Method,
			Path:   strings.TrimSuffix(req.URL.Path, "/"),
			Params: map[string]string{
				"index": "kibana_sample_data_ecommerce", //TODO
			},
			Headers:         req.Header,
			QueryParams:     req.URL.Query(),
			Body:            bodyAsString,
			ParsedBody:      types.ParseRequestBody(bodyAsString),
			OriginalRequest: req,
		}
	}

}
