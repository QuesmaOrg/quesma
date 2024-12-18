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

const (
	SearchIndexTargetKey = "search_index_target"
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
	esBackendConnector := p.GetBackendConnector(quesma_api.ElasticsearchBackend)
	if esBackendConnector == nil {
		return fmt.Errorf("backend connector for Elasticsearch not found")
	}
	esBackendConnectorCasted, ok := esBackendConnector.(*backend_connectors.ElasticsearchBackendConnector) // OKAY JUST FOR NOW
	if !ok {
		return fmt.Errorf("failed to cast Elasticsearch backend connector")
	}

	queryRunner := p.prepareTemporaryQueryProcessor(chBackendConnector, esBackendConnectorCasted)
	p.queryRunner = queryRunner
	return nil
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
	queryRunner := quesm.NewQueryRunner2(logManager, oldQuesmaConfig, nil, nil, schemaRegistry, nil, NewNextGenTableResolver(), tableDisco)
	queryRunner.DateMathRenderer = queryparser.DateMathExpressionFormatLiteral

	return queryRunner
}

func (p *ElasticsearchToClickHouseQueryProcessor) Handle(metadata map[string]interface{}, message ...any) (map[string]interface{}, any, error) {
	var data []byte
	indexNameFromIncomingReq := metadata[SearchIndexTargetKey].(string)
	if indexNameFromIncomingReq == "" {
		fmt.Printf("Missing index name in metadata") // SHOULD NEVER HAPPEN AND NOT BE VERIFIED HERE I GUESS
		return nil, data, nil
	}

	for _, m := range message {

		req, err := quesma_api.CheckedCast[*http.Request](m)
		if err != nil {
			fmt.Printf("Another cast failed: invalid message type: %v", err)
			return nil, data, err
		}
		quesmaReq := ToQuesmaRequest(req)

		switch findQueryTarget(indexNameFromIncomingReq, p.config) {
		case config.ClickhouseTarget:
			res, _ := quesm.HandleIndexSearch2(context.Background(), quesmaReq, p.queryRunner)

			return metadata, res, nil
		case config.ElasticsearchTarget:
			println("POSZLO DO ELASTICSEARCHA ")
			return nil, data, fmt.Errorf("invalid query target")
		default:
			return nil, data, fmt.Errorf("invalid query target")
		}
		//if _, present := p.config.IndexConfig[indexNameFromIncomingReq]; !present && p.con {
		//
		//	resp := p.legacyIngestProcessor.SendToElasticsearch(messageAsHttpReq)
		//	respBody, err := ReadResponseBody(resp)
		//	if err != nil {
		//		println(err)
		//	}
		//	return metadata, respBody, nil
		//}

		//bodyAsBytes, err := frontend_connectors.ReadRequestBody(messageAsHttpReq)
		//if err != nil {
		//	panic("ElasticsearchToClickHouseQueryProcessor: invalid message type")
		//}

		//switch metadata[SearchIndexTargetKey] {
		//case DocIndexAction:
		//	//payloadJson, err := types.ExpectJSON(types.ParseRequestBody(string(bodyAsBytes)))
		//	//if err != nil {
		//	//	println(err)
		//	//}
		//	//result, err := p.handleDocIndex(payloadJson, index\NameFromIncomingReq)
		//	//if err != nil {
		//	//	println(err)
		//	//}
		//	//if respBody, err := json.Marshal(result.Index); err == nil {
		//	//	return metadata, respBody, nil
		//	//}
		//case BulkIndexAction:
		//	//payloadNDJson, err := types.ExpectNDJSON(types.ParseRequestBody(string(bodyAsBytes)))
		//	//if err != nil {
		//	//	println(err)
		//	//}
		//	////results, err := p.handleBulkIndex(payloadNDJson, indexNameFromIncomingReq)
		//	//if err != nil {
		//	//	println(err)
		//	//}
		//	//if respBody, err := json.Marshal(results); err == nil {
		//	//	return metadata, respBody, nil
		//	//}
		//	//println("BulkIndexAction")
		//default:
		//	log.Info().Msg("Rethink you whole life and start over again")
		//}

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
