// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package es_to_ch_ingest

import (
	"bytes"
	"fmt"
	"github.com/QuesmaOrg/quesma/quesma/backend_connectors"
	"github.com/QuesmaOrg/quesma/quesma/clickhouse"
	"github.com/QuesmaOrg/quesma/quesma/common_table"
	"github.com/QuesmaOrg/quesma/quesma/frontend_connectors"
	"github.com/QuesmaOrg/quesma/quesma/ingest"
	"github.com/QuesmaOrg/quesma/quesma/persistence"
	"github.com/QuesmaOrg/quesma/quesma/processors"
	"github.com/QuesmaOrg/quesma/quesma/quesma/config"
	"github.com/QuesmaOrg/quesma/quesma/quesma/types"
	"github.com/QuesmaOrg/quesma/quesma/schema"
	"github.com/goccy/go-json"
	"github.com/rs/zerolog/log"
	"io"
	"net/http"
	"net/url"
	"quesma_v2/core"
)

const (
	IngestAction    = "ingest_action"
	DocIndexAction  = "_doc"
	BulkIndexAction = "_bulk"
	IngestTargetKey = "ingest_target"
)

type ElasticsearchToClickHouseIngestProcessor struct {
	processors.BaseProcessor
	config                config.QuesmaProcessorConfig
	legacyIngestProcessor *ingest.IngestProcessor2
}

func NewElasticsearchToClickHouseIngestProcessor(conf config.QuesmaProcessorConfig) *ElasticsearchToClickHouseIngestProcessor {
	return &ElasticsearchToClickHouseIngestProcessor{
		BaseProcessor: processors.NewBaseProcessor(),
		config:        conf,
	}
}

func (p *ElasticsearchToClickHouseIngestProcessor) InstanceName() string {
	return "elasticsearch_to_clickhouse_ingest" // TODO return name from config
}

func (p *ElasticsearchToClickHouseIngestProcessor) Init() error {
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

	p.legacyIngestProcessor = p.prepareTemporaryIngestProcessor(chBackendConnector, *esBackendConnectorCasted)

	return nil
}

func (p *ElasticsearchToClickHouseIngestProcessor) GetId() string {
	return "elasticsearch_to_clickhouse_ingest"
}

// prepareTemporaryIngestProcessor creates a temporary ingest processor which is a new version of the ingest processor,
// which uses `quesma_api.BackendConnector` instead of `*sql.DB` for the database connection.
func (p *ElasticsearchToClickHouseIngestProcessor) prepareTemporaryIngestProcessor(chBackendConn quesma_api.BackendConnector, esBackendConn backend_connectors.ElasticsearchBackendConnector) *ingest.IngestProcessor2 {

	oldQuesmaConfig := &config.QuesmaConfiguration{
		IndexConfig: p.config.IndexConfig,
		ClickHouse:  config.RelationalDbConfiguration{Url: (*config.Url)(&url.URL{Scheme: "clickhouse", Host: "localhost:9000"})},
		// Elasticsearch section is here only for the phone home agent not to blow up
		Elasticsearch: config.ElasticsearchConfiguration{Url: (*config.Url)(&url.URL{Scheme: "http", Host: "localhost:9200"})},
	}

	connectionPool := clickhouse.InitDBConnectionPool(oldQuesmaConfig)

	// TODO see if we can get away with that
	//phoneHomeAgent := telemetry.NewPhoneHomeAgent(oldQuesmaConfig, connectionPool, "dummy-id")
	//phoneHomeAgent.Start()

	virtualTableStorage := persistence.NewElasticJSONDatabase(esBackendConn.GetConfig(), common_table.VirtualTableElasticIndexName)
	tableDisco := clickhouse.NewTableDiscovery(oldQuesmaConfig, connectionPool, virtualTableStorage)
	schemaRegistry := schema.NewSchemaRegistry(clickhouse.TableDiscoveryTableProviderAdapter{TableDiscovery: tableDisco}, oldQuesmaConfig, clickhouse.SchemaTypeAdapter{})

	ip := ingest.NewIngestProcessor2(oldQuesmaConfig, chBackendConn, nil, tableDisco, schemaRegistry, virtualTableStorage, esBackendConn)

	ip.Start()
	return ip
}

func (p *ElasticsearchToClickHouseIngestProcessor) Handle(metadata map[string]interface{}, message ...any) (map[string]interface{}, any, error) {
	var data []byte
	indexNameFromIncomingReq := metadata[IngestTargetKey].(string)
	if indexNameFromIncomingReq == "" {
		fmt.Printf("Missing index name in metadata") // SHOULD NEVER HAPPEN AND NOT BE VERIFIED HERE I GUESS
		return nil, data, nil
	}

	for _, m := range message {
		mCasted, err := quesma_api.CheckedCast[*quesma_api.Request](m)
		if err != nil {
			fmt.Printf("ElasticsearchToClickHouseIngestProcessor: invalid message type: %v", err)
			return nil, data, err
		}
		messageAsHttpReq := mCasted.OriginalRequest

		if _, present := p.config.IndexConfig[indexNameFromIncomingReq]; !present && metadata[IngestAction] == DocIndexAction {
			// `_doc` at this point can go directly to Elasticsearch,
			// `_bulk` request might be still sent to ClickHouse as the req payload may contain documents targeting CH tables
			resp := p.legacyIngestProcessor.SendToElasticsearch(messageAsHttpReq)
			respBody, err := ReadResponseBody(resp)
			if err != nil {
				println(err)
			}
			return metadata, respBody, nil
		}

		bodyAsBytes, err := frontend_connectors.ReadRequestBody(messageAsHttpReq)
		if err != nil {
			panic("ElasticsearchToClickHouseIngestProcessor: invalid message type")
		}

		switch metadata[IngestAction] {
		case DocIndexAction:
			payloadJson, err := types.ExpectJSON(types.ParseRequestBody(string(bodyAsBytes)))
			if err != nil {
				println(err)
			}
			result, err := p.handleDocIndex(payloadJson, indexNameFromIncomingReq)
			if err != nil {
				println(err)
			}
			if respBody, err := json.Marshal(result.Index); err == nil {
				return metadata, respBody, nil
			}
		case BulkIndexAction:
			payloadNDJson, err := types.ExpectNDJSON(types.ParseRequestBody(string(bodyAsBytes)))
			if err != nil {
				println(err)
			}
			results, err := p.handleBulkIndex(payloadNDJson, indexNameFromIncomingReq)
			if err != nil {
				println(err)
			}
			if respBody, err := json.Marshal(results); err == nil {
				return metadata, respBody, nil
			}
			println("BulkIndexAction")
		default:
			log.Info().Msg("Rethink you whole life and start over again")
		}

	}
	return metadata, data, nil
}

func (p *ElasticsearchToClickHouseIngestProcessor) GetSupportedBackendConnectors() []quesma_api.BackendConnectorType {
	return []quesma_api.BackendConnectorType{quesma_api.ClickHouseSQLBackend, quesma_api.ElasticsearchBackend}
}

func ReadResponseBody(resp *http.Response) ([]byte, error) {
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	resp.Body = io.NopCloser(bytes.NewBuffer(respBody))
	return respBody, nil
}
