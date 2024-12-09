// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package es_to_ch_ingest

import (
	"bytes"
	"fmt"
	"github.com/goccy/go-json"
	"github.com/rs/zerolog/log"
	"io"
	"net/http"
	"quesma/backend_connectors"
	"quesma/clickhouse"
	"quesma/common_table"
	"quesma/frontend_connectors"
	"quesma/ingest"
	"quesma/persistence"
	"quesma/processors"
	"quesma/quesma/config"
	"quesma/quesma/types"
	"quesma/schema"
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

func (p *ElasticsearchToClickHouseIngestProcessor) Init() error {
	chBackendConnector := p.GetBackendConnector(quesma_api.ClickHouseSQLBackend)
	if chBackendConnector == nil {
		return fmt.Errorf("ClickHouse backend connector not found")
	}
	esBackendConnector := p.GetBackendConnector(quesma_api.ElasticsearchBackend)
	if esBackendConnector == nil {
		return fmt.Errorf("Elasticsearch backend connector not found")
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

	elasticsearchConfig := esBackendConn.GetConfig()

	oldQuesmaConfig := &config.QuesmaConfiguration{
		IndexConfig: p.config.IndexConfig,
	}

	virtualTableStorage := persistence.NewElasticJSONDatabase(elasticsearchConfig, common_table.VirtualTableElasticIndexName)
	tableDisco := clickhouse.NewTableDiscovery2(oldQuesmaConfig, chBackendConn, virtualTableStorage)
	schemaRegistry := schema.NewSchemaRegistry(clickhouse.TableDiscoveryTableProviderAdapter{TableDiscovery: tableDisco}, oldQuesmaConfig, clickhouse.SchemaTypeAdapter{})

	v2TableResolver := NewNextGenTableResolver()

	ip := ingest.NewIngestProcessor2(oldQuesmaConfig, chBackendConn, nil, tableDisco, schemaRegistry, virtualTableStorage, v2TableResolver)
	ip.Start()
	return ip
}

func (p *ElasticsearchToClickHouseIngestProcessor) Handle(metadata map[string]interface{}, message ...any) (map[string]interface{}, any, error) {
	var data []byte
	indexNameFromIncomingReq := metadata[IngestTargetKey].(string)
	if indexNameFromIncomingReq == "" {
		panic("NO INDEX NAME?!?!?")
	}

	for _, m := range message {
		messageAsHttpReq, err := quesma_api.CheckedCast[*http.Request](m)
		if err != nil {
			panic("ElasticsearchToClickHouseIngestProcessor: invalid message type")
		}

		if _, present := p.config.IndexConfig[indexNameFromIncomingReq]; !present && metadata[IngestAction] == DocIndexAction {
			// route to Elasticsearch, `bulk` request might be sent to ClickHouse depending on the request payload
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
