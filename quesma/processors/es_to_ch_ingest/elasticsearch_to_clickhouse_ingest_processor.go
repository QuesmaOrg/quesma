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
	"net/url"
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
	config config.QuesmaProcessorConfig
}

func NewElasticsearchToClickHouseIngestProcessor(conf config.QuesmaProcessorConfig) *ElasticsearchToClickHouseIngestProcessor {
	return &ElasticsearchToClickHouseIngestProcessor{
		BaseProcessor: processors.NewBaseProcessor(),
		config:        conf,
	}
}

func (p *ElasticsearchToClickHouseIngestProcessor) GetId() string {
	return "elasticsearch_to_clickhouse_ingest"
}

// prepareTemporaryIngestProcessor creates a temporary ingest processor which is a new version of the ingest processor,
// which uses `quesma_api.BackendConnector` instead of `*sql.DB` for the database connection.
func (p *ElasticsearchToClickHouseIngestProcessor) prepareTemporaryIngestProcessor(connector quesma_api.BackendConnector) *ingest.IngestProcessor2 {
	u, _ := url.Parse("http://localhost:9200")

	elasticsearchConfig := config.ElasticsearchConfiguration{
		Url: (*config.Url)(u),
	}
	emptyConfig := &config.QuesmaConfiguration{
		IndexConfig: p.config.IndexConfig,
	}

	virtualTableStorage := persistence.NewElasticJSONDatabase(elasticsearchConfig, common_table.VirtualTableElasticIndexName)
	tableDisco := clickhouse.NewTableDiscovery2(emptyConfig, connector, virtualTableStorage)
	schemaRegistry := schema.NewSchemaRegistry(clickhouse.TableDiscoveryTableProviderAdapter{TableDiscovery: tableDisco}, emptyConfig, clickhouse.SchemaTypeAdapter{})

	v2TableResolver := NewNextGenTableResolver()

	ip := ingest.NewIngestProcessor2(emptyConfig, connector, nil, tableDisco, schemaRegistry, virtualTableStorage, v2TableResolver)
	ip.Start()
	return ip
}

func (p *ElasticsearchToClickHouseIngestProcessor) Handle(metadata map[string]interface{}, message ...any) (map[string]interface{}, any, error) {
	var data []byte
	var chBackend, esBackend quesma_api.BackendConnector
	indexNameFromIncomingReq := metadata[IngestTargetKey].(string)
	if indexNameFromIncomingReq == "" {
		panic("NO INDEX NAME?!?!?")
	}

	chBackend = p.GetBackendConnector(quesma_api.ClickHouseSQLBackend)
	if chBackend == nil {
		fmt.Println("Backend connector not found")
		return metadata, data, nil
	}

	tempIngestProcessor := p.prepareTemporaryIngestProcessor(chBackend)

	esBackend = p.GetBackendConnector(quesma_api.ElasticsearchBackend)
	if esBackend == nil {
		fmt.Println("Backend connector not found")
		return metadata, data, nil
	}
	es, ok := esBackend.(*backend_connectors.ElasticsearchBackendConnector) // OKAY JUST FOR NOW
	if !ok {
		panic(" !!! ")
	}

	for _, m := range message {
		messageAsHttpReq, err := quesma_api.CheckedCast[*http.Request](m)
		if err != nil {
			panic("ElasticsearchToClickHouseIngestProcessor: invalid message type")
		}

		if _, present := p.config.IndexConfig[indexNameFromIncomingReq]; !present {
			// route to Elasticsearch
			resp := es.Send(messageAsHttpReq)
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
			result, err := handleDocIndex(payloadJson, indexNameFromIncomingReq, tempIngestProcessor)
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
			results, err := handleBulkIndex(payloadNDJson, indexNameFromIncomingReq, tempIngestProcessor)
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
