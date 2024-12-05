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
}

func NewElasticsearchToClickHouseIngestProcessor() *ElasticsearchToClickHouseIngestProcessor {
	return &ElasticsearchToClickHouseIngestProcessor{
		BaseProcessor: processors.NewBaseProcessor(),
	}
}

func (p *ElasticsearchToClickHouseIngestProcessor) GetId() string {
	return "elasticsearch_to_clickhouse_ingest"
}

// prepareTemporaryIngestProcessor creates a temporary ingest processor which is a new version of the ingest processor,
// which uses `quesma_api.BackendConnector` instead of `*sql.DB` for the database connection.
func (p *ElasticsearchToClickHouseIngestProcessor) prepareTemporaryIngestProcessor(connector quesma_api.BackendConnector, indexName string) *ingest.IngestProcessor2 {
	u, _ := url.Parse("http://localhost:9200")

	elasticsearchConfig := config.ElasticsearchConfiguration{
		Url: (*config.Url)(u),
	}
	emptyConfig := &config.QuesmaConfiguration{
		IndexConfig: map[string]config.IndexConfiguration{
			indexName: {
				Name: indexName,
			},
		},
	}

	virtualTableStorage := persistence.NewElasticJSONDatabase(elasticsearchConfig, common_table.VirtualTableElasticIndexName)
	tableDisco := clickhouse.NewTableDiscovery2(emptyConfig, connector, virtualTableStorage)
	schemaRegistry := schema.NewSchemaRegistry(clickhouse.TableDiscoveryTableProviderAdapter{TableDiscovery: tableDisco}, emptyConfig, clickhouse.SchemaTypeAdapter{})

	v2TableResolver := NewNextGenTableResolver()

	ip := ingest.NewIngestProcessor2(emptyConfig, connector, nil, tableDisco, schemaRegistry, virtualTableStorage, v2TableResolver)
	ip.Start()
	return ip
}

func ReadRespBody(resp *http.Response) ([]byte, error) {
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	resp.Body = io.NopCloser(bytes.NewBuffer(respBody))
	return respBody, nil
}

func (p *ElasticsearchToClickHouseIngestProcessor) Handle(metadata map[string]interface{}, message ...any) (map[string]interface{}, any, error) {
	var data []byte

	indexName := metadata[IngestTargetKey].(string)
	if indexName == "" {
		panic("NO INDEX NAME?!?!?")
	}
	chBackendConn := p.GetBackendConnector(quesma_api.ClickHouseSQLBackend)
	if chBackendConn == nil {
		fmt.Println("Backend connector not found")
		return metadata, data, nil
	}

	tempIngestProcessor := p.prepareTemporaryIngestProcessor(chBackendConn, indexName)

	esBackendConn := p.GetBackendConnector(quesma_api.ElasticsearchBackend)
	if esBackendConn == nil {
		fmt.Println("Backend connector not found")
		return metadata, data, nil
	}
	_, ok := esBackendConn.(*backend_connectors.ElasticsearchBackendConnector)
	if !ok {
		panic(" !!! ")
	}

	for _, m := range message {
		/* // TODO okay just sending the req works
		messageAsReq, err := quesma_api.CheckedCast[*http.Request](m)
		if err != nil {
			panic("ElasticsearchToClickHouseIngestProcessor: invalid message type")
		}
		resp := es.Send(messageAsReq)
		respBody, err := ReadRespBody(resp)
		if err != nil {
			println(err)
		}
		return metadata, respBody, nil
		*/
		bodyAsBytes, err := quesma_api.CheckedCast[[]byte](m)
		if err != nil {
			panic("ElasticsearchToClickHouseIngestProcessor: invalid message type")
		}

		switch metadata[IngestAction] {
		case DocIndexAction:
			payloadJson, err := types.ExpectJSON(types.ParseRequestBody(string(bodyAsBytes)))
			if err != nil {
				println(err)
			}
			result, err := handleDocIndex(payloadJson, indexName, tempIngestProcessor)
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
			results, err := handleBulkIndex(payloadNDJson, indexName, tempIngestProcessor)
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
	return []quesma_api.BackendConnectorType{quesma_api.ClickHouseSQLBackend}
}
