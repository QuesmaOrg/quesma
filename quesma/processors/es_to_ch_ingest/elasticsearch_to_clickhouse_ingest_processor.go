// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package es_to_ch_ingest

import (
	"bytes"
	"context"
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

type ElasticsearchToClickHouseIngestProcessor struct {
	processors.BaseProcessor
	config                config.QuesmaProcessorConfig
	legacyIngestProcessor *ingest.IngestProcessor
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

	// TODO so we initialize the connection pool in `prepareTemporaryIngestProcessor`, maybe we should do it here?
	p.legacyIngestProcessor = p.prepareTemporaryIngestProcessor(chBackendConnector, *esBackendConnectorCasted)

	return nil
}

func (p *ElasticsearchToClickHouseIngestProcessor) getElasticsearchBackendConnector() (*backend_connectors.ElasticsearchBackendConnector, error) {
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

func (p *ElasticsearchToClickHouseIngestProcessor) GetId() string {
	return "elasticsearch_to_clickhouse_ingest"
}

func (p *ElasticsearchToClickHouseIngestProcessor) GetSchemaRegistry() schema.Registry {
	return p.legacyIngestProcessor.GetSchemaRegistry()
}

// prepareTemporaryIngestProcessor creates a temporary ingest processor which is a new version of the ingest processor,
// which uses `quesma_api.BackendConnector` instead of `*sql.DB` for the database connection.
func (p *ElasticsearchToClickHouseIngestProcessor) prepareTemporaryIngestProcessor(_ quesma_api.BackendConnector, esBackendConn backend_connectors.ElasticsearchBackendConnector) *ingest.IngestProcessor {

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

	dummyTableResolver := table_resolver.NewDummyTableResolver()

	ip := ingest.NewIngestProcessor(oldQuesmaConfig, connectionPool, nil, tableDisco, schemaRegistry, virtualTableStorage, dummyTableResolver)

	ip.Start()
	return ip
}

func (p *ElasticsearchToClickHouseIngestProcessor) Handle(metadata map[string]interface{}, message ...any) (map[string]interface{}, any, error) {
	var data []byte

	for _, m := range message {
		req, err := quesma_api.CheckedCast[*http.Request](m)
		if err != nil {
			fmt.Printf("ElasticsearchToClickHouseIngestProcessor: invalid message type: %v", err)
			return nil, data, err
		}

		var pathPattern string
		if val, ok := metadata[es_to_ch_common.PathPattern]; ok {
			pathPattern = val.(string)
		}
		indexPatterFromRequestUri := es_to_ch_common.GetParamFromRequestURI(req, pathPattern, "index")

		if _, present := p.config.IndexConfig[indexPatterFromRequestUri]; !present && pathPattern == es_to_ch_common.IndexDocPath {
			// `_doc` at this point can go directly to Elasticsearch,
			// `_bulk` request might be still sent to ClickHouse as the req payload may contain documents targeting CH tables
			return p.routeToElasticsearch(metadata, req)
		}

		var reqBodyBytes []byte
		if reqBodyBytes, err = frontend_connectors.ReadRequestBody(req); err != nil {
			panic("ElasticsearchToClickHouseIngestProcessor: invalid message type")
		}

		// TODO this comes from Quesma general config and should be passed to the processor
		ingestStats := false

		ctx := context.Background()
		esConn, err := p.getElasticsearchBackendConnector()
		if err != nil {
			return metadata, nil, fmt.Errorf("failed to fetch Elasticsearch backend connector")
		}
		switch metadata[es_to_ch_common.PathPattern] {
		case es_to_ch_common.IndexDocPath:
			payloadJson, err := types.ExpectJSON(types.ParseRequestBody(string(reqBodyBytes)))
			if err != nil {
				return metadata, nil, err
			}
			res, err := quesm.HandleIndexDoc(ctx, indexPatterFromRequestUri, payloadJson, p.legacyIngestProcessor, ingestStats, esConn, nil, p.legacyIngestProcessor.GetTableResolver())
			if err != nil {
				return metadata, nil, err
			}
			return metadata, res, nil
		case es_to_ch_common.IndexBulkPath:
			payloadNDJson, err := types.ExpectNDJSON(types.ParseRequestBody(string(reqBodyBytes)))
			if err != nil {
				return metadata, nil, err
			}
			res, err := quesm.HandleBulkIndex(ctx, indexPatterFromRequestUri, payloadNDJson, p.legacyIngestProcessor, ingestStats, esConn, nil, p.legacyIngestProcessor.GetTableResolver())
			if err != nil {
				return metadata, nil, err
			}
			return metadata, res, nil
		case es_to_ch_common.BulkPath:
			payloadNDJson, err := types.ExpectNDJSON(types.ParseRequestBody(string(reqBodyBytes)))
			if err != nil {

			}
			res, err := quesm.HandleBulk(ctx, payloadNDJson, p.legacyIngestProcessor, ingestStats, esConn, nil, p.legacyIngestProcessor.GetTableResolver())
			if err != nil {
				return metadata, nil, err
			}
			return metadata, res, nil
		case es_to_ch_common.IndexMappingPath:
			payloadJson, err := types.ExpectJSON(types.ParseRequestBody(string(reqBodyBytes)))
			if err != nil {
				return metadata, nil, err
			}
			res, err := quesm.HandlePutIndex(indexPatterFromRequestUri, payloadJson, p.GetSchemaRegistry())
			if err != nil {
				return metadata, nil, err
			}
			return metadata, res, nil
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

func (p *ElasticsearchToClickHouseIngestProcessor) routeToElasticsearch(metadata map[string]interface{}, req *http.Request) (map[string]interface{}, *quesma_api.Result, error) {
	metadata[es_to_ch_common.RealSourceHeader] = es_to_ch_common.RealSourceElasticsearch
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
