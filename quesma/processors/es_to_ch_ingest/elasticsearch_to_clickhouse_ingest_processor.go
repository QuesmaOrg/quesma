// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package es_to_ch_ingest

import (
	"bytes"
	"context"
	"fmt"
	"github.com/QuesmaOrg/quesma/quesma/backend_connectors"
	"github.com/QuesmaOrg/quesma/quesma/frontend_connectors"
	"github.com/QuesmaOrg/quesma/quesma/ingest"
	"github.com/QuesmaOrg/quesma/quesma/logger"
	quesma_api "github.com/QuesmaOrg/quesma/quesma/v2/core"

	"github.com/QuesmaOrg/quesma/quesma/processors"
	"github.com/QuesmaOrg/quesma/quesma/processors/es_to_ch_common"
	quesm "github.com/QuesmaOrg/quesma/quesma/quesma"
	"github.com/QuesmaOrg/quesma/quesma/quesma/config"
	"github.com/QuesmaOrg/quesma/quesma/quesma/types"
	"github.com/QuesmaOrg/quesma/quesma/schema"
	"github.com/rs/zerolog/log"
	"io"
	"net/http"
)

type ElasticsearchToClickHouseIngestProcessor struct {
	processors.BaseProcessor
	config                config.QuesmaProcessorConfig
	legacyIngestProcessor *ingest.IngestProcessor
	legacyDependencies    *es_to_ch_common.LegacyQuesmaDependencies
}

func NewElasticsearchToClickHouseIngestProcessor(conf config.QuesmaProcessorConfig, legacyDependencies *es_to_ch_common.LegacyQuesmaDependencies) *ElasticsearchToClickHouseIngestProcessor {
	return &ElasticsearchToClickHouseIngestProcessor{
		BaseProcessor:      processors.NewBaseProcessor(),
		config:             conf,
		legacyDependencies: legacyDependencies,
	}
}

func (p *ElasticsearchToClickHouseIngestProcessor) InstanceName() string {
	return "elasticsearch_to_clickhouse_ingest" // TODO return name from config
}

func (p *ElasticsearchToClickHouseIngestProcessor) Init() error {
	// TODO so we initialize the connection pool in `prepareTemporaryIngestProcessor`, maybe we should do it here?
	p.legacyIngestProcessor = p.prepareTemporaryIngestProcessor()

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
func (p *ElasticsearchToClickHouseIngestProcessor) prepareTemporaryIngestProcessor() *ingest.IngestProcessor {

	ip := ingest.NewIngestProcessor(
		p.legacyDependencies.OldQuesmaConfig,
		p.legacyDependencies.ConnectionPool,
		p.legacyDependencies.PhoneHomeAgent(),
		p.legacyDependencies.TableDiscovery,
		p.legacyDependencies.SchemaRegistry,
		&p.legacyDependencies.VirtualTableStorage,
		p.legacyDependencies.TableResolver,
	)

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
		ingestStats := true

		ctx := context.Background()
		esConn, err := p.getElasticsearchBackendConnector()
		if err != nil {
			return metadata, nil, fmt.Errorf("failed to fetch Elasticsearch backend connector")
		}
		switch metadata[es_to_ch_common.PathPattern] {
		case es_to_ch_common.IndexDocPath:
			logger.Info().Msgf("_DOC CALLED: %s", indexPatterFromRequestUri)
			payloadJson, err := types.ExpectJSON(types.ParseRequestBody(string(reqBodyBytes)))
			if err != nil {
				return metadata, nil, err
			}
			res, err := quesm.HandleIndexDoc(ctx, indexPatterFromRequestUri, payloadJson, p.legacyIngestProcessor, ingestStats, esConn, p.legacyDependencies, p.legacyIngestProcessor.GetTableResolver())
			if err != nil {
				return metadata, nil, err
			}
			return metadata, res, nil
		case es_to_ch_common.IndexBulkPath:
			logger.Info().Msgf("BULK CALLED: %s", indexPatterFromRequestUri)
			if _, present := p.config.IndexConfig[indexPatterFromRequestUri]; !present {
				// TODO PERHAPS WE SHOULD SUPPORT IT
				// but index:/_bulk calls to .kibana internal indices are more complex
				logger.Info().Msgf("BULK CALLED: %s AND RIGHT AWAY PASS TO ELASTIC", indexPatterFromRequestUri)
				return p.routeToElasticsearch(metadata, req)
			}
			payloadNDJson, err := types.ExpectNDJSON(types.ParseRequestBody(string(reqBodyBytes)))
			if err != nil {
				return metadata, nil, err
			}
			res, err := quesm.HandleBulkIndex(ctx, indexPatterFromRequestUri, payloadNDJson, p.legacyIngestProcessor, ingestStats, esConn, p.legacyDependencies, p.legacyIngestProcessor.GetTableResolver())
			if err != nil {
				return metadata, nil, err
			}
			return metadata, res, nil
		case es_to_ch_common.BulkPath:
			logger.Info().Msgf("BULK CALLED ( just /_bulk ) ")
			payloadNDJson, err := types.ExpectNDJSON(types.ParseRequestBody(string(reqBodyBytes)))
			if err != nil {
				return metadata, nil, err
			}
			res, err := quesm.HandleBulk(ctx, payloadNDJson, p.legacyIngestProcessor, ingestStats, esConn, p.legacyDependencies, p.legacyIngestProcessor.GetTableResolver())
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
	resp, err := esConn.Send(req)
	if err != nil {
		return metadata, nil, err
	}
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
