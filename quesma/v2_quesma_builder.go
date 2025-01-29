// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package main

import (
	"github.com/QuesmaOrg/quesma/quesma/backend_connectors"
	"github.com/QuesmaOrg/quesma/quesma/frontend_connectors"
	"github.com/QuesmaOrg/quesma/quesma/licensing"
	"github.com/QuesmaOrg/quesma/quesma/logger"
	"github.com/QuesmaOrg/quesma/quesma/processors/es_to_ch_common"
	"github.com/QuesmaOrg/quesma/quesma/processors/es_to_ch_ingest"
	"github.com/QuesmaOrg/quesma/quesma/processors/es_to_ch_query"
	"github.com/QuesmaOrg/quesma/quesma/quesma/config"
	quesma_api "github.com/QuesmaOrg/quesma/quesma/v2/core"
	"log"
	"os"
	"os/signal"
	"syscall"
)

// BuildNewQuesma creates a new quesma instance with both Ingest And Query Processors, unused yet
func BuildNewQuesma() quesma_api.QuesmaBuilder {

	var newConfiguration = config.LoadV2Config()
	var cfg = newConfiguration.TranslateToLegacyConfig()

	if err := cfg.Validate(); err != nil {
		log.Fatalf("error validating configuration: %v", err)
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	doneCh := make(chan struct{})
	licenseMod := licensing.Init(&cfg)
	logChan := logger.InitLogger(logger.Configuration{
		FileLogging:       cfg.Logging.FileLogging,
		Path:              cfg.Logging.Path,
		RemoteLogDrainUrl: cfg.Logging.RemoteLogDrainUrl.ToUrl(),
		Level:             *cfg.Logging.Level,
		ClientId:          licenseMod.License.ClientID,
	}, sig, doneCh)

	deps := quesma_api.EmptyDependencies()

	legacyDependencies := es_to_ch_common.InitializeLegacyQuesmaDependencies(deps, &cfg, logChan)

	var quesmaBuilder quesma_api.QuesmaBuilder = quesma_api.NewQuesma(legacyDependencies)

	queryConn := newConfiguration.GetFrontendConnectorByType(config.ElasticsearchFrontendQueryConnectorName)

	queryFrontendConnector := frontend_connectors.NewElasticsearchQueryFrontendConnector(":"+queryConn.Config.ListenPort.String(), cfg.Elasticsearch, queryConn.Config.DisableAuth)

	var queryPipeline quesma_api.PipelineBuilder = quesma_api.NewPipeline()
	queryPipeline.AddFrontendConnector(queryFrontendConnector)

	queryProc := newConfiguration.GetProcessorByType(config.QuesmaV1ProcessorQuery)

	queryProcessor := es_to_ch_query.NewElasticsearchToClickHouseQueryProcessor(queryProc.Config, legacyDependencies)

	ingestConn := newConfiguration.GetFrontendConnectorByType(config.ElasticsearchFrontendIngestConnectorName)

	ingestFrontendConnector := frontend_connectors.NewElasticsearchIngestFrontendConnector(":"+queryConn.Config.ListenPort.String(), cfg.Elasticsearch, ingestConn.Config.DisableAuth)
	var ingestPipeline quesma_api.PipelineBuilder = quesma_api.NewPipeline()
	ingestPipeline.AddFrontendConnector(ingestFrontendConnector)

	ingestProc := newConfiguration.GetProcessorByType(config.QuesmaV1ProcessorIngest)
	ingestProcessor := es_to_ch_ingest.NewElasticsearchToClickHouseIngestProcessor(ingestProc.Config, legacyDependencies)
	ingestPipeline.AddProcessor(ingestProcessor)
	quesmaBuilder.AddPipeline(ingestPipeline)

	queryPipeline.AddProcessor(queryProcessor)
	quesmaBuilder.AddPipeline(queryPipeline)

	chBackendConn := newConfiguration.GetBackendConnectorByType(config.ClickHouseOSBackendConnectorName)
	clickHouseBackendConnector := backend_connectors.NewClickHouseBackendConnector(&chBackendConn.Config)

	esBackendConn := newConfiguration.GetBackendConnectorByType(config.ElasticsearchBackendConnectorName)
	esCfg := esBackendConn.Config
	elasticsearchBackendConnector := backend_connectors.NewElasticsearchBackendConnector2(esCfg)

	queryPipeline.AddBackendConnector(clickHouseBackendConnector)
	queryPipeline.AddBackendConnector(elasticsearchBackendConnector)

	ingestPipeline.AddBackendConnector(clickHouseBackendConnector)
	ingestPipeline.AddBackendConnector(elasticsearchBackendConnector)

	quesmaInstance, err := quesmaBuilder.Build()
	if err != nil {
		log.Fatalf("error building quesma instance: %v", err)
	}
	return quesmaInstance
}

func buildQuesmaFromV2Config(cfg config.QuesmaNewConfiguration) quesma_api.QuesmaBuilder {
	//var cfg = config.LoadV2Config()

	var quesmaBuilder quesma_api.QuesmaBuilder = quesma_api.NewQuesma(nil)

	for _, p := range cfg.Pipelines {
		var pipeline quesma_api.PipelineBuilder = quesma_api.NewNamedPipeline(p.Name)
		for _, fcName := range p.FrontendConnectors {
			fc := cfg.GetFrontendConnectorByName(fcName)
			switch fc.Type {
			case config.ElasticsearchFrontendQueryConnectorName:
				pipeline.AddFrontendConnector(frontend_connectors.NewElasticsearchQueryFrontendConnector())
			case config.ElasticsearchFrontendIngestConnectorName:
				pipeline.AddFrontendConnector(frontend_connectors.NewElasticsearchIngestFrontendConnector())
			default:
				log.Fatalf("unknown frontend connector type: %s", fc.Type)
			}
		}
		for _, procName := range p.Processors {
			proc := cfg.GetProcessorByName(procName)
			switch proc.Type {
			case config.QuesmaV1ProcessorQuery:
				pipeline.AddProcessor(es_to_ch_query.NewElasticsearchToClickHouseQueryProcessor())
			case config.QuesmaV1ProcessorIngest:
				pipeline.AddProcessor(es_to_ch_ingest.NewElasticsearchToClickHouseIngestProcessor())
			default:
				log.Fatalf("unknown processor type: %s", proc.Type)
			}
		}
		for _, bcName := range p.BackendConnectors {
			bc := cfg.GetBackendConnectorByName(bcName)
			switch bc.Type {
			case config.ClickHouseOSBackendConnectorName:
				// TODO: implement
			}
		}
		quesmaBuilder.AddPipeline(pipeline)

	}

}
