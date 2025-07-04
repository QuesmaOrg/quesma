// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package main

import (
	"github.com/QuesmaOrg/quesma/platform/backend_connectors"
	"github.com/QuesmaOrg/quesma/platform/config"
	"github.com/QuesmaOrg/quesma/platform/frontend_connectors"
	"github.com/QuesmaOrg/quesma/platform/licensing"
	"github.com/QuesmaOrg/quesma/platform/logger"
	"github.com/QuesmaOrg/quesma/platform/processors/es_to_ch_common"
	"github.com/QuesmaOrg/quesma/platform/processors/es_to_ch_ingest"
	"github.com/QuesmaOrg/quesma/platform/processors/es_to_ch_query"
	quesma_api "github.com/QuesmaOrg/quesma/platform/v2/core"
	"log"
	"os"
	"os/signal"
	"syscall"
)

// BuildNewQuesma creates a new quesma instance with both Ingest And Query Processors, unused yet
func BuildNewQuesma() quesma_api.QuesmaBuilder {

	var newConfiguration, configErr = config.LoadV2Config()
	if configErr != nil {
		os.Exit(0) // We log error in LoadV2Config, likely replace with returning an error
	}
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

	logger.Info().Msgf("loaded config: %s", cfg.String())

	var legacyDependencies *es_to_ch_common.LegacyQuesmaDependencies
	if cfg.ClickHouse.ConnectorType == "doris" {
		legacyDependencies = es_to_ch_common.InitializeLegacyDorisQuesmaDependencies(deps, &cfg, logChan)
	} else {
		legacyDependencies = es_to_ch_common.InitializeLegacyQuesmaDependencies(deps, &cfg, logChan)
	}

	return buildQuesmaFromV2Config(newConfiguration, legacyDependencies)
}

func buildQuesmaFromV2Config(cfg config.QuesmaNewConfiguration, deps *es_to_ch_common.LegacyQuesmaDependencies) quesma_api.QuesmaBuilder {

	var quesmaBuilder quesma_api.QuesmaBuilder = quesma_api.NewQuesma(deps)

	for _, p := range cfg.Pipelines {
		var pipeline quesma_api.PipelineBuilder = quesma_api.NewNamedPipeline(p.Name)
		for _, fcName := range p.FrontendConnectors {
			fc := cfg.GetFrontendConnectorByName(fcName)
			switch fc.Type {
			case config.ElasticsearchFrontendQueryConnectorName:
				pipeline.AddFrontendConnector(frontend_connectors.NewElasticsearchQueryFrontendConnector(":"+fc.Config.ListenPort.String(), deps.OldQuesmaConfig.Elasticsearch, fc.Config.DisableAuth))
			case config.ElasticsearchFrontendIngestConnectorName:
				pipeline.AddFrontendConnector(frontend_connectors.NewElasticsearchIngestFrontendConnector(":"+fc.Config.ListenPort.String(), deps.OldQuesmaConfig.Elasticsearch, fc.Config.DisableAuth))
			default:
				log.Fatalf("unknown frontend connector type: %s", fc.Type)
			}
		}
		for _, procName := range p.Processors {
			proc := cfg.GetProcessorByName(procName)
			switch proc.Type {
			case config.QuesmaV1ProcessorQuery:
				pipeline.AddProcessor(es_to_ch_query.NewElasticsearchToClickHouseQueryProcessor(proc.Config, deps))
			case config.QuesmaV1ProcessorIngest:
				pipeline.AddProcessor(es_to_ch_ingest.NewElasticsearchToClickHouseIngestProcessor(proc.Config, deps))
			default:
				log.Fatalf("unknown processor type: %s", proc.Type)
			}
		}
		for _, bcName := range p.BackendConnectors {
			bc := cfg.GetBackendConnectorByName(bcName)
			switch bc.Type {
			case config.ClickHouseOSBackendConnectorName:
				connectorDeclaration := cfg.GetBackendConnectorByType(config.ClickHouseOSBackendConnectorName)
				backendConnector := backend_connectors.NewClickHouseBackendConnector(&connectorDeclaration.Config)
				pipeline.AddBackendConnector(backendConnector)
			case config.ClickHouseBackendConnectorName:
				connectorDeclaration := cfg.GetBackendConnectorByType(config.ClickHouseBackendConnectorName)
				backendConnector := backend_connectors.NewClickHouseBackendConnector(&connectorDeclaration.Config)
				pipeline.AddBackendConnector(backendConnector)
			case config.HydrolixBackendConnectorName:
				connectorDeclaration := cfg.GetBackendConnectorByType(config.HydrolixBackendConnectorName)
				backendConnector := backend_connectors.NewClickHouseBackendConnector(&connectorDeclaration.Config)
				pipeline.AddBackendConnector(backendConnector)
			case config.ElasticsearchBackendConnectorName:
				connectorDeclaration := cfg.GetBackendConnectorByType(config.ElasticsearchBackendConnectorName)
				backendConnector := backend_connectors.NewElasticsearchBackendConnectorFromDbConfig(connectorDeclaration.Config)
				pipeline.AddBackendConnector(backendConnector)
			case config.DorisBackendConnectorName:
				connectorDeclaration := cfg.GetBackendConnectorByType(config.DorisBackendConnectorName)
				backendConnector := backend_connectors.NewDorisBackendConnector(&connectorDeclaration.Config)
				pipeline.AddBackendConnector(backendConnector)
			default:
				log.Fatalf("unknown backend connector type: %s", bc.Type)
			}
		}
		quesmaBuilder.AddPipeline(pipeline)

	}
	quesmaInstance, err := quesmaBuilder.Build()
	if err != nil {
		log.Fatalf("error building quesma instance: %v", err)
	}
	return quesmaInstance
}
