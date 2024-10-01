// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package config

import (
	"errors"
	"fmt"
	"github.com/hashicorp/go-multierror"
	"github.com/knadh/koanf/providers/env"
	"github.com/rs/zerolog"
	"log"
	"quesma/network"
	"reflect"
	"slices"
	"strings"
)

var DefaultLogLevel = zerolog.InfoLevel

const (
	ElasticsearchFrontendQueryConnectorName  = "elasticsearch-fe-query"
	ElasticsearchFrontendIngestConnectorName = "elasticsearch-fe-ingest"

	ElasticsearchBackendConnectorName = "elasticsearch"
	ClickHouseOSBackendConnectorName  = "clickhouse-os"
	ClickHouseBackendConnectorName    = "clickhouse"
	HydrolixBackendConnectorName      = "hydrolix"

	ElasticABOptimizerName = "elastic_ab_testing"
)

type ProcessorType string

const (
	QuesmaV1ProcessorNoOp   ProcessorType = "quesma-v1-processor-noop"
	QuesmaV1ProcessorQuery  ProcessorType = "quesma-v1-processor-query"
	QuesmaV1ProcessorIngest ProcessorType = "quesma-v1-processor-ingest"
)

type QuesmaNewConfiguration struct {
	BackendConnectors          []BackendConnector   `koanf:"backendConnectors"`
	FrontendConnectors         []FrontendConnector  `koanf:"frontendConnectors"`
	InstallationId             string               `koanf:"installationId"`
	LicenseKey                 string               `koanf:"licenseKey"`
	Logging                    LoggingConfiguration `koanf:"logging"`
	IngestStatistics           bool                 `koanf:"ingestStatistics"`
	QuesmaInternalTelemetryUrl *Url                 `koanf:"internalTelemetryUrl"`
	Processors                 []Processor          `koanf:"processors"`
	Pipelines                  []Pipeline           `koanf:"pipelines"`
	DisableTelemetry           bool                 `koanf:"disableTelemetry"`
}

type Pipeline struct {
	Name               string   `koanf:"name"`
	FrontendConnectors []string `koanf:"frontendConnectors"`
	Processors         []string `koanf:"processors"`
	BackendConnectors  []string `koanf:"backendConnectors"`
}

type FrontendConnector struct {
	Name   string                         `koanf:"name"`
	Type   string                         `koanf:"type"`
	Config FrontendConnectorConfiguration `koanf:"config"`
}

type FrontendConnectorConfiguration struct {
	ListenPort  network.Port `koanf:"listenPort"`
	DisableAuth bool         `koanf:"disableAuth"`
}

type BackendConnector struct {
	Name   string                    `koanf:"name"`
	Type   string                    `koanf:"type"`
	Config RelationalDbConfiguration `koanf:"config"`
}

type Processor struct {
	Name   string                `koanf:"name"`
	Type   ProcessorType         `koanf:"type"`
	Config QuesmaProcessorConfig `koanf:"config"`
}

var (
	DefaultIngestTarget = []string{ElasticsearchTarget}
	DefaultQueryTarget  = []string{ElasticsearchTarget}
)

// An index configuration under this name in IndexConfig
// specifies the default configuration for all (non-configured) indexes
const DefaultWildcardIndexName = "*"

// Configuration of QuesmaV1ProcessorQuery and QuesmaV1ProcessorIngest
type QuesmaProcessorConfig struct {
	IndexConfig map[string]IndexConfiguration `koanf:"indexes"`
}

func LoadV2Config() QuesmaNewConfiguration {
	var v2config QuesmaNewConfiguration
	v2config.QuesmaInternalTelemetryUrl = telemetryUrl
	v2config.Logging.RemoteLogDrainUrl = telemetryUrl

	loadConfigFile()
	if err := k.Load(env.Provider("QUESMA_", ".", func(s string) string {
		// This enables overriding config values with environment variables. It's case-sensitive, just like the YAML.
		// Examples:
		// `QUESMA_logging_level=debug` overrides `logging.level` in the config file
		// `QUESMA_licenseKey=arbitrary-license-key` overrides `licenseKey` in the config file
		return strings.Replace(strings.TrimPrefix(s, "QUESMA_"), "_", ".", -1)
	}), nil); err != nil {
		log.Fatalf("error loading config form supplied env vars: %v", err)
	}
	if err := k.Unmarshal("", &v2config); err != nil {
		log.Fatalf("error unmarshalling config: %v", err)
	}
	if err := v2config.Validate(); err != nil {
		log.Fatalf("Config validation failed: %v", err)
	}
	return v2config
}

// validate at this level verifies the basic assumptions behind pipelines/processors/connectors,
// many of which being just stubs for future impl
func (c *QuesmaNewConfiguration) Validate() error {
	var errAcc error
	for _, pipeline := range c.Pipelines {
		errAcc = multierror.Append(errAcc, c.validatePipeline(pipeline))
	}
	for _, fc := range c.FrontendConnectors {
		errAcc = multierror.Append(errAcc, c.validateFrontendConnector(fc))
	}
	errAcc = multierror.Append(errAcc, c.validateFrontendConnectors())
	for _, pr := range c.Processors {
		errAcc = multierror.Append(errAcc, c.validateProcessor(pr))
	}
	errAcc = multierror.Append(errAcc, c.validatePipelines())
	errAcc = multierror.Append(errAcc, c.validateBackendConnectors())

	var multiErr *multierror.Error
	if errors.As(errAcc, &multiErr) {
		if len(multiErr.Errors) > 0 {
			log.Fatalf("Config validation failed: %v", multiErr)
			return multiErr
		}
	}
	return nil
}

func (c *QuesmaNewConfiguration) getFrontendConnectorByName(name string) *FrontendConnector {
	for _, fc := range c.FrontendConnectors {
		if fc.Name == name {
			return &fc
		}
	}
	return nil
}

func (c *QuesmaNewConfiguration) validateFrontendConnectors() error {
	if len(c.FrontendConnectors) == 0 {
		return fmt.Errorf("no frontend connectors defined")
	}
	if len(c.FrontendConnectors) > 2 {
		return fmt.Errorf("only one or two frontend connectors are supported at this moment")
	}
	if len(c.FrontendConnectors) == 2 {
		if c.FrontendConnectors[0].Config.ListenPort != c.FrontendConnectors[1].Config.ListenPort {
			return fmt.Errorf("both frontend connectors must listen on the same port")
		}
	}
	return nil
}

func (c *QuesmaNewConfiguration) getPipelinesType() (isSinglePipeline, isDualPipeline bool) {
	isSinglePipeline, isDualPipeline = false, false
	if len(c.Pipelines) == 1 {
		isSinglePipeline = true
	}
	if len(c.Pipelines) == 2 {
		isDualPipeline = true
	}
	return isSinglePipeline, isDualPipeline
}

func (c *QuesmaNewConfiguration) validatePipelines() error {
	if len(c.Pipelines) == 0 {
		return fmt.Errorf("no pipelines defined, must define at least one")
	}
	if len(c.Pipelines) > 2 {
		return fmt.Errorf("only one or two pipelines are supported at this moment")
	}
	isSinglePipeline, isDualPipeline := c.getPipelinesType()

	if isSinglePipeline {
		// We plan to only support a case of a single pipeline for querying (this code validates this).
		// However, we haven't yet implemented the case of disabling ingest, so single pipeline case is not yet supported.
		fcName := c.Pipelines[0].FrontendConnectors[0]
		if fc := c.getFrontendConnectorByName(fcName); fc != nil {
			if fc.Type != ElasticsearchFrontendQueryConnectorName {
				return fmt.Errorf("single-pipeline Quesma can only be used for querying, but the frontend connector is not of query type")
			}
			proc := c.getProcessorByName(c.Pipelines[0].Processors[0])
			if proc == nil {
				return fmt.Errorf(fmt.Sprintf("processor named [%s] not found in configuration", c.Pipelines[0].Processors[0]))
			}
			declaredBackendConnectors := c.Pipelines[0].BackendConnectors
			if proc.Type == QuesmaV1ProcessorNoOp {
				if len(declaredBackendConnectors) != 1 {
					return fmt.Errorf("noop processor supports only one backend connector")
				}
				if conn := c.getBackendConnectorByName(declaredBackendConnectors[0]); conn.Type != ElasticsearchBackendConnectorName {
					return fmt.Errorf("noop processor can be connected only to elasticsearch backend connector")
				}
			} else if proc.Type == QuesmaV1ProcessorQuery {
				if len(declaredBackendConnectors) != 2 {
					return fmt.Errorf("query processor requires two backend connectors")
				}
				var backendConnectorTypes []string
				for _, con := range declaredBackendConnectors {
					backendConnectorTypes = append(backendConnectorTypes, c.getBackendConnectorByName(con).Type)
				}
				if !slices.Contains(backendConnectorTypes, ElasticsearchBackendConnectorName) {
					return fmt.Errorf("query processor requires having one elasticsearch backend connector")
				}
				if !slices.Contains(backendConnectorTypes, ClickHouseBackendConnectorName) &&
					!slices.Contains(backendConnectorTypes, ClickHouseOSBackendConnectorName) &&
					!slices.Contains(backendConnectorTypes, HydrolixBackendConnectorName) {
					return fmt.Errorf("query processor requires having one Clickhouse-compatible backend connector")
				}
				if _, found := proc.Config.IndexConfig[DefaultWildcardIndexName]; !found {
					return fmt.Errorf("the default index configuration (under the name '%s') must be defined in the query processor", DefaultWildcardIndexName)
				}
			} else {
				return fmt.Errorf("single pipeline Quesma can only be used for querying, but the processor is not of query type")
			}

		} else {
			return fmt.Errorf(fmt.Sprintf("frontend connector named [%s] referred in pipeline [%s] not found in configuration", fcName, c.Pipelines[0].Name))
		}
	}
	if isDualPipeline {
		fc1, fc2 := c.getFrontendConnectorByName(c.Pipelines[0].FrontendConnectors[0]), c.getFrontendConnectorByName(c.Pipelines[1].FrontendConnectors[0])
		if fc1 == nil {
			return fmt.Errorf(fmt.Sprintf("frontend connector named [%s] not found in configuration", c.Pipelines[0].FrontendConnectors[0]))
		}
		if fc2 == nil {
			return fmt.Errorf(fmt.Sprintf("frontend connector named [%s] not found in configuration", c.Pipelines[1].FrontendConnectors[0]))
		}
		if !((fc1.Type == ElasticsearchFrontendQueryConnectorName && fc2.Type == ElasticsearchFrontendIngestConnectorName) ||
			(fc2.Type == ElasticsearchFrontendQueryConnectorName && fc1.Type == ElasticsearchFrontendIngestConnectorName)) {
			return fmt.Errorf("when declaring two frontend connector types, one must be of query type and the other of ingest type")
		}
		var queryPipeline, ingestPipeline Pipeline
		if fc1.Type == ElasticsearchFrontendQueryConnectorName {
			queryPipeline, ingestPipeline = c.Pipelines[0], c.Pipelines[1]
		} else {
			queryPipeline, ingestPipeline = c.Pipelines[1], c.Pipelines[0]
		}
		ingestProcessor := c.getProcessorByName(ingestPipeline.Processors[0])
		if ingestProcessor == nil {
			return fmt.Errorf(fmt.Sprintf("ingest processor named [%s] not found in configuration", ingestPipeline.Processors[0]))
		}
		if ingestProcessor.Type != QuesmaV1ProcessorIngest && ingestProcessor.Type != QuesmaV1ProcessorNoOp {
			return fmt.Errorf("ingest pipeline must have ingest-type or noop processor")
		}
		for _, indexConf := range ingestProcessor.Config.IndexConfig {
			if len(indexConf.Optimizers) != 0 {
				return fmt.Errorf("configuration of index '%s' in '%s' processor cannot have any optimizers, this is only a feature of query processor", ingestPipeline.Processors[0], indexConf.Name)
			}
		}
		queryProcessor := c.getProcessorByName(queryPipeline.Processors[0])
		if queryProcessor == nil {
			return fmt.Errorf(fmt.Sprintf("query processor named [%s] not found in configuration", ingestPipeline.Processors[0]))
		}
		if (queryProcessor.Type == QuesmaV1ProcessorNoOp && ingestProcessor.Type != QuesmaV1ProcessorNoOp) ||
			(ingestProcessor.Type == QuesmaV1ProcessorNoOp && queryProcessor.Type != QuesmaV1ProcessorNoOp) {
			return fmt.Errorf("at this moment, noop processor is allowed only if used in both pipelines")
		}
		if queryProcessor.Type != QuesmaV1ProcessorQuery &&
			queryProcessor.Type != QuesmaV1ProcessorNoOp {
			return fmt.Errorf("query pipeline must have query or noop processor")
		}
		if !(queryProcessor.Type == QuesmaV1ProcessorNoOp) {
			if _, found := queryProcessor.Config.IndexConfig[DefaultWildcardIndexName]; !found {
				return fmt.Errorf("the default index configuration (under the name '%s') must be defined in the query processor", DefaultWildcardIndexName)
			}
			if _, found := ingestProcessor.Config.IndexConfig[DefaultWildcardIndexName]; !found {
				return fmt.Errorf("the default index configuration (under the name '%s') must be defined in the ingest processor", DefaultWildcardIndexName)
			}
			for indexName, queryIndexConf := range queryProcessor.Config.IndexConfig {
				// If an index is configured in both query and ingest processors,
				// they must have the same configuration
				ingestIndexConf, found := ingestProcessor.Config.IndexConfig[indexName]
				if !found {
					// Only defined in query processor
					continue
				}
				if queryIndexConf.Override != ingestIndexConf.Override {
					return fmt.Errorf("ingest and query processors must have the same configuration of 'override' for index '%s' due to current limitations", indexName)
				}
				if queryIndexConf.UseCommonTable != ingestIndexConf.UseCommonTable {
					return fmt.Errorf("ingest and query processors must have the same configuration of 'useCommonTable' for index '%s' due to current limitations", indexName)
				}
				if queryIndexConf.SchemaOverrides == nil || ingestIndexConf.SchemaOverrides == nil {
					if queryIndexConf.SchemaOverrides != ingestIndexConf.SchemaOverrides {
						return fmt.Errorf("ingest and query processors must have the same configuration of 'schemaOverrides' for index '%s' due to current limitations", indexName)
					}
				} else if !reflect.DeepEqual(*queryIndexConf.SchemaOverrides, *ingestIndexConf.SchemaOverrides) {
					return fmt.Errorf("ingest and query processors must have the same configuration of 'schemaOverrides' for index '%s' due to current limitations", indexName)
				}
			}
		}
	}
	return nil
}

func (c *QuesmaNewConfiguration) validateFrontendConnector(fc FrontendConnector) error {
	if fc.Type != ElasticsearchFrontendIngestConnectorName && fc.Type != ElasticsearchFrontendQueryConnectorName {
		return fmt.Errorf(fmt.Sprintf("frontend connector's [%s] type not recognized, only `%s` and `%s` are supported at this moment", fc.Name, ElasticsearchFrontendIngestConnectorName, ElasticsearchFrontendQueryConnectorName))
	}
	return nil
}

func (c *QuesmaNewConfiguration) definedFrontedConnectorNames() []string {
	var names []string
	for _, fc := range c.FrontendConnectors {
		names = append(names, fc.Name)
	}
	return names
}

func (c *QuesmaNewConfiguration) definedBackendConnectorNames() []string {
	var names []string
	for _, bc := range c.BackendConnectors {
		names = append(names, bc.Name)
	}
	return names
}

func (c *QuesmaNewConfiguration) definedProcessorNames() []string {
	var names []string
	for _, p := range c.Processors {
		names = append(names, p.Name)
	}
	return names
}

func (c *QuesmaNewConfiguration) validateProcessor(p Processor) error {
	if !slices.Contains(getAllowedProcessorTypes(), p.Type) {
		return fmt.Errorf("processor type not recognized, only `quesma-v1-processor-noop`, `quesma-v1-processor-query` and `quesma-v1-processor-ingest` are supported at this moment")
	}
	if p.Type == QuesmaV1ProcessorQuery || p.Type == QuesmaV1ProcessorIngest {
		for indexName, indexConfig := range p.Config.IndexConfig {
			if indexName != DefaultWildcardIndexName && strings.ContainsAny(indexName, "*,") {
				return fmt.Errorf("index name '%s' in processor configuration is an index pattern, not allowed", indexName)
			}
			if p.Type == QuesmaV1ProcessorQuery {
				if len(indexConfig.Target) != 1 && len(indexConfig.Target) != 2 {
					return fmt.Errorf("configuration of index %s must have one or two targets (query processor)", indexName)
				}
			} else {
				if len(indexConfig.Target) > 2 {
					return fmt.Errorf("configuration of index %s must have at most two targets (ingest processor)", indexName)
				}
			}

			for _, target := range indexConfig.Target {
				if c.getBackendConnectorByName(target) == nil {
					return fmt.Errorf("invalid target %s in configuration of index %s", target, indexName)
				}
			}
		}
	}
	return nil
}

func (c *QuesmaNewConfiguration) validatePipeline(pipeline Pipeline) error {
	var _, errAcc error
	if len(pipeline.FrontendConnectors) != 1 {
		errAcc = multierror.Append(errAcc, fmt.Errorf("pipeline must have exactly one frontend connector"))
	} else if len(pipeline.FrontendConnectors) == 0 {
		return multierror.Append(errAcc, fmt.Errorf("pipeline must have exactly one frontend connector, none defined"))
	}
	if !slices.Contains(c.definedFrontedConnectorNames(), pipeline.FrontendConnectors[0]) {
		errAcc = multierror.Append(errAcc, fmt.Errorf(fmt.Sprintf("frontend connector named %s referenced in %s not found in configuration", pipeline.FrontendConnectors[0], pipeline.Name)))
	}

	if len(pipeline.BackendConnectors) == 0 || len(pipeline.BackendConnectors) > 2 {
		return multierror.Append(errAcc, fmt.Errorf(fmt.Sprintf("pipeline must define exactly one or two backend connectors, %d defined", len(pipeline.BackendConnectors))))
	}
	if !slices.Contains(c.definedBackendConnectorNames(), pipeline.BackendConnectors[0]) {
		errAcc = multierror.Append(errAcc, fmt.Errorf(fmt.Sprintf("backend connector named %s referenced in %s not found in configuration", pipeline.BackendConnectors[0], pipeline.Name)))
	}
	if len(pipeline.BackendConnectors) == 2 {
		if !slices.Contains(c.definedBackendConnectorNames(), pipeline.BackendConnectors[1]) {
			errAcc = multierror.Append(errAcc, fmt.Errorf(fmt.Sprintf("backend connector named %s referenced in %s not found in configuration", pipeline.BackendConnectors[1], pipeline.Name)))
		}
	}

	if len(pipeline.Processors) != 1 {
		return multierror.Append(errAcc, fmt.Errorf(fmt.Sprintf("pipeline must have exactly one processor, [%s] has %d defined", pipeline.Name, len(pipeline.Processors))))
	}
	if !slices.Contains(c.definedProcessorNames(), pipeline.Processors[0]) {
		errAcc = multierror.Append(errAcc, fmt.Errorf(fmt.Sprintf("processor named %s referenced in %s not found in configuration", pipeline.Processors[0], pipeline.Name)))
	} else {
		onlyProcessorInPipeline := c.getProcessorByName(pipeline.Processors[0])
		if onlyProcessorInPipeline.Type == QuesmaV1ProcessorNoOp {
			if len(pipeline.BackendConnectors) != 1 {
				return multierror.Append(errAcc, fmt.Errorf(fmt.Sprintf("pipeline %s has a noop processor supports only one backend connector", pipeline.Name)))
			}
			if conn := c.getBackendConnectorByName(pipeline.BackendConnectors[0]); conn.Type != ElasticsearchBackendConnectorName {
				return multierror.Append(errAcc, fmt.Errorf(fmt.Sprintf("pipeline %s has a noop processor which can be connected only to elasticsearch backend connector", pipeline.Name)))
			}
		}
		if onlyProcessorInPipeline.Type == QuesmaV1ProcessorQuery || onlyProcessorInPipeline.Type == QuesmaV1ProcessorIngest {
			if len(pipeline.BackendConnectors) != 2 {
				return multierror.Append(errAcc, fmt.Errorf(fmt.Sprintf("pipeline %s has a processor of type %s which requires two backend connectors", pipeline.Name, onlyProcessorInPipeline.Type)))
			}
			bConn1, bConn2 := c.getBackendConnectorByName(pipeline.BackendConnectors[0]), c.getBackendConnectorByName(pipeline.BackendConnectors[1])
			if bConn1 == nil {
				return multierror.Append(errAcc, fmt.Errorf(fmt.Sprintf("backend connector named %s referenced in %s not found in configuration", pipeline.BackendConnectors[0], pipeline.Name)))
			}
			if bConn2 == nil {
				return multierror.Append(errAcc, fmt.Errorf(fmt.Sprintf("backend connector named %s referenced in %s not found in configuration", pipeline.BackendConnectors[1], pipeline.Name)))
			}
			backendConnTypes := []string{bConn1.Type, bConn2.Type}
			if !slices.Contains(backendConnTypes, ElasticsearchBackendConnectorName) {
				return multierror.Append(errAcc, fmt.Errorf(fmt.Sprintf("pipeline %s has a processor of type %s which requires having one elasticsearch backend connector", pipeline.Name, onlyProcessorInPipeline.Type)))
			}
			if !slices.Contains(backendConnTypes, ClickHouseBackendConnectorName) && !slices.Contains(backendConnTypes, ClickHouseOSBackendConnectorName) && !slices.Contains(backendConnTypes, HydrolixBackendConnectorName) {
				return multierror.Append(errAcc, fmt.Errorf(fmt.Sprintf("pipeline %s has a processor of type %s which requires having one Clickhouse-compatible backend connector", pipeline.Name, onlyProcessorInPipeline.Type)))
			}
		}
	}

	return errAcc
}

func (c *QuesmaNewConfiguration) getBackendConnectorByName(name string) *BackendConnector {
	for _, b := range c.BackendConnectors {
		if b.Name == name {
			return &b
		}
	}
	return nil
}

func (c *QuesmaNewConfiguration) getProcessorByName(name string) *Processor {
	for _, p := range c.Processors {
		if p.Name == name {
			return &p
		}
	}
	return nil
}

func (c *QuesmaNewConfiguration) TranslateToLegacyConfig() QuesmaConfiguration {
	var err, errAcc error
	var conf QuesmaConfiguration
	if conf.PublicTcpPort, err = c.getPublicTcpPort(); err != nil {
		errAcc = multierror.Append(errAcc, err)
	}
	if conf.Elasticsearch, err = c.getElasticsearchConfig(); err != nil {
		errAcc = multierror.Append(errAcc, err)
	}
	if !c.DisableTelemetry {
		conf.QuesmaInternalTelemetryUrl = telemetryUrl
		conf.Logging.RemoteLogDrainUrl = telemetryUrl
	}
	// This is perhaps a little oversimplification, **but** in case any of the FE connectors has auth disabled, we disable auth for the whole incomming traffic
	// After all, the "duality" of frontend connectors is still an architectural choice we tend to question
	for _, fConn := range c.FrontendConnectors {
		if fConn.Config.DisableAuth {
			conf.DisableAuth = true
		}
	}

	conf.Logging = c.Logging
	if conf.Logging.Level == nil {
		conf.Logging.Level = &DefaultLogLevel
	}

	conf.InstallationId = c.InstallationId
	conf.LicenseKey = c.LicenseKey

	conf.AutodiscoveryEnabled = false
	conf.Connectors = make(map[string]RelationalDbConfiguration)
	relDBConn, connType, relationalDBErr := c.getRelationalDBConf()

	isSinglePipeline, isDualPipeline := c.getPipelinesType()

	if isSinglePipeline {
		processor := c.getProcessorByName(c.Pipelines[0].Processors[0])
		procType := processor.Type
		if procType == QuesmaV1ProcessorNoOp {
			conf.TransparentProxy = true
		} else if procType == QuesmaV1ProcessorQuery {

			queryProcessor := processor

			// this a COPY-PASTE from the dual pipeline case, but we need to do it here as well
			// TODO refactor this to a separate function

			elasticBackendName := c.getElasticsearchBackendConnector().Name
			var relationalDBBackendName string
			if relationalDBBackend, _ := c.getRelationalDBBackendConnector(); relationalDBBackend != nil {
				relationalDBBackendName = relationalDBBackend.Name
			}

			conf.IndexConfig = make(map[string]IndexConfiguration)
			for indexName, indexConfig := range queryProcessor.Config.IndexConfig {
				processedConfig := indexConfig
				processedConfig.Name = indexName

				if slices.Contains(indexConfig.Target, elasticBackendName) {
					processedConfig.QueryTarget = append(processedConfig.QueryTarget, ElasticsearchTarget)
				}
				if slices.Contains(indexConfig.Target, relationalDBBackendName) {
					processedConfig.QueryTarget = append(processedConfig.QueryTarget, ClickhouseTarget)
				}

				if len(indexConfig.QueryTarget) == 2 && !(indexConfig.QueryTarget[0] == ClickhouseTarget && indexConfig.QueryTarget[1] == ElasticsearchTarget) {
					errAcc = multierror.Append(errAcc, fmt.Errorf("index %s has invalid dual query target configuration - when you specify two targets, ClickHouse has to be the primary one and Elastic has to be the secondary one", indexName))
					continue
				}
				if len(indexConfig.QueryTarget) == 2 {
					// Turn on A/B testing
					processedConfig.Optimizers = make(map[string]OptimizerConfiguration)
					processedConfig.Optimizers[ElasticABOptimizerName] = OptimizerConfiguration{
						Disabled:   false,
						Properties: map[string]string{},
					}
				}

				conf.IndexConfig[indexName] = processedConfig
			}

			// Handle default index configuration
			defaultConfig := conf.IndexConfig[DefaultWildcardIndexName]
			if !reflect.DeepEqual(defaultConfig.QueryTarget, []string{ElasticsearchTarget}) {
				errAcc = multierror.Append(errAcc, fmt.Errorf("the target configuration of default index ('%s') of query processor is not currently supported", DefaultWildcardIndexName))
			}
			delete(conf.IndexConfig, DefaultWildcardIndexName)
		} else {
			errAcc = multierror.Append(errAcc, fmt.Errorf("unsupported processor %s in single pipeline", procType))
		}
	}

	if isDualPipeline {
		fc1 := c.getFrontendConnectorByName(c.Pipelines[0].FrontendConnectors[0])
		var queryPipeline, ingestPipeline Pipeline
		if fc1.Type == ElasticsearchFrontendQueryConnectorName {
			queryPipeline, ingestPipeline = c.Pipelines[0], c.Pipelines[1]
		} else {
			queryPipeline, ingestPipeline = c.Pipelines[1], c.Pipelines[0]
		}
		queryProcessor, ingestProcessor := c.getProcessorByName(queryPipeline.Processors[0]), c.getProcessorByName(ingestPipeline.Processors[0])

		if queryProcessor.Type == QuesmaV1ProcessorNoOp && ingestProcessor.Type == QuesmaV1ProcessorNoOp {
			conf.TransparentProxy = true
			goto END
		}

		elasticBackendName := c.getElasticsearchBackendConnector().Name
		var relationalDBBackendName string
		if relationalDBBackend, _ := c.getRelationalDBBackendConnector(); relationalDBBackend != nil {
			relationalDBBackendName = relationalDBBackend.Name
		}

		conf.IndexConfig = make(map[string]IndexConfiguration)
		for indexName, indexConfig := range queryProcessor.Config.IndexConfig {
			processedConfig := indexConfig
			processedConfig.Name = indexName

			processedConfig.IngestTarget = DefaultIngestTarget

			if slices.Contains(indexConfig.Target, elasticBackendName) {
				processedConfig.QueryTarget = append(processedConfig.QueryTarget, ElasticsearchTarget)
			}
			if slices.Contains(indexConfig.Target, relationalDBBackendName) {
				processedConfig.QueryTarget = append(processedConfig.QueryTarget, ClickhouseTarget)
			}

			if len(indexConfig.QueryTarget) == 2 && !(indexConfig.QueryTarget[0] == ClickhouseTarget && indexConfig.QueryTarget[1] == ElasticsearchTarget) {
				errAcc = multierror.Append(errAcc, fmt.Errorf("index %s has invalid dual query target configuration - when you specify two targets, ClickHouse has to be the primary one and Elastic has to be the secondary one", indexName))
				continue
			}
			if len(indexConfig.QueryTarget) == 2 {
				// Turn on A/B testing
				processedConfig.Optimizers = make(map[string]OptimizerConfiguration)
				processedConfig.Optimizers[ElasticABOptimizerName] = OptimizerConfiguration{
					Disabled:   false,
					Properties: map[string]string{},
				}
			}

			conf.IndexConfig[indexName] = processedConfig
		}

		conf.EnableIngest = true
		conf.IngestStatistics = true

		for indexName, indexConfig := range ingestProcessor.Config.IndexConfig {
			processedConfig, found := conf.IndexConfig[indexName]
			if !found {
				// Index is only configured in ingest processor, not in query processor,
				// use the ingest processor's configuration as the base (similarly as in the previous loop)
				processedConfig = indexConfig
				processedConfig.Name = indexName
				processedConfig.QueryTarget = DefaultQueryTarget
			}

			processedConfig.IngestTarget = make([]string, 0) // reset previously set DefaultIngestTarget
			if slices.Contains(indexConfig.Target, elasticBackendName) {
				processedConfig.IngestTarget = append(processedConfig.IngestTarget, ElasticsearchTarget)
			}
			if slices.Contains(indexConfig.Target, relationalDBBackendName) {
				processedConfig.IngestTarget = append(processedConfig.IngestTarget, ClickhouseTarget)
			}

			conf.IndexConfig[indexName] = processedConfig
		}

		// Handle default index configuration
		defaultConfig := conf.IndexConfig[DefaultWildcardIndexName]
		if !reflect.DeepEqual(defaultConfig.QueryTarget, []string{ElasticsearchTarget}) {
			errAcc = multierror.Append(errAcc, fmt.Errorf("the target configuration of default index ('%s') of query processor is not currently supported", DefaultWildcardIndexName))
		}
		if !reflect.DeepEqual(defaultConfig.IngestTarget, []string{ElasticsearchTarget}) {
			errAcc = multierror.Append(errAcc, fmt.Errorf("the target configuration of default index ('%s') of ingest processor is not currently supported", DefaultWildcardIndexName))
		}
		delete(conf.IndexConfig, DefaultWildcardIndexName)
	}

END:

	if relationalDBErr != nil && !conf.TransparentProxy {
		errAcc = multierror.Append(errAcc, relationalDBErr)
	} else if relationalDBErr != nil && conf.TransparentProxy {
		relDBConn := RelationalDbConfiguration{
			ConnectorType: ClickHouseOSBackendConnectorName,
			Url: &Url{
				Host: "localhost",
			},
		}
		conf.Connectors["mock-for-transparent-proxy"] = relDBConn
		conf.ClickHouse = relDBConn
	} else {
		relDBConn.ConnectorType = connType
		if connType == HydrolixBackendConnectorName {
			conf.Connectors["injected-hydrolix-connector"] = *relDBConn
			conf.Hydrolix = *relDBConn
		} else {
			conf.Connectors["injected-clickhouse-connector"] = *relDBConn
			conf.ClickHouse = *relDBConn
		}
	}

	if errAcc != nil {
		var multiErr *multierror.Error
		if errors.As(errAcc, &multiErr) {
			if len(multiErr.Errors) > 0 {
				log.Fatalf("Internal config rewrite failed: %v", multiErr)
			}
		}
	}
	return conf
}

func (c *QuesmaNewConfiguration) getPublicTcpPort() (network.Port, error) {
	// per validation, there's always at least one frontend connector,
	// even if there's a second one, it has to listen on the same port
	return c.FrontendConnectors[0].Config.ListenPort, nil
}

func (c *QuesmaNewConfiguration) getElasticsearchBackendConnector() *BackendConnector {
	for _, backendConn := range c.BackendConnectors {
		if backendConn.Type == ElasticsearchBackendConnectorName {
			return &backendConn
		}
	}
	return nil
}

func (c *QuesmaNewConfiguration) getRelationalDBBackendConnector() (*BackendConnector, string) {
	for _, backendConn := range c.BackendConnectors {
		if backendConn.Type == ClickHouseBackendConnectorName || backendConn.Type == ClickHouseOSBackendConnectorName || backendConn.Type == HydrolixBackendConnectorName {
			return &backendConn, backendConn.Type
		}
	}
	return nil, ""
}

func (c *QuesmaNewConfiguration) getElasticsearchConfig() (ElasticsearchConfiguration, error) {
	if esBackendConn := c.getElasticsearchBackendConnector(); esBackendConn != nil {
		return ElasticsearchConfiguration{
			Url:      esBackendConn.Config.Url,
			User:     esBackendConn.Config.User,
			Password: esBackendConn.Config.Password,
		}, nil
	}
	return ElasticsearchConfiguration{}, fmt.Errorf("elasticsearch backend connector must be configured")
}

func (c *QuesmaNewConfiguration) getRelationalDBConf() (*RelationalDbConfiguration, string, error) {
	if backendConn, typ := c.getRelationalDBBackendConnector(); backendConn != nil {
		return &backendConn.Config, typ, nil
	}
	return nil, "", fmt.Errorf("exactly one backend connector of type `clickhouse`, `clickhouse-os` or `hydrolix` must be configured")
}

func (c *QuesmaNewConfiguration) validateBackendConnectors() error {
	elasticBackendConnectors, clickhouseBackendConnectors := 0, 0
	for _, backendConn := range c.BackendConnectors {
		if backendConn.Type == ElasticsearchBackendConnectorName {
			elasticBackendConnectors += 1
		} else if backendConn.Type == ClickHouseBackendConnectorName || backendConn.Type == ClickHouseOSBackendConnectorName || backendConn.Type == HydrolixBackendConnectorName {
			clickhouseBackendConnectors += 1
		} else {
			return fmt.Errorf("backend connector type '%s' not recognized", backendConn.Type)
		}
	}
	if elasticBackendConnectors > 1 {
		return fmt.Errorf("only one elasticsearch backend connector is allowed, found %d many", elasticBackendConnectors)
	}
	if clickhouseBackendConnectors > 1 {
		return fmt.Errorf("only one clickhouse-compatible backend connector is allowed, found %d many", clickhouseBackendConnectors)
	}
	return nil
}

func getAllowedProcessorTypes() []ProcessorType {
	return []ProcessorType{QuesmaV1ProcessorNoOp, QuesmaV1ProcessorQuery, QuesmaV1ProcessorIngest}
}
