// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package config

import (
	"errors"
	"fmt"
	"github.com/hashicorp/go-multierror"
	"github.com/knadh/koanf/providers/env"
	"log"
	"quesma/network"
	"reflect"
	"slices"
	"strings"
)

const (
	ElasticsearchFrontendQueryConnectorName  = "elasticsearch-fe-query"
	ElasticsearchFrontendIngestConnectorName = "elasticsearch-fe-ingest"

	ElasticsearchBackendConnectorName = "elasticsearch"
	ClickHouseOSBackendConnectorName  = "clickhouse-os"
	ClickHouseBackendConnectorName    = "clickhouse"
	HydrolixBackendConnectorName      = "hydrolix"
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
	ListenPort network.Port `koanf:"listenPort"`
}

type BackendConnector struct {
	Name   string                    `koanf:"name"`
	Type   string                    `koanf:"type"`
	Config RelationalDbConfiguration `koanf:"config"`
}

type Processor struct {
	Name   string              `koanf:"name"`
	Type   ProcessorType       `koanf:"type"`
	Config QuesmaConfiguration `koanf:"config"`
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
	if err := v2config.validate(); err != nil {
		var multiErr *multierror.Error
		if errors.As(err, &multiErr) {
			if len(multiErr.Errors) > 0 {
				log.Fatalf("Config validation failed: %v", multiErr)
			}
		}
	}
	return v2config
}

// validate at this level verifies the basic assumptions behind pipelines/processors/connectors,
// many of which being just stubs for future impl
func (c *QuesmaNewConfiguration) validate() error {
	var _, errAcc error
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

	return errAcc
}

// unsafe to use!
func (c *QuesmaNewConfiguration) getProcessorsConfiguredInPipelines() (processors []*Processor) {
	for _, p := range c.Pipelines {
		processors = append(processors, c.getProcessorByName(p.Processors[0]))
	}
	return processors
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
		return errors.New("no frontend connectors defined")
	}
	if len(c.FrontendConnectors) > 2 {
		return errors.New("only one or two frontend connectors are supported at this moment")
	}
	if len(c.FrontendConnectors) == 2 {
		if c.FrontendConnectors[0].Config.ListenPort != c.FrontendConnectors[1].Config.ListenPort {
			return errors.New("both frontend connectors must listen on the same port")
		}
	}
	return nil
}

func (c *QuesmaNewConfiguration) validatePipelines() error {
	var isSinglePipeline, isDualPipeline bool // currently only supported options
	if len(c.Pipelines) == 0 {
		return errors.New("no pipelines defined, must define at least one")
	}
	if len(c.Pipelines) == 1 {
		isSinglePipeline = true
	}
	if len(c.Pipelines) == 2 {
		isDualPipeline = true
	}
	if len(c.Pipelines) > 2 {
		return errors.New("only one or two pipelines are supported at this moment")
	}
	if isSinglePipeline { // for single pipelines we can support only querying
		if fc := c.getFrontendConnectorByName(c.Pipelines[0].FrontendConnectors[0]); fc != nil {
			if fc.Type != ElasticsearchFrontendQueryConnectorName {
				return errors.New("single pipeline Quesma can only be used for querying, but the frontend connector is not of query type")
			}
			proc := c.getProcessorByName(c.Pipelines[0].Processors[0])
			if proc.Type != QuesmaV1ProcessorQuery && proc.Type != QuesmaV1ProcessorNoOp {
				return errors.New("single pipeline Quesma can only be used for querying, but the processor is not of query type")
			}
			declaredBackendConnectors := c.Pipelines[0].BackendConnectors
			if proc.Type == QuesmaV1ProcessorNoOp {
				if len(declaredBackendConnectors) != 1 {
					return errors.New("noop processor supports only one backend connector")
				}
				if conn := c.getBackendConnectorByName(declaredBackendConnectors[0]); conn.Type != ElasticsearchBackendConnectorName {
					return errors.New("noop processor can be connected only to elasticsearch backend connector")
				}
				if proc.Type == QuesmaV1ProcessorQuery {
					if len(declaredBackendConnectors) != 2 {
						return errors.New("query processor requires two backend connectors")
					}
					var backendConnectorTypes []string
					for _, con := range declaredBackendConnectors {
						backendConnectorTypes = append(backendConnectorTypes, c.getBackendConnectorByName(con).Type)
					}
					if !slices.Contains(backendConnectorTypes, ElasticsearchBackendConnectorName) {
						return errors.New("query processor requires having one elasticsearch backend connector")
					}
					if !slices.Contains(backendConnectorTypes, ClickHouseBackendConnectorName) &&
						!slices.Contains(backendConnectorTypes, ClickHouseOSBackendConnectorName) &&
						!slices.Contains(backendConnectorTypes, HydrolixBackendConnectorName) {
						return errors.New("query processor requires having one Clickhouse-compatible backend connector")
					}
				}
			}
		} else {
			return errors.New(fmt.Sprintf("frontend connector named [%s] referred in piepeline[%s] not found in configuration", fc.Name, c.Pipelines[0].Name))
		}
	}
	if isDualPipeline {
		fc1, fc2 := c.getFrontendConnectorByName(c.Pipelines[0].FrontendConnectors[0]), c.getFrontendConnectorByName(c.Pipelines[1].FrontendConnectors[0])
		if !((fc1.Type == ElasticsearchFrontendQueryConnectorName && fc2.Type == ElasticsearchFrontendIngestConnectorName) ||
			(fc2.Type == ElasticsearchFrontendQueryConnectorName && fc1.Type == ElasticsearchFrontendIngestConnectorName)) {
			return errors.New("when declaring two fronted connector types, one must be of query type and the other of ingest type")
		}
		var queryPipeline, ingestPipeline Pipeline
		if fc1.Type == ElasticsearchFrontendQueryConnectorName {
			queryPipeline, ingestPipeline = c.Pipelines[0], c.Pipelines[1]
		} else {
			queryPipeline, ingestPipeline = c.Pipelines[1], c.Pipelines[0]
		}
		ingestProcessor := c.getProcessorByName(ingestPipeline.Processors[0])
		if ingestProcessor.Type != QuesmaV1ProcessorIngest {
			return errors.New("ingest pipeline must have ingest processor")
		}
		queryProcessor := c.getProcessorByName(queryPipeline.Processors[0])
		if queryProcessor.Type != QuesmaV1ProcessorQuery &&
			queryProcessor.Type != QuesmaV1ProcessorNoOp {
			return errors.New("query pipeline must have query or noop processor")
		}
		if !reflect.DeepEqual(ingestProcessor.Config, queryProcessor.Config) {
			return errors.New("ingest and query processors must have the same configuration due to current limitations")
		}
	}
	return nil
}

func (c *QuesmaNewConfiguration) validateFrontendConnector(fc FrontendConnector) error {
	if fc.Type != ElasticsearchFrontendIngestConnectorName && fc.Type != ElasticsearchFrontendQueryConnectorName {
		return errors.New(fmt.Sprintf("frontend connector's %s type not recognized, only `%s` and `%s` are supported at this moment", fc.Name, ElasticsearchFrontendIngestConnectorName, ElasticsearchFrontendQueryConnectorName))
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
		return errors.New("processor type not recognized, only `quesma-v1-processor-noop`, `quesma-v1-processor-query` and `quesma-v1-processor-ingest` are supported at this moment")
	}
	return nil
}

func (c *QuesmaNewConfiguration) validatePipeline(pipeline Pipeline) error {
	var _, errAcc error
	if len(pipeline.FrontendConnectors) != 1 {
		errAcc = multierror.Append(errAcc, errors.New("pipeline must have exactly one frontend connector"))
	} else if len(pipeline.FrontendConnectors) == 0 {
		return multierror.Append(errAcc, errors.New("pipeline must have exactly one frontend connector, none defined"))
	}
	if !slices.Contains(c.definedFrontedConnectorNames(), pipeline.FrontendConnectors[0]) {
		errAcc = multierror.Append(errAcc, errors.New(fmt.Sprintf("frontend connector named %s referenced in %s not found in configuration", pipeline.FrontendConnectors[0], pipeline.Name)))
	}
	if len(pipeline.BackendConnectors) != 0 && len(pipeline.BackendConnectors) > 2 {
		return multierror.Append(errAcc, errors.New(fmt.Sprintf("pipeline must define exactly one or two backend connectors, %d defined", len(pipeline.BackendConnectors))))
	}
	if !slices.Contains(c.definedBackendConnectorNames(), pipeline.BackendConnectors[0]) {
		errAcc = multierror.Append(errAcc, errors.New(fmt.Sprintf("backend connector named %s referenced in %s not found in configuration", pipeline.BackendConnectors[0], pipeline.Name)))
	}
	if len(pipeline.BackendConnectors) == 2 {
		if !slices.Contains(c.definedBackendConnectorNames(), pipeline.BackendConnectors[1]) {
			errAcc = multierror.Append(errAcc, errors.New(fmt.Sprintf("backend connector named %s referenced in %s not found in configuration", pipeline.BackendConnectors[1], pipeline.Name)))
		}
	}
	if len(pipeline.Processors) != 1 {
		return multierror.Append(errAcc, errors.New(fmt.Sprintf("pipeline must have exactly one processor, [%s] has %d defined", pipeline.Name, len(pipeline.Processors))))
	}
	if !slices.Contains(c.definedProcessorNames(), pipeline.Processors[0]) {
		errAcc = multierror.Append(errAcc, errors.New(fmt.Sprintf("processor named %s referenced in %s not found in configuration", pipeline.Processors[0], pipeline.Name)))
	} else {
		onlyProcessorInPipeline := c.getProcessorByName(pipeline.Processors[0])
		if onlyProcessorInPipeline.Type == QuesmaV1ProcessorNoOp {
			if len(pipeline.BackendConnectors) != 1 {
				return multierror.Append(errAcc, errors.New(fmt.Sprintf("pipeline %s has a noop processor supports only one backend connector", pipeline.Name)))
			}
			if conn := c.getBackendConnectorByName(pipeline.BackendConnectors[0]); conn.Type != ElasticsearchBackendConnectorName {
				return multierror.Append(errAcc, errors.New(fmt.Sprintf("pipeline %s has a noop processor which can be connected only to elasticsearch backend connector", pipeline.Name)))
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
	conf.Logging = c.Logging
	conf.InstallationId = c.InstallationId
	conf.LicenseKey = c.LicenseKey
	conf.IngestStatistics = c.IngestStatistics
	conf.Connectors = make(map[string]RelationalDbConfiguration)
	relDBConn, connType, err := c.getRelationalDBConf()
	if err != nil {
		errAcc = multierror.Append(errAcc, err)
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
	// Now determine Quesma final state with following heuristic
	// if 2 pipelines with noop processor -> switch to proxy mode, ditch the whole config
	// if one query, one ingest pipeline with noop processor -> switch to "dual-write-query-clickhouse" mode
	procList := c.getProcessorsConfiguredInPipelines()
	if len(procList) == 1 {
		if procList[0].Type == QuesmaV1ProcessorNoOp {
			conf.Mode = ProxyInspect
		}
	} else { // per validation its sage to assume there are two pipelines
		conf.Mode = DualWriteQueryClickhouse
		if v1processor := procList[0]; v1processor != nil {
			conf.IndexConfig = v1processor.Config.IndexConfig
			for indexName, indexConfig := range v1processor.Config.IndexConfig {
				indexConfig.Name = indexName
				conf.IndexConfig[indexName] = indexConfig
			}
		} else {
			errAcc = multierror.Append(errAcc, err)
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
	// per validation, there's always at least one frontend connector, and even if there's a second one, it must listen on the same port
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
	return ElasticsearchConfiguration{}, errors.New("elasticsearch backend connector must be configured")
}

func (c *QuesmaNewConfiguration) getRelationalDBConf() (*RelationalDbConfiguration, string, error) {
	if backendConn, typ := c.getRelationalDBBackendConnector(); backendConn != nil {
		return &backendConn.Config, typ, nil
	}
	return nil, "", errors.New("exactly one backend connector of type `clickhouse`, `clickhouse-os` or `hydrolix` must be configured")
}

func getAllowedProcessorTypes() []ProcessorType {
	return []ProcessorType{QuesmaV1ProcessorNoOp, QuesmaV1ProcessorQuery, QuesmaV1ProcessorIngest}
}
