package config

import (
	"github.com/knadh/koanf/providers/env"
	"log"
	"quesma/network"
	"strings"
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
	Type   string              `koanf:"type"`
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

	return v2config
}

func (c *QuesmaNewConfiguration) TranslateToLegacyConfig() QuesmaConfiguration {
	var conf QuesmaConfiguration
	conf.PublicTcpPort = c.getPublicTcpPort()
	conf.Elasticsearch = c.getElasticsearchConfig()
	conf.Connectors = make(map[string]RelationalDbConfiguration)
	conf.Logging = c.Logging
	conf.QuesmaInternalTelemetryUrl = telemetryUrl
	conf.Logging.RemoteLogDrainUrl = telemetryUrl
	conf.InstallationId = c.InstallationId
	conf.LicenseKey = c.LicenseKey
	conf.IngestStatistics = c.IngestStatistics
	relDBConn, connType := c.getRelationalDBConf()
	relDBConn.ConnectorType = connType
	if connType == "hydrolix" {
		conf.Connectors["injected-hydrolix-connector"] = *relDBConn
		conf.Hydrolix = *relDBConn
	} else {
		conf.Connectors["injected-clickhouse-connector"] = *relDBConn
		conf.ClickHouse = *relDBConn
	}

	if v1processor := c.getProcessorConfig(); v1processor != nil {
		conf.Mode = v1processor.Config.Mode
		conf.IndexConfig = v1processor.Config.IndexConfig
		for indexName, indexConfig := range v1processor.Config.IndexConfig {
			indexConfig.Name = indexName
			conf.IndexConfig[indexName] = indexConfig
		}
	} else {
		panic("Processor must be configured")
	}

	return conf
}

func (c *QuesmaNewConfiguration) getPublicTcpPort() network.Port {
	if len(c.FrontendConnectors) == 1 {
		if c.FrontendConnectors[0].Type == "elasticsearch-fe" {
			return c.FrontendConnectors[0].Config.ListenPort
		} else {
			panic("Frontend connector type not recognized, only `elasticsearch-fe` is supported at this moment")
		}
	}
	panic("Exactly one frontend connector must be defined at this moment")
}

func (c *QuesmaNewConfiguration) getElasticsearchBackendConnector() *BackendConnector {
	for _, backendConn := range c.BackendConnectors {
		if backendConn.Type == "elasticsearch" {
			return &backendConn
		}
	}
	return nil
}

func (c *QuesmaNewConfiguration) getRelationalDBBackendConnector() (*BackendConnector, string) {
	for _, backendConn := range c.BackendConnectors {
		if backendConn.Type == "clickhouse" || backendConn.Type == "clickhouse-os" || backendConn.Type == "hydrolix" {
			return &backendConn, backendConn.Type
		}
	}
	panic("Relational DB backend connector type not recognized, only `clickhouse`, `clickhouse-os` and `hydrolix` are supported at this moment")
}

func (c *QuesmaNewConfiguration) getElasticsearchConfig() ElasticsearchConfiguration {
	if esBackendConn := c.getElasticsearchBackendConnector(); esBackendConn != nil {
		return ElasticsearchConfiguration{
			Url:      esBackendConn.Config.Url,
			User:     esBackendConn.Config.User,
			Password: esBackendConn.Config.Password,
			Call:     true, // TODO this is hardcoded for now, but eventually should get deleted
		}
	}
	panic("Elasticsearch backend connector must be configured")
}

func (c *QuesmaNewConfiguration) getRelationalDBConf() (*RelationalDbConfiguration, string) {
	if esBackendConn, typ := c.getRelationalDBBackendConnector(); esBackendConn != nil {
		return &esBackendConn.Config, typ
	}
	panic("Elasticsearch backend connector must be configured")
}

func (c *QuesmaNewConfiguration) getProcessorConfig() *Processor {
	if len(c.Processors) == 1 {
		if c.Processors[0].Type == "quesma-v1-processor" {
			return &c.Processors[0]
		} else {
			panic("Processor type not recognized, only `quesma-v1-processor` is supported at this moment")
		}
	}
	panic("Exactly one processor must be defined at this moment")
}
