package config

import (
	"errors"
	"github.com/hashicorp/go-multierror"
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
		if connType == "hydrolix" {
			conf.Connectors["injected-hydrolix-connector"] = *relDBConn
			conf.Hydrolix = *relDBConn
		} else {
			conf.Connectors["injected-clickhouse-connector"] = *relDBConn
			conf.ClickHouse = *relDBConn
		}
	}
	if v1processor, err := c.getProcessorConfig(); err == nil {
		conf.Mode = v1processor.Config.Mode
		conf.IndexConfig = v1processor.Config.IndexConfig
		for indexName, indexConfig := range v1processor.Config.IndexConfig {
			indexConfig.Name = indexName
			conf.IndexConfig[indexName] = indexConfig
		}
	} else {
		errAcc = multierror.Append(errAcc, err)
	}

	if errAcc != nil {
		log.Fatalf("config validation failed: %v", errAcc)
	}
	return conf
}

func (c *QuesmaNewConfiguration) getPublicTcpPort() (network.Port, error) {
	if len(c.FrontendConnectors) == 1 {
		if c.FrontendConnectors[0].Type == "elasticsearch-fe" {
			return c.FrontendConnectors[0].Config.ListenPort, nil
		} else {
			return 0, errors.New("frontend connector type not recognized, only `elasticsearch-fe` is supported at this moment")
		}
	}
	return 0, errors.New("exactly one frontend connector must be defined at this moment")
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

func (c *QuesmaNewConfiguration) getProcessorConfig() (*Processor, error) {
	if len(c.Processors) == 1 {
		if c.Processors[0].Type == "quesma-v1-processor" {
			return &c.Processors[0], nil
		} else {
			return nil, errors.New("processor type not recognized, only `quesma-v1-processor` is supported at this moment")
		}
	}
	return nil, errors.New("exactly one processor must be defined at this moment")
}
