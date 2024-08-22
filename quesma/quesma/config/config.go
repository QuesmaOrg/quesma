// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package config

import (
	"fmt"
	"github.com/hashicorp/go-multierror"
	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/env"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/v2"
	"github.com/rs/zerolog"
	"log"
	"os"
	"quesma/elasticsearch/elasticsearch_field_types"
	"quesma/index"
	"quesma/network"
	"slices"
	"strings"
)

const (
	defaultConfigFileName    = "config.yaml"
	configFileLocationEnvVar = "QUESMA_CONFIG_FILE"
)

var (
	telemetryUrl = &Url{Scheme: "https", Host: "api.quesma.com", Path: "/phone-home"}
)

type QuesmaConfiguration struct {
	// both clickhouse and hydrolix connections are going to be deprecated and everything is going to live under connector
	Connectors     map[string]RelationalDbConfiguration `koanf:"connectors"`
	Mode           operationMode                        `koanf:"mode"`
	InstallationId string                               `koanf:"installationId"`
	LicenseKey     string                               `koanf:"licenseKey"`
	//deprecated
	ClickHouse RelationalDbConfiguration `koanf:"clickhouse"`
	//deprecated
	Hydrolix                   RelationalDbConfiguration     `koanf:"hydrolix"`
	Elasticsearch              ElasticsearchConfiguration    `koanf:"elasticsearch"`
	IndexConfig                map[string]IndexConfiguration `koanf:"indexes"`
	Logging                    LoggingConfiguration          `koanf:"logging"`
	PublicTcpPort              network.Port                  `koanf:"port"`
	EnableElasticsearchIngest  bool                          `koanf:"enableElasticsearchIngest"`
	IngestStatistics           bool                          `koanf:"ingestStatistics"`
	QuesmaInternalTelemetryUrl *Url                          `koanf:"internalTelemetryUrl"`
}

type LoggingConfiguration struct {
	Path              string        `koanf:"path"`
	Level             zerolog.Level `koanf:"level"`
	RemoteLogDrainUrl *Url          `koanf:"remoteUrl"`
	FileLogging       bool          `koanf:"fileLogging"`
}

type RelationalDbConfiguration struct {
	//ConnectorName string `koanf:"name"`
	ConnectorType string `koanf:"type"`
	Url           *Url   `koanf:"url"`
	User          string `koanf:"user"`
	Password      string `koanf:"password"`
	Database      string `koanf:"database"`
	AdminUrl      *Url   `koanf:"adminUrl"`
	DisableTLS    bool   `koanf:"disableTLS"`
}

type OptimizerConfiguration struct {
	Enabled    bool              `koanf:"enabled"`
	Properties map[string]string `koanf:"properties"`
}

func (c *RelationalDbConfiguration) IsEmpty() bool {
	return c != nil && c.Url == nil && c.User == "" && c.Password == "" && c.Database == ""
}

func (c *RelationalDbConfiguration) IsNonEmpty() bool {
	return !c.IsEmpty()
}

type FieldAlias struct {
	// TargetFieldName is the field name in the ClickHouse Table
	TargetFieldName string `koanf:"target"`
	// SourceFieldName is the field name in received in the ES Query
	SourceFieldName string `koanf:"source"`
}

func (c *QuesmaConfiguration) IsFullTextMatchField(indexName, fieldName string) bool {
	if indexConfig, found := c.IndexConfig[indexName]; found {
		return indexConfig.HasFullTextField(fieldName)
	}
	return false
}

func (c *QuesmaConfiguration) AliasFields(indexName string) map[string]FieldAlias {
	if indexConfig, found := c.IndexConfig[indexName]; found {
		return indexConfig.Aliases
	}
	return map[string]FieldAlias{}
}

func MatchName(pattern, name string) bool {
	return index.TableNamePatternRegexp(pattern).MatchString(name)
}

var k = koanf.New(".")

func Load() QuesmaConfiguration {
	var config QuesmaConfiguration
	config.QuesmaInternalTelemetryUrl = telemetryUrl
	config.Logging.RemoteLogDrainUrl = telemetryUrl

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
	if err := k.Unmarshal("", &config); err != nil {
		log.Fatalf("error unmarshalling config: %v", err)
	}
	for name, idxConfig := range config.IndexConfig {
		idxConfig.Name = name
		config.IndexConfig[name] = idxConfig
		if idxConfig.SchemaConfiguration != nil {
			for fieldName, configuration := range idxConfig.SchemaConfiguration.Fields {
				configuration.Name = fieldName
				idxConfig.SchemaConfiguration.Fields[fieldName] = configuration
			}
		}
	}
	return config
}

func loadConfigFile() {
	var configPath string
	if configFileName, isSet := os.LookupEnv(configFileLocationEnvVar); isSet {
		configPath = configFileName
	} else {
		configPath = defaultConfigFileName
	}
	fmt.Printf("Using config file: [%s]\n", configPath)
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		fmt.Printf("Error loading config file [%v], proceeding without it...\n", err)
		return
	}
	if err := k.Load(file.Provider(configPath), yaml.Parser()); err != nil {
		log.Fatalf("error loading config: %v", err)
	}
}

func (c *QuesmaConfiguration) Validate() error {
	var result error
	// at some point we might move to dedicated validation per each nested object,
	// e.g. c.Elasticsearch.Validate()
	if c.PublicTcpPort == 0 { // unmarshalling defaults to 0 if not present
		result = multierror.Append(result, fmt.Errorf("specifying Quesma TCP port for incoming traffic is required"))
	}
	connectorCount := len(c.Connectors)
	if connectorCount != 1 {
		result = multierror.Append(result, fmt.Errorf("%d connectors configured - at this moment Quesma requires **exactly** one connector specified", connectorCount))
	}
	//for _, conn := range c.Connectors {
	//	if conn.Url == nil {
	//		result = multierror.Append(result, fmt.Errorf("connector %s requires setting the URL", conn.ConnectorType))
	//	}
	//}
	if c.ClickHouse.Url == nil && c.Hydrolix.Url == nil {
		result = multierror.Append(result, fmt.Errorf("clickHouse or hydrolix URL is required"))
	}
	if c.ClickHouse.IsNonEmpty() && c.Hydrolix.IsNonEmpty() {
		result = multierror.Append(result, fmt.Errorf("only one of ClickHouse and Hydrolix can be configured"))
	}
	if c.Elasticsearch.Url == nil {
		result = multierror.Append(result, fmt.Errorf("elasticsearch URL is required"))
	}
	if c.Mode == "" {
		result = multierror.Append(result, fmt.Errorf("quesma operating mode is required"))
	}
	for indexName, indexConfig := range c.IndexConfig {
		result = c.validateIndexName(indexName, result)
		// TODO enable when rolling out schema configuration
		//result = c.validateDeprecated(indexConfig, result)
		result = c.validateSchemaConfiguration(indexConfig, result)
	}
	if c.Hydrolix.IsNonEmpty() {
		// At this moment we share the code between ClickHouse and Hydrolix which use only different names
		// for the same configuration object.
		c.ClickHouse = c.Hydrolix
	}
	return result
}

// TODO remove ignore when rolling out schema configuration
//
//lint:ignore U1000 Ignore unused function temporarily for debugging
func (c *QuesmaConfiguration) validateDeprecated(indexName IndexConfiguration, result error) error {
	if len(indexName.FullTextFields) > 0 {
		fmt.Printf("index configuration %s contains deprecated field 'fullTextFields'", indexName.Name)
	}
	if len(indexName.Aliases) > 0 {
		fmt.Printf("index configuration %s contains deprecated field 'aliases'", indexName.Name)
	}
	if len(indexName.IgnoredFields) > 0 {
		fmt.Printf("index configuration %s contains deprecated field 'ignoredFields'", indexName.Name)
	}
	if indexName.TimestampField != nil {
		fmt.Printf("index configuration %s contains deprecated field 'timestampField'", indexName.Name)
	}
	return result
}

func (c *QuesmaConfiguration) validateIndexName(indexName string, result error) error {
	if strings.Contains(indexName, "*") || indexName == "_all" {
		result = multierror.Append(result, fmt.Errorf("wildcard patterns are not allowed in index configuration: %s", indexName))
	}
	return result
}

func (c *QuesmaConfiguration) ReadsFromClickhouse() bool {
	return c.Mode == DualWriteQueryClickhouse || c.Mode == DualWriteQueryClickhouseFallback ||
		c.Mode == DualWriteQueryClickhouseVerify || c.Mode == ClickHouse
}

func (c *QuesmaConfiguration) ReadsFromElasticsearch() bool {
	return c.Mode == Proxy || c.Mode == ProxyInspect || c.Mode == DualWriteQueryElastic ||
		c.Mode == DualWriteQueryClickhouse || c.Mode == DualWriteQueryClickhouseFallback ||
		c.Mode == DualWriteQueryClickhouseVerify
}

func (c *QuesmaConfiguration) WritesToClickhouse() bool {
	return c.Mode != Proxy && c.Mode != ProxyInspect
}

func (c *QuesmaConfiguration) WritesToElasticsearch() bool {
	return c.Mode != ClickHouse
}

func (c *QuesmaConfiguration) optimizersConfigAsString(s string, cfg map[string]OptimizerConfiguration) string {

	var lines []string

	lines = append(lines, fmt.Sprintf("        %s:", s))
	for k, v := range cfg {
		lines = append(lines, fmt.Sprintf("            %s: %v", k, v.Enabled))
		if v.Properties != nil && len(v.Properties) > 0 {
			lines = append(lines, fmt.Sprintf("                properties: %v", v.Properties))
		}
	}

	return strings.Join(lines, "\n")
}

func (c *QuesmaConfiguration) OptimizersConfigAsString() string {

	var lines []string

	lines = append(lines, "\n")

	for indexName, indexConfig := range c.IndexConfig {
		if indexConfig.EnabledOptimizers != nil && len(indexConfig.EnabledOptimizers) > 0 {
			lines = append(lines, c.optimizersConfigAsString(indexName, indexConfig.EnabledOptimizers))
		}
	}

	lines = append(lines, "\n")
	return strings.Join(lines, "\n")
}

func (c *QuesmaConfiguration) String() string {
	var indexConfigs string
	for _, idx := range c.IndexConfig {
		indexConfigs += idx.String()
	}

	elasticUrl := "<nil>"
	if c.Elasticsearch.Url != nil {
		elasticUrl = c.Elasticsearch.Url.String()
	}
	elasticsearchExtra := ""
	if c.Elasticsearch.User != "" {
		elasticsearchExtra = fmt.Sprintf("\n        Elasticsearch user: %s", c.Elasticsearch.User)
	}
	if c.Elasticsearch.Password != "" {
		elasticsearchExtra += "\n        Elasticsearch password: ***"
	}
	clickhouseUrl := "<nil>"
	clickhouseExtra := ""
	if c.ClickHouse.User != "" {
		clickhouseExtra = fmt.Sprintf("\n      ClickHouse user: %s", c.ClickHouse.User)
	}
	if c.ClickHouse.Url != nil {
		clickhouseUrl = c.ClickHouse.Url.String()
	}
	if c.ClickHouse.Password != "" {
		clickhouseExtra += "\n      ClickHouse password: ***"
	}
	if c.ClickHouse.Database != "" {
		clickhouseExtra += fmt.Sprintf("\n      ClickHouse database: %s", c.ClickHouse.Database)
	}
	var connectorString strings.Builder
	for connName, conn := range c.Connectors {
		connectorString.WriteString(fmt.Sprintf("\n        - [%s] connector", connName))
		connectorString.WriteString(fmt.Sprintf("\n          Type: %s", conn.ConnectorType))
		if conn.Url != nil {
			connectorString.WriteString(fmt.Sprintf("\n          Url: %s", conn.Url.String()))
		}
		if conn.User != "" {
			connectorString.WriteString(fmt.Sprintf("\n          User: %s", conn.User))
		}
		if conn.Password != "" {
			connectorString.WriteString("\n          Password: ***")
		}
		if conn.Database != "" {
			connectorString.WriteString(fmt.Sprintf("\n          Database: %s", conn.Database))
		}
	}
	quesmaInternalTelemetryUrl := "disabled"
	if c.QuesmaInternalTelemetryUrl != nil {
		quesmaInternalTelemetryUrl = c.QuesmaInternalTelemetryUrl.String()
	}
	return fmt.Sprintf(`
Quesma Configuration:
	Mode: %s
	Elasticsearch URL: %s%s
	ClickhouseUrl: %s%s
	Connectors: %s
	Call Elasticsearch: %v
	Indexes: %s
	Logs Path: %s
	Log Level: %v
	Public TCP Port: %d
	Ingest Statistics: %t,
	Quesma Telemetry URL: %s
    Optimizers: %s`,
		c.Mode.String(),
		elasticUrl,
		elasticsearchExtra,
		clickhouseUrl,
		clickhouseExtra,
		connectorString.String(),
		c.EnableElasticsearchIngest,
		indexConfigs,
		c.Logging.Path,
		c.Logging.Level,
		c.PublicTcpPort,
		c.IngestStatistics,
		quesmaInternalTelemetryUrl,
		c.OptimizersConfigAsString(),
	)
}

func (c *QuesmaConfiguration) validateSchemaConfiguration(config IndexConfiguration, err error) error {
	if config.SchemaConfiguration == nil {
		return err
	}

	fmt.Println("schema configuration is not yet in use!")

	for fieldName, fieldConfig := range config.SchemaConfiguration.Fields {
		if fieldConfig.Type == "" {
			err = multierror.Append(err, fmt.Errorf("field %s in index %s has no type", fieldName, config.Name))
		} else if !elasticsearch_field_types.IsValid(fieldConfig.Type.AsString()) {
			err = multierror.Append(err, fmt.Errorf("field %s in index %s has invalid type %s", fieldName, config.Name, fieldConfig.Type))
		}

		if slices.Contains(config.SchemaConfiguration.Ignored, fieldName.AsString()) {
			err = multierror.Append(err, fmt.Errorf("field %s in index %s is both enabled and ignored", fieldName, config.Name))
		}

		if field, found := config.SchemaConfiguration.Fields[fieldName]; found && field.Type.AsString() == elasticsearch_field_types.FieldTypeAlias && field.AliasedField == "" {
			err = multierror.Append(err, fmt.Errorf("field %s in index %s is aliased to an empty field", fieldName, config.Name))
		}

		if countPrimaryKeys(config) > 1 {
			err = multierror.Append(err, fmt.Errorf("index %s has more than one primary key", config.Name))
		}
	}

	return err
}

func countPrimaryKeys(config IndexConfiguration) (count int) {
	for _, configuration := range config.SchemaConfiguration.Fields {
		if configuration.IsPrimaryKey {
			count++
		}
	}
	return count
}
