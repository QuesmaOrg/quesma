// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package config

import (
	"fmt"
	"github.com/hashicorp/go-multierror"
	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/v2"
	"log"
	"os"
	"quesma/elasticsearch/elasticsearch_field_types"
	"quesma/index"
	"quesma/util"
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
	Connectors       map[string]RelationalDbConfiguration
	TransparentProxy bool
	InstallationId   string
	LicenseKey       string
	//deprecated
	ClickHouse RelationalDbConfiguration
	//deprecated
	Hydrolix                   RelationalDbConfiguration
	Elasticsearch              ElasticsearchConfiguration
	IndexConfig                map[string]IndexConfiguration
	Logging                    LoggingConfiguration
	PublicTcpPort              util.Port
	IngestStatistics           bool
	QuesmaInternalTelemetryUrl *Url
	DisableAuth                bool
	AutodiscoveryEnabled       bool

	EnableIngest              bool // this is computed from the configuration 2.0
	CreateCommonTable         bool
	UseCommonTableForWildcard bool //the meaning of this is to use a common table for wildcard (default) indexes
	DefaultIngestTarget       []string
	DefaultQueryTarget        []string
}

func (c *QuesmaConfiguration) AliasFields(indexName string) map[string]string {
	aliases := make(map[string]string)
	if indexConfig, found := c.IndexConfig[indexName]; found {
		if indexConfig.SchemaOverrides != nil {
			for fieldName, FieldConf := range indexConfig.SchemaOverrides.Fields {
				aliases[fieldName.AsString()] = FieldConf.TargetColumnName
			}
		}
	}
	return aliases
}

func MatchName(pattern, name string) bool {
	return index.TableNamePatternRegexp(pattern).MatchString(name)
}

var k = koanf.New(".")

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
	if c.PublicTcpPort == 0 { // unmarshalling defaults to 0 if not present
		result = multierror.Append(result, fmt.Errorf("specifying TCP port for incoming traffic is required, please verify your frontend connector settings"))
	}
	connectorCount := len(c.Connectors)
	if connectorCount != 1 {
		if !(connectorCount == 0 && c.TransparentProxy) { // no connectors for transparent proxy is fine
			result = multierror.Append(result, fmt.Errorf("%d connectors configured - at this moment Quesma requires **exactly** one connector specified", connectorCount))
		}
	}
	if c.ClickHouse.Url == nil && c.Hydrolix.Url == nil && !c.TransparentProxy {
		result = multierror.Append(result, fmt.Errorf("clickHouse or hydrolix URL is required"))
	}
	if c.ClickHouse.IsNonEmpty() && c.Hydrolix.IsNonEmpty() {
		result = multierror.Append(result, fmt.Errorf("only one of ClickHouse and Hydrolix can be configured"))
	}
	if c.Elasticsearch.Url == nil {
		result = multierror.Append(result, fmt.Errorf("elasticsearch URL is required"))
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
	return nil
}

func (c *QuesmaConfiguration) validateIndexName(indexName string, result error) error {
	if indexName == DefaultWildcardIndexName {
		return result
	}
	if strings.Contains(indexName, "*") || indexName == "_all" {
		result = multierror.Append(result, fmt.Errorf("wildcard patterns are not allowed in index configuration: %s", indexName))
	}
	return result
}

func (c *QuesmaConfiguration) ReadsFromClickhouse() bool {
	return !c.TransparentProxy
}

func (c *QuesmaConfiguration) ReadsFromElasticsearch() bool {
	return true
}

func (c *QuesmaConfiguration) WritesToClickhouse() bool {
	return !c.TransparentProxy
}

func (c *QuesmaConfiguration) WritesToElasticsearch() bool {
	return true
}

func (c *QuesmaConfiguration) optimizersConfigAsString(s string, cfg map[string]OptimizerConfiguration) string {

	var lines []string

	lines = append(lines, fmt.Sprintf("        %s:", s))
	for k, v := range cfg {
		var status string
		if v.Disabled {
			status = "<disabled>"
		} else {
			status = "enabled"
		}
		lines = append(lines, fmt.Sprintf("            %s: %s", k, status))
		if len(v.Properties) > 0 {
			lines = append(lines, fmt.Sprintf("                properties: %v", v.Properties))
		}
	}

	return strings.Join(lines, "\n")
}

func (c *QuesmaConfiguration) OptimizersConfigAsString() string {

	var lines []string
	for indexName, indexConfig := range c.IndexConfig {
		if len(indexConfig.Optimizers) > 0 {
			lines = append(lines, c.optimizersConfigAsString(indexName, indexConfig.Optimizers))
		}
	}
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
	Transparent proxy mode: %t
	Elasticsearch URL: %s%s
	ClickhouseUrl: %s%s
	Connectors: %s
	Indexes: %s
	Logs Path: %s
	Log Level: %v
	Public TCP Port: %d
	Ingest Statistics: %t,
	Quesma Telemetry URL: %s,
	Optimizers: %s,
	DisableAuth: %t,
	AutodiscoveryEnabled: %t,
	EnableIngest: %t,
	CreateCommonTable: %t,
	UseCommonTableForWildcard: %t,
	DefaultIngestTarget: %v,
	DefaultQueryTarget: %v,
`,
		c.TransparentProxy,
		elasticUrl,
		elasticsearchExtra,
		clickhouseUrl,
		clickhouseExtra,
		connectorString.String(),
		indexConfigs,
		c.Logging.Path,
		c.Logging.Level,
		c.PublicTcpPort,
		c.IngestStatistics,
		quesmaInternalTelemetryUrl,
		c.OptimizersConfigAsString(),
		c.DisableAuth,
		c.AutodiscoveryEnabled,
		c.EnableIngest,
		c.CreateCommonTable,
		c.UseCommonTableForWildcard,
		c.DefaultIngestTarget,
		c.DefaultQueryTarget,
	)
}

func (c *QuesmaConfiguration) validateSchemaConfiguration(config IndexConfiguration, err error) error {
	if config.SchemaOverrides == nil {
		return err
	}

	for fieldName, fieldConfig := range config.SchemaOverrides.Fields {
		if fieldConfig.Type == "" && !fieldConfig.Ignored {
			err = multierror.Append(err, fmt.Errorf("field [%s] in index [%s] has no type", fieldName, config.Name))
		} else if !elasticsearch_field_types.IsValid(fieldConfig.Type.AsString()) && !fieldConfig.Ignored {
			err = multierror.Append(err, fmt.Errorf("field [%s] in index [%s] has invalid type %s", fieldName, config.Name, fieldConfig.Type))
		}
		if fieldConfig.Type == TypeAlias && fieldConfig.TargetColumnName == "" {
			err = multierror.Append(err, fmt.Errorf("field [%s] of type alias in index [%s] cannot have `targetColumnName` property unset", fieldName, config.Name))
		}

		// TODO This validation will be fixed on further field config cleanup
		//if slices.Contains(config.SchemaOverrides.Ignored, fieldName.AsString()) {
		//	err = multierror.Append(err, fmt.Errorf("field %s in index %s is both enabled and ignored", fieldName, config.Name))
		//}

		//if field, found := config.SchemaOverrides.Fields[fieldName]; found && field.Type.AsString() == elasticsearch_field_types.FieldTypeAlias && field.AliasedField == "" {
		//	err = multierror.Append(err, fmt.Errorf("field %s in index %s is aliased to an empty field", fieldName, config.Name))
		//}

		//if countPrimaryKeys(config) > 1 {
		//	err = multierror.Append(err, fmt.Errorf("index %s has more than one primary key", config.Name))
		//}
	}

	return err
}

func (c *QuesmaConfiguration) IndexAutodiscoveryEnabled() bool {
	return c.AutodiscoveryEnabled
}
