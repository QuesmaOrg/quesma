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
	"mitmproxy/quesma/buildinfo"
	"mitmproxy/quesma/elasticsearch/elasticsearch_field_types"
	"mitmproxy/quesma/index"
	"mitmproxy/quesma/network"
	"os"
	"slices"
	"strings"
)

const (
	LicenseHeader   = "X-License-Key"          // Used to pass license key by phone home service
	RemoteLogHeader = "X-Telemetry-Remote-Log" // Used to inform telemetry endpoint that the payload contains logs
)
const (
	defaultConfigFileName    = "config.yaml"
	configFileLocationEnvVar = "QUESMA_CONFIG_FILE"
)

var (
	telemetryUrl = &Url{Scheme: "https", Host: "api.quesma.com", Path: "/phone-home"}
)

type QuesmaConfiguration struct {
	Mode                       operationMode                 `koanf:"mode"`
	LicenseKey                 string                        `koanf:"licenseKey"`
	ClickHouse                 RelationalDbConfiguration     `koanf:"clickhouse"`
	Hydrolix                   RelationalDbConfiguration     `koanf:"hydrolix"`
	Elasticsearch              ElasticsearchConfiguration    `koanf:"elasticsearch"`
	IndexConfig                map[string]IndexConfiguration `koanf:"indexes"`
	Logging                    LoggingConfiguration          `koanf:"logging"`
	PublicTcpPort              network.Port                  `koanf:"port"`
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
	Url      *Url   `koanf:"url"`
	User     string `koanf:"user"`
	Password string `koanf:"password"`
	Database string `koanf:"database"`
	AdminUrl *Url   `koanf:"adminUrl"`
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
	// SourceFieldName is the field name in sent to Quesma in ES Query
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
	config.configureLicenseKey()
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

func MaskLicenseKey(licenseKey string) string {
	if len(licenseKey) > 4 {
		return "****" + licenseKey[len(licenseKey)-4:]
	} else {
		return "****"
	}
}

func (c *QuesmaConfiguration) configureLicenseKey() {
	// This condition implies that we're dealing with customer-specific build,
	// which has license key injected at the build time via ldflags, see `docs/private-beta-releases.md`
	if buildinfo.LicenseKey != buildinfo.DevelopmentLicenseKey && buildinfo.LicenseKey != "" {
		// `buildinfo.LicenseKey` can be injected at the build time, don't get fooled by the IDE warning above
		fmt.Printf("Using license key from build: %s\n", MaskLicenseKey(buildinfo.LicenseKey))
		c.LicenseKey = buildinfo.LicenseKey
		return
	} else if c.LicenseKey != "" { // In case of **any other** setup, we fall back to what's been configured by user (==config or env vars)
		fmt.Printf("Using license key from configuration: %s\n", MaskLicenseKey(c.LicenseKey))
		return
	} else {
		log.Fatalf("missing license key. Quiting...")
	}
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
	if c.ClickHouse.Url != nil {
		clickhouseUrl = c.ClickHouse.Url.String()
	}

	clickhouseExtra := ""
	if c.ClickHouse.User != "" {
		clickhouseExtra = fmt.Sprintf("\n      ClickHouse user: %s", c.ClickHouse.User)
	}
	if c.ClickHouse.Password != "" {
		clickhouseExtra += "\n      ClickHouse password: ***"
	}
	if c.ClickHouse.Database != "" {
		clickhouseExtra += fmt.Sprintf("\n      ClickHouse database: %s", c.ClickHouse.Database)
	}
	quesmaInternalTelemetryUrl := "disabled"
	if c.QuesmaInternalTelemetryUrl != nil {
		quesmaInternalTelemetryUrl = c.QuesmaInternalTelemetryUrl.String()
	}
	return fmt.Sprintf(`
Quesma Configuration:
	Mode: %s
	Elasticsearch URL: %s%s
	ClickHouse URL: %s%s
	Call Elasticsearch: %v
	Indexes: %s
	Logs Path: %s
	Log Level: %v
	Public TCP Port: %d
	Ingest Statistics: %t,
	Quesma Telemetry URL: %s`,
		c.Mode.String(),
		elasticUrl,
		elasticsearchExtra,
		clickhouseUrl,
		clickhouseExtra,
		c.Elasticsearch.Call,
		indexConfigs,
		c.Logging.Path,
		c.Logging.Level,
		c.PublicTcpPort,
		c.IngestStatistics,
		quesmaInternalTelemetryUrl,
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
