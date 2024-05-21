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
	"mitmproxy/quesma/index"
	"mitmproxy/quesma/network"
	"os"
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

type ElasticsearchConfiguration struct {
	Url      *Url   `koanf:"url"`
	User     string `koanf:"user"`
	Password string `koanf:"password"`
	Call     bool   `koanf:"call"`
	AdminUrl *Url   `koanf:"adminUrl"`
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

type IndexFieldConfiguration struct {
	// Valid types: ignored, fulltext, alias, primary-timestamp
	Type         *string `koanf:"type"`
	AliasTo      *string `koanf:"aliasTo"`
	DbColumnName *string `koanf:"dbColumnName"`
}

type IndexConfiguration struct {
	Name    string                             `koanf:"name"`
	Enabled bool                               `koanf:"enabled"`
	Fields  map[string]IndexFieldConfiguration `koanf:"fields"`
	// Valid: Add unmapped, add all, add none
	DefaultFields *string `koanf:"policy"`
}

func (c IndexConfiguration) Matches(indexName string) bool {
	return c.Name == indexName
}

func (c IndexConfiguration) FullTextFields() (fullTextFields []string) {
	for fieldName, fieldConfig := range c.Fields {
		if fieldConfig.Type != nil && *fieldConfig.Type == "fulltext" {
			fullTextFields = append(fullTextFields, fieldName)
		}
	}
	return
}

func (c IndexConfiguration) String() string {
	var build strings.Builder
	for fieldName, fieldConfig := range c.Fields {
		if build.Len() > 0 {
			build.WriteString(", ")
		}
		setType := ""
		if fieldConfig.Type != nil {
			setType = *fieldConfig.Type
		}
		build.WriteString(fmt.Sprintf("%s(%s)", fieldName, setType))
		if fieldConfig.AliasTo != nil {
			build.WriteString(fmt.Sprintf("alias to %s", *fieldConfig.AliasTo))
		}
		if fieldConfig.DbColumnName != nil {
			build.WriteString(fmt.Sprintf("->%s", *fieldConfig.DbColumnName))
		}
	}
	var str = fmt.Sprintf("\n\t\t%s, enabled: %t",
		c.Name,
		c.Enabled,
	)

	if build.Len() > 0 {
		str = fmt.Sprintf("%s, fields: %s", str, build.String())
	}

	return str
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
	// TODO remove once when code does not depend on Name property
	for name, idxConfig := range config.IndexConfig {
		idxConfig.Name = name
		config.IndexConfig[name] = idxConfig
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
	for indexName := range c.IndexConfig {
		if strings.Contains(indexName, "*") || indexName == "_all" {
			result = multierror.Append(result, fmt.Errorf("wildcard patterns are not allowed in index configuration: %s", indexName))
		}
	}
	if c.Hydrolix.IsNonEmpty() {
		// At this moment we share the code between ClickHouse and Hydrolix which use only different names
		// for the same configuration object.
		c.ClickHouse = c.Hydrolix
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
