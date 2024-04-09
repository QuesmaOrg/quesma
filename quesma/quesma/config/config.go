package config

import (
	"fmt"
	"github.com/rs/zerolog"
	"github.com/spf13/cast"
	"github.com/spf13/viper"
	"mitmproxy/quesma/buildinfo"
	"mitmproxy/quesma/index"
	"mitmproxy/quesma/network"
	"net/url"
	"os"
	"slices"
	"strings"
)

const (
	LicenseHeader   = "X-License-Key"          // Used to pass license key by phone home service
	RemoteLogHeader = "X-Telemetry-Remote-Log" // Used to inform telemetry endpoint that the payload contains logs
)
const (
	defaultConfigFileName = "config"
	defaultConfigType     = "yaml"
	configEnvVar          = "QUESMA_CONFIG"
)

const (
	modeConfigName = "mode"
)

const (
	prefix                     = "quesma"
	indexConfig                = "index"
	enabledConfig              = "enabled"
	fullTextFields             = "fulltext_fields"
	aliasFields                = "alias_fields"
	ignoredFields              = "ignored_fields"
	logsPathConfig             = "logs_path"
	logLevelConfig             = "log_level"
	callElasticsearch          = "call_elasticsearch"
	disableFileLoggingConfig   = "disable_file_logging"
	publicTcpPort              = "port"
	elasticsearchUrl           = "elasticsearch_url"
	clickhouseUrl              = "clickhouse_url"
	clickhouseDatabase         = "clickhouse_database"
	ingestStatistics           = "ingest_statistics"
	quesmaInternalTelemetryUrl = "quesma_internal_telemetry_url"
	remoteLogDrainUrl          = "remote_log_drain_url"
	licenseKeyConfig           = "license_key"
)

const (
	clickhouseUserEnv        = "CLICKHOUSE_USER"
	clickhousePasswordEnv    = "CLICKHOUSE_PASSWORD"
	elasticsearchUserEnv     = "ELASTICSEARCH_USER"
	elasticsearchPasswordEnv = "ELASTICSEARCH_PASSWORD"
	disableFileLoggingEnv    = "DISABLE_FILE_LOGGING"
)

type (
	QuesmaConfiguration struct {
		Mode                       operationMode
		LicenseKey                 string
		ElasticsearchUrl           *url.URL
		ElasticsearchUser          string
		ElasticsearchPassword      string
		ClickHouseUrl              *url.URL
		ClickHouseUser             string
		ClickHousePassword         string
		ClickHouseDatabase         string
		IndexConfig                []IndexConfiguration
		LogsPath                   string
		LogLevel                   zerolog.Level
		PublicTcpPort              network.Port
		CallElasticsearch          bool
		IngestStatistics           bool
		QuesmaInternalTelemetryUrl *url.URL
		RemoteLogDrainUrl          *url.URL
		DisableFileLogging         bool
	}

	FieldAlias struct {
		TargetFieldName string
		SourceFieldName string
	}

	IndexConfiguration struct {
		NamePattern    string
		Enabled        bool
		FullTextFields []string
		Aliases        map[string]FieldAlias
		IgnoredFields  map[string]bool
	}
)

func (c IndexConfiguration) Matches(indexName string) bool {
	return MatchName(c.NamePattern, indexName)
}

func (c IndexConfiguration) FullTextField(indexName, fieldName string) bool {
	if !c.Matches(indexName) {
		return false
	}

	return slices.Contains(c.FullTextFields, fieldName)
}

func (c IndexConfiguration) String() string {
	var extraString string
	extraString = ""
	if len(c.Aliases) > 0 {
		extraString += "; aliases: "
		var aliases []string
		for _, alias := range c.Aliases {
			aliases = append(aliases, fmt.Sprintf("%s <- %s", alias.SourceFieldName, alias.TargetFieldName))
		}
		extraString += strings.Join(aliases, ", ")
	}
	if len(c.IgnoredFields) > 0 {
		extraString += "; ignored fields: "
		var fields []string
		for field := range c.IgnoredFields {
			fields = append(fields, field)
		}
		extraString += strings.Join(fields, ", ")
	}
	return fmt.Sprintf("\n\t\t%s, enabled: %t, fullTextFields: %s%s",
		c.NamePattern,
		c.Enabled,
		strings.Join(c.FullTextFields, ", "),
		extraString,
	)
}

func (cfg *QuesmaConfiguration) IsFullTextMatchField(indexName, fieldName string) bool {
	for _, indexConfig := range cfg.IndexConfig {
		if indexConfig.FullTextField(indexName, fieldName) {
			return true
		}
	}
	return false
}

func (cfg *QuesmaConfiguration) AliasFields(indexName string) map[string]FieldAlias {
	for _, indexConfig := range cfg.IndexConfig {
		if indexConfig.Matches(indexName) {
			return indexConfig.Aliases
		}
	}
	return map[string]FieldAlias{}
}

func MatchName(pattern, name string) bool {
	return index.TableNamePatternRegexp(pattern).MatchString(name)
}

func Load() QuesmaConfiguration {
	// TODO Add wiser config parsing which fails for good and accumulates errors using https://github.com/hashicorp/go-multierror

	v := viper.New()

	if configFileName, isSet := os.LookupEnv(configEnvVar); isSet {
		fmt.Printf("Using config file: %s\n", configFileName)
		v.SetConfigFile(configFileName)
	} else {
		v.SetConfigName(defaultConfigFileName)
		v.SetConfigType(defaultConfigType)
		v.AddConfigPath(".")
	}

	if err := v.ReadInConfig(); err != nil {
		return QuesmaConfiguration{LicenseKey: buildinfo.LicenseKey}
	}

	parser := NewQuesmaConfigurationParser(v)
	return parser.Parse()
}

type QuesmaConfigurationParser struct {
	parsedViper *viper.Viper
}

func NewQuesmaConfigurationParser(v *viper.Viper) *QuesmaConfigurationParser {
	return &QuesmaConfigurationParser{parsedViper: v}
}

func (p *QuesmaConfigurationParser) Parse() QuesmaConfiguration {

	var mode = p.getMandatoryConfig(modeConfigName).(string)
	var indexBypass = make([]IndexConfiguration, 0)

	for indexNamePattern, config := range p.parsedViper.Get(fullyQualifiedConfig(indexConfig)).(map[string]interface{}) {
		fields := []string{"message"}
		aliases := make(map[string]FieldAlias)
		ignored := make(map[string]bool)

		if v, ok := config.(map[string]interface{})[fullTextFields]; ok {
			if v == nil {
				fields = []string{}
			} else {
				fields = strings.Split(v.(string), ",")
			}
		}

		if v, ok := config.(map[string]interface{})[aliasFields]; ok && v != nil {
			for _, part := range strings.Split(v.(string), ",") {
				parts := strings.Split(part, "<-")
				if len(parts) == 2 {
					sourceFieldName := strings.TrimSpace(parts[0])
					targetFieldName := strings.TrimSpace(parts[1])
					aliases[sourceFieldName] = FieldAlias{SourceFieldName: sourceFieldName, TargetFieldName: targetFieldName}
				} else {
					fmt.Printf("Invalid alias field: %s\n", part)
				}
			}
		}

		if v, ok := config.(map[string]interface{})[ignoredFields]; ok && v != nil {
			for _, field := range strings.Split(v.(string), ",") {
				ignored[field] = true
			}
		}

		indexConfig := IndexConfiguration{
			NamePattern:    indexNamePattern,
			Enabled:        config.(map[string]interface{})[enabledConfig].(bool),
			FullTextFields: fields,
			Aliases:        aliases,
			IgnoredFields:  ignored,
		}

		indexBypass = append(indexBypass, indexConfig)
	}

	ingestStatistics, ok := p.parsedViper.Get(fullyQualifiedConfig(ingestStatistics)).(bool)
	if !ok {
		ingestStatistics = true
	}

	return QuesmaConfiguration{
		Mode:                       operationMode(mode),
		LicenseKey:                 p.configureLicenseKey(),
		PublicTcpPort:              p.configurePublicTcpPort(),
		CallElasticsearch:          p.configureElasticsearchCalls(),
		ElasticsearchUrl:           p.configureUrl(elasticsearchUrl),
		ElasticsearchUser:          configureOptionalEnvVar(elasticsearchUserEnv),
		ElasticsearchPassword:      configureOptionalEnvVar(elasticsearchPasswordEnv),
		ClickHouseUrl:              p.configureUrl(clickhouseUrl),
		IndexConfig:                indexBypass,
		LogsPath:                   p.configureLogsPath(),
		LogLevel:                   p.configureLogLevel(),
		ClickHouseUser:             configureOptionalEnvVar(clickhouseUserEnv),
		ClickHousePassword:         configureOptionalEnvVar(clickhousePasswordEnv),
		ClickHouseDatabase:         p.configureOptionalConfig(clickhouseDatabase),
		IngestStatistics:           ingestStatistics,
		QuesmaInternalTelemetryUrl: p.configureUrl(quesmaInternalTelemetryUrl),
		RemoteLogDrainUrl:          p.configureUrl(remoteLogDrainUrl),
		DisableFileLogging:         p.configureFileLoggingDisabled(disableFileLoggingEnv),
	}
}

func (p *QuesmaConfigurationParser) getMandatoryConfig(configName string) any {
	fullName := fullyQualifiedConfig(configName)
	if !p.parsedViper.IsSet(fullName) {
		panic(fmt.Errorf("missing mandatory config: %s", fullName))
	}
	return p.parsedViper.Get(fullName)
}

func (p *QuesmaConfigurationParser) configureUrl(configParamName string) *url.URL {
	var urlString string
	var isSet bool
	if urlString, isSet = os.LookupEnv(strings.ToUpper(configParamName)); !isSet {
		if p.parsedViper.IsSet(fullyQualifiedConfig(configParamName)) {
			urlString = p.parsedViper.GetString(fullyQualifiedConfig(configParamName))
		} else {
			return nil
		}
	}
	esUrl, err := url.Parse(urlString)
	if err != nil {
		panic(fmt.Errorf("error parsing %s: %s", configParamName, err))
	}
	return esUrl
}

func MaskLicenseKey(licenseKey string) string {
	if len(licenseKey) > 4 {
		return "****" + licenseKey[len(licenseKey)-4:]
	} else {
		return "****"
	}
}

func (p *QuesmaConfigurationParser) configureLicenseKey() string {
	// `buildinfo.LicenseKey` can be injected at the build time, don't get fooled by the IDE warning below
	if buildinfo.LicenseKey != buildinfo.DevelopmentLicenseKey && buildinfo.LicenseKey != "" {
		// This means it's customer-specific build, so continue using the license key from the build
		fmt.Printf("Using license key from build: %s\n", MaskLicenseKey(buildinfo.LicenseKey))
		return buildinfo.LicenseKey
	}
	// In case of **any other** setup, we fall back to default config handling
	if licenseKey, isSet := os.LookupEnv("LICENSE_KEY"); isSet {
		fmt.Printf("Using license key from env: %s\n", MaskLicenseKey(licenseKey))
		return licenseKey
	}
	if key := p.parsedViper.GetString(fullyQualifiedConfig(licenseKeyConfig)); key != "" {
		fmt.Printf("Using license key from config: %s\n", MaskLicenseKey(key))
		return key
	}
	panic("license key missing")
}

func (p *QuesmaConfigurationParser) configurePublicTcpPort() network.Port {
	var portNumberStr string
	var isSet bool
	if portNumberStr, isSet = os.LookupEnv("PORT"); !isSet {
		portNumberStr = p.parsedViper.GetString(fullyQualifiedConfig(publicTcpPort))
	}
	port, err := network.ParsePort(portNumberStr)
	if err != nil {
		panic(fmt.Errorf("error configuring public tcp port: %v", err))
	}
	return port
}

func (p *QuesmaConfigurationParser) configureElasticsearchCalls() bool {
	value := p.parsedViper.GetString(fullyQualifiedConfig(callElasticsearch))
	switch value {
	case "always":
		return true
	case "when-needed", "":
		return false
	}
	panic(fmt.Errorf("error configuring elasticsearch calls: %v", value))
}

func fullyQualifiedConfig(config string) string {
	return fmt.Sprintf("%s.%s", prefix, config)
}

func configureOptionalEnvVar(envVarName string) string {
	if value, isSet := os.LookupEnv(envVarName); isSet {
		return value
	}
	return ""
}

func (p *QuesmaConfigurationParser) configureOptionalConfig(configName string) string {
	if envVar := configureOptionalEnvVar(strings.ToUpper(configName)); envVar != "" {
		return envVar
	}
	if p.parsedViper.IsSet(fullyQualifiedConfig(configName)) {
		value := p.parsedViper.GetString(fullyQualifiedConfig(configName))
		return value
	}
	return ""
}

func (p *QuesmaConfigurationParser) configureLogsPath() string {
	if logsPathEnv, isSet := os.LookupEnv("LOGS_PATH"); isSet {
		return logsPathEnv
	} else {
		return p.parsedViper.GetString(fullyQualifiedConfig(logsPathConfig))
	}
}

func (p *QuesmaConfigurationParser) configureFileLoggingDisabled(envVar string) bool {
	if val, isSet := os.LookupEnv(envVar); isSet {
		return cast.ToBool(val)
	} else {
		return p.parsedViper.GetBool(fullyQualifiedConfig(disableFileLoggingConfig))
	}
}

func (p *QuesmaConfigurationParser) configureLogLevel() zerolog.Level {
	var logLevelStr string
	var isSet bool
	if logLevelStr, isSet = os.LookupEnv("LOG_LEVEL"); !isSet {
		if p.parsedViper.IsSet(fullyQualifiedConfig(logLevelConfig)) {
			isSet = true
			logLevelStr = p.parsedViper.GetString(fullyQualifiedConfig(logLevelConfig))
		} else {
			logLevelStr = zerolog.LevelDebugValue
		}
	}
	level, err := zerolog.ParseLevel(logLevelStr)
	if err != nil {
		panic(fmt.Errorf("error configuring log level: %parsedViper, string: %s, isSet: %t", err, logLevelStr, isSet))
	}
	return level
}

func (c *QuesmaConfiguration) GetIndexConfig(indexName string) (IndexConfiguration, bool) {
	for _, indexConfig := range c.IndexConfig {
		if indexConfig.Matches(indexName) {
			return indexConfig, true
		}
	}
	return IndexConfiguration{}, false
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
	if c.ElasticsearchUrl != nil {
		elasticUrl = c.ElasticsearchUrl.String()
	}
	elasticsearchExtra := ""
	if c.ElasticsearchUser != "" {
		elasticsearchExtra = fmt.Sprintf("\n        Elasticsearch user: %s", c.ElasticsearchUser)
	}
	if c.ElasticsearchPassword != "" {
		elasticsearchExtra += "\n        Elasticsearch password: ***"
	}

	clickhouseUrl := "<nil>"
	if c.ClickHouseUrl != nil {
		clickhouseUrl = c.ClickHouseUrl.String()
	}

	clickhouseExtra := ""
	if c.ClickHouseUser != "" {
		clickhouseExtra = fmt.Sprintf("\n      ClickHouse user: %s", c.ClickHouseUser)
	}
	if c.ClickHousePassword != "" {
		clickhouseExtra += "\n      ClickHouse password: ***"
	}
	if c.ClickHouseDatabase != "" {
		clickhouseExtra += fmt.Sprintf("\n      ClickHouse database: %s", c.ClickHouseDatabase)
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
		c.CallElasticsearch,
		indexConfigs,
		c.LogsPath,
		c.LogLevel,
		c.PublicTcpPort,
		c.IngestStatistics,
		quesmaInternalTelemetryUrl,
	)
}
