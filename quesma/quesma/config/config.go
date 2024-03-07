package config

import (
	"fmt"
	"github.com/rs/zerolog"
	"github.com/spf13/viper"
	"mitmproxy/quesma/index"
	"mitmproxy/quesma/network"
	"net/url"
	"os"
	"slices"
	"strings"
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
	prefix             = "quesma"
	indexConfig        = "index"
	enabledConfig      = "enabled"
	fullTextFields     = "fulltext_fields"
	logsPathConfig     = "logs_path"
	logLevelConfig     = "log_level"
	publicTcpPort      = "port"
	elasticsearchUrl   = "elasticsearch_url"
	clickhouseUrl      = "clickhouse_url"
	clickhouseDatabase = "clickhouse_database"
	ingestStatistics   = "ingest_statistics"
)

const (
	clickhouseUserEnv     = "CLICKHOUSE_USER"
	clickhousePasswordEnv = "CLICKHOUSE_PASSWORD"
)

type (
	OperationMode       int
	QuesmaConfiguration struct {
		Mode               OperationMode
		ElasticsearchUrl   *url.URL
		ClickHouseUrl      *url.URL
		ClickHouseUser     *string
		ClickHousePassword *string
		ClickHouseDatabase *string
		IndexConfig        []IndexConfiguration
		LogsPath           string
		LogLevel           zerolog.Level
		PublicTcpPort      network.Port
		IngestStatistics   bool
	}

	IndexConfiguration struct {
		NamePattern    string
		Enabled        bool
		FullTextFields []string
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

func (cfg QuesmaConfiguration) IsFullTextMatchField(indexName, fieldName string) bool {

	// This is hardcoded default. We assume that field 'message' is always full text.
	if fieldName == "message" {
		return true
	}

	for _, indexConfig := range cfg.IndexConfig {
		if indexConfig.FullTextField(indexName, fieldName) {
			return true
		}
	}
	return false

}

func MatchName(pattern, name string) bool {
	return index.TableNamePatternRegexp(pattern).MatchString(name)
}

func Load() QuesmaConfiguration {
	// TODO Add wiser config parsing which fails for good and accumulates errors using https://github.com/hashicorp/go-multierror

	if configFileName, isSet := os.LookupEnv(configEnvVar); isSet {
		fmt.Printf("Using config file: %s\n", configFileName)
		viper.SetConfigFile(configFileName)
	} else {
		viper.SetConfigName(defaultConfigFileName)
		viper.SetConfigType(defaultConfigType)
		viper.AddConfigPath(".")
	}

	if err := viper.ReadInConfig(); err != nil {
		return QuesmaConfiguration{}
	}

	var mode = viper.Get(fullyQualifiedConfig(modeConfigName)).(string)
	var indexBypass = make([]IndexConfiguration, 0)
	for indexNamePattern, config := range viper.Get(fullyQualifiedConfig(indexConfig)).(map[string]interface{}) {
		var fields []string
		v, ok := config.(map[string]interface{})[fullTextFields]

		if ok {
			fields = strings.Split(v.(string), ",")
		}

		indexBypass = append(indexBypass, IndexConfiguration{NamePattern: indexNamePattern, Enabled: config.(map[string]interface{})[enabledConfig].(bool), FullTextFields: fields})
	}
	ingestStatistics, ok := viper.Get(fullyQualifiedConfig(ingestStatistics)).(bool)
	if !ok {
		ingestStatistics = true
	}

	return QuesmaConfiguration{
		Mode:               parseOperationMode(mode),
		PublicTcpPort:      configurePublicTcpPort(),
		ElasticsearchUrl:   configureUrl(elasticsearchUrl),
		ClickHouseUrl:      configureUrl(clickhouseUrl),
		IndexConfig:        indexBypass,
		LogsPath:           configureLogsPath(),
		LogLevel:           configureLogLevel(),
		ClickHouseUser:     configureOptionalEnvVar(clickhouseUserEnv),
		ClickHousePassword: configureOptionalEnvVar(clickhousePasswordEnv),
		ClickHouseDatabase: configureOptionalConfig(clickhouseDatabase),
		IngestStatistics:   ingestStatistics,
	}
}

func configureUrl(configParamName string) *url.URL {
	var urlString string
	var isSet bool
	if urlString, isSet = os.LookupEnv(strings.ToUpper(configParamName)); !isSet {
		urlString = viper.GetString(fullyQualifiedConfig(configParamName))
	}
	esUrl, err := url.Parse(urlString)
	if err != nil {
		panic(fmt.Errorf("error parsing %s: %s", configParamName, err))
	}
	return esUrl
}

func configurePublicTcpPort() network.Port {
	var portNumberStr string
	var isSet bool
	if portNumberStr, isSet = os.LookupEnv("TCP_PORT"); !isSet {
		portNumberStr = viper.GetString(fullyQualifiedConfig(publicTcpPort))
	}
	port, err := network.ParsePort(portNumberStr)
	if err != nil {
		panic(fmt.Errorf("error configuring public tcp port: %v", err))
	}
	return port
}

func fullyQualifiedConfig(config string) string {
	return fmt.Sprintf("%s.%s", prefix, config)
}

func configureOptionalEnvVar(envVarName string) *string {
	if value, isSet := os.LookupEnv(envVarName); isSet {
		return &value
	}
	return nil
}

func configureOptionalConfig(configName string) *string {
	if envVar := configureOptionalEnvVar(strings.ToUpper(configName)); envVar != nil {
		return envVar
	}
	if viper.IsSet(fullyQualifiedConfig(configName)) {
		value := viper.GetString(fullyQualifiedConfig(configName))
		return &value
	}
	return nil
}

func configureLogsPath() string {
	if logsPathEnv, isSet := os.LookupEnv("LOGS_PATH"); isSet {
		return logsPathEnv
	} else {
		return viper.GetString(fullyQualifiedConfig(logsPathConfig))
	}
}

func configureLogLevel() zerolog.Level {
	var logLevelStr string
	var isSet bool
	if logLevelStr, isSet = os.LookupEnv("LOG_LEVEL"); !isSet {
		if viper.IsSet(fullyQualifiedConfig(logLevelConfig)) {
			isSet = true
			logLevelStr = viper.GetString(fullyQualifiedConfig(logLevelConfig))
		} else {
			logLevelStr = zerolog.LevelDebugValue
		}
	}
	level, err := zerolog.ParseLevel(logLevelStr)
	if err != nil {
		panic(fmt.Errorf("error configuring log level: %v, string: %s, isSet: %t", err, logLevelStr, isSet))
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
	for _, index := range c.IndexConfig {
		indexConfigs += fmt.Sprintf("\n\t\t%s, enabled: %t, fullTextFields: %s", index.NamePattern, index.Enabled, strings.Join(index.FullTextFields, ", "))
	}

	elasticUrl := "<nil>"
	if c.ElasticsearchUrl != nil {
		elasticUrl = c.ElasticsearchUrl.String()
	}

	clickhouseUrl := "<nil>"
	if c.ClickHouseUrl != nil {
		clickhouseUrl = c.ClickHouseUrl.String()
	}

	clickhouseExtra := ""
	if c.ClickHouseUser != nil {
		clickhouseExtra = fmt.Sprintf("\n      ClickHouse user: %s", *c.ClickHouseUser)
	}
	if c.ClickHousePassword != nil {
		clickhouseExtra += "\n      ClickHouse password: ***"
	}
	if c.ClickHouseDatabase != nil {
		clickhouseExtra += fmt.Sprintf("\n      ClickHouse database: %s", *c.ClickHouseDatabase)
	}

	return fmt.Sprintf(`
Quesma Configuration:
	Mode: %s
	Elasticsearch URL: %s
	ClickHouse URL: %s%s
	Indexes: %s
	Logs Path: %s
	Log Level: %v
	Public TCP Port: %d
	Ingest Statistics: %t`,
		c.Mode.String(),
		elasticUrl,
		clickhouseUrl,
		clickhouseExtra,
		indexConfigs,
		c.LogsPath,
		c.LogLevel,
		c.PublicTcpPort,
		c.IngestStatistics)
}
