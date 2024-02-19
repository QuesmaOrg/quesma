package config

import (
	"fmt"
	"github.com/rs/zerolog"
	"github.com/spf13/viper"
	"mitmproxy/quesma/network"
	"net/url"
	"os"
	"regexp"
	"strings"
)

const (
	configFileName = "config"
	configType     = "yaml"
)

const (
	modeConfigName = "mode"
)

const (
	prefix           = "quesma"
	indexConfig      = "index"
	enabledConfig    = "enabled"
	logsPathConfig   = "logs_path"
	logLevelConfig   = "log_level"
	publicTcpPort    = "port"
	elasticsearchUrl = "elasticsearch_url"
	clickhouseUrl    = "clickhouse_url"
)

type (
	OperationMode       int
	QuesmaConfiguration struct {
		Mode             OperationMode
		ElasticsearchUrl *url.URL
		ClickHouseUrl    *url.URL
		IndexConfig      []IndexConfiguration
		LogsPath         string
		LogLevel         zerolog.Level
		PublicTcpPort    network.Port
	}

	IndexConfiguration struct {
		NamePattern string
		Enabled     bool
	}
)

func (c IndexConfiguration) Matches(indexName string) bool {
	return regexp.MustCompile(fmt.Sprintf("^%s$", strings.Replace(c.NamePattern, "*", ".*", -1))).MatchString(indexName)
}

func Load() QuesmaConfiguration {
	// TODO Add wiser config parsing which fails for good and accumulates errors using https://github.com/hashicorp/go-multierror
	viper.SetConfigName(configFileName)
	viper.SetConfigType(configType)
	viper.AddConfigPath(".")
	if err := viper.ReadInConfig(); err != nil {
		return QuesmaConfiguration{}
	}

	var mode = viper.Get(fullyQualifiedConfig(modeConfigName)).(string)
	var indexBypass = make([]IndexConfiguration, 0)
	for indexNamePattern, config := range viper.Get(fullyQualifiedConfig(indexConfig)).(map[string]interface{}) {
		indexBypass = append(indexBypass, IndexConfiguration{NamePattern: indexNamePattern, Enabled: config.(map[string]interface{})[enabledConfig].(bool)})
	}
	return QuesmaConfiguration{
		Mode:             parseOperationMode(mode),
		PublicTcpPort:    configurePublicTcpPort(),
		ElasticsearchUrl: configureUrl(elasticsearchUrl),
		ClickHouseUrl:    configureUrl(clickhouseUrl),
		IndexConfig:      indexBypass,
		LogsPath:         configureLogsPath(),
		LogLevel:         configureLogLevel(),
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
	return c.Mode == Proxy || c.Mode == ProxyInspect || c.Mode == DualWriteQueryElastic
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
		indexConfigs += fmt.Sprintf("\n\t\t%s, enabled: %t", index.NamePattern, index.Enabled)
	}

	elasticUrl := "<nil>"
	if c.ElasticsearchUrl != nil {
		elasticUrl = c.ElasticsearchUrl.String()
	}

	clickhouseUrl := "<nil>"
	if c.ClickHouseUrl != nil {
		clickhouseUrl = c.ClickHouseUrl.String()
	}

	return fmt.Sprintf(`
Quesma Configuration:
	Mode: %s
	Elasticsearch URL: %s
	ClickHouse URL: %s
	Indexes: %s
	Logs Path: %s
	Log Level: %v
	Public TCP Port: %d`,
		c.Mode.String(),
		elasticUrl,
		clickhouseUrl,
		indexConfigs,
		c.LogsPath,
		c.LogLevel,
		c.PublicTcpPort,
	)
}
