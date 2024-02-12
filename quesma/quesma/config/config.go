package config

import (
	"fmt"
	"github.com/spf13/viper"
	"mitmproxy/quesma/network"
	"net/url"
	"os"
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
		PublicTcpPort    network.Port
	}

	IndexConfiguration struct {
		NamePattern string
		Enabled     bool
	}
)

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
