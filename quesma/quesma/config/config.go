package config

import (
	"fmt"
	"github.com/spf13/viper"
	"mitmproxy/quesma/logger"
	"regexp"
	"strings"
	"sync/atomic"
)

const (
	configFileName = "config"
	configType     = "yaml"
)

const (
	modeConfigName = "mode"
)

const (
	prefix        = "quesma"
	indexConfig   = "index"
	enabledConfig = "enabled"
)

type (
	OperationMode       int
	QuesmaConfiguration struct {
		Mode        OperationMode
		IndexConfig []IndexConfiguration
	}

	IndexConfiguration struct {
		NamePattern string
		Enabled     bool
	}
)

func Load() QuesmaConfiguration {
	viper.SetConfigName(configFileName)
	viper.SetConfigType(configType)
	viper.AddConfigPath(".")
	if err := viper.ReadInConfig(); err != nil {
		logger.Error().Msgf("Could not read config, using default values: %v", err)
		return QuesmaConfiguration{}
	}

	var mode = viper.Get(fullyQualifiedConfig(modeConfigName)).(string)
	var indexBypass = make([]IndexConfiguration, 0)
	for indexNamePattern, config := range viper.Get(fullyQualifiedConfig(indexConfig)).(map[string]interface{}) {
		indexBypass = append(indexBypass, IndexConfiguration{NamePattern: indexNamePattern, Enabled: config.(map[string]interface{})[enabledConfig].(bool)})
	}
	return QuesmaConfiguration{Mode: parseOperationMode(mode), IndexConfig: indexBypass}
}

func fullyQualifiedConfig(config string) string {
	return fmt.Sprintf("%s.%s", prefix, config)
}

func matches(indexName string, indexNamePattern string) bool {
	r, err := regexp.Compile(strings.Replace(indexNamePattern, "*", ".*", -1))
	if err != nil {
		logger.Error().Msgf("invalid index name pattern [%s]: %s", indexNamePattern, err)
		return false
	}

	return r.MatchString(indexName)
}

var matchCounter = atomic.Int32{}

func FindMatchingConfig(indexName string, config QuesmaConfiguration) (IndexConfiguration, bool) {
	matchCounter.Add(1)
	for _, config := range config.IndexConfig {
		if matchCounter.Load()%100 == 1 {
			logger.Debug().Msgf("matching index %s with config: %+v, ctr: %d", indexName, config.NamePattern, matchCounter.Load())
		}
		if matches(indexName, config.NamePattern) {
			if matchCounter.Load()%100 == 1 {
				logger.Debug().Msgf("  ╚═ matched index %s with config: %+v, ctr: %d", indexName, config.NamePattern, matchCounter.Load())
			}
			return config, true
		} else {
			if matchCounter.Load()%100 == 1 {
				logger.Info().Msgf("  ╚═ not matched index %s with config: %+v, ctr: %d", indexName, config.NamePattern, matchCounter.Load())
			}
		}
	}
	return IndexConfiguration{}, false
}
