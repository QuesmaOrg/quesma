package config

import (
	"fmt"
	"github.com/spf13/viper"
	"log"
	"regexp"
	"strings"
)

const (
	configFileName = "config"
	configType     = "yaml"
)

const (
	shadowMode    = "shadow-mode"
	dualWriteMode = "dual-write-mode"
)

const (
	prefix        = "quesma"
	indexConfig   = "index"
	enabledConfig = "enabled"
)

type (
	QuesmaConfiguration struct {
		Shadow      bool
		DualWrite   bool
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
		log.Printf("Could not read config, using default values: %s\n", err)
		return QuesmaConfiguration{}
	}

	var shadow = viper.Get(fullyQualifiedConfig(shadowMode)).(bool)
	var dualWrite = viper.Get(fullyQualifiedConfig(dualWriteMode)).(bool)
	var indexBypass = make([]IndexConfiguration, 0)
	for indexNamePattern, config := range viper.Get(fullyQualifiedConfig(indexConfig)).(map[string]interface{}) {
		indexBypass = append(indexBypass, IndexConfiguration{NamePattern: indexNamePattern, Enabled: config.(map[string]interface{})[enabledConfig].(bool)})
	}
	return QuesmaConfiguration{Shadow: shadow, DualWrite: dualWrite, IndexConfig: indexBypass}
}

func fullyQualifiedConfig(config string) string {
	return fmt.Sprintf("%s.%s", prefix, config)
}

func matches(indexName string, indexNamePattern string) bool {
	r, err := regexp.Compile(strings.Replace(indexNamePattern, "*", ".*", -1))
	if err != nil {
		log.Printf("invalid index name pattern [%s]: %s\n", indexNamePattern, err)
		return false
	}

	return r.MatchString(indexName)
}

func FindMatchingConfig(indexName string, config QuesmaConfiguration) (IndexConfiguration, bool) {
	for _, config := range config.IndexConfig {
		log.Printf("matching index %s with config: %+v\n", indexName, config.NamePattern)
		if matches(indexName, config.NamePattern) {
			log.Printf("  ╚═ matched index %s with config: %+v\n", indexName, config.NamePattern)
			return config, true
		} else {
			log.Printf("  ╚═ not matched index %s with config: %+v\n", indexName, config.NamePattern)
		}
	}
	return IndexConfiguration{}, false
}
