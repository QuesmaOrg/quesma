// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package config

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestQuesmaConfigurationLoading(t *testing.T) {

	os.Setenv(configFileLocationEnvVar, "./test_config_v2.yaml")

	logLevelPassedAsEnvVar := "debug"
	licenseKeyPassedAsEnvVar := "arbitraty-license-key"
	os.Setenv("QUESMA_logging_level", logLevelPassedAsEnvVar) // overrides what's in the config file
	os.Setenv("QUESMA_licenseKey", licenseKeyPassedAsEnvVar)  // overrides what's in the config file
	cfg := LoadV2Config()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("error validating config: %v", err)
	}

	legacyCfg := cfg.TranslateToLegacyConfig()

	assert.Equal(t, licenseKeyPassedAsEnvVar, legacyCfg.LicenseKey)
	assert.Equal(t, false, legacyCfg.TransparentProxy)
	assert.Equal(t, 8080, int(legacyCfg.PublicTcpPort))
	assert.Equal(t, "http://localhost:9200", legacyCfg.Elasticsearch.Url.String())
	assert.Equal(t, "clickhouse://localhost:9000", legacyCfg.ClickHouse.Url.String())
	assert.Equal(t, true, legacyCfg.IngestStatistics)
	assert.Equal(t, "logs", legacyCfg.Logging.Path)
	assert.Equal(t, logLevelPassedAsEnvVar, legacyCfg.Logging.Level.String())
	assert.Equal(t, 11, len(legacyCfg.IndexConfig))

	findIndexConfig := func(name string) *IndexConfiguration {
		if configuration, found := legacyCfg.IndexConfig[name]; found {
			return &configuration
		} else {
			return nil
		}
	}

	tests := []struct {
		name         string
		queryTarget  []string
		ingestTarget []string
	}{
		{"logs-generic-default", []string{ClickhouseTarget}, []string{ClickhouseTarget}},
		{"device-logs", []string{ClickhouseTarget}, []string{ClickhouseTarget}},
		{"example-elastic-index", []string{ElasticsearchTarget}, []string{ElasticsearchTarget}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ic := findIndexConfig(tt.name)
			assert.NotNil(t, ic)
			assert.Equal(t, tt.queryTarget, ic.QueryTarget)
			assert.Equal(t, tt.ingestTarget, ic.IngestTarget)
		})
	}
}

func TestMatchName(t *testing.T) {
	type args struct {
		indexName        string
		indexNamePattern string
	}
	tests := []struct {
		args args
		want bool
	}{
		{args: args{"logs-generic-default", "logs-generic-default*"}, want: true},
		{args: args{"logs-generic-default", "logs-generic-*"}, want: true},
		{args: args{"logs-generic-default-foo", "logs-generic-*"}, want: true},
		{args: args{"logs-generic-", "logs-generic-*"}, want: true},
		{args: args{"logs-generic", "logs-generic-*"}, want: false},
		{args: args{"logs2-generic", "logs-generic-*"}, want: false},
		{args: args{"logs-generic-default", "logs-*-default"}, want: true},
		{args: args{"logs-specific", "logs-generic-*"}, want: false},
		{args: args{"logs-generic-123", "logs-generic-*"}, want: true},
		{args: args{"logs-generic-default-foo-bar", "logs-generic-*"}, want: true},
		{args: args{"logs-generic-abc", "logs-generic-*"}, want: true},
		{args: args{"logs-custom-default", "logs-*-default"}, want: true},
		{args: args{"logs-custom-default", "logs-generic-*"}, want: false},
		{args: args{"logs-custom-specific", "logs-custom-*"}, want: true},
		{args: args{"logs-custom-specific-123", "logs-custom-*"}, want: true},
		{args: args{"logs-custom-abc", "logs-custom-*"}, want: true},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("%s->%s[%v]", tt.args.indexName, tt.args.indexNamePattern, tt.want), func(t *testing.T) {
			assert.Equalf(t, tt.want, MatchName(tt.args.indexNamePattern, tt.args.indexName), "matches(%v, %v)", tt.args.indexName, tt.args.indexNamePattern)
		})
	}
}
