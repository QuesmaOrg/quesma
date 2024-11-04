// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package config

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"os"
	"strings"
	"testing"
)

func TestQuesmaConfigurationLoading(t *testing.T) {

	os.Setenv(configFileLocationEnvVar, "./test_configs/test_config_v2.yaml")

	logLevelPassedAsEnvVar := "debug"
	licenseKeyPassedAsEnvVar := "arbitraty-license-key"
	os.Setenv("QUESMA_logging_level", logLevelPassedAsEnvVar)   // overrides what's in the config file
	os.Setenv("QUESMA_licenseKey", licenseKeyPassedAsEnvVar)    // overrides what's in the config file
	os.Setenv("QUESMA_backendConnectors_1_config_user", "user") // overrides what's in the config file
	t.Cleanup(func() {
		os.Unsetenv(configFileLocationEnvVar)
		os.Unsetenv("QUESMA_logging_level")
		os.Unsetenv("QUESMA_licenseKey")
		os.Unsetenv("QUESMA_backendConnectors_1_config_user")
	})
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
	assert.Equal(t, "user", legacyCfg.ClickHouse.User)
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

func TestQuesmaTransparentProxyConfiguration(t *testing.T) {
	os.Setenv(configFileLocationEnvVar, "./test_configs/quesma_as_transparent_proxy.yml")
	cfg := LoadV2Config()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("error validating config: %v", err)
	}
	legacyConf := cfg.TranslateToLegacyConfig()
	assert.True(t, legacyConf.TransparentProxy)
	assert.Equal(t, false, legacyConf.EnableIngest)
	assert.Equal(t, false, legacyConf.CreateCommonTable)
}

func TestQuesmaTransparentProxyWithoutNoopConfiguration(t *testing.T) {
	t.Skip("not working yet")

	os.Setenv(configFileLocationEnvVar, "./test_configs/quesma_as_transparent_proxy_without_noop.yml")
	cfg := LoadV2Config()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("error validating config: %v", err)
	}
	legacyConf := cfg.TranslateToLegacyConfig()
	assert.False(t, legacyConf.TransparentProxy) // even though transparent proxy would work similarly, the user explicitly requested two Quesma pipelines
	assert.Equal(t, 2, len(legacyConf.IndexConfig))
	siemIndexConf := legacyConf.IndexConfig["siem"]
	logsIndexConf := legacyConf.IndexConfig["logs"]

	assert.Equal(t, []string{ElasticsearchTarget}, siemIndexConf.QueryTarget)
	assert.Equal(t, []string{ElasticsearchTarget}, siemIndexConf.IngestTarget)

	assert.Equal(t, []string{ElasticsearchTarget}, logsIndexConf.QueryTarget)
	assert.Equal(t, []string{ElasticsearchTarget}, logsIndexConf.IngestTarget)
	assert.Equal(t, true, legacyConf.EnableIngest)
	assert.Equal(t, false, legacyConf.CreateCommonTable)
}

func TestQuesmaAddingHydrolixTablesToExistingElasticsearch(t *testing.T) {
	os.Setenv(configFileLocationEnvVar, "./test_configs/quesma_adding_two_hydrolix_tables.yaml")
	cfg := LoadV2Config()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("error validating config: %v", err)
	}
	legacyConf := cfg.TranslateToLegacyConfig()
	assert.False(t, legacyConf.TransparentProxy)
	assert.Equal(t, 2, len(legacyConf.IndexConfig))
	siemIndexConf := legacyConf.IndexConfig["siem"]
	logsIndexConf := legacyConf.IndexConfig["logs"]

	assert.Equal(t, []string{ClickhouseTarget}, siemIndexConf.QueryTarget)
	assert.Equal(t, []string{ElasticsearchTarget}, siemIndexConf.IngestTarget)

	assert.Equal(t, []string{ClickhouseTarget}, logsIndexConf.QueryTarget)
	assert.Equal(t, []string{ElasticsearchTarget}, logsIndexConf.IngestTarget)
	assert.Equal(t, true, legacyConf.EnableIngest)
	assert.Equal(t, false, legacyConf.CreateCommonTable)
}

func TestIngestWithSingleConnector(t *testing.T) {
	os.Setenv(configFileLocationEnvVar, "./test_configs/ingest_with_single_connector.yaml")
	cfg := LoadV2Config()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("error validating config: %v", err)
	}
	legacyConf := cfg.TranslateToLegacyConfig()
	assert.False(t, legacyConf.TransparentProxy)
	assert.Equal(t, 2, len(legacyConf.IndexConfig))
	siemIndexConf := legacyConf.IndexConfig["siem"]
	logsIndexConf := legacyConf.IndexConfig["logs"]

	assert.Equal(t, []string{ClickhouseTarget}, siemIndexConf.QueryTarget)
	assert.Equal(t, []string{ElasticsearchTarget}, siemIndexConf.IngestTarget)

	assert.Equal(t, []string{ClickhouseTarget}, logsIndexConf.QueryTarget)
	assert.Equal(t, []string{ElasticsearchTarget}, logsIndexConf.IngestTarget)
	assert.Equal(t, true, legacyConf.EnableIngest)
	assert.Equal(t, false, legacyConf.CreateCommonTable)
}

func TestQuesmaHydrolixQueryOnly(t *testing.T) {
	os.Setenv(configFileLocationEnvVar, "./test_configs/quesma_hydrolix_tables_query_only.yaml")
	cfg := LoadV2Config()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("error validating config: %v", err)
	}
	legacyConf := cfg.TranslateToLegacyConfig()
	assert.False(t, legacyConf.TransparentProxy)
	assert.Equal(t, 2, len(legacyConf.IndexConfig))

	siemIndexConf, ok := legacyConf.IndexConfig["siem"]
	assert.True(t, ok)
	logsIndexConf, ok := legacyConf.IndexConfig["logs"]
	assert.True(t, ok)

	assert.Equal(t, []string{ClickhouseTarget}, siemIndexConf.QueryTarget)

	assert.Equal(t, []string{ClickhouseTarget}, logsIndexConf.QueryTarget)

	assert.Equal(t, false, legacyConf.EnableIngest)
	assert.Equal(t, false, legacyConf.IngestStatistics)
	assert.Equal(t, false, legacyConf.CreateCommonTable)
}

func TestHasCommonTable(t *testing.T) {
	os.Setenv(configFileLocationEnvVar, "./test_configs/has_common_table.yaml")
	cfg := LoadV2Config()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("error validating config: %v", err)
	}
	legacyConf := cfg.TranslateToLegacyConfig()

	assert.Equal(t, true, legacyConf.EnableIngest)
	assert.Equal(t, true, legacyConf.CreateCommonTable)
}

func TestInvalidDualTarget(t *testing.T) {
	os.Setenv(configFileLocationEnvVar, "./test_configs/invalid_dual_target.yaml")
	cfg := LoadV2Config()
	if err := cfg.Validate(); err != nil {

		if !strings.Contains(err.Error(), "has invalid dual query target configuration - when you specify two targets") {
			t.Fatalf("unexpected error: %v", err)
		}

		t.Fatalf("expected error, but got none")
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

func TestTargetNewVariant(t *testing.T) {
	os.Setenv(configFileLocationEnvVar, "./test_configs/target_new_variant.yaml")
	cfg := LoadV2Config()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("error validating config: %v", err)
	}
	legacyConf := cfg.TranslateToLegacyConfig()
	assert.False(t, legacyConf.TransparentProxy)
	assert.Equal(t, 2, len(legacyConf.IndexConfig))
	ecommerce := legacyConf.IndexConfig["kibana_sample_data_ecommerce"]
	flights := legacyConf.IndexConfig["kibana_sample_data_flights"]

	assert.Equal(t, []string{ClickhouseTarget}, ecommerce.QueryTarget)
	assert.Equal(t, []string{ClickhouseTarget}, ecommerce.IngestTarget)

	assert.Equal(t, []string{ClickhouseTarget}, flights.QueryTarget)
	assert.Equal(t, []string{ClickhouseTarget}, flights.IngestTarget)
	assert.Equal(t, false, flights.UseCommonTable)
	assert.Equal(t, true, ecommerce.UseCommonTable)
	assert.Equal(t, true, legacyConf.EnableIngest)
}
