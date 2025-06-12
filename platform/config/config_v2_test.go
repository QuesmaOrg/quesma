// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package config

import (
	"fmt"
	"github.com/QuesmaOrg/quesma/platform/util"
	"github.com/stretchr/testify/assert"
	"os"
	"strings"
	"testing"
)

func loadConfig(t *testing.T) QuesmaNewConfiguration {
	cfg, cfgErr := LoadV2Config()
	assert.NoError(t, cfgErr, "error loading config")
	if err := cfg.Validate(); err != nil {
		t.Fatalf("error validating config: %v", err)
	}
	return cfg
}

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
	cfg := loadConfig(t)
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

	for i, tt := range tests {
		t.Run(util.PrettyTestName(tt.name, i), func(t *testing.T) {
			ic := findIndexConfig(tt.name)
			assert.NotNil(t, ic)
			assert.Equal(t, tt.queryTarget, ic.QueryTarget)
			assert.Equal(t, tt.ingestTarget, ic.IngestTarget)
		})
	}
}

func TestQuesmaTransparentProxyConfiguration(t *testing.T) {
	os.Setenv(configFileLocationEnvVar, "./test_configs/quesma_as_transparent_proxy.yml")
	cfg := loadConfig(t)
	legacyConf := cfg.TranslateToLegacyConfig()
	assert.True(t, legacyConf.TransparentProxy)
	assert.Equal(t, false, legacyConf.EnableIngest)
	assert.Equal(t, false, legacyConf.CreateCommonTable)
}

func TestQuesmaTransparentProxyWithoutNoopConfiguration(t *testing.T) {
	t.Skip("not working yet")

	os.Setenv(configFileLocationEnvVar, "./test_configs/quesma_as_transparent_proxy_without_noop.yml")
	cfg := loadConfig(t)

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
	cfg := loadConfig(t)
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
	cfg := loadConfig(t)
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
	cfg := loadConfig(t)
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
	cfg := loadConfig(t)
	legacyConf := cfg.TranslateToLegacyConfig()

	assert.Equal(t, true, legacyConf.EnableIngest)
	assert.Equal(t, true, legacyConf.CreateCommonTable)
}

func TestInvalidDualTarget(t *testing.T) {
	os.Setenv(configFileLocationEnvVar, "./test_configs/invalid_dual_target.yaml")
	cfg, cfgErr := LoadV2Config()
	assert.NoError(t, cfgErr, "error loading config")
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
	for i, tt := range tests {
		t.Run(fmt.Sprintf("%s->%s[%v](%d)", tt.args.indexName, tt.args.indexNamePattern, tt.want, i), func(t *testing.T) {
			assert.Equalf(t, tt.want, MatchName(tt.args.indexNamePattern, tt.args.indexName), "matches(%v, %v)", tt.args.indexName, tt.args.indexNamePattern)
		})
	}
}

func TestTargetNewVariant(t *testing.T) {
	os.Setenv(configFileLocationEnvVar, "./test_configs/target_new_variant.yaml")
	cfg := loadConfig(t)
	legacyConf := cfg.TranslateToLegacyConfig()
	assert.False(t, legacyConf.TransparentProxy)
	assert.Equal(t, 4, len(legacyConf.IndexConfig))
	ecommerce := legacyConf.IndexConfig["kibana_sample_data_ecommerce"]
	flights := legacyConf.IndexConfig["kibana_sample_data_flights"]
	logs := legacyConf.IndexConfig["kibana_sample_data_logs"]
	override := legacyConf.IndexConfig["test_override"]

	assert.Equal(t, []string{ClickhouseTarget}, ecommerce.QueryTarget)
	assert.Equal(t, []string{ClickhouseTarget}, ecommerce.IngestTarget)

	assert.Equal(t, []string{ClickhouseTarget}, flights.QueryTarget)
	assert.Equal(t, []string{ClickhouseTarget}, flights.IngestTarget)

	assert.Equal(t, []string{ClickhouseTarget}, logs.QueryTarget)
	assert.Equal(t, []string{ClickhouseTarget}, logs.IngestTarget)

	assert.Equal(t, []string{ClickhouseTarget}, override.QueryTarget)
	assert.Equal(t, []string{ClickhouseTarget}, override.IngestTarget)

	assert.Equal(t, false, flights.UseCommonTable)
	assert.Equal(t, "", flights.Override)
	assert.Equal(t, false, ecommerce.UseCommonTable)
	assert.Equal(t, "", ecommerce.Override)
	assert.Equal(t, true, logs.UseCommonTable)
	assert.Equal(t, "", logs.Override)
	assert.Equal(t, true, legacyConf.EnableIngest)

	const expectedOverride = "new_override"
	assert.Equal(t, expectedOverride, override.Override)
}

func TestTargetLegacyVariant(t *testing.T) {
	os.Setenv(configFileLocationEnvVar, "./test_configs/target_legacy_variant.yaml")
	cfg := loadConfig(t)
	legacyConf := cfg.TranslateToLegacyConfig()
	assert.False(t, legacyConf.TransparentProxy)
	assert.Equal(t, 3, len(legacyConf.IndexConfig))
	ecommerce := legacyConf.IndexConfig["kibana_sample_data_ecommerce"]
	flights := legacyConf.IndexConfig["kibana_sample_data_flights"]
	logs := legacyConf.IndexConfig["kibana_sample_data_logs"]

	assert.Equal(t, []string{ClickhouseTarget}, ecommerce.QueryTarget)
	assert.Equal(t, []string{ClickhouseTarget, ElasticsearchTarget}, ecommerce.IngestTarget)

	assert.Equal(t, []string{ClickhouseTarget}, flights.QueryTarget)
	assert.Equal(t, []string{ClickhouseTarget}, flights.IngestTarget)

	assert.Equal(t, []string{ElasticsearchTarget, ClickhouseTarget}, logs.QueryTarget)
	assert.Equal(t, []string{ClickhouseTarget, ElasticsearchTarget}, logs.IngestTarget)

	assert.Equal(t, false, flights.UseCommonTable)
	assert.Equal(t, "", flights.Override)
	assert.Equal(t, false, ecommerce.UseCommonTable)
	assert.Equal(t, "", ecommerce.Override)
	assert.Equal(t, true, legacyConf.EnableIngest)
}

func TestUseCommonTableGlobalProperty(t *testing.T) {
	os.Setenv(configFileLocationEnvVar, "./test_configs/use_common_table_global_property.yaml")
	cfg := loadConfig(t)
	legacyConf := cfg.TranslateToLegacyConfig()
	assert.False(t, legacyConf.TransparentProxy)
	assert.Equal(t, 2, len(legacyConf.IndexConfig))
	ecommerce := legacyConf.IndexConfig["kibana_sample_data_ecommerce"]
	flights := legacyConf.IndexConfig["kibana_sample_data_flights"]

	assert.Equal(t, []string{ClickhouseTarget}, ecommerce.QueryTarget)
	assert.Equal(t, []string{ClickhouseTarget}, ecommerce.IngestTarget)

	assert.Equal(t, []string{ClickhouseTarget}, flights.QueryTarget)
	assert.Equal(t, []string{ClickhouseTarget}, flights.IngestTarget)

	assert.Equal(t, true, flights.UseCommonTable)
	assert.Equal(t, false, ecommerce.UseCommonTable)
}

func TestIngestOptimizers(t *testing.T) {
	os.Setenv(configFileLocationEnvVar, "./test_configs/ingest_only_optimizers.yaml")
	cfg := loadConfig(t)
	legacyConf := cfg.TranslateToLegacyConfig()
	assert.False(t, legacyConf.TransparentProxy)
	assert.Equal(t, 1, len(legacyConf.IndexConfig))
	logs1, ok := legacyConf.IndexConfig["logs-1"]

	assert.True(t, ok)
	assert.Equal(t, 2, len(logs1.Optimizers))
	assert.NotNil(t, legacyConf.DefaultIngestOptimizers)
	assert.Equal(t, 1, len(legacyConf.DefaultIngestOptimizers))
	assert.NotNil(t, legacyConf.DefaultIngestOptimizers["ingest_only"])

	_, ok = legacyConf.DefaultIngestOptimizers["query_only"]
	assert.False(t, ok)
}

func TestPartitionBy(t *testing.T) {
	os.Setenv(configFileLocationEnvVar, "./test_configs/partition_by.yaml")
	cfg := loadConfig(t)
	legacyConf := cfg.TranslateToLegacyConfig()

	assert.Equal(t, 2, len(legacyConf.IndexConfig))

	ecommerce := legacyConf.IndexConfig["kibana_sample_data_ecommerce"]
	assert.Equal(t, Daily, ecommerce.PartitioningStrategy)

	flights := legacyConf.IndexConfig["kibana_sample_data_flights"]
	assert.Equal(t, None, flights.PartitioningStrategy)

	assert.Equal(t, Hourly, legacyConf.DefaultPartitioningStrategy)
}

func TestIndexNameRewriteRules(t *testing.T) {

	os.Setenv(configFileLocationEnvVar, "./test_configs/index_name_rewrite_rules.yaml")
	cfg := loadConfig(t)
	legacyConf := cfg.TranslateToLegacyConfig()

	assert.Equal(t, 4, len(legacyConf.IndexNameRewriteRules))

	for _, rule := range legacyConf.IndexNameRewriteRules {
		assert.Equal(t, "$1", rule.To)
	}

	assert.Equal(t, "(.*?)(-\\d{4}\\.\\d{2}\\.\\d{2})$", legacyConf.IndexNameRewriteRules[0].From)
	assert.Equal(t, "(.*?)(-\\d{4}\\.\\d{2})$", legacyConf.IndexNameRewriteRules[1].From)
	assert.Equal(t, "(.*?)(.\\d{4}-\\d{2})$", legacyConf.IndexNameRewriteRules[2].From)
	assert.Equal(t, "(.*?)(.\\d{4}-\\d{2}-\\d{2})$", legacyConf.IndexNameRewriteRules[3].From) // empty string means no rewrite rule
}

func TestStringColumnIsTextDefaultBehavior(t *testing.T) {
	os.Setenv(configFileLocationEnvVar, "./test_configs/partition_by.yaml")
	cfg := loadConfig(t)
	legacyConf := cfg.TranslateToLegacyConfig()

	assert.Equal(t, "text", legacyConf.DefaultStringColumnType)

}

func TestStringColumnIsKeyword(t *testing.T) {
	os.Setenv(configFileLocationEnvVar, "./test_configs/string_column_is_keyword_field.yaml")
	cfg := loadConfig(t)
	legacyConf := cfg.TranslateToLegacyConfig()

	assert.Equal(t, "keyword", legacyConf.DefaultStringColumnType)

}
