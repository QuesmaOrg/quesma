package config

import (
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestIndexConfiguration_Matches(t *testing.T) {
	type fields struct {
		Name    string
		Enabled bool
	}
	tests := []struct {
		name      string
		fields    fields
		indexName string
		want      bool
	}{
		{"logs-generic-default", fields{"logs-generic-default", true}, "logs-generic-default", true},
		{"logs-generic-default", fields{"logs-generic-default", true}, "logs-generic-default2", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := IndexConfiguration{
				Name:    tt.fields.Name,
				Enabled: tt.fields.Enabled,
			}
			assert.Equalf(t, tt.want, c.Matches(tt.indexName), "Matches(%v)", tt.indexName)
		})
	}
}

func TestIndexConfiguration_FullTextField(t *testing.T) {

	indexConfig := map[string]IndexConfiguration{
		"none": {
			Name:           "none",
			Enabled:        true,
			FullTextFields: []string{},
		},
		"foo-bar": {
			Name:           "foo-bar",
			Enabled:        true,
			FullTextFields: []string{"sometext"},
		},
		"bar-logs": {
			Name:           "bar-logs",
			Enabled:        true,
			FullTextFields: []string{},
		},
		"logs-generic-default": {
			Name:           "logs-generic-default",
			Enabled:        true,
			FullTextFields: []string{"message", "content"},
		},
	}

	cfg := QuesmaConfiguration{IndexConfig: indexConfig}

	tests := []struct {
		name      string
		indexName string
		fieldName string
		want      bool
	}{
		{"has full text field", "logs-generic-default", "message", true},
		{"has full text field", "logs-generic-default", "content", true},
		{"dont have full text field", "foo-bar", "content", false},
		{"dont have full text field", "bar-logs", "content", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, cfg.IsFullTextMatchField(tt.indexName, tt.fieldName), "IsFullTextMatchField(%parsedViper, %parsedViper)", tt.indexName, tt.fieldName)
		})
	}

}

func TestQuesmaConfigurationLoading(t *testing.T) {

	os.Setenv(configFileLocationEnvVar, "./test_config.yaml")

	logLevelPassedAsEnvVar := "debug"
	licenseKeyPassedAsEnvVar := "arbitraty-license-key"
	os.Setenv("QUESMA_logging_level", logLevelPassedAsEnvVar) // overrides what's in the config file
	os.Setenv("QUESMA_licenseKey", licenseKeyPassedAsEnvVar)  // overrides what's in the config file
	cfg := Load()

	assert.Equal(t, licenseKeyPassedAsEnvVar, cfg.LicenseKey)
	assert.Equal(t, DualWriteQueryClickhouse, cfg.Mode)
	assert.Equal(t, 8080, int(cfg.PublicTcpPort))
	assert.Equal(t, "http://localhost:9200", cfg.Elasticsearch.Url.String())
	assert.Equal(t, false, cfg.Elasticsearch.Call)
	assert.Equal(t, "clickhouse://localhost:9000", cfg.ClickHouse.Url.String())
	assert.Equal(t, true, cfg.IngestStatistics)
	assert.Equal(t, "logs", cfg.Logging.Path)
	assert.Equal(t, logLevelPassedAsEnvVar, cfg.Logging.Level.String())
	assert.Equal(t, 10, len(cfg.IndexConfig))

	findIndexConfig := func(name string) *IndexConfiguration {
		if configuration, found := cfg.IndexConfig[name]; found {
			return &configuration
		} else {
			return nil
		}
	}

	tests := []struct {
		name           string
		enabled        bool
		fullTextFields []string
	}{
		{"logs-generic-default", true, []string{"message", "host.name"}},
		{"device-logs", true, []string{"message"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ic := findIndexConfig(tt.name)
			assert.NotNil(t, ic)
			assert.Equal(t, tt.enabled, ic.Enabled)
			assert.Equal(t, tt.fullTextFields, ic.FullTextFields)
		})
	}

}
