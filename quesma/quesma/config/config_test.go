package config

import (
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
)

func TestIndexConfiguration_Matches(t *testing.T) {
	type fields struct {
		NamePattern string
		Enabled     bool
	}
	tests := []struct {
		name      string
		fields    fields
		indexName string
		want      bool
	}{
		{"logs-generic-default", fields{"logs-generic-default", true}, "logs-generic-default", true},
		{"logs-generic-default", fields{"logs-generic-default", true}, "logs-generic-default2", false},
		{"logs-generic-*", fields{"logs-generic-*", true}, "logs-generic-default", true},
		{"logs-generic-*", fields{"logs-generic-*", true}, "logs2-generic-default", false},
		{"logs-*-*", fields{"logs-*-*", true}, "logs-generic-default", true},
		{"logs-*-*", fields{"logs-*-*", true}, "generic-default", false},
		{"logs-*", fields{"logs-*", true}, "logs-generic-default", true},
		{"logs-*", fields{"logs-*", true}, "blogs-generic-default", false},
		{"*", fields{"*", true}, "logs-generic-default", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := IndexConfiguration{
				NamePattern: tt.fields.NamePattern,
				Enabled:     tt.fields.Enabled,
			}
			assert.Equalf(t, tt.want, c.Matches(tt.indexName), "Matches(%v)", tt.indexName)
		})
	}
}

func TestIndexConfiguration_FullTextField(t *testing.T) {

	indexConfig := []IndexConfiguration{
		{
			NamePattern:    "none",
			Enabled:        true,
			FullTextFields: []string{},
		},
		{
			NamePattern:    "foo-*",
			Enabled:        true,
			FullTextFields: []string{"sometext"},
		},
		{
			NamePattern:    "bar-*",
			Enabled:        true,
			FullTextFields: []string{},
		},
		{
			NamePattern:    "logs-*",
			Enabled:        true,
			FullTextFields: []string{"message", "content"},
		},
	}

	cfg := QuesmaConfiguration{
		IndexConfig: indexConfig,
	}

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

func TestQuesmaConfigurationParser_Parse(t *testing.T) {

	given := `
quesma:
  mode: "dual-write-query-clickhouse"
  license_key: "1234567890"
  port: 8080  # public tcp port to listen for incoming traffic
  elasticsearch_url: "http://localhost:9200"
  clickhouse_url: "clickhouse://localhost:9000"
  ingest_statistics: true
  logs_path: "logs"
  log_level: "info"
  index:
    kibana_sample_data*:
      enabled: true
    kafka-example-topi*:
      enabled: true
    logs-generic-*:
      enabled: true
      fulltext_fields: message,host.name
    device*:
      enabled: true
      fulltext_fields: 
`

	// then
	v := viper.New()
	v.SetConfigType(defaultConfigType)
	v.ReadConfig(strings.NewReader(given))

	if err := v.ReadConfig(strings.NewReader(given)); err != nil {
		t.Errorf("Error reading config: %v", err)
	}

	p := NewQuesmaConfigurationParser(v)
	cfg := p.Parse()

	// when

	assert.Equal(t, "1234567890", cfg.LicenseKey)
	assert.Equal(t, DualWriteQueryClickhouse, cfg.Mode)
	assert.Equal(t, int(8080), int(cfg.PublicTcpPort))
	assert.Equal(t, "http://localhost:9200", cfg.ElasticsearchUrl.String())
	assert.Equal(t, "clickhouse://localhost:9000", cfg.ClickHouseUrl.String())
	assert.Equal(t, true, cfg.IngestStatistics)
	assert.Equal(t, "logs", cfg.LogsPath)
	assert.Equal(t, "info", cfg.LogLevel.String())
	assert.Equal(t, 4, len(cfg.IndexConfig))

	findIndexConfig := func(name string) *IndexConfiguration {
		for _, ic := range cfg.IndexConfig {
			if ic.NamePattern == name {
				return &ic
			}
		}
		return nil
	}

	tests := []struct {
		name           string
		enabled        bool
		fullTextFields []string
	}{
		{"kibana_sample_data*", true, []string{"message"}},
		{"kafka-example-topi*", true, []string{"message"}},
		{"logs-generic-*", true, []string{"message", "host.name"}},
		{"device*", true, []string{}},
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
