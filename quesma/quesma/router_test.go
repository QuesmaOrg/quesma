package quesma

import (
	"github.com/stretchr/testify/assert"
	"mitmproxy/quesma/quesma/config"
	"testing"
)

func Test_matchedAgainstConfig(t *testing.T) {
	tests := []struct {
		name   string
		index  string
		body   string
		config config.QuesmaConfiguration
		want   bool
	}{
		{
			name:   "index enabled",
			index:  "index",
			config: indexConfig("index", true),
			want:   true,
		},
		{
			name:   "index disabled",
			index:  "index",
			config: indexConfig("index", false),
			want:   false,
		},
		{
			name:   "index not configured",
			index:  "index",
			config: indexConfig("logs", false),
			want:   false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, matchedExact(tt.config)(map[string]string{"index": tt.index}, tt.body), "matchedAgainstConfig(%v), index: %s", tt.config, tt.index)
		})
	}
}

func Test_matchedAgainstPattern(t *testing.T) {
	tests := []struct {
		name          string
		index         string
		body          string
		configuration config.QuesmaConfiguration
		want          bool
	}{
		{
			name:          "multiple indexes, one internal",
			index:         "index,.kibana",
			configuration: indexConfig("index", true),
			want:          false,
		},
		{
			name:          "index enabled and table present",
			index:         "index",
			configuration: indexConfig("index", true),
			want:          true,
		},
		{
			name:          "index disabled and table present",
			index:         "index",
			configuration: indexConfig("index", false),
			want:          false,
		},
		{
			name:          "index disabled and table not present",
			index:         "index",
			configuration: indexConfig("index", false),
			want:          false,
		},
		{
			name:          "index enabled, * pattern, table present",
			index:         "*",
			configuration: indexConfig("logs-generic-default", true),
			want:          true,
		},
		{
			name:          "index enabled, _all pattern, table present",
			index:         "_all",
			configuration: indexConfig("logs-generic-default", true),
			want:          true,
		},
		{
			name:          "index enabled, multiple patterns, table present",
			index:         "logs-*-*, logs-*",
			configuration: indexConfig("logs-generic-default", true),
			want:          true,
		},
		{
			name:          "index enabled, multiple patterns, table present",
			index:         "logs-*-*, logs-generic-default",
			configuration: indexConfig("logs-generic-default", true),
			want:          true,
		},
		{
			name:          "index disabled, wide pattern, table present",
			index:         "logs-*-*",
			configuration: indexConfig("logs-generic-default", false),
			want:          false,
		},
		{
			name:          "index enabled, same pattern, table present",
			index:         "logs-*-*",
			configuration: indexConfig("logs-generic-default", true),
			want:          true,
		},
		{
			name:          "index disabled, same pattern, table present",
			index:         "logs-*-*",
			configuration: indexConfig("logs-generic-default", false),
			want:          false,
		},
		{
			name:          "index disabled, multiple patterns, table present",
			index:         "logs-*-*,*",
			configuration: indexConfig("logs-*-*", false),
			want:          false,
		},
		{
			name:          "index enabled, narrow pattern, table present",
			index:         "logs-generic-*",
			configuration: indexConfig("logs-generic-default", true),
			want:          true,
		},
		{
			name:          "index disabled, narrow pattern, table present",
			index:         "logs-generic-*",
			configuration: indexConfig("logs-*", false),
			want:          false,
		},
		{
			name:          "logs-elastic_agent-*, excluded via config",
			index:         "logs-elastic_agent-*",
			configuration: indexConfig("*", false),
			want:          false,
		},
		{
			name:          "traces-apm*, not configured",
			index:         "traces-apm*",
			configuration: indexConfig("logs-*", true),
			want:          false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, matchedAgainstPattern(tt.configuration)(map[string]string{"index": tt.index}, tt.body), "matchedAgainstPattern(%v)", tt.configuration)
		})
	}
}

func indexConfig(pattern string, enabled bool) config.QuesmaConfiguration {
	return config.QuesmaConfiguration{IndexConfig: []config.IndexConfiguration{{Name: pattern, Enabled: enabled}}}
}

func Test_matchedAgainstBulkBody(t *testing.T) {
	tests := []struct {
		name   string
		body   string
		config config.QuesmaConfiguration
		want   bool
	}{
		{
			name:   "single index, config present",
			body:   `{"create":{"_index":"logs-generic-default"}}`,
			config: indexConfig("logs-generic-default", true),
			want:   true,
		},
		{
			name:   "single index, table not present",
			body:   `{"create":{"_index":"logs-generic-default"}}`,
			config: indexConfig("foo", true),
			want:   false,
		},
		{
			name:   "multiple indexes, table present",
			body:   `{"create":{"_index":"logs-generic-default"}}` + "\n{}\n" + `{"create":{"_index":"logs-generic-default"}}`,
			config: indexConfig("logs-generic-default", true),
			want:   true,
		},
		{
			name:   "multiple indexes, some tables not present",
			body:   `{"create":{"_index":"logs-generic-default"}}` + "\n{}\n" + `{"create":{"_index":"non-existent"}}`,
			config: indexConfig("logs-generic-default", true),
			want:   false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, matchedAgainstBulkBody(tt.config)(map[string]string{}, tt.body), "matchedAgainstBulkBody(%+v)", tt.config)
		})
	}
}
