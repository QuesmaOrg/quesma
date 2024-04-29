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
		pattern       string
		body          string
		configuration config.QuesmaConfiguration
		want          bool
	}{
		{
			name:          "multiple indexes, one matches configuration",
			pattern:       "logs-1,logs-2,foo-*,index",
			configuration: indexConfig("index", true),
			want:          true,
		},
		{
			name:          "multiple indexes, one internal",
			pattern:       "index,.kibana",
			configuration: indexConfig("index", true),
			want:          false,
		},
		{
			name:          "index explicitly enabled",
			pattern:       "index",
			configuration: indexConfig("index", true),
			want:          true,
		},
		{
			name:          "index explicitly disabled",
			pattern:       "index",
			configuration: indexConfig("index", false),
			want:          false,
		},
		{
			name:          "index enabled, * pattern",
			pattern:       "*",
			configuration: indexConfig("logs-generic-default", true),
			want:          true,
		},
		{
			name:          "index enabled, _all pattern",
			pattern:       "_all",
			configuration: indexConfig("logs-generic-default", true),
			want:          true,
		},
		{
			name:          "index enabled, multiple patterns",
			pattern:       "logs-*-*, logs-*",
			configuration: indexConfig("logs-generic-default", true),
			want:          true,
		},
		{
			name:          "index enabled, multiple patterns",
			pattern:       "logs-*-*, logs-generic-default",
			configuration: indexConfig("logs-generic-default", true),
			want:          true,
		},
		{
			name:          "index disabled, wide pattern",
			pattern:       "logs-*-*",
			configuration: indexConfig("logs-generic-default", false),
			want:          false,
		},
		{
			name:          "index enabled, narrow pattern",
			pattern:       "logs-generic-*",
			configuration: indexConfig("logs-generic-default", true),
			want:          true,
		},
		{
			name:          "logs-elastic_agent-*",
			pattern:       "logs-elastic_agent-*",
			configuration: indexConfig("logs-generic-default", false),
			want:          false,
		},
		{
			name:          "traces-apm*, not configured",
			pattern:       "traces-apm*",
			configuration: indexConfig("logs-generic-default", true),
			want:          false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, matchedAgainstPattern(tt.configuration)(map[string]string{"index": tt.pattern}, tt.body), "matchedAgainstPattern(%v)", tt.configuration)
		})
	}
}

func indexConfig(name string, enabled bool) config.QuesmaConfiguration {
	return config.QuesmaConfiguration{IndexConfig: map[string]config.IndexConfiguration{name: {Name: name, Enabled: enabled}}}
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
