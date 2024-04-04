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
			name:   "index enabled via pattern",
			index:  "index",
			config: indexConfig("ind*", true),
			want:   true,
		},
		{
			name:   "index enabled via complex pattern",
			index:  "index",
			config: indexConfig("i*d*x", true),
			want:   true,
		},
		{
			name:   "index disabled via complex pattern",
			index:  "index",
			config: indexConfig("i*d*x", false),
			want:   false,
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
			config: indexConfig("logs-*", false),
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
		tables        []string
		configuration config.QuesmaConfiguration
		want          bool
	}{
		{
			name:          "multiple indexes, one internal",
			index:         "index,.kibana",
			tables:        []string{"index"},
			configuration: indexConfig("index", true),
			want:          false,
		},
		{
			name:          "index enabled and table present",
			index:         "index",
			tables:        []string{"index"},
			configuration: indexConfig("index", true),
			want:          true,
		},
		{
			name:          "index disabled and table present",
			index:         "index",
			tables:        []string{"index"},
			configuration: indexConfig("index", false),
			want:          false,
		},
		{
			name:          "index enabled and table not present",
			index:         "index",
			tables:        []string{},
			configuration: indexConfig("index", true),
			want:          false,
		},
		{
			name:          "index disabled and table not present",
			index:         "index",
			tables:        []string{},
			configuration: indexConfig("index", false),
			want:          false,
		},
		{
			name:          "index enabled, wide pattern, table present",
			index:         "logs-*-*",
			tables:        []string{"logs-generic-default"},
			configuration: indexConfig("logs-generic-*", true),
			want:          true,
		},
		{
			name:          "index enabled, * pattern, table present",
			index:         "*",
			tables:        []string{"logs-generic-default"},
			configuration: indexConfig("logs-generic-*", true),
			want:          true,
		},
		{
			name:          "index enabled, _all pattern, table present",
			index:         "_all",
			tables:        []string{"logs-generic-default"},
			configuration: indexConfig("logs-generic-*", true),
			want:          true,
		},
		{
			name:          "index enabled, multiple patterns, table present",
			index:         "logs-*-*, logs-*",
			tables:        []string{"logs-generic-default"},
			configuration: indexConfig("logs-generic-*", true),
			want:          true,
		},
		{
			name:          "index enabled, multiple patterns, table present",
			index:         "logs-*-*, logs-generic-default",
			tables:        []string{"logs-generic-default"},
			configuration: indexConfig("logs-generic-*", true),
			want:          true,
		},
		{
			name:          "index disabled, wide pattern, table present",
			index:         "logs-*-*",
			tables:        []string{"logs-generic-default"},
			configuration: indexConfig("logs-generic-*", false),
			want:          false,
		},
		{
			name:          "index enabled, same pattern, table present",
			index:         "logs-*-*",
			tables:        []string{"logs-generic-default"},
			configuration: indexConfig("logs-*-*", true),
			want:          true,
		},
		{
			name:          "index disabled, same pattern, table present",
			index:         "logs-*-*",
			tables:        []string{"logs-generic-default"},
			configuration: indexConfig("logs-*-*", false),
			want:          false,
		},
		{
			name:          "index disabled, multiple patterns, table present",
			index:         "logs-*-*,*",
			tables:        []string{"logs-generic-default"},
			configuration: indexConfig("logs-*-*", false),
			want:          false,
		},
		{
			name:          "index enabled, narrow pattern, table present",
			index:         "logs-generic-*",
			tables:        []string{"logs-generic-default"},
			configuration: indexConfig("logs-*", true),
			want:          true,
		},
		{
			name:          "index disabled, narrow pattern, table present",
			index:         "logs-generic-*",
			tables:        []string{"logs-generic-default"},
			configuration: indexConfig("logs-*", false),
			want:          false,
		},
		{
			name:          "logs-elastic_agent-*, excluded via config",
			index:         "logs-elastic_agent-*",
			tables:        []string{},
			configuration: indexConfig("*", false),
			want:          false,
		},
		{
			name:          "traces-apm*, not configured",
			index:         "traces-apm*",
			tables:        []string{},
			configuration: indexConfig("logs-*", true),
			want:          false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, matchedAgainstPattern(tt.configuration, func() []string {
				return tt.tables
			})(map[string]string{"index": tt.index}, tt.body), "matchedAgainstPattern(%v)", tt.configuration)
		})
	}
}

func indexConfig(pattern string, enabled bool) config.QuesmaConfiguration {
	return config.QuesmaConfiguration{IndexConfig: []config.IndexConfiguration{{NamePattern: pattern, Enabled: enabled}}}
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
			config: indexConfig("logs-*", true),
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
			config: indexConfig("logs-*", true),
			want:   true,
		},
		{
			name:   "multiple indexes, some tables not present",
			body:   `{"create":{"_index":"logs-generic-default"}}` + "\n{}\n" + `{"create":{"_index":"non-existent"}}`,
			config: indexConfig("logs-*", true),
			want:   false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, matchedAgainstBulkBody(tt.config)(map[string]string{}, tt.body), "matchedAgainstBulkBody(%+v)", tt.config)
		})
	}
}
