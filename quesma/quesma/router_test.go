// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package quesma

import (
	"github.com/stretchr/testify/assert"
	"quesma/quesma/config"
	"quesma/quesma/mux"
	"quesma/schema"
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
			config: indexConfig("index", false),
			want:   true,
		},
		{
			name:   "index disabled",
			index:  "index",
			config: indexConfig("index", true),
			want:   false,
		},
		{
			name:   "index not configured",
			index:  "index",
			config: indexConfig("logs", true),
			want:   false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			req := &mux.Request{Params: map[string]string{"index": tt.index}, Body: tt.body}

			assert.Equalf(t, tt.want, matchedExactQueryPath(&tt.config).Matches(req), "matchedExactQueryPath(%v), index: %s", tt.config, tt.index)
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
			configuration: indexConfig("index", false),
			want:          true,
		},
		{
			name:          "multiple indexes, one internal",
			pattern:       "index,.kibana",
			configuration: indexConfig("index", false),
			want:          false,
		},
		{
			name:          "index explicitly enabled",
			pattern:       "index",
			configuration: indexConfig("index", false),
			want:          true,
		},
		{
			name:          "index explicitly disabled",
			pattern:       "index",
			configuration: indexConfig("index", true),
			want:          false,
		},
		{
			name:          "index enabled, * pattern",
			pattern:       "*",
			configuration: indexConfig("logs-generic-default", false),
			want:          true,
		},
		{
			name:          "index enabled, _all pattern",
			pattern:       "_all",
			configuration: indexConfig("logs-generic-default", false),
			want:          true,
		},
		{
			name:          "index enabled, multiple patterns",
			pattern:       "logs-*-*, logs-*",
			configuration: indexConfig("logs-generic-default", false),
			want:          true,
		},
		{
			name:          "index enabled, multiple patterns",
			pattern:       "logs-*-*, logs-generic-default",
			configuration: indexConfig("logs-generic-default", false),
			want:          true,
		},
		{
			name:          "index disabled, wide pattern",
			pattern:       "logs-*-*",
			configuration: indexConfig("logs-generic-default", true),
			want:          false,
		},
		{
			name:          "index enabled, narrow pattern",
			pattern:       "logs-generic-*",
			configuration: indexConfig("logs-generic-default", false),
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
			configuration: indexConfig("logs-generic-default", false),
			want:          false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			req := &mux.Request{Params: map[string]string{"index": tt.pattern}, Body: tt.body}
			assert.Equalf(t, tt.want, matchedAgainstPattern(&tt.configuration, schema.StaticRegistry{}).Matches(req), "matchedAgainstPattern(%v)", tt.configuration)
		})
	}
}

func indexConfig(name string, elastic bool) config.QuesmaConfiguration {
	var targets []string
	if elastic {
		targets = []string{config.ElasticsearchTarget}
	} else {
		targets = []string{config.ClickhouseTarget}
	}
	return config.QuesmaConfiguration{IndexConfig: map[string]config.IndexConfiguration{name: {Name: name, QueryTarget: targets, IngestTarget: targets}}}
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
			config: indexConfig("logs-generic-default", false),
			want:   true,
		},
		{
			name:   "single index, table not present",
			body:   `{"create":{"_index":"logs-generic-default"}}`,
			config: indexConfig("foo", false),
			want:   false,
		},
		{
			name:   "multiple indexes, table present",
			body:   `{"create":{"_index":"logs-generic-default"}}` + "\n{}\n" + `{"create":{"_index":"logs-generic-default"}}`,
			config: indexConfig("logs-generic-default", false),
			want:   true,
		},
		{
			name:   "multiple indexes, some tables not present",
			body:   `{"create":{"_index":"logs-generic-default"}}` + "\n{}\n" + `{"create":{"_index":"non-existent"}}`,
			config: indexConfig("logs-generic-default", false),
			want:   true,
		},
		{
			name:   "multiple indexes, all tables not present",
			body:   `{"create":{"_index":"not-there"}}` + "\n{}\n" + `{"create":{"_index":"non-existent"}}`,
			config: indexConfig("logs-generic-default", false),
			want:   false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			req := &mux.Request{Body: tt.body}
			assert.Equalf(t, tt.want, matchedAgainstBulkBody(&tt.config).Matches(req), "matchedAgainstBulkBody(%+v)", tt.config)
		})
	}
}
