// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package quesma

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"quesma/clickhouse"
	"quesma/frontend_connectors"
	"quesma/frontend_connectors/routes"
	"quesma/ingest"
	"quesma/quesma/config"
	"quesma/quesma/mux"
	"quesma/quesma/ui"
	"quesma/schema"
	"quesma/table_resolver"
	"quesma/telemetry"
	"strings"
	"testing"
)

var skipMessage = "Skipping test. These will be replaced with table resolver tests."

func Test_matchedAgainstConfig(t *testing.T) {

	t.Skip(skipMessage)

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

	resolver := table_resolver.NewEmptyTableResolver()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			req := &mux.Request{Params: map[string]string{"index": tt.index}, Body: tt.body}
			res := matchedExactQueryPath(resolver).Matches(req)

			assert.Equalf(t, tt.want, res.Matched, "matchedExactQueryPath(%v), index: %s, desision %s", tt.config, tt.index, res.Decision)
		})
	}
}

func Test_matchedAgainstPattern(t *testing.T) {

	t.Skip(skipMessage)

	tests := []struct {
		name          string
		pattern       string
		body          string
		configuration config.QuesmaConfiguration
		registry      schema.Registry
		want          bool
	}{
		{
			name:          "multiple indexes, one non-wildcard matches configuration",
			pattern:       "logs-1,logs-2,foo-*,index",
			configuration: indexConfig("index", false),
			registry:      &schema.StaticRegistry{},
			want:          true,
		},
		{
			name:          "multiple indexes, one wildcard matches configuration",
			pattern:       "logs-1,logs-2,foo-*,index",
			configuration: indexConfig("foo-5", false),
			registry:      &schema.StaticRegistry{},
			want:          true,
		},
		{
			name:          "multiple indexes, one internal",
			pattern:       "index,.kibana",
			configuration: indexConfig("index", false),
			registry:      &schema.StaticRegistry{},
			want:          false,
		},
		{
			name:          "index explicitly enabled",
			pattern:       "index",
			configuration: indexConfig("index", false),
			registry:      &schema.StaticRegistry{},
			want:          true,
		},
		{
			name:          "index explicitly disabled",
			pattern:       "index",
			configuration: indexConfig("index", true),
			registry:      &schema.StaticRegistry{},
			want:          false,
		},
		{
			name:          "index enabled, * pattern",
			pattern:       "*",
			configuration: indexConfig("logs-generic-default", false),
			registry:      &schema.StaticRegistry{},
			want:          true,
		},
		{
			name:          "index enabled, _all pattern",
			pattern:       "_all",
			configuration: indexConfig("logs-generic-default", false),
			registry:      &schema.StaticRegistry{},
			want:          true,
		},
		{
			name:          "index enabled, multiple patterns",
			pattern:       "logs-*-*, logs-*",
			configuration: indexConfig("logs-generic-default", false),
			registry:      &schema.StaticRegistry{},
			want:          true,
		},
		{
			name:          "index enabled, multiple patterns",
			pattern:       "logs-*-*, logs-generic-default",
			configuration: indexConfig("logs-generic-default", false),
			registry:      &schema.StaticRegistry{},
			want:          true,
		},
		{
			name:          "index disabled, wide pattern",
			pattern:       "logs-*-*",
			configuration: indexConfig("logs-generic-default", true),
			registry:      &schema.StaticRegistry{},
			want:          false,
		},
		{
			name:          "index enabled, narrow pattern",
			pattern:       "logs-generic-*",
			configuration: indexConfig("logs-generic-default", false),
			registry:      &schema.StaticRegistry{},
			want:          true,
		},
		{
			name:          "logs-elastic_agent-*",
			pattern:       "logs-elastic_agent-*",
			configuration: indexConfig("logs-generic-default", false),
			registry:      &schema.StaticRegistry{},
			want:          false,
		},
		{
			name:          "traces-apm*, not configured",
			pattern:       "traces-apm*",
			configuration: indexConfig("logs-generic-default", false),
			registry:      &schema.StaticRegistry{},
			want:          false,
		},
		{
			name:          "index autodiscovery (non-wildcard)",
			pattern:       "my_index",
			configuration: withAutodiscovery(indexConfig("another-index", false)),
			registry: &schema.StaticRegistry{
				Tables: map[schema.TableName]schema.Schema{
					"my_index": {ExistsInDataSource: true},
				},
			},
			want: true,
		},
		{
			name:          "index autodiscovery (wildcard)",
			pattern:       "my_index*",
			configuration: withAutodiscovery(indexConfig("another-index", false)),
			registry: &schema.StaticRegistry{
				Tables: map[schema.TableName]schema.Schema{
					"my_index8": {ExistsInDataSource: true},
				},
			},
			want: true,
		},
	}

	resolver := table_resolver.NewEmptyTableResolver()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			req := &mux.Request{Params: map[string]string{"index": tt.pattern}, Body: tt.body}
			assert.Equalf(t, tt.want, matchedAgainstPattern(resolver).Matches(req).Matched, "matchedAgainstPattern(%v)", tt.configuration)
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

func withAutodiscovery(cfg config.QuesmaConfiguration) config.QuesmaConfiguration {
	cfg.AutodiscoveryEnabled = true
	return cfg
}

func Test_matchedAgainstBulkBody(t *testing.T) {

	t.Skip(skipMessage)

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

	resolver := table_resolver.NewEmptyTableResolver()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			req := &mux.Request{Body: tt.body}

			assert.Equalf(t, tt.want, matchedAgainstBulkBody(&tt.config, resolver).Matches(req), "matchedAgainstBulkBody(%+v)", tt.config)
		})
	}
}

const testIndexName = "indexName"

func TestConfigureRouter(t *testing.T) {
	cfg := &config.QuesmaConfiguration{
		IndexConfig: map[string]config.IndexConfiguration{
			testIndexName: {
				Name: testIndexName,
			},
		},
	}
	tr := TestTableResolver{}
	testRouter := ConfigureRouter(cfg, schema.NewSchemaRegistry(fixedTableProvider{}, cfg, clickhouse.SchemaTypeAdapter{}), &clickhouse.LogManager{}, &ingest.IngestProcessor{}, &ui.QuesmaManagementConsole{}, telemetry.NewPhoneHomeAgent(cfg, nil, ""), &QueryRunner{}, tr)

	tests := []struct {
		path                string
		method              string
		shouldReturnHandler bool
	}{
		// Routes explicitly registered in the router code
		{routes.ClusterHealthPath, "GET", true},
		// {routes.BulkPath, "POST", true}, // TODO later on, it requires body parsing
		{routes.IndexRefreshPath, "POST", true},
		{routes.IndexDocPath, "POST", true},
		{routes.IndexBulkPath, "POST", true},
		{routes.IndexBulkPath, "PUT", true},
		{routes.ResolveIndexPath, "GET", true},
		{routes.IndexCountPath, "GET", true},
		{routes.GlobalSearchPath, "GET", false},
		{routes.GlobalSearchPath, "POST", false},
		{routes.GlobalSearchPath, "PUT", false},
		{routes.IndexSearchPath, "GET", true},
		{routes.IndexSearchPath, "POST", true},
		{routes.IndexAsyncSearchPath, "POST", true},
		{routes.IndexMappingPath, "PUT", true},
		{routes.IndexMappingPath, "GET", true},
		{routes.AsyncSearchStatusPath, "GET", true},
		{routes.AsyncSearchIdPath, "GET", true},
		{routes.AsyncSearchIdPath, "DELETE", true},
		{routes.FieldCapsPath, "GET", true},
		{routes.FieldCapsPath, "POST", true},
		{routes.TermsEnumPath, "POST", true},
		{routes.EQLSearch, "GET", true},
		{routes.EQLSearch, "POST", true},
		{routes.IndexPath, "PUT", true},
		{routes.IndexPath, "GET", true},
		{routes.QuesmaTableResolverPath, "GET", true},
		// Few cases where the router should not match
		{"/invalid/path", "GET", false},
		{routes.ClusterHealthPath, "POST", false},
		//{routes.BulkPath, "GET", false}, // TODO later on, it requires body parsing
		{routes.IndexRefreshPath, "GET", false},
		{routes.IndexDocPath, "GET", false},
		{routes.IndexBulkPath, "DELETE", false},
		{routes.ResolveIndexPath, "POST", false},
		{routes.IndexCountPath, "POST", false},
		{routes.IndexSearchPath, "DELETE", false},
		{routes.IndexAsyncSearchPath, "GET", false},
		{routes.IndexMappingPath, "POST", false},
		{routes.AsyncSearchStatusPath, "POST", false},
		{routes.AsyncSearchIdPath, "PUT", false},
		{routes.FieldCapsPath, "DELETE", false},
		{routes.TermsEnumPath, "GET", false},
		{routes.EQLSearch, "DELETE", false},
		{routes.IndexPath, "POST", false},
		{routes.QuesmaTableResolverPath, "POST", false},
		{routes.QuesmaTableResolverPath, "PUT", false},
		{routes.QuesmaTableResolverPath, "DELETE", false},
	}

	for _, tt := range tests {
		tt.path = strings.Replace(tt.path, ":id", "quesma_async_absurd_test_id", -1)
		tt.path = strings.Replace(tt.path, ":index", testIndexName, -1)
		t.Run(tt.method+"-at-"+tt.path, func(t *testing.T) {
			req := &mux.Request{Path: tt.path, Method: tt.method}
			reqHandler, _ := testRouter.Matches(req)
			assert.Equal(t, tt.shouldReturnHandler, reqHandler != nil, "Expected route match result for path: %s and method: %s", tt.path, tt.method)
		})
	}
}

// TestTableResolver should be used only within tests
type TestTableResolver struct{}

func (t TestTableResolver) Start() {}

func (t TestTableResolver) Stop() {}

func (t TestTableResolver) Resolve(_ string, indexPattern string) *frontend_connectors.Decision {
	if indexPattern == testIndexName {
		return &frontend_connectors.Decision{
			UseConnectors: []frontend_connectors.ConnectorDecision{
				&frontend_connectors.ConnectorDecisionClickhouse{},
			},
		}
	} else {
		return &frontend_connectors.Decision{
			Err:          fmt.Errorf("TestTableResolver err"),
			Reason:       "TestTableResolver reason",
			ResolverName: "TestTableResolver",
		}
	}
}

func (t TestTableResolver) Pipelines() []string { return []string{} }

func (t TestTableResolver) RecentDecisions() []frontend_connectors.PatternDecisions {
	return []frontend_connectors.PatternDecisions{}
}
