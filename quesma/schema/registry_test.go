package schema_test

import (
	"mitmproxy/quesma/clickhouse"
	"mitmproxy/quesma/elasticsearch"
	"mitmproxy/quesma/quesma/config"
	"mitmproxy/quesma/schema"
	"reflect"
	"testing"
)

func Test_schemaRegistry_FindSchema(t *testing.T) {
	tests := []struct {
		name           string
		cfg            config.QuesmaConfiguration
		tableDiscovery schema.TableProvider
		tableName      schema.TableName
		want           schema.Schema
		exists         bool
	}{
		{
			name:           "schema not found",
			cfg:            config.QuesmaConfiguration{},
			tableDiscovery: fixedTableProvider{tables: map[string]schema.Table{}},
			tableName:      "nonexistent",
			want:           schema.Schema{},
			exists:         false,
		},
		{
			name: "schema inferred, no mappings",
			cfg:  config.QuesmaConfiguration{},
			tableDiscovery: fixedTableProvider{tables: map[string]schema.Table{
				"some_table": {Columns: map[string]schema.Column{
					"message":    {Name: "message", Type: "String"},
					"event_date": {Name: "event_date", Type: "DateTime64"},
					"count":      {Name: "count", Type: "Int64"},
				}},
			}},
			tableName: "some_table",
			want: schema.Schema{Fields: map[schema.FieldName]schema.Field{
				"message":    {Name: "message", Type: schema.TypeText},
				"event_date": {Name: "event_date", Type: schema.TypeTimestamp},
				"count":      {Name: "count", Type: schema.TypeLong}},
				Aliases: map[schema.FieldName]schema.FieldName{}},
			exists: true,
		},
		{
			name: "schema inferred, with type mappings (deprecated)",
			cfg: config.QuesmaConfiguration{
				IndexConfig: map[string]config.IndexConfiguration{
					"some_table": {Enabled: true, TypeMappings: map[string]string{"message": "keyword"}},
				},
			},
			tableDiscovery: fixedTableProvider{tables: map[string]schema.Table{
				"some_table": {Columns: map[string]schema.Column{
					"message":    {Name: "message", Type: "LowCardinality(String)"},
					"event_date": {Name: "event_date", Type: "DateTime64"},
					"count":      {Name: "count", Type: "Int64"},
				}},
			}},
			tableName: "some_table",
			want: schema.Schema{Fields: map[schema.FieldName]schema.Field{
				"message":    {Name: "message", Type: schema.TypeKeyword},
				"event_date": {Name: "event_date", Type: schema.TypeTimestamp},
				"count":      {Name: "count", Type: schema.TypeLong}},
				Aliases: map[schema.FieldName]schema.FieldName{}},
			exists: true,
		},
		{
			name: "schema inferred, with mapping overrides",
			cfg: config.QuesmaConfiguration{
				IndexConfig: map[string]config.IndexConfiguration{
					"some_table": {Enabled: true, SchemaConfiguration: &config.SchemaConfiguration{
						Fields: map[config.FieldName]config.FieldConfiguration{
							"message": {Name: "message", Type: "keyword"},
						},
					}},
				},
			},
			tableDiscovery: fixedTableProvider{tables: map[string]schema.Table{
				"some_table": {Columns: map[string]schema.Column{
					"message":    {Name: "message", Type: "LowCardinality(String)"},
					"event_date": {Name: "event_date", Type: "DateTime64"},
					"count":      {Name: "count", Type: "Int64"},
				},
				}}},
			tableName: "some_table",
			want: schema.Schema{Fields: map[schema.FieldName]schema.Field{
				"message":    {Name: "message", Type: schema.TypeKeyword},
				"event_date": {Name: "event_date", Type: schema.TypeTimestamp},
				"count":      {Name: "count", Type: schema.TypeLong}},
				Aliases: map[schema.FieldName]schema.FieldName{}},
			exists: true,
		},
		{
			name: "schema inferred, with aliases",
			cfg: config.QuesmaConfiguration{
				IndexConfig: map[string]config.IndexConfiguration{
					"some_table": {Enabled: true, SchemaConfiguration: &config.SchemaConfiguration{
						Fields: map[config.FieldName]config.FieldConfiguration{
							"message":       {Name: "message", Type: "keyword"},
							"message_alias": {Name: "message_alias", Type: "alias", AliasedField: "message"},
						},
					}},
				},
			},
			tableDiscovery: fixedTableProvider{tables: map[string]schema.Table{
				"some_table": {Columns: map[string]schema.Column{
					"message":    {Name: "message", Type: "LowCardinality(String)"},
					"event_date": {Name: "event_date", Type: "DateTime64"},
					"count":      {Name: "count", Type: "Int64"},
				}},
			}},
			tableName: "some_table",
			want: schema.Schema{Fields: map[schema.FieldName]schema.Field{
				"message":    {Name: "message", Type: schema.TypeKeyword},
				"event_date": {Name: "event_date", Type: schema.TypeTimestamp},
				"count":      {Name: "count", Type: schema.TypeLong}},
				Aliases: map[schema.FieldName]schema.FieldName{
					"message_alias": "message",
				}},
			exists: true,
		},
		{
			name: "schema inferred, requesting nonexistent schema",
			cfg: config.QuesmaConfiguration{
				IndexConfig: map[string]config.IndexConfiguration{
					"some_table": {Enabled: true, TypeMappings: map[string]string{"message": "keyword"}},
				},
			},
			tableDiscovery: fixedTableProvider{tables: map[string]schema.Table{
				"some_table": {Columns: map[string]schema.Column{
					"message":    {Name: "message", Type: "LowCardinality(String)"},
					"event_date": {Name: "event_date", Type: "DateTime64"},
					"count":      {Name: "count", Type: "Int64"},
				}},
			}},
			tableName: "foo",
			want:      schema.Schema{},
			exists:    false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := schema.NewSchemaRegistry(tt.tableDiscovery, tt.cfg, clickhouse.SchemaTypeAdapter{}, elasticsearch.SchemaTypeAdapter{})
			s.Start()
			got, got1 := s.FindSchema(tt.tableName)
			if got1 != tt.exists {
				t.Errorf("FindSchema() got1 = %v, want %v", got1, tt.exists)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("FindSchema() got = %v, want %v", got, tt.want)
			}
		})
	}
}

type fixedTableProvider struct {
	tables map[string]schema.Table
}

func (f fixedTableProvider) TableDefinitions() map[string]schema.Table {
	return f.tables
}
