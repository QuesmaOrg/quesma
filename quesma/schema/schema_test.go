package schema

import (
	"mitmproxy/quesma/clickhouse"
	"mitmproxy/quesma/concurrent"
	"mitmproxy/quesma/quesma/config"
	"reflect"
	"testing"
)

func Test_schemaRegistry_FindSchema(t *testing.T) {
	tests := []struct {
		name           string
		cfg            config.QuesmaConfiguration
		tableDiscovery clickhouse.TableDiscovery
		tableName      TableName
		want           Schema
		exists         bool
	}{
		{
			name:           "schema not found",
			cfg:            config.QuesmaConfiguration{},
			tableDiscovery: fakeTableDiscovery{map[string]*clickhouse.Table{}},
			tableName:      "nonexistent",
			want:           Schema{},
			exists:         false,
		},
		{
			name: "schema inferred, no mappings",
			cfg:  config.QuesmaConfiguration{},
			tableDiscovery: fakeTableDiscovery{map[string]*clickhouse.Table{
				"some_table": {
					Name: "some_table",
					Cols: map[string]*clickhouse.Column{
						"message":    {Name: "message", Type: clickhouse.NewBaseType("LowCardinality(String)")},
						"event_date": {Name: "event_date", Type: clickhouse.NewBaseType("DateTime64")},
						"count":      {Name: "count", Type: clickhouse.NewBaseType("Int64")},
					},
				},
			}},
			tableName: "some_table",
			want: Schema{Fields: map[FieldName]Field{
				"message":    {Name: "message", Type: "text"},
				"event_date": {Name: "event_date", Type: "timestamp"},
				"count":      {Name: "count", Type: "long"}},
			},
			exists: true,
		},
		{
			name: "schema inferred, with type mappings",
			cfg: config.QuesmaConfiguration{
				IndexConfig: map[string]config.IndexConfiguration{
					"some_table": {Enabled: true, TypeMappings: map[string]string{"message": "keyword"}},
				},
			},
			tableDiscovery: fakeTableDiscovery{map[string]*clickhouse.Table{
				"some_table": {
					Name: "some_table",
					Cols: map[string]*clickhouse.Column{
						"message":    {Name: "message", Type: clickhouse.NewBaseType("LowCardinality(String)")},
						"event_date": {Name: "event_date", Type: clickhouse.NewBaseType("DateTime64")},
						"count":      {Name: "count", Type: clickhouse.NewBaseType("Int64")},
					},
				},
			}},
			tableName: "some_table",
			want: Schema{Fields: map[FieldName]Field{
				"message":    {Name: "message", Type: "keyword"},
				"event_date": {Name: "event_date", Type: "timestamp"},
				"count":      {Name: "count", Type: "long"}},
			},
			exists: true,
		},
		{
			name: "schema inferred, requesting nonexistent schema",
			cfg: config.QuesmaConfiguration{
				IndexConfig: map[string]config.IndexConfiguration{
					"some_table": {Enabled: true, TypeMappings: map[string]string{"message": "keyword"}},
				},
			},
			tableDiscovery: fakeTableDiscovery{map[string]*clickhouse.Table{
				"some_table": {
					Name: "some_table",
					Cols: map[string]*clickhouse.Column{
						"message":    {Name: "message", Type: clickhouse.NewBaseType("LowCardinality(String)")},
						"event_date": {Name: "event_date", Type: clickhouse.NewBaseType("DateTime64")},
						"count":      {Name: "count", Type: clickhouse.NewBaseType("Int64")},
					},
				},
			}},
			tableName: "foo",
			want:      Schema{},
			exists:    false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := NewSchemaRegistry(tt.tableDiscovery, tt.cfg)
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

type fakeTableDiscovery struct {
	tables map[string]*clickhouse.Table
}

func (f fakeTableDiscovery) ReloadTableDefinitions() {
	// no-op
}

func (f fakeTableDiscovery) TableDefinitions() *clickhouse.TableMap {
	return concurrent.NewMapFrom(f.tables)
}

func (f fakeTableDiscovery) TableDefinitionsFetchError() error {
	return nil
}
