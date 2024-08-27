// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package schema_test

import (
	"github.com/stretchr/testify/assert"
	"quesma/clickhouse"
	"quesma/quesma/config"
	"quesma/schema"
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
		found          bool
	}{
		{
			name:           "schema not found",
			cfg:            config.QuesmaConfiguration{},
			tableDiscovery: fixedTableProvider{tables: map[string]schema.Table{}},
			tableName:      "nonexistent",
			want:           schema.Schema{},
			found:          false,
		},
		{
			name: "schema inferred, no mappings",
			cfg: config.QuesmaConfiguration{
				IndexConfig: map[string]config.IndexConfiguration{
					"some_table": {Enabled: true},
				},
			},
			tableDiscovery: fixedTableProvider{tables: map[string]schema.Table{
				"some_table": {Columns: map[string]schema.Column{
					"message":    {Name: "message", Type: "String"},
					"event_date": {Name: "event_date", Type: "DateTime64"},
					"count":      {Name: "count", Type: "Int64"},
				}},
			}},
			tableName: "some_table",
			want: schema.NewSchema(map[schema.FieldName]schema.Field{
				"message":    {PropertyName: "message", InternalPropertyName: "message", Type: schema.TypeKeyword},
				"event_date": {PropertyName: "event_date", InternalPropertyName: "event_date", Type: schema.TypeTimestamp},
				"count":      {PropertyName: "count", InternalPropertyName: "count", Type: schema.TypeLong}},
				true),
			found: true,
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
			want: schema.NewSchema(map[schema.FieldName]schema.Field{
				"message":    {PropertyName: "message", InternalPropertyName: "message", Type: schema.TypeKeyword},
				"event_date": {PropertyName: "event_date", InternalPropertyName: "event_date", Type: schema.TypeTimestamp},
				"count":      {PropertyName: "count", InternalPropertyName: "count", Type: schema.TypeLong}},
				true),
			found: true,
		},
		{
			name: "schema inferred, with type mappings not backed by db (deprecated)",
			cfg: config.QuesmaConfiguration{
				IndexConfig: map[string]config.IndexConfiguration{
					"some_table": {Enabled: true, TypeMappings: map[string]string{"message": "keyword"}},
				},
			},
			tableDiscovery: fixedTableProvider{tables: map[string]schema.Table{
				"some_table": {Columns: map[string]schema.Column{
					"event_date": {Name: "event_date", Type: "DateTime64"},
					"count":      {Name: "count", Type: "Int64"},
				}},
			}},
			tableName: "some_table",
			want: schema.NewSchema(map[schema.FieldName]schema.Field{
				"message":    {PropertyName: "message", InternalPropertyName: "message", Type: schema.TypeKeyword},
				"event_date": {PropertyName: "event_date", InternalPropertyName: "event_date", Type: schema.TypeTimestamp},
				"count":      {PropertyName: "count", InternalPropertyName: "count", Type: schema.TypeLong}},
				true),
			found: true,
		},
		{
			name: "schema inferred, with type mappings not backed by db",
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
					"event_date": {Name: "event_date", Type: "DateTime64"},
					"count":      {Name: "count", Type: "Int64"},
				}},
			}},
			tableName: "some_table",
			want: schema.NewSchema(map[schema.FieldName]schema.Field{
				"message":    {PropertyName: "message", InternalPropertyName: "message", Type: schema.TypeKeyword},
				"event_date": {PropertyName: "event_date", InternalPropertyName: "event_date", Type: schema.TypeTimestamp},
				"count":      {PropertyName: "count", InternalPropertyName: "count", Type: schema.TypeLong}},
				true),
			found: true,
		},
		{
			name: "schema explicitly configured, nothing in db",
			cfg: config.QuesmaConfiguration{
				IndexConfig: map[string]config.IndexConfiguration{
					"some_table": {Enabled: true, SchemaConfiguration: &config.SchemaConfiguration{
						Fields: map[config.FieldName]config.FieldConfiguration{
							"message": {Name: "message", Type: "keyword"},
						},
					}},
				},
			},
			tableDiscovery: fixedTableProvider{tables: map[string]schema.Table{}},
			tableName:      "some_table",
			want:           schema.NewSchema(map[schema.FieldName]schema.Field{"message": {PropertyName: "message", InternalPropertyName: "message", Type: schema.TypeKeyword}}, false),
			found:          true,
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
			want: schema.NewSchema(map[schema.FieldName]schema.Field{
				"message":    {PropertyName: "message", InternalPropertyName: "message", Type: schema.TypeKeyword},
				"event_date": {PropertyName: "event_date", InternalPropertyName: "event_date", Type: schema.TypeTimestamp},
				"count":      {PropertyName: "count", InternalPropertyName: "count", Type: schema.TypeLong}},
				true),
			found: true,
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
			want: schema.NewSchemaWithAliases(map[schema.FieldName]schema.Field{
				"message":    {PropertyName: "message", InternalPropertyName: "message", Type: schema.TypeKeyword},
				"event_date": {PropertyName: "event_date", InternalPropertyName: "event_date", Type: schema.TypeTimestamp},
				"count":      {PropertyName: "count", InternalPropertyName: "count", Type: schema.TypeLong}}, map[schema.FieldName]schema.FieldName{
				"message_alias": "message",
			}, true),
			found: true,
		},
		{
			name: "schema inferred, with aliases [deprecated config]",
			cfg: config.QuesmaConfiguration{
				IndexConfig: map[string]config.IndexConfiguration{
					"some_table": {Enabled: true,
						TypeMappings: map[string]string{"message": "keyword"},
						SchemaOverrides: &config.SchemaConfiguration{
							Fields: map[config.FieldName]config.FieldConfiguration{
								"message_alias": {Type: "alias", TargetColumnName: "message"},
							},
						},
					},
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
			want: schema.NewSchemaWithAliases(map[schema.FieldName]schema.Field{
				"message":    {PropertyName: "message", InternalPropertyName: "message", Type: schema.TypeKeyword},
				"event_date": {PropertyName: "event_date", InternalPropertyName: "event_date", Type: schema.TypeTimestamp},
				"count":      {PropertyName: "count", InternalPropertyName: "count", Type: schema.TypeLong}}, map[schema.FieldName]schema.FieldName{
				"message_alias": "message",
			}, true),
			found: true,
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
			found:     false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := schema.NewSchemaRegistry(tt.tableDiscovery, tt.cfg, clickhouse.SchemaTypeAdapter{})
			resultSchema, resultFound := s.FindSchema(tt.tableName)
			if resultFound != tt.found {
				t.Errorf("FindSchema() got1 = %v, want %v", resultFound, tt.found)
			}
			if !reflect.DeepEqual(resultSchema, tt.want) {
				t.Errorf("FindSchema() got = %v, want %v", resultSchema, tt.want)
			}
		})
	}
}

func Test_schemaRegistry_UpdateDynamicConfiguration(t *testing.T) {
	// Test that updating dynamic configuration correctly affects schemas returned by the registry

	tableName := "some_table"
	cfg := config.QuesmaConfiguration{
		IndexConfig: map[string]config.IndexConfiguration{
			tableName: {Enabled: true},
		},
	}
	tableDiscovery := fixedTableProvider{tables: map[string]schema.Table{
		tableName: {Columns: map[string]schema.Column{
			"message":    {Name: "message", Type: "String"},
			"event_date": {Name: "event_date", Type: "DateTime64"},
			"count":      {Name: "count", Type: "Int64"},
		}},
	}}

	s := schema.NewSchemaRegistry(tableDiscovery, cfg, clickhouse.SchemaTypeAdapter{})

	expectedSchema := schema.NewSchema(map[schema.FieldName]schema.Field{
		"message":    {PropertyName: "message", InternalPropertyName: "message", Type: schema.TypeKeyword},
		"event_date": {PropertyName: "event_date", InternalPropertyName: "event_date", Type: schema.TypeTimestamp},
		"count":      {PropertyName: "count", InternalPropertyName: "count", Type: schema.TypeLong}},
		true)
	resultSchema, resultFound := s.FindSchema(schema.TableName(tableName))
	assert.True(t, resultFound, "schema not found")
	if !reflect.DeepEqual(resultSchema, expectedSchema) {
		t.Errorf("FindSchema() got = %v, want %v", resultSchema, expectedSchema)
	}

	// now update the dynamic configuration
	s.UpdateDynamicConfiguration(schema.TableName(tableName), schema.Table{
		Columns: map[string]schema.Column{
			"new_column": {Name: "new_column", Type: "text"},
		},
	})

	expectedSchema = schema.NewSchema(map[schema.FieldName]schema.Field{
		"message":    {PropertyName: "message", InternalPropertyName: "message", Type: schema.TypeKeyword},
		"event_date": {PropertyName: "event_date", InternalPropertyName: "event_date", Type: schema.TypeTimestamp},
		"count":      {PropertyName: "count", InternalPropertyName: "count", Type: schema.TypeLong},
		"new_column": {PropertyName: "new_column", InternalPropertyName: "new_column", Type: schema.TypeText}},
		true)
	resultSchema, resultFound = s.FindSchema(schema.TableName(tableName))
	assert.True(t, resultFound, "schema not found")
	if !reflect.DeepEqual(resultSchema, expectedSchema) {
		t.Errorf("FindSchema() got = %v, want %v", resultSchema, expectedSchema)
	}
}

type fixedTableProvider struct {
	tables map[string]schema.Table
}

func (f fixedTableProvider) TableDefinitions() map[string]schema.Table {
	return f.tables
}
