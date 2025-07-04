// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package schema_test

import (
	"github.com/QuesmaOrg/quesma/platform/clickhouse"
	"github.com/QuesmaOrg/quesma/platform/config"
	"github.com/QuesmaOrg/quesma/platform/schema"
	"github.com/QuesmaOrg/quesma/platform/types"
	"github.com/QuesmaOrg/quesma/platform/util"
	"github.com/k0kubun/pp"
	"github.com/stretchr/testify/assert"
	"reflect"
	"testing"
)

func Test_schemaRegistry_FindSchema(t *testing.T) {
	tests := []struct {
		name           string
		cfg            config.QuesmaConfiguration
		tableDiscovery schema.TableProvider
		tableName      schema.IndexName
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
					"some_table": {},
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
				"message":    {PropertyName: "message", InternalPropertyName: "message", Type: schema.QuesmaTypeText, InternalPropertyType: "String"},
				"event_date": {PropertyName: "event_date", InternalPropertyName: "event_date", Type: schema.QuesmaTypeTimestamp, InternalPropertyType: "DateTime64"},
				"count":      {PropertyName: "count", InternalPropertyName: "count", Type: schema.QuesmaTypeLong, InternalPropertyType: "Int64"}},
				true, ""),
			found: true,
		},
		{
			name: "schema inferred, with type mappings (deprecated)",
			cfg: config.QuesmaConfiguration{
				IndexConfig: map[string]config.IndexConfiguration{
					"some_table": {
						SchemaOverrides: &config.SchemaConfiguration{
							Fields: map[config.FieldName]config.FieldConfiguration{
								"message": {Type: "keyword"},
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
			want: schema.NewSchema(map[schema.FieldName]schema.Field{
				"message":    {PropertyName: "message", InternalPropertyName: "message", Type: schema.QuesmaTypeKeyword, InternalPropertyType: "LowCardinality(String)"},
				"event_date": {PropertyName: "event_date", InternalPropertyName: "event_date", Type: schema.QuesmaTypeTimestamp, InternalPropertyType: "DateTime64"},
				"count":      {PropertyName: "count", InternalPropertyName: "count", Type: schema.QuesmaTypeLong, InternalPropertyType: "Int64"}},

				true, ""),
			found: true,
		},
		{
			name: "schema inferred, with type mappings not backed by db (deprecated)",
			cfg: config.QuesmaConfiguration{
				IndexConfig: map[string]config.IndexConfiguration{
					"some_table": {
						SchemaOverrides: &config.SchemaConfiguration{
							Fields: map[config.FieldName]config.FieldConfiguration{
								"message": {Type: "keyword"},
							},
						},
					},
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
				"message":    {PropertyName: "message", InternalPropertyName: "message", Type: schema.QuesmaTypeKeyword, InternalPropertyType: ""},
				"event_date": {PropertyName: "event_date", InternalPropertyName: "event_date", Type: schema.QuesmaTypeTimestamp, InternalPropertyType: "DateTime64"},
				"count":      {PropertyName: "count", InternalPropertyName: "count", Type: schema.QuesmaTypeLong, InternalPropertyType: "Int64"}},
				true, ""),
			found: true,
		},
		{
			name: "schema inferred, with type mappings not backed by db",
			cfg: config.QuesmaConfiguration{
				IndexConfig: map[string]config.IndexConfiguration{
					"some_table": {
						SchemaOverrides: &config.SchemaConfiguration{
							Fields: map[config.FieldName]config.FieldConfiguration{
								"message": {Type: "keyword"},
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
				"message":    {PropertyName: "message", InternalPropertyName: "message", Type: schema.QuesmaTypeKeyword},
				"event_date": {PropertyName: "event_date", InternalPropertyName: "event_date", Type: schema.QuesmaTypeTimestamp, InternalPropertyType: "DateTime64"},
				"count":      {PropertyName: "count", InternalPropertyName: "count", Type: schema.QuesmaTypeLong, InternalPropertyType: "Int64"}},
				true, ""),
			found: true,
		},
		{
			name: "schema explicitly configured, nothing in db",
			cfg: config.QuesmaConfiguration{
				IndexConfig: map[string]config.IndexConfiguration{
					"some_table": {
						SchemaOverrides: &config.SchemaConfiguration{
							Fields: map[config.FieldName]config.FieldConfiguration{
								"message": {Type: "keyword"},
							},
						}},
				},
			},
			tableDiscovery: fixedTableProvider{tables: map[string]schema.Table{}},
			tableName:      "some_table",
			want:           schema.NewSchema(map[schema.FieldName]schema.Field{"message": {PropertyName: "message", InternalPropertyName: "message", Type: schema.QuesmaTypeKeyword}}, false, ""),
			found:          true,
		},
		{
			name: "schema inferred, with mapping overrides",
			cfg: config.QuesmaConfiguration{
				IndexConfig: map[string]config.IndexConfiguration{
					"some_table": {
						SchemaOverrides: &config.SchemaConfiguration{
							Fields: map[config.FieldName]config.FieldConfiguration{
								"message": {Type: "keyword"},
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
				"message":    {PropertyName: "message", InternalPropertyName: "message", Type: schema.QuesmaTypeKeyword, InternalPropertyType: "LowCardinality(String)"},
				"event_date": {PropertyName: "event_date", InternalPropertyName: "event_date", Type: schema.QuesmaTypeTimestamp, InternalPropertyType: "DateTime64"},
				"count":      {PropertyName: "count", InternalPropertyName: "count", Type: schema.QuesmaTypeLong, InternalPropertyType: "Int64"}},
				true, ""),
			found: true,
		},
		{
			name: "schema inferred, with aliases",
			cfg: config.QuesmaConfiguration{
				IndexConfig: map[string]config.IndexConfiguration{
					"some_table": {
						SchemaOverrides: &config.SchemaConfiguration{
							Fields: map[config.FieldName]config.FieldConfiguration{
								"message":       {Type: "keyword"},
								"message_alias": {Type: "alias", TargetColumnName: "message"},
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
				"message":    {PropertyName: "message", InternalPropertyName: "message", Type: schema.QuesmaTypeKeyword, InternalPropertyType: "LowCardinality(String)"},
				"event_date": {PropertyName: "event_date", InternalPropertyName: "event_date", Type: schema.QuesmaTypeTimestamp, InternalPropertyType: "DateTime64"},
				"count":      {PropertyName: "count", InternalPropertyName: "count", Type: schema.QuesmaTypeLong, InternalPropertyType: "Int64"}}, map[schema.FieldName]schema.FieldName{
				"message_alias": "message",
			}, true, ""),
			found: true,
		},
		{
			name: "schema inferred, with aliases [deprecated config]",
			cfg: config.QuesmaConfiguration{
				IndexConfig: map[string]config.IndexConfiguration{
					"some_table": {
						SchemaOverrides: &config.SchemaConfiguration{
							Fields: map[config.FieldName]config.FieldConfiguration{
								"message_alias": {Type: "alias", TargetColumnName: "message"},
								"message":       {Type: "keyword"},
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
				"message":    {PropertyName: "message", InternalPropertyName: "message", Type: schema.QuesmaTypeKeyword, InternalPropertyType: "LowCardinality(String)"},
				"event_date": {PropertyName: "event_date", InternalPropertyName: "event_date", Type: schema.QuesmaTypeTimestamp, InternalPropertyType: "DateTime64"},
				"count":      {PropertyName: "count", InternalPropertyName: "count", Type: schema.QuesmaTypeLong, InternalPropertyType: "Int64"}}, map[schema.FieldName]schema.FieldName{
				"message_alias": "message",
			}, true, ""),
			found: true,
		},
		{
			name: "schema inferred, requesting nonexistent schema",
			cfg: config.QuesmaConfiguration{
				IndexConfig: map[string]config.IndexConfiguration{
					"some_table": {
						SchemaOverrides: &config.SchemaConfiguration{
							Fields: map[config.FieldName]config.FieldConfiguration{
								"message": {Type: "keyword"},
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
			tableName: "foo",
			want:      schema.Schema{},
			found:     false,
		},
	}
	for i, tt := range tests {
		t.Run(util.PrettyTestName(tt.name, i), func(t *testing.T) {
			s := schema.NewSchemaRegistry(tt.tableDiscovery, &tt.cfg, clickhouse.ClickhouseSchemaTypeAdapter{})
			s.Start()
			defer s.Stop()

			resultSchema, resultFound := s.FindSchema(tt.tableName)
			if resultFound != tt.found {
				t.Errorf("FindSchema() got1 = %v, want %v", resultFound, tt.found)
			}
			if !reflect.DeepEqual(resultSchema, tt.want) {
				pp.Println("Expected:")
				pp.Println(tt.want)
				pp.Println("Actual:")
				pp.Println(resultSchema)
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
			tableName: {
				QueryTarget: []string{config.ClickhouseTarget}, IngestTarget: []string{config.ClickhouseTarget},
			},
		},
	}
	tableDiscovery := fixedTableProvider{tables: map[string]schema.Table{
		tableName: {Columns: map[string]schema.Column{
			"message":    {Name: "message", Type: "String"},
			"event_date": {Name: "event_date", Type: "DateTime64"},
			"count":      {Name: "count", Type: "Int64"},
		}},
	}}

	s := schema.NewSchemaRegistry(tableDiscovery, &cfg, clickhouse.ClickhouseSchemaTypeAdapter{})
	s.Start()
	defer s.Stop()

	expectedSchema := schema.NewSchema(map[schema.FieldName]schema.Field{
		"message":    {PropertyName: "message", InternalPropertyName: "message", Type: schema.QuesmaTypeText, InternalPropertyType: "String"},
		"event_date": {PropertyName: "event_date", InternalPropertyName: "event_date", Type: schema.QuesmaTypeTimestamp, InternalPropertyType: "DateTime64"},
		"count":      {PropertyName: "count", InternalPropertyName: "count", Type: schema.QuesmaTypeLong, InternalPropertyType: "Int64"}},
		true, "")
	resultSchema, resultFound := s.FindSchema(schema.IndexName(tableName))
	assert.True(t, resultFound, "schema not found")
	if !reflect.DeepEqual(resultSchema, expectedSchema) {
		pp.Println("Expected:", expectedSchema)
		pp.Println("Actual:", resultSchema)
		t.Errorf("FindSchema() got = %v, want %v", resultSchema, expectedSchema)
	}

	// now update the dynamic configuration
	s.UpdateDynamicConfiguration(schema.IndexName(tableName), schema.Table{
		Columns: map[string]schema.Column{
			"new_column": {Name: "new_column", Type: "text"},
		},
	})

	expectedSchema = schema.NewSchema(map[schema.FieldName]schema.Field{
		"message":    {PropertyName: "message", InternalPropertyName: "message", Type: schema.QuesmaTypeText, InternalPropertyType: "String"},
		"event_date": {PropertyName: "event_date", InternalPropertyName: "event_date", Type: schema.QuesmaTypeTimestamp, InternalPropertyType: "DateTime64"},
		"count":      {PropertyName: "count", InternalPropertyName: "count", Type: schema.QuesmaTypeLong, InternalPropertyType: "Int64"},
		"new_column": {PropertyName: "new_column", InternalPropertyName: "new_column", Type: schema.QuesmaTypeText, Origin: schema.FieldSourceMapping}},
		true, "")
	resultSchema, resultFound = s.FindSchema(schema.IndexName(tableName))
	assert.True(t, resultFound, "schema not found")
	if !reflect.DeepEqual(resultSchema, expectedSchema) {
		pp.Println("Expected:", expectedSchema)
		pp.Println("Actual:", resultSchema)

		t.Errorf("FindSchema() got = %v, want %v", resultSchema, expectedSchema)
	}
}

type fixedTableProvider struct {
	tables map[string]schema.Table
}

func (f fixedTableProvider) TableDefinitions() map[string]schema.Table               { return f.tables }
func (f fixedTableProvider) AutodiscoveryEnabled() bool                              { return false }
func (f fixedTableProvider) RegisterTablesReloadListener(chan<- types.ReloadMessage) {}
