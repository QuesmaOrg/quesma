// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package frontend_connectors

import (
	"github.com/QuesmaOrg/quesma/platform/clickhouse"
	"github.com/QuesmaOrg/quesma/platform/common_table"
	"github.com/QuesmaOrg/quesma/platform/config"
	"github.com/QuesmaOrg/quesma/platform/model"
	"github.com/QuesmaOrg/quesma/platform/schema"
	"github.com/QuesmaOrg/quesma/platform/types"
	"github.com/stretchr/testify/assert"
	"strconv"
	"testing"
)

type fixedTableProvider struct {
	tables map[string]schema.Table
}

func (f fixedTableProvider) TableDefinitions() map[string]schema.Table {
	return f.tables
}
func (f fixedTableProvider) AutodiscoveryEnabled() bool                              { return false }
func (f fixedTableProvider) RegisterTablesReloadListener(chan<- types.ReloadMessage) {}

func Test_ipRangeTransform(t *testing.T) {
	const isIPAddressInRangePrimitive = "isIPAddressInRange"
	const CASTPrimitive = "CAST"
	const COALESCEPrimitive = "COALESCE"
	const StringLiteral = "String"
	const IpFieldContent = "'111.42.223.209/16'"
	IpFieldName := "clientip"

	indexConfig := map[string]config.IndexConfiguration{
		"kibana_sample_data_logs": {
			SchemaOverrides: &config.SchemaConfiguration{Fields: map[config.FieldName]config.FieldConfiguration{
				config.FieldName(IpFieldName): {Type: "ip"},
				"message":                     {Type: "text"},
				"content":                     {Type: "text"},
			}},
		},
		// Identical to kibana_sample_data_logs, but with "nested.clientip"
		// instead of "clientip"
		"kibana_sample_data_logs_nested": {
			SchemaOverrides: &config.SchemaConfiguration{Fields: map[config.FieldName]config.FieldConfiguration{
				"nested.clientip": {Type: "ip"},
				"message":         {Type: "text"},
				"content":         {Type: "text"},
			}},
		},
		"kibana_sample_data_flights": {
			SchemaOverrides: &config.SchemaConfiguration{Fields: map[config.FieldName]config.FieldConfiguration{
				config.FieldName(IpFieldName): {Type: "ip"},
				"DestLocation":                {Type: "geo_point"},
				"message":                     {Type: "text"},
				"content":                     {Type: "text"},
			}},
		},
	}
	cfg := config.QuesmaConfiguration{
		IndexConfig: indexConfig,
	}

	tableMap := clickhouse.NewTableMap()

	tableDiscovery := clickhouse.NewEmptyTableDiscovery()
	tableDiscovery.TableMap = tableMap
	for indexName := range indexConfig {
		tableMap.Store(indexName, clickhouse.NewEmptyTable(indexName))
	}

	tableProvider :=
		fixedTableProvider{tables: map[string]schema.Table{
			"kibana_sample_data_flights": {Columns: map[string]schema.Column{
				"destlocation": {Name: "destlocation", Type: "geo_point"},
				"clientip":     {Name: "clientip", Type: "ip"},
			}},
			"kibana_sample_data_logs_nested": {Columns: map[string]schema.Column{
				"destlocation":    {Name: "destlocation", Type: "geo_point"},
				"nested_clientip": {Name: "nested_clientip", Type: "ip"},
			}},
		}}
	fieldEncodings := map[schema.FieldEncodingKey]schema.EncodedFieldName{
		{
			TableName: "kibana_sample_data_logs_nested", FieldName: "DestLocation"}: "destlocation",
		{
			TableName: "kibana_sample_data_logs_nested", FieldName: "nested.clientip"}: "nested_clientip",
	}
	s := schema.NewSchemaRegistry(tableProvider, &cfg, clickhouse.SchemaTypeAdapter{})
	s.Start()
	defer s.Stop()
	transform := NewSchemaCheckPass(&cfg, tableDiscovery, defaultSearchAfterStrategy)
	s.UpdateFieldEncodings(fieldEncodings)

	selectColumns := []model.Expr{model.NewColumnRef("message")}

	expectedQueries := []*model.Query{
		{
			TableName: "kibana_sample_data_logs",
			SelectCommand: model.SelectCommand{
				FromClause: model.NewTableRef("kibana_sample_data_logs"),
				Columns:    selectColumns,
				WhereClause: &model.FunctionExpr{
					Name: isIPAddressInRangePrimitive,
					Args: []model.Expr{
						&model.FunctionExpr{
							Name: CASTPrimitive,
							Args: []model.Expr{
								&model.AliasedExpr{
									Expr: &model.FunctionExpr{
										Name: COALESCEPrimitive,
										Args: []model.Expr{
											&model.LiteralExpr{Value: IpFieldName},
											&model.LiteralExpr{Value: "'0.0.0.0'"},
										},
									},
									Alias: StringLiteral,
								},
							},
						},
						&model.LiteralExpr{Value: IpFieldContent},
					},
				},
			},
		},
		{
			TableName: "kibana_sample_data_logs_nested",
			SelectCommand: model.SelectCommand{
				FromClause: model.NewTableRef("kibana_sample_data_logs_nested"),
				Columns:    selectColumns,
				WhereClause: &model.FunctionExpr{
					Name: isIPAddressInRangePrimitive,
					Args: []model.Expr{
						&model.FunctionExpr{
							Name: CASTPrimitive,
							Args: []model.Expr{
								&model.AliasedExpr{
									Expr: &model.FunctionExpr{
										Name: COALESCEPrimitive,
										Args: []model.Expr{
											&model.ColumnRef{ColumnName: "nested_clientip"},
											&model.LiteralExpr{Value: "'0.0.0.0'"},
										},
									},
									Alias: StringLiteral,
								},
							},
						},
						&model.LiteralExpr{Value: IpFieldContent},
					},
				},
			},
		},
		{
			TableName: "kibana_sample_data_logs",
			SelectCommand: model.SelectCommand{
				FromClause: model.NewTableRef("kibana_sample_data_logs"),
				Columns:    selectColumns,
				WhereClause: &model.FunctionExpr{
					Name: isIPAddressInRangePrimitive,
					Args: []model.Expr{
						&model.FunctionExpr{
							Name: CASTPrimitive,
							Args: []model.Expr{
								&model.AliasedExpr{
									Expr: &model.FunctionExpr{
										Name: COALESCEPrimitive,
										Args: []model.Expr{
											&model.ColumnRef{ColumnName: IpFieldName},
											&model.LiteralExpr{Value: "'0.0.0.0'"},
										},
									},
									Alias: StringLiteral,
								},
							},
						},
						&model.LiteralExpr{Value: IpFieldContent},
					},
				},
			},
		},
		{
			TableName: "kibana_sample_data_logs",
			SelectCommand: model.SelectCommand{
				FromClause: model.NewTableRef("kibana_sample_data_logs"),
				Columns:    selectColumns,
				WhereClause: &model.InfixExpr{
					Left:  &model.LiteralExpr{Value: IpFieldName},
					Op:    "<",
					Right: &model.LiteralExpr{Value: IpFieldContent},
				},
			},
		},
		{
			TableName: "kibana_sample_data_logs",
			SelectCommand: model.SelectCommand{
				FromClause: model.NewTableRef("kibana_sample_data_logs"),
				Columns:    selectColumns,
				WhereClause: &model.FunctionExpr{
					Name: isIPAddressInRangePrimitive,
					Args: []model.Expr{
						&model.FunctionExpr{
							Name: CASTPrimitive,
							Args: []model.Expr{
								&model.AliasedExpr{
									Expr: &model.FunctionExpr{
										Name: COALESCEPrimitive,
										Args: []model.Expr{
											&model.LiteralExpr{Value: IpFieldName},
											&model.LiteralExpr{Value: "'0.0.0.0'"},
										},
									},
									Alias: StringLiteral,
								},
							},
						},
						&model.LiteralExpr{Value: IpFieldContent},
					},
				},
			},
		},
		//SELECT * FROM "default"."kibana_sample_data_logs" WHERE
		//(("@timestamp">=parseDateTime64BestEffort('2024-06-06T09:58:50.387Z') AND
		//"@timestamp"<=parseDateTime64BestEffort('2024-06-10T09:58:50.387Z')) AND
		//isIPAddressInRange(CAST(clientip,'String'),'32.208.36.11/16'))
		{
			TableName: "kibana_sample_data_logs",
			SelectCommand: model.SelectCommand{
				FromClause: model.NewTableRef("kibana_sample_data_logs"),
				Columns:    selectColumns,
				WhereClause: &model.InfixExpr{
					Left: &model.InfixExpr{
						Left: &model.InfixExpr{
							Left: &model.LiteralExpr{Value: strconv.Quote("@timestamp")},
							Op:   ">=",
							Right: &model.FunctionExpr{
								Name: "parseDateTime64BestEffort",
								Args: []model.Expr{&model.LiteralExpr{Value: "'2024-06-06T09:58:50.387Z'"}}},
						},
						Op: "AND",
						Right: &model.InfixExpr{
							Left: &model.LiteralExpr{Value: strconv.Quote("@timestamp")},
							Op:   "<=",
							Right: &model.FunctionExpr{
								Name: "parseDateTime64BestEffort",
								Args: []model.Expr{&model.LiteralExpr{Value: "'2024-06-10T09:58:50.387Z'"}}},
						},
					},
					Op: "AND",

					Right: &model.FunctionExpr{
						Name: isIPAddressInRangePrimitive,
						Args: []model.Expr{
							&model.FunctionExpr{
								Name: CASTPrimitive,
								Args: []model.Expr{
									&model.AliasedExpr{
										Expr: &model.FunctionExpr{
											Name: COALESCEPrimitive,
											Args: []model.Expr{
												&model.LiteralExpr{Value: IpFieldName},
												&model.LiteralExpr{Value: "'0.0.0.0'"},
											},
										},
										Alias: StringLiteral,
									},
								},
							},
							&model.LiteralExpr{Value: IpFieldContent},
						},
					},
				},
			}},
		{
			TableName: "kibana_sample_data_flights",
			SelectCommand: model.SelectCommand{
				FromClause: model.NewTableRef("kibana_sample_data_flights"),
				Columns: []model.Expr{model.NewAliasedExpr(model.NewFunction("map",
					model.NewLiteral("'lat'"),
					model.NewColumnRef("destlocation_lat"),
					model.NewLiteral("'lon'"),
					model.NewColumnRef("destlocation_lon")), "destlocation")},
			}},
	}
	queries := [][]*model.Query{
		{
			{
				TableName: "kibana_sample_data_logs",
				SelectCommand: model.SelectCommand{
					FromClause: model.NewTableRef("kibana_sample_data_logs"),
					Columns:    selectColumns,
					WhereClause: &model.InfixExpr{
						Left:  &model.LiteralExpr{Value: IpFieldName},
						Op:    "=",
						Right: &model.LiteralExpr{Value: IpFieldContent},
					},
				}},
		},
		{
			{
				TableName: "kibana_sample_data_logs_nested",
				SelectCommand: model.SelectCommand{
					FromClause: model.NewTableRef("kibana_sample_data_logs_nested"),
					Columns:    selectColumns,
					WhereClause: &model.InfixExpr{
						Left:  &model.ColumnRef{ColumnName: "nested.clientip"},
						Op:    "=",
						Right: &model.LiteralExpr{Value: IpFieldContent},
					},
				}},
		},
		{
			{
				TableName: "kibana_sample_data_logs",
				SelectCommand: model.SelectCommand{
					FromClause: model.NewTableRef("kibana_sample_data_logs"),
					Columns:    selectColumns,
					WhereClause: &model.InfixExpr{
						Left:  &model.ColumnRef{ColumnName: IpFieldName},
						Op:    "=",
						Right: &model.LiteralExpr{Value: IpFieldContent},
					},
				}},
		},
		{
			{
				TableName: "kibana_sample_data_logs",
				SelectCommand: model.SelectCommand{
					FromClause: model.NewTableRef("kibana_sample_data_logs"),
					Columns:    selectColumns,
					WhereClause: &model.InfixExpr{
						Left:  &model.LiteralExpr{Value: IpFieldName},
						Op:    "<",
						Right: &model.LiteralExpr{Value: IpFieldContent},
					},
				},
			},
		},
		{
			{
				TableName: "kibana_sample_data_logs",
				SelectCommand: model.SelectCommand{
					FromClause: model.NewTableRef("kibana_sample_data_logs"),
					Columns:    selectColumns,
					WhereClause: &model.InfixExpr{
						Left:  &model.LiteralExpr{Value: IpFieldName},
						Op:    "iLIKE",
						Right: &model.LiteralExpr{Value: IpFieldContent},
					},
				},
			},
		},
		//SELECT * FROM "default"."kibana_sample_data_logs" WHERE
		//(("@timestamp">=parseDateTime64BestEffort('2024-06-06T09:58:50.387Z') AND
		//"@timestamp"<=parseDateTime64BestEffort('2024-06-10T09:58:50.387Z')) AND
		//"clientip" iLIKE '%32.208.36.11/16%')
		{
			{
				TableName: "kibana_sample_data_logs",
				SelectCommand: model.SelectCommand{
					FromClause: model.NewTableRef("kibana_sample_data_logs"),
					Columns:    selectColumns,
					WhereClause: &model.InfixExpr{
						Left: &model.InfixExpr{
							Left: &model.InfixExpr{
								Left: &model.LiteralExpr{Value: strconv.Quote("@timestamp")},
								Op:   ">=",
								Right: &model.FunctionExpr{
									Name: "parseDateTime64BestEffort",
									Args: []model.Expr{&model.LiteralExpr{Value: "'2024-06-06T09:58:50.387Z'"}}},
							},
							Op: "AND",
							Right: &model.InfixExpr{
								Left: &model.LiteralExpr{Value: strconv.Quote("@timestamp")},
								Op:   "<=",
								Right: &model.FunctionExpr{
									Name: "parseDateTime64BestEffort",
									Args: []model.Expr{&model.LiteralExpr{Value: "'2024-06-10T09:58:50.387Z'"}}},
							},
						},
						Op: "AND",
						Right: &model.InfixExpr{
							Left:  &model.LiteralExpr{Value: "clientip"},
							Op:    "iLIKE",
							Right: &model.LiteralExpr{Value: IpFieldContent},
						},
					},
				}},
		},
		{
			{
				TableName: "kibana_sample_data_flights",
				SelectCommand: model.SelectCommand{
					FromClause: model.NewTableRef("kibana_sample_data_flights"),
					Columns:    []model.Expr{model.NewColumnRef("DestLocation")},
				}},
		},
	}

	for k := range queries {
		t.Run(strconv.Itoa(k), func(t *testing.T) {

			queriesToTransform := queries[k]

			for _, q := range queriesToTransform {

				currentSchema, ok := s.FindSchema(schema.IndexName(q.TableName))
				if !ok {
					t.Fatalf("schema not found for table %s", q.TableName)
				}
				q.Schema = currentSchema
				q.Indexes = []string{q.TableName}
			}

			resultQueries, err := transform.Transform(queries[k])
			assert.NoError(t, err)
			assert.Equal(t, expectedQueries[k].SelectCommand.String(), resultQueries[0].SelectCommand.String())
		})
	}
}

func Test_arrayType(t *testing.T) {

	indexConfig := map[string]config.IndexConfiguration{
		"kibana_sample_data_ecommerce": {},
	}
	fields := map[schema.FieldName]schema.Field{
		"@timestamp":         {PropertyName: "@timestamp", InternalPropertyName: "@timestamp", InternalPropertyType: "DateTime64", Type: schema.QuesmaTypeDate},
		"products.name":      {PropertyName: "products.name", InternalPropertyName: "products_name", InternalPropertyType: "Array(String)", Type: schema.QuesmaTypeArray},
		"products.quantity":  {PropertyName: "products.quantity", InternalPropertyName: "products_quantity", InternalPropertyType: "Array(Int64)", Type: schema.QuesmaTypeArray},
		"products.sku":       {PropertyName: "products.sku", InternalPropertyName: "products_sku", InternalPropertyType: "Array(String)", Type: schema.QuesmaTypeArray},
		"order_date":         {PropertyName: "order_date", InternalPropertyName: "order_date", InternalPropertyType: "DateTime64", Type: schema.QuesmaTypeDate},
		"taxful_total_price": {PropertyName: "taxful_total_price", InternalPropertyName: "taxful_total_price", InternalPropertyType: "Float64", Type: schema.QuesmaTypeFloat},
	}

	indexSchema := schema.Schema{
		Fields: fields,
	}

	tableMap := clickhouse.NewTableMap()

	tableDiscovery := clickhouse.NewEmptyTableDiscovery()
	tableDiscovery.TableMap = tableMap
	for indexName := range indexConfig {
		tableMap.Store(indexName, clickhouse.NewEmptyTable(indexName))
	}

	transform := NewSchemaCheckPass(&config.QuesmaConfiguration{IndexConfig: indexConfig}, tableDiscovery, defaultSearchAfterStrategy)

	tests := []struct {
		name     string
		query    *model.Query
		expected *model.Query
	}{
		{
			name: "simple array",
			query: &model.Query{
				TableName: "kibana_sample_data_ecommerce",
				SelectCommand: model.SelectCommand{
					FromClause: model.NewTableRef("kibana_sample_data_ecommerce"),
					Columns:    []model.Expr{model.NewWildcardExpr},
				},
			},
			expected: &model.Query{
				TableName: "kibana_sample_data_ecommerce",
				SelectCommand: model.SelectCommand{
					FromClause: model.NewTableRef("kibana_sample_data_ecommerce"),
					Columns:    []model.Expr{model.NewColumnRef("@timestamp"), model.NewColumnRef("order_date"), model.NewColumnRef("products_name"), model.NewColumnRef("products_quantity"), model.NewColumnRef("products_sku"), model.NewColumnRef("taxful_total_price")},
				},
			},
		},

		{
			name: "arrayReduce",
			//SELECT "order_date", sumOrNull("products::quantity") FROM "kibana_sample_data_ecommerce" GROUP BY "order_date"
			query: &model.Query{
				TableName: "kibana_sample_data_ecommerce",
				SelectCommand: model.SelectCommand{
					FromClause: model.NewTableRef("kibana_sample_data_ecommerce"),
					Columns: []model.Expr{
						model.NewColumnRef("order_date"),
						model.NewFunction("sumOrNull", model.NewColumnRef("products.quantity")),
					},
					GroupBy: []model.Expr{model.NewColumnRef("order_date")},
				},
			},
			//SELECT "order_date", sumOrNull(arrayReduce('sumOrNull',"products::quantity")) FROM "kibana_sample_data_ecommerce" GROUP BY "order_date"
			expected: &model.Query{
				TableName: "kibana_sample_data_ecommerce",
				SelectCommand: model.SelectCommand{
					FromClause: model.NewTableRef("kibana_sample_data_ecommerce"),
					Columns: []model.Expr{
						model.NewColumnRef("order_date"),
						model.NewAliasedExpr(model.NewFunction("sumArrayOrNull", model.NewColumnRef("products_quantity")), "column_1"),
					},
					GroupBy: []model.Expr{model.NewColumnRef("order_date")},
				},
			},
		},

		{
			name: "arrayReducePancake",
			//SELECT "order_date", avgOrNullMerge(avgOrNullState("products::quantity"")) OVER (), sumOrNull("products::quantity") FROM "kibana_sample_data_ecommerce" GROUP BY "order_date"
			query: &model.Query{
				TableName: "kibana_sample_data_ecommerce",
				SelectCommand: model.SelectCommand{
					FromClause: model.NewTableRef("kibana_sample_data_ecommerce"),
					Columns: []model.Expr{
						model.NewColumnRef("order_date"),
						model.NewWindowFunction("avgOrNullMerge", []model.Expr{model.NewFunction("avgOrNullState", model.NewColumnRef("products.quantity"))}, []model.Expr{}, []model.OrderByExpr{}),
						model.NewFunction("sumOrNull", model.NewColumnRef("products.quantity")),
					},
					GroupBy: []model.Expr{model.NewColumnRef("order_date")},
				},
			},
			//SELECT "order_date", avgArrayOrNullMerge(avgArrayOrNullMerge("products::quantity"")) OVER (), sumOrNull("products::quantity") FROM "kibana_sample_data_ecommerce" GROUP BY "order_date"
			expected: &model.Query{
				TableName: "kibana_sample_data_ecommerce",
				SelectCommand: model.SelectCommand{
					FromClause: model.NewTableRef("kibana_sample_data_ecommerce"),
					Columns: []model.Expr{
						model.NewColumnRef("order_date"),
						model.NewAliasedExpr(model.NewWindowFunction("avgArrayOrNullMerge", []model.Expr{model.NewFunction("avgArrayOrNullState", model.NewColumnRef("products_quantity"))}, []model.Expr{}, []model.OrderByExpr{}), "column_1"),
						model.NewAliasedExpr(model.NewFunction("sumArrayOrNull", model.NewColumnRef("products_quantity")), "column_2"),
					},
					GroupBy: []model.Expr{model.NewColumnRef("order_date")},
				},
			},
		},

		{
			name: "ilike array",
			//SELECT "order_date", count() FROM "kibana_sample_data_ecommerce" WHERE  "products::name" ILIKE '%bag%	 GROUP BY "order_date"
			//SELECT "order_date", count() FROM "kibana_sample_data_ecommerce" WHERE arrayExists((x) -> x ILIKE '%bag%',"products::product_name") GROUP BY "order_date"

			query: &model.Query{
				TableName: "kibana_sample_data_ecommerce",
				SelectCommand: model.SelectCommand{
					FromClause: model.NewTableRef("kibana_sample_data_ecommerce"),
					Columns: []model.Expr{
						model.NewColumnRef("order_date"),
						model.NewCountFunc(),
					},
					WhereClause: model.NewInfixExpr(
						model.NewColumnRef("products.name"),
						"ILIKE",
						model.NewLiteral("%foo%"),
					),
					GroupBy: []model.Expr{model.NewColumnRef("order_date")},
				},
			},
			expected: &model.Query{
				TableName: "kibana_sample_data_ecommerce",
				SelectCommand: model.SelectCommand{
					FromClause: model.NewTableRef("kibana_sample_data_ecommerce"),
					Columns: []model.Expr{
						model.NewColumnRef("order_date"),
						model.NewAliasedExpr(model.NewCountFunc(), "column_1"),
					},
					WhereClause: model.NewFunction(
						"arrayExists",
						model.NewLambdaExpr([]string{"x"}, model.NewInfixExpr(model.NewLiteral("x"), "ILIKE", model.NewLiteral("%foo%"))),
						model.NewColumnRef("products_name")),
					GroupBy: []model.Expr{model.NewColumnRef("order_date")},
				},
			},
		},

		//SELECT "order_date", count() FROM "kibana_sample_data_ecommerce" WHERE "products.sku" = 'XYZ' group by  "order_date"
		//SELECT "order_date", count() FROM "kibana_sample_data_ecommerce" WHERE has("products.sku",'XYZ') group by "order_date"

		{
			name: "equals array",
			query: &model.Query{
				TableName: "kibana_sample_data_ecommerce",
				SelectCommand: model.SelectCommand{
					FromClause: model.NewTableRef("kibana_sample_data_ecommerce"),
					Columns: []model.Expr{
						model.NewColumnRef("order_date"),
						model.NewCountFunc(),
					},
					WhereClause: model.NewInfixExpr(
						model.NewColumnRef("products.sku"),
						"=",
						model.NewLiteral("'XYZ'"),
					),
					GroupBy: []model.Expr{model.NewColumnRef("order_date")},
				},
			},
			expected: &model.Query{
				TableName: "kibana_sample_data_ecommerce",
				SelectCommand: model.SelectCommand{
					FromClause: model.NewTableRef("kibana_sample_data_ecommerce"),
					Columns: []model.Expr{
						model.NewColumnRef("order_date"),
						model.NewAliasedExpr(model.NewCountFunc(), "column_1"),
					},
					WhereClause: model.NewFunction(
						"has",
						model.NewColumnRef("products_sku"),
						model.NewLiteral("'XYZ'")),
					GroupBy: []model.Expr{model.NewColumnRef("order_date")},
				},
			},
		},

		{
			name: "kibana_sample_data_ecommerce dashboard regression test",
			query: &model.Query{
				TableName: "kibana_sample_data_ecommerce",
				SelectCommand: model.SelectCommand{
					FromClause: model.NewTableRef("kibana_sample_data_ecommerce"),
					Columns: []model.Expr{
						model.NewFunction("sumOrNullIf",
							model.NewColumnRef("taxful_total_price"),
							model.NewInfixExpr(
								model.NewColumnRef("products.name"),
								"ILIKE",
								model.NewLiteralWithEscapeType("%watch%", model.FullyEscaped),
							),
						),
					},
				},
			},
			expected: &model.Query{
				TableName: "kibana_sample_data_ecommerce",
				SelectCommand: model.SelectCommand{
					FromClause: model.NewTableRef("kibana_sample_data_ecommerce"),
					Columns: []model.Expr{
						model.NewAliasedExpr(
							model.NewFunction("sumOrNullIf",
								model.NewColumnRef("taxful_total_price"),
								model.NewFunction("arrayExists",
									model.NewLambdaExpr([]string{"x"}, model.NewInfixExpr(model.NewLiteral("x"), "ILIKE", model.NewLiteralWithEscapeType("%watch%", model.FullyEscaped))),
									model.NewColumnRef("products_name"),
								),
							),
							"column_0",
						),
					},
				},
			},
		},
	}

	asString := func(query *model.Query) string {
		return query.SelectCommand.String()
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.query.Schema = indexSchema
			tt.query.Indexes = []string{tt.query.TableName}
			actual, err := transform.Transform([]*model.Query{tt.query})
			assert.NoError(t, err)

			if err != nil {
				t.Fatal(err)
			}

			assert.True(t, len(actual) == 1, "len queries == 1")

			expectedJson := asString(tt.expected)
			actualJson := asString(actual[0])

			assert.Equal(t, expectedJson, actualJson)
		})
	}
}

func TestApplyWildCard(t *testing.T) {

	indexConfig := map[string]config.IndexConfiguration{
		"kibana_sample_data_ecommerce": {},
	}

	indexSchema := schema.Schema{
		Fields: map[schema.FieldName]schema.Field{
			"a": {PropertyName: "a", InternalPropertyName: "a", InternalPropertyType: "String", Type: schema.QuesmaTypeText},
			"b": {PropertyName: "b", InternalPropertyName: "b", InternalPropertyType: "String", Type: schema.QuesmaTypeText},
			"c": {PropertyName: "c", InternalPropertyName: "c", InternalPropertyType: "String", Type: schema.QuesmaTypeText},
		},
	}

	s := schema.StaticRegistry{
		Tables: map[schema.IndexName]schema.Schema{
			"test": indexSchema,
		},
	}

	transform := NewSchemaCheckPass(&config.QuesmaConfiguration{IndexConfig: indexConfig}, nil, defaultSearchAfterStrategy)

	tests := []struct {
		name     string
		input    []string
		expected []string
	}{
		{"test1", []string{"a", "b", "c"}, []string{"a", "b", "c"}},
		{"test2", []string{"*"}, []string{"a", "b", "c"}},
		{"test3", []string{"a", "*"}, []string{"a", "a", "b", "c"}},
		{"test4", []string{"count", "*"}, []string{"count", "a", "b", "c"}},
	}

	toSelectColumn := func(cols []string) (res []model.Expr) {
		for _, col := range cols {
			if col == "*" {
				res = append(res, model.NewWildcardExpr)
			} else {
				res = append(res, model.NewColumnRef(col))
			}
		}
		return res
	}

	indexSchema, ok := s.FindSchema("test")
	if !ok {
		t.Fatal("schema not found")
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			query := &model.Query{
				TableName: "test",
				SelectCommand: model.SelectCommand{
					Columns: toSelectColumn(tt.input),
				},
			}

			actual, err := transform.applyWildcardExpansion(indexSchema, query)

			if err != nil {
				t.Fatal(err)
			}

			expectedColumns := toSelectColumn(tt.expected)

			assert.Equal(t, expectedColumns, actual.SelectCommand.Columns)
		})
	}
}

func TestApplyPhysicalFromExpression(t *testing.T) {

	indexConfig := map[string]config.IndexConfiguration{
		"test":  {},
		"test2": {UseCommonTable: true},
		"test3": {UseCommonTable: true},
	}
	cfg := config.QuesmaConfiguration{
		IndexConfig: indexConfig,
		DefaultQueryOptimizers: map[string]config.OptimizerConfiguration{
			"group_common_table_indexes": {
				Disabled: false,
				Properties: map[string]string{
					"daily-": "true",
				}}}}

	lm := clickhouse.NewLogManagerEmpty()

	tableDiscovery :=
		fixedTableProvider{tables: map[string]schema.Table{
			"test": {Columns: map[string]schema.Column{
				"a": {Name: "a", Type: "String"},
				"b": {Name: "b", Type: "String"},
				"c": {Name: "c", Type: "String"},
			}},
		}}

	tableDefinition := clickhouse.Table{
		Name:   "test",
		Config: clickhouse.NewDefaultCHConfig(),
		Cols: map[string]*clickhouse.Column{
			"a": {Name: "a", Type: clickhouse.NewBaseType("Array(String)")},
			"b": {Name: "b", Type: clickhouse.NewBaseType("Array(Int64)")},
			"c": {Name: "c", Type: clickhouse.NewBaseType("Array(String)")},
		},
	}

	td, err := lm.GetTableDefinitions()
	if err != nil {
		t.Fatal(err)
	}
	td.Store(tableDefinition.Name, &tableDefinition)

	s := schema.NewSchemaRegistry(tableDiscovery, &cfg, clickhouse.SchemaTypeAdapter{})
	s.Start()
	defer s.Stop()
	transform := NewSchemaCheckPass(&cfg, nil, defaultSearchAfterStrategy)

	tests := []struct {
		name     string
		indexes  []string // default is []string{"test"}
		input    model.SelectCommand
		expected model.SelectCommand
	}{
		{
			name: "single table",
			input: model.SelectCommand{
				FromClause: model.NewTableRef(model.SingleTableNamePlaceHolder),
				Columns: []model.Expr{
					model.NewColumnRef("a"),
					model.NewCountFunc(),
				},
			},
			expected: model.SelectCommand{
				FromClause: model.NewTableRef("test"),
				Columns: []model.Expr{
					model.NewColumnRef("a"),
					model.NewCountFunc(),
				},
			},
		},

		{
			name:    "single table with common table",
			indexes: []string{"test2"},
			input: model.SelectCommand{
				FromClause: model.NewTableRef(model.SingleTableNamePlaceHolder),
				Columns: []model.Expr{
					model.NewColumnRef("a"),
					model.NewCountFunc(),
				},
			},
			expected: model.SelectCommand{
				FromClause: model.NewTableRef(common_table.TableName),
				Columns: []model.Expr{
					model.NewColumnRef("a"),
					model.NewCountFunc(),
				},
				WhereClause: model.NewInfixExpr(model.NewColumnRef(common_table.IndexNameColumn), "=", model.NewLiteral("'test2'")),
			},
		},

		{
			name:    "two tables  with common table",
			indexes: []string{"test2", "test3"},
			input: model.SelectCommand{
				FromClause: model.NewTableRef(model.SingleTableNamePlaceHolder),
				Columns: []model.Expr{
					model.NewColumnRef("a"),
					model.NewCountFunc(),
				},
			},
			expected: model.SelectCommand{
				FromClause: model.NewTableRef(common_table.TableName),
				Columns: []model.Expr{
					model.NewColumnRef("a"),
					model.NewCountFunc(),
				},
				WhereClause: model.Or([]model.Expr{model.NewInfixExpr(model.NewColumnRef(common_table.IndexNameColumn), "=", model.NewLiteral("'test2'")),
					model.NewInfixExpr(model.NewColumnRef(common_table.IndexNameColumn), "=", model.NewLiteral("'test3'"))}),
			},
		},

		{
			name:    "two daily tables tables  with common table (group_common_table_indexes optimizer)",
			indexes: []string{"daily-1", "daily-2"},
			input: model.SelectCommand{
				FromClause: model.NewTableRef(model.SingleTableNamePlaceHolder),
				Columns: []model.Expr{
					model.NewColumnRef("a"),
					model.NewCountFunc(),
				},
			},
			expected: model.SelectCommand{
				FromClause: model.NewTableRef(common_table.TableName),
				Columns: []model.Expr{
					model.NewColumnRef("a"),
					model.NewCountFunc(),
				},
				WhereClause: model.NewFunction("startsWith", model.NewColumnRef(common_table.IndexNameColumn), model.NewLiteral("'daily-'")),
			},
		},

		{
			name: "cte with fixed table name",
			input: model.SelectCommand{
				FromClause: model.NewTableRef(model.SingleTableNamePlaceHolder),
				Columns: []model.Expr{
					model.NewColumnRef("a"),
					model.NewCountFunc(),
				},
				NamedCTEs: []*model.CTE{
					{
						Name: "cte_1",
						SelectCommand: &model.SelectCommand{
							FromClause: model.NewTableRef("other_table"),
							Columns: []model.Expr{
								model.NewColumnRef("a"),
							},
						},
					},
				},
			},
			expected: model.SelectCommand{
				FromClause: model.NewTableRef("test"),
				Columns: []model.Expr{
					model.NewColumnRef("a"),
					model.NewCountFunc(),
				},
				NamedCTEs: []*model.CTE{
					{
						Name: "cte_1",
						SelectCommand: &model.SelectCommand{
							FromClause: model.NewTableRef("other_table"),
							Columns: []model.Expr{
								model.NewColumnRef("a"),
							},
						},
					},
				},
			},
		},

		{
			name: "cte with  table name",
			input: model.SelectCommand{
				FromClause: model.NewTableRef(model.SingleTableNamePlaceHolder),
				Columns: []model.Expr{
					model.NewColumnRef("order_date"),
					model.NewCountFunc(),
				},
				NamedCTEs: []*model.CTE{
					{
						Name: "cte_1",
						SelectCommand: &model.SelectCommand{
							FromClause: model.NewTableRef(model.SingleTableNamePlaceHolder),
							Columns: []model.Expr{
								model.NewColumnRef("order_date"),
							},
						},
					},
				},
			},
			expected: model.SelectCommand{
				FromClause: model.NewTableRef("test"),
				Columns: []model.Expr{
					model.NewColumnRef("order_date"),
					model.NewCountFunc(),
				},
				NamedCTEs: []*model.CTE{
					{
						Name: "cte_1",
						SelectCommand: &model.SelectCommand{
							FromClause: model.NewTableRef("test"),
							Columns: []model.Expr{
								model.NewColumnRef("order_date"),
							},
						},
					},
				},
			},
		},
	}

	indexSchema, ok := s.FindSchema("test")
	if !ok {
		t.Fatal("schema not found")
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			indexes := tt.indexes
			if len(indexes) == 0 {
				indexes = []string{"test"}
			}

			query := &model.Query{
				TableName:     "test",
				SelectCommand: tt.input,
				Schema:        indexSchema,
				Indexes:       indexes,
			}

			expectedAsString := model.AsString(tt.expected)

			actual, err := transform.applyPhysicalFromExpression(indexSchema, query)

			if err != nil {
				t.Fatal(err)
			}

			actualAsString := model.AsString(actual.SelectCommand)

			assert.Equal(t, expectedAsString, actualAsString)
		})
	}
}

func TestFullTextFields(t *testing.T) {

	tests := []struct {
		name           string
		fullTextFields []string
		input          model.SelectCommand
		expected       model.SelectCommand
	}{
		{
			"no full text field column",
			[]string{},
			model.SelectCommand{
				FromClause: model.NewTableRef("test"),
				Columns: []model.Expr{
					model.NewColumnRef("a"),
					model.NewCountFunc(),
				},
				WhereClause: model.NewInfixExpr(model.NewColumnRef(model.FullTextFieldNamePlaceHolder), "=", model.NewLiteral("foo")),
			},
			model.SelectCommand{
				FromClause: model.NewTableRef("test"),
				Columns: []model.Expr{
					model.NewColumnRef("a"),
					model.NewCountFunc(),
				},
				WhereClause: model.NewLiteral(false),
			},
		},

		{
			"single column",
			[]string{"b"},
			model.SelectCommand{
				FromClause: model.NewTableRef("test"),
				Columns: []model.Expr{
					model.NewColumnRef("a"),
					model.NewCountFunc(),
				},
				WhereClause: model.NewInfixExpr(model.NewColumnRef(model.FullTextFieldNamePlaceHolder), "=", model.NewLiteral("foo")),
			},
			model.SelectCommand{
				FromClause: model.NewTableRef("test"),
				Columns: []model.Expr{
					model.NewColumnRef("a"),
					model.NewCountFunc(),
				},
				WhereClause: model.NewInfixExpr(model.NewColumnRef("b"), "=", model.NewLiteral("foo")),
			},
		},

		{
			"two columns",
			[]string{"a", "b"},
			model.SelectCommand{
				FromClause: model.NewTableRef("test"),
				Columns: []model.Expr{
					model.NewColumnRef("a"),
					model.NewCountFunc(),
				},
				WhereClause: model.NewInfixExpr(model.NewColumnRef(model.FullTextFieldNamePlaceHolder), "=", model.NewLiteral("foo")),
			},
			model.SelectCommand{
				FromClause: model.NewTableRef("test"),
				Columns: []model.Expr{
					model.NewColumnRef("a"),
					model.NewCountFunc(),
				},
				WhereClause: model.Or([]model.Expr{
					model.NewInfixExpr(model.NewColumnRef("a"), "=", model.NewLiteral("foo")),
					model.NewInfixExpr(model.NewColumnRef("b"), "=", model.NewLiteral("foo")),
				}),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			query := &model.Query{
				TableName:     "test",
				SelectCommand: tt.input,
			}

			columns := []string{"a", "b", "c"}

			var schemaColumns []schema.Column

			for _, col := range columns {
				schemaColumns = append(schemaColumns, schema.Column{Name: col, Type: "String"})
			}

			columnMap := make(map[string]schema.Column)
			for _, col := range schemaColumns {
				columnMap[col.Name] = col
			}

			schemaTable := schema.Table{
				Columns: columnMap,
			}

			tableDiscovery :=
				fixedTableProvider{tables: map[string]schema.Table{
					"test": schemaTable,
				}}

			fieldOverrides := make(map[config.FieldName]config.FieldConfiguration)

			for _, fullTextField := range tt.fullTextFields {
				fieldOverrides[config.FieldName(fullTextField)] = config.FieldConfiguration{
					Type: "text",
				}
			}

			indexConfig := map[string]config.IndexConfiguration{
				"test": {
					SchemaOverrides: &config.SchemaConfiguration{
						Fields: fieldOverrides,
					},
				},
			}

			cfg := config.QuesmaConfiguration{
				IndexConfig: indexConfig,
			}

			s := schema.NewSchemaRegistry(tableDiscovery, &cfg, clickhouse.SchemaTypeAdapter{})
			s.Start()
			defer s.Stop()
			transform := NewSchemaCheckPass(&config.QuesmaConfiguration{IndexConfig: indexConfig}, nil, defaultSearchAfterStrategy)

			indexSchema, ok := s.FindSchema("test")
			if !ok {
				t.Fatal("schema not found")
			}

			expectedAsString := model.AsString(tt.expected)

			actual, err := transform.applyFullTextField(indexSchema, query)

			if err != nil {
				t.Fatal(err)
			}

			actualAsString := model.AsString(actual.SelectCommand)

			assert.Equal(t, expectedAsString, actualAsString)
		})
	}
}

func Test_applyMatchOperator(t *testing.T) {
	schemaTable := schema.Table{
		Columns: map[string]schema.Column{
			"message": {Name: "message", Type: "String"},
			"count":   {Name: "count", Type: "Int64"},
		},
	}

	tests := []struct {
		name     string
		query    *model.Query
		expected *model.Query
	}{
		{
			name: "match operator transformation for String (ILIKE)",
			query: &model.Query{
				TableName: "test",
				SelectCommand: model.SelectCommand{
					FromClause: model.NewTableRef("test"),
					Columns:    []model.Expr{model.NewColumnRef("message")},
					WhereClause: model.NewInfixExpr(
						model.NewColumnRef("message"),
						model.MatchOperator,
						model.NewLiteralWithEscapeType("'needle'", model.NotEscapedLikeFull),
					),
				},
			},
			expected: &model.Query{
				TableName: "test",
				SelectCommand: model.SelectCommand{
					FromClause: model.NewTableRef("test"),
					Columns:    []model.Expr{model.NewColumnRef("message")},
					WhereClause: model.NewInfixExpr(
						model.NewColumnRef("message"),
						"iLIKE",
						model.NewLiteralWithEscapeType("needle", model.NotEscapedLikeFull),
					),
				},
			},
		},
		{
			name: "match operator transformation for Int64 (=)",
			query: &model.Query{
				TableName: "test",
				SelectCommand: model.SelectCommand{
					FromClause: model.NewTableRef("test"),
					Columns:    []model.Expr{model.NewColumnRef("message")},
					WhereClause: model.NewInfixExpr(
						model.NewColumnRef("count"),
						model.MatchOperator,
						model.NewLiteral("'123'"),
					),
				},
			},
			expected: &model.Query{
				TableName: "test",
				SelectCommand: model.SelectCommand{
					FromClause: model.NewTableRef("test"),
					Columns:    []model.Expr{model.NewColumnRef("message")},
					WhereClause: model.NewInfixExpr(
						model.NewColumnRef("count"),
						"=",
						model.NewLiteral("123"),
					),
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tableDiscovery :=
				fixedTableProvider{tables: map[string]schema.Table{
					"test": schemaTable,
				}}

			indexConfig := map[string]config.IndexConfiguration{
				"test": {},
			}

			cfg := config.QuesmaConfiguration{
				IndexConfig: indexConfig,
			}

			s := schema.NewSchemaRegistry(tableDiscovery, &cfg, clickhouse.SchemaTypeAdapter{})
			s.Start()
			defer s.Stop()

			transform := NewSchemaCheckPass(&cfg, nil, defaultSearchAfterStrategy)

			indexSchema, ok := s.FindSchema("test")
			if !ok {
				t.Fatal("schema not found")
			}

			actual, err := transform.applyMatchOperator(indexSchema, tt.query)
			if err != nil {
				t.Fatal(err)
			}

			assert.Equal(t, model.AsString(tt.expected.SelectCommand), model.AsString(actual.SelectCommand))
		})
	}
}

func Test_checkAggOverUnsupportedType(t *testing.T) {
	schemaTable := schema.Table{
		Columns: map[string]schema.Column{
			"@timestamp": {Name: "@timestamp", Type: "DateTime64"},
			"message":    {Name: "message", Type: "String"},
			"count":      {Name: "count", Type: "Int64"},
		},
	}

	tests := []struct {
		name     string
		query    *model.Query
		expected *model.Query
	}{
		{
			name: "String",
			query: &model.Query{
				TableName: "test",
				SelectCommand: model.SelectCommand{
					FromClause: model.NewTableRef("test"),
					Columns:    []model.Expr{model.NewFunction("sum", model.NewColumnRef("message"))},
				},
			},
			expected: &model.Query{
				TableName: "test",
				SelectCommand: model.SelectCommand{
					FromClause: model.NewTableRef("test"),
					Columns:    []model.Expr{model.NewFunction("sum", model.NewLiteral("NULL"))},
				},
			},
		},
		{
			name: "do not int field",
			query: &model.Query{
				TableName: "test",
				SelectCommand: model.SelectCommand{
					FromClause: model.NewTableRef("test"),
					Columns:    []model.Expr{model.NewFunction("sum", model.NewColumnRef("count"))},
				},
			},
			expected: &model.Query{
				TableName: "test",
				SelectCommand: model.SelectCommand{
					FromClause: model.NewTableRef("test"),
					Columns:    []model.Expr{model.NewFunction("sum", model.NewColumnRef("count"))},
				},
			},
		},
		{
			name: "DateTime",
			query: &model.Query{
				TableName: "test",
				SelectCommand: model.SelectCommand{
					FromClause: model.NewTableRef("test"),
					Columns:    []model.Expr{model.NewFunction("sum", model.NewColumnRef("@timestamp"))},
				},
			},
			expected: &model.Query{
				TableName: "test",
				SelectCommand: model.SelectCommand{
					FromClause: model.NewTableRef("test"),
					Columns:    []model.Expr{model.NewFunction("sum", model.NewLiteral("NULL"))},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tableDiscovery :=
				fixedTableProvider{tables: map[string]schema.Table{
					"test": schemaTable,
				}}

			indexConfig := map[string]config.IndexConfiguration{
				"test": {},
			}

			cfg := config.QuesmaConfiguration{
				IndexConfig: indexConfig,
			}

			s := schema.NewSchemaRegistry(tableDiscovery, &cfg, clickhouse.SchemaTypeAdapter{})
			s.Start()
			defer s.Stop()
			transform := NewSchemaCheckPass(&cfg, nil, defaultSearchAfterStrategy)

			indexSchema, ok := s.FindSchema("test")
			if !ok {
				t.Fatal("schema not found")
			}

			actual, err := transform.checkAggOverUnsupportedType(indexSchema, tt.query)
			if err != nil {
				t.Fatal(err)
			}

			assert.Equal(t, model.AsString(tt.expected.SelectCommand), model.AsString(actual.SelectCommand))
		})
	}
}

func Test_mapKeys(t *testing.T) {

	indexConfig := map[string]config.IndexConfiguration{
		"test":  {EnableFieldMapSyntax: true},
		"test2": {EnableFieldMapSyntax: false},
	}

	fields := map[schema.FieldName]schema.Field{
		"@timestamp": {PropertyName: "@timestamp", InternalPropertyName: "@timestamp", InternalPropertyType: "DateTime64", Type: schema.QuesmaTypeDate},
		"foo":        {PropertyName: "foo", InternalPropertyName: "foo", InternalPropertyType: "Map(String, String)", Type: schema.QuesmaTypeMap},
		"sizes":      {PropertyName: "sizes", InternalPropertyName: "sizes", InternalPropertyType: "Map(String, Int64)", Type: schema.QuesmaTypeMap},
	}

	indexSchema := schema.Schema{
		Fields: fields,
	}

	tableMap := clickhouse.NewTableMap()

	tableDiscovery := clickhouse.NewEmptyTableDiscovery()
	tableDiscovery.TableMap = tableMap
	for indexName := range indexConfig {
		tableMap.Store(indexName, clickhouse.NewEmptyTable(indexName))
	}

	transform := NewSchemaCheckPass(&config.QuesmaConfiguration{IndexConfig: indexConfig}, tableDiscovery, defaultSearchAfterStrategy)

	tests := []struct {
		name     string
		query    *model.Query
		expected *model.Query
	}{

		{
			name: "match operator transformation for String (ILIKE)",
			query: &model.Query{
				TableName: "test",
				SelectCommand: model.SelectCommand{
					FromClause: model.NewTableRef("test"),
					Columns:    []model.Expr{model.NewColumnRef("foo")},
					WhereClause: model.NewInfixExpr(
						model.NewColumnRef("foo.bar"),
						model.MatchOperator,
						model.NewLiteral("'baz'"),
					),
				},
			},
			expected: &model.Query{
				TableName: "test",
				SelectCommand: model.SelectCommand{
					FromClause: model.NewTableRef("test"),
					Columns:    []model.Expr{model.NewColumnRef("foo")},
					WhereClause: model.NewInfixExpr(
						model.NewFunction("arrayElement", model.NewColumnRef("foo"), model.NewLiteral("'bar'")),
						"iLIKE",
						model.NewLiteral("'%baz%'"),
					),
				},
			},
		},

		{
			name: "match operator transformation for int (=)",
			query: &model.Query{
				TableName: "test",
				SelectCommand: model.SelectCommand{
					FromClause: model.NewTableRef("test"),
					Columns:    []model.Expr{model.NewColumnRef("foo")},
					WhereClause: model.NewInfixExpr(
						model.NewColumnRef("sizes.bar"),
						model.MatchOperator,
						model.NewLiteral("1"),
					),
				},
			},
			expected: &model.Query{
				TableName: "test",
				SelectCommand: model.SelectCommand{
					FromClause: model.NewTableRef("test"),
					Columns:    []model.Expr{model.NewColumnRef("foo")},
					WhereClause: model.NewInfixExpr(
						model.NewFunction("arrayElement", model.NewColumnRef("sizes"), model.NewLiteral("'bar'")),
						"=",
						model.NewLiteral("1"),
					),
				},
			},
		},

		{
			name: "not enabled opt-in flag, we do not transform at all",
			query: &model.Query{
				TableName: "test2",
				SelectCommand: model.SelectCommand{
					FromClause: model.NewTableRef("test2"),
					Columns:    []model.Expr{model.NewColumnRef("foo")},
					WhereClause: model.NewInfixExpr(
						model.NewColumnRef("foo.bar"),
						model.MatchOperator,
						model.NewLiteral("'baz'"),
					),
				},
			},
			expected: &model.Query{
				TableName: "test2",
				SelectCommand: model.SelectCommand{
					FromClause: model.NewTableRef("test2"),
					Columns:    []model.Expr{model.NewColumnRef("foo")},
					WhereClause: model.NewInfixExpr(
						model.NewLiteral("NULL"),
						model.MatchOperator,
						model.NewLiteral("'baz'"),
					),
				},
			},
		},
	}

	asString := func(query *model.Query) string {
		return query.SelectCommand.String()
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.query.Schema = indexSchema
			tt.query.Indexes = []string{tt.query.TableName}
			actual, err := transform.Transform([]*model.Query{tt.query})
			assert.NoError(t, err)

			if err != nil {
				t.Fatal(err)
			}

			assert.True(t, len(actual) == 1, "len queries == 1")

			expectedJson := asString(tt.expected)
			actualJson := asString(actual[0])

			assert.Equal(t, expectedJson, actualJson)
		})
	}

}
