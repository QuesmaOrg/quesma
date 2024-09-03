// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package quesma

import (
	"github.com/stretchr/testify/assert"
	"quesma/clickhouse"
	"quesma/model"
	"quesma/quesma/config"
	"quesma/schema"
	"strconv"
	"testing"
)

type fixedTableProvider struct {
	tables map[string]schema.Table
}

func (f fixedTableProvider) TableDefinitions() map[string]schema.Table {
	return f.tables
}

func Test_ipRangeTransform(t *testing.T) {
	const isIPAddressInRangePrimitive = "isIPAddressInRange"
	const CASTPrimitive = "CAST"
	const COALESCEPrimitive = "COALESCE"
	const StringLiteral = "String"
	const IpFieldContent = "'111.42.223.209/16'"
	IpFieldName := strconv.Quote("clientip")

	indexConfig := map[string]config.IndexConfiguration{
		"kibana_sample_data_logs": {
			Name:           "kibana_sample_data_logs",
			FullTextFields: []string{"message", "content"},
			SchemaOverrides: &config.SchemaConfiguration{Fields: map[config.FieldName]config.FieldConfiguration{
				config.FieldName(IpFieldName): {Type: "ip"},
			}},
		},
		// Identical to kibana_sample_data_logs, but with "nested.clientip"
		// instead of "clientip"
		"kibana_sample_data_logs_nested": {
			Name:           "kibana_sample_data_logs_nested",
			FullTextFields: []string{"message", "content"},
			SchemaOverrides: &config.SchemaConfiguration{Fields: map[config.FieldName]config.FieldConfiguration{
				"nested.clientip": {Type: "ip"},
			}},
		},
		"kibana_sample_data_flights": {
			Name:           "kibana_sample_data_flights",
			FullTextFields: []string{"message", "content"},
			SchemaOverrides: &config.SchemaConfiguration{Fields: map[config.FieldName]config.FieldConfiguration{
				config.FieldName(IpFieldName): {Type: "ip"},
				"DestLocation":                {Type: "geo_point"},
			}},
		},
	}
	cfg := config.QuesmaConfiguration{
		IndexConfig: indexConfig,
	}

	tableDiscovery :=
		fixedTableProvider{tables: map[string]schema.Table{
			"kibana_sample_data_flights": {Columns: map[string]schema.Column{
				"DestLocation": {Name: "DestLocation", Type: "geo_point"},
				"clientip":     {Name: "clientip", Type: "ip"},
			}},
			"kibana_sample_data_logs_nested": {Columns: map[string]schema.Column{
				"DestLocation":     {Name: "DestLocation", Type: "geo_point"},
				"nested::clientip": {Name: "nested::clientip", Type: "ip"},
			}},
		}}
	s := schema.NewSchemaRegistry(tableDiscovery, &cfg, clickhouse.SchemaTypeAdapter{})
	transform := &SchemaCheckPass{cfg: indexConfig, schemaRegistry: s, logManager: clickhouse.NewLogManagerEmpty()}

	expectedQueries := []*model.Query{
		{
			TableName: "kibana_sample_data_logs",
			SelectCommand: model.SelectCommand{
				FromClause: model.NewTableRef("kibana_sample_data_logs"),
				Columns:    []model.Expr{model.NewWildcardExpr},
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
				Columns:    []model.Expr{model.NewWildcardExpr},
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
											&model.ColumnRef{ColumnName: "nested::clientip"},
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
				Columns:    []model.Expr{model.NewWildcardExpr},
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
				Columns:    []model.Expr{model.NewWildcardExpr},
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
				Columns:    []model.Expr{model.NewWildcardExpr},
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
				Columns:    []model.Expr{model.NewWildcardExpr},
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
				Columns: []model.Expr{model.NewColumnRef("DestLocation::lat"),
					model.NewColumnRef("DestLocation::lon")},
			}},
	}
	queries := [][]*model.Query{
		{
			{
				TableName: "kibana_sample_data_logs",
				SelectCommand: model.SelectCommand{
					FromClause: model.NewTableRef("kibana_sample_data_logs"),
					Columns:    []model.Expr{model.NewWildcardExpr},
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
					Columns:    []model.Expr{model.NewWildcardExpr},
					WhereClause: &model.InfixExpr{
						Left:  &model.ColumnRef{ColumnName: "nested::clientip"},
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
					Columns:    []model.Expr{model.NewWildcardExpr},
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
					Columns:    []model.Expr{model.NewWildcardExpr},
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
					Columns:    []model.Expr{model.NewWildcardExpr},
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
					Columns:    []model.Expr{model.NewWildcardExpr},
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
							Left:  &model.LiteralExpr{Value: strconv.Quote("clientip")},
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
			resultQueries, err := transform.Transform(queries[k])
			assert.NoError(t, err)
			assert.Equal(t, expectedQueries[k].SelectCommand.String(), resultQueries[0].SelectCommand.String())
		})
	}
}

func Test_arrayType(t *testing.T) {

	indexConfig := map[string]config.IndexConfiguration{
		"kibana_sample_data_ecommerce": {
			Name: "kibana_sample_data_ecommerce",
		},
	}
	cfg := config.QuesmaConfiguration{
		IndexConfig: indexConfig,
	}

	tableDiscovery :=
		fixedTableProvider{tables: map[string]schema.Table{
			"kibana_sample_data_ecommerce": {Columns: map[string]schema.Column{
				"@timestamp":         {Name: "@timestamp", Type: "DateTime64"},
				"products::name":     {Name: "products::name", Type: "keyword"},
				"products::quantity": {Name: "products::quantity", Type: "long"},
				"products::sku":      {Name: "products::sku", Type: "keyword"},
				"order_date":         {Name: "order_date", Type: "timestamp"},
			}},
		}}

	tableDefinition := clickhouse.Table{
		Name:   "kibana_sample_data_ecommerce",
		Config: clickhouse.NewDefaultCHConfig(),
		Cols: map[string]*clickhouse.Column{
			"@timestamp":         {Name: "@timestamp", Type: clickhouse.NewBaseType("DateTime64")},
			"products::name":     {Name: "products::name", Type: clickhouse.NewBaseType("Array(String)")},
			"products::quantity": {Name: "products::quantity", Type: clickhouse.NewBaseType("Array(Int64)")},
			"products::sku":      {Name: "products::sku", Type: clickhouse.NewBaseType("Array(String)")},
			"order_date":         {Name: "order_date", Type: clickhouse.NewBaseType("DateTime64")},
		},
	}

	lm := clickhouse.NewLogManagerEmpty()

	td, err := lm.GetTableDefinitions()
	if err != nil {
		t.Fatal(err)
	}
	td.Store("kibana_sample_data_ecommerce", &tableDefinition)

	s := schema.NewSchemaRegistry(tableDiscovery, &cfg, clickhouse.SchemaTypeAdapter{})
	transform := &SchemaCheckPass{cfg: indexConfig, schemaRegistry: s, logManager: lm}

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
					Columns:    []model.Expr{model.NewColumnRef("order_date"), model.NewColumnRef("products::name"), model.NewColumnRef("products::quantity"), model.NewColumnRef("products::sku")},
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
						model.NewFunction("sumOrNull", model.NewColumnRef("products::quantity")),
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
						model.NewFunction("sumOrNull", model.NewFunction("arrayReduce", model.NewLiteral("'sumOrNull'"), model.NewColumnRef("products::quantity"))),
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
						model.NewFunction("count"),
					},
					WhereClause: model.NewInfixExpr(
						model.NewColumnRef("products::name"),
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
						model.NewFunction("count"),
					},
					WhereClause: model.NewFunction(
						"arrayExists",
						model.NewLambdaExpr([]string{"x"}, model.NewInfixExpr(model.NewLiteral("x"), "ILIKE", model.NewLiteral("%foo%"))),
						model.NewColumnRef("products::name")),
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
						model.NewFunction("count"),
					},
					WhereClause: model.NewInfixExpr(
						model.NewColumnRef("products::sku"),
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
						model.NewFunction("count"),
					},
					WhereClause: model.NewFunction(
						"has",
						model.NewColumnRef("products::sku"),
						model.NewLiteral("'XYZ'")),
					GroupBy: []model.Expr{model.NewColumnRef("order_date")},
				},
			},
		},
	}

	asString := func(query *model.Query) string {
		return query.SelectCommand.String()
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
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
		"kibana_sample_data_ecommerce": {
			Name: "kibana_sample_data_ecommerce",
		},
	}
	cfg := config.QuesmaConfiguration{
		IndexConfig: indexConfig,
	}

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
	transform := &SchemaCheckPass{cfg: indexConfig, schemaRegistry: s, logManager: lm}

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

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			query := &model.Query{
				TableName: "test",
				SelectCommand: model.SelectCommand{
					Columns: toSelectColumn(tt.input),
				},
			}

			actual, err := transform.applyWildcardExpansion(query)

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
		"test": {
			Name: "kibana_sample_data_ecommerce",
		},
	}
	cfg := config.QuesmaConfiguration{
		IndexConfig: indexConfig,
	}

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
	transform := &SchemaCheckPass{cfg: indexConfig, schemaRegistry: s, logManager: lm}

	tests := []struct {
		name     string
		input    model.SelectCommand
		expected model.SelectCommand
	}{
		{
			"single table",
			model.SelectCommand{
				FromClause: model.NewTableRef(model.SingleTableNamePlaceHolder),
				Columns: []model.Expr{
					model.NewColumnRef("a"),
					model.NewFunction("count"),
				},
			},
			model.SelectCommand{
				FromClause: model.NewTableRef("test"),
				Columns: []model.Expr{
					model.NewColumnRef("a"),
					model.NewFunction("count"),
				},
			},
		},

		{
			"cte with fixed table name",
			model.SelectCommand{
				FromClause: model.NewTableRef(model.SingleTableNamePlaceHolder),
				Columns: []model.Expr{
					model.NewColumnRef("a"),
					model.NewFunction("count"),
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
			model.SelectCommand{
				FromClause: model.NewTableRef("test"),
				Columns: []model.Expr{
					model.NewColumnRef("a"),
					model.NewFunction("count"),
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
			"cte with  table name",
			model.SelectCommand{
				FromClause: model.NewTableRef(model.SingleTableNamePlaceHolder),
				Columns: []model.Expr{
					model.NewColumnRef("order_date"),
					model.NewFunction("count"),
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
			model.SelectCommand{
				FromClause: model.NewTableRef("test"),
				Columns: []model.Expr{
					model.NewColumnRef("order_date"),
					model.NewFunction("count"),
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

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			query := &model.Query{
				TableName:     "test",
				SelectCommand: tt.input,
			}

			expectedAsString := model.AsString(tt.expected)

			actual, err := transform.applyPhysicalFromExpression(query)

			if err != nil {
				t.Fatal(err)
			}

			actualAsString := model.AsString(actual.SelectCommand)

			assert.Equal(t, expectedAsString, actualAsString)
		})
	}
}

func TestFullTextFields(t *testing.T) {

	lm := clickhouse.NewLogManagerEmpty()

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
					model.NewFunction("count"),
				},
				WhereClause: model.NewInfixExpr(model.NewColumnRef(model.FullTextFieldNamePlaceHolder), "=", model.NewLiteral("foo")),
			},
			model.SelectCommand{
				FromClause: model.NewTableRef("test"),
				Columns: []model.Expr{
					model.NewColumnRef("a"),
					model.NewFunction("count"),
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
					model.NewFunction("count"),
				},
				WhereClause: model.NewInfixExpr(model.NewColumnRef(model.FullTextFieldNamePlaceHolder), "=", model.NewLiteral("foo")),
			},
			model.SelectCommand{
				FromClause: model.NewTableRef("test"),
				Columns: []model.Expr{
					model.NewColumnRef("a"),
					model.NewFunction("count"),
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
					model.NewFunction("count"),
				},
				WhereClause: model.NewInfixExpr(model.NewColumnRef(model.FullTextFieldNamePlaceHolder), "=", model.NewLiteral("foo")),
			},
			model.SelectCommand{
				FromClause: model.NewTableRef("test"),
				Columns: []model.Expr{
					model.NewColumnRef("a"),
					model.NewFunction("count"),
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
					Name: "test",
					SchemaOverrides: &config.SchemaConfiguration{
						Fields: fieldOverrides,
					},
				},
			}

			cfg := config.QuesmaConfiguration{
				IndexConfig: indexConfig,
			}

			s := schema.NewSchemaRegistry(tableDiscovery, &cfg, clickhouse.SchemaTypeAdapter{})
			transform := &SchemaCheckPass{cfg: indexConfig, schemaRegistry: s, logManager: lm}

			expectedAsString := model.AsString(tt.expected)

			actual, err := transform.applyFullTextField(query)

			if err != nil {
				t.Fatal(err)
			}

			actualAsString := model.AsString(actual.SelectCommand)

			assert.Equal(t, expectedAsString, actualAsString)
		})
	}
}
