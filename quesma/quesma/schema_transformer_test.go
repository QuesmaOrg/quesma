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
	const StringLiteral = "'String'"
	const IpFieldContent = "'111.42.223.209/16'"
	IpFieldName := strconv.Quote("clientip")

	indexConfig := map[string]config.IndexConfiguration{
		"kibana_sample_data_logs": {
			Name:           "kibana_sample_data_logs",
			Enabled:        true,
			FullTextFields: []string{"message", "content"},
			TypeMappings:   map[string]string{IpFieldName: "ip"},
		},
		"kibana_sample_data_flights": {
			Name:           "kibana_sample_data_flights",
			Enabled:        true,
			FullTextFields: []string{"message", "content"},
			TypeMappings: map[string]string{IpFieldName: "ip",
				"DestLocation": "geo_point"},
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
		}}
	s := schema.NewSchemaRegistry(tableDiscovery, cfg, clickhouse.SchemaTypeAdapter{})
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
								&model.LiteralExpr{Value: IpFieldName},
								&model.LiteralExpr{Value: StringLiteral},
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
								&model.LiteralExpr{Value: IpFieldName},
								&model.LiteralExpr{Value: StringLiteral},
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
									&model.LiteralExpr{Value: IpFieldName},
									&model.LiteralExpr{Value: StringLiteral},
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
		resultQueries, err := transform.Transform(queries[k])
		assert.NoError(t, err)
		assert.Equal(t, expectedQueries[k].SelectCommand.String(), resultQueries[0].SelectCommand.String())
	}
}

func Test_arrayType(t *testing.T) {

	indexConfig := map[string]config.IndexConfiguration{
		"kibana_sample_data_ecommerce": {
			Name:    "kibana_sample_data_ecommerce",
			Enabled: true,
		},
	}
	cfg := config.QuesmaConfiguration{
		IndexConfig: indexConfig,
	}

	tableDiscovery :=
		fixedTableProvider{tables: map[string]schema.Table{
			"kibana_sample_data_ecommerce": {Columns: map[string]schema.Column{
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

	s := schema.NewSchemaRegistry(tableDiscovery, cfg, clickhouse.SchemaTypeAdapter{})
	transform := &SchemaCheckPass{cfg: indexConfig, schemaRegistry: s, logManager: lm}

	tests := []struct {
		name     string
		query    *model.Query
		expected *model.Query
	}{
		{
			name: "simple array",
			query: &model.Query{
				TableName: "kibana_sample_data_logs",
				SelectCommand: model.SelectCommand{
					FromClause: model.NewTableRef("kibana_sample_data_logs"),
					Columns:    []model.Expr{model.NewWildcardExpr},
				},
			},
			expected: &model.Query{
				TableName: "kibana_sample_data_logs",
				SelectCommand: model.SelectCommand{
					FromClause: model.NewTableRef("kibana_sample_data_logs"),
					Columns:    []model.Expr{model.NewWildcardExpr},
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
