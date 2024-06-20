package quesma

import (
	"github.com/stretchr/testify/assert"
	"mitmproxy/quesma/clickhouse"
	"mitmproxy/quesma/elasticsearch"
	"mitmproxy/quesma/model"
	"mitmproxy/quesma/quesma/config"
	"mitmproxy/quesma/schema"
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
	s := schema.NewSchemaRegistry(tableDiscovery, cfg, clickhouse.SchemaTypeAdapter{}, elasticsearch.SchemaTypeAdapter{})
	s.Start()
	transform := &SchemaCheckPass{cfg: indexConfig, schemaRegistry: s}

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
