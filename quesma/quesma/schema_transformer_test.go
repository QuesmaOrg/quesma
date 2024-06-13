package quesma

import (
	"context"
	"github.com/stretchr/testify/assert"
	"mitmproxy/quesma/model"
	"mitmproxy/quesma/quesma/config"
	"strconv"
	"testing"
)

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
	}
	transform := &SchemaCheckPass{cfg: indexConfig}

	expectedQueries := []*model.Query{
		{
			FromClause: model.NewTableRef("kibana_sample_data_logs"),
			TableName:  "kibana_sample_data_logs",
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
		{
			FromClause: model.NewTableRef("kibana_sample_data_logs"),
			TableName:  "kibana_sample_data_logs",
			Columns:    []model.Expr{model.NewWildcardExpr},
			WhereClause: &model.InfixExpr{
				Left:  &model.LiteralExpr{Value: IpFieldName},
				Op:    "<",
				Right: &model.LiteralExpr{Value: IpFieldContent},
			},
		},
		{
			FromClause: model.NewTableRef("kibana_sample_data_logs"),
			TableName:  "kibana_sample_data_logs",
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
		//SELECT * FROM "default"."kibana_sample_data_logs" WHERE
		//(("@timestamp">=parseDateTime64BestEffort('2024-06-06T09:58:50.387Z') AND
		//"@timestamp"<=parseDateTime64BestEffort('2024-06-10T09:58:50.387Z')) AND
		//isIPAddressInRange(CAST(clientip,'String'),'32.208.36.11/16'))
		{
			FromClause: model.NewTableRef("kibana_sample_data_logs"),
			TableName:  "kibana_sample_data_logs",
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
		},
	}
	queries := [][]*model.Query{
		{
			{
				FromClause: model.NewTableRef("kibana_sample_data_logs"),
				TableName:  "kibana_sample_data_logs",
				Columns:    []model.Expr{model.NewWildcardExpr},
				WhereClause: &model.InfixExpr{
					Left:  &model.LiteralExpr{Value: IpFieldName},
					Op:    "=",
					Right: &model.LiteralExpr{Value: IpFieldContent},
				},
			},
		},
		{
			{
				FromClause: model.NewTableRef("kibana_sample_data_logs"),
				TableName:  "kibana_sample_data_logs",
				Columns:    []model.Expr{model.NewWildcardExpr},
				WhereClause: &model.InfixExpr{
					Left:  &model.LiteralExpr{Value: IpFieldName},
					Op:    "<",
					Right: &model.LiteralExpr{Value: IpFieldContent},
				},
			},
		},
		{
			{
				FromClause: model.NewTableRef("kibana_sample_data_logs"),
				TableName:  "kibana_sample_data_logs",
				Columns:    []model.Expr{model.NewWildcardExpr},
				WhereClause: &model.InfixExpr{
					Left:  &model.LiteralExpr{Value: IpFieldName},
					Op:    "iLIKE",
					Right: &model.LiteralExpr{Value: IpFieldContent},
				},
			},
		},
		//SELECT * FROM "default"."kibana_sample_data_logs" WHERE
		//(("@timestamp">=parseDateTime64BestEffort('2024-06-06T09:58:50.387Z') AND
		//"@timestamp"<=parseDateTime64BestEffort('2024-06-10T09:58:50.387Z')) AND
		//"clientip" iLIKE '%32.208.36.11/16%')
		{
			{
				FromClause: model.NewTableRef("kibana_sample_data_logs"),
				TableName:  "kibana_sample_data_logs",
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
			},
		},
	}
	for k := range queries {
		resultQueries, err := transform.Transform(queries[k])
		assert.NoError(t, err)
		assert.Equal(t, expectedQueries[k].String(context.Background()), resultQueries[0].String(context.Background()))
	}
}
