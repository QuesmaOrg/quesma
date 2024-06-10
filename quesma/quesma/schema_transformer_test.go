package quesma

import (
	"context"
	"github.com/stretchr/testify/assert"
	"mitmproxy/quesma/model"
	"mitmproxy/quesma/queryparser/where_clause"
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
			FromClause: model.NewSelectColumnFromString("kibana_sample_data_logs"),
			TableName:  "kibana_sample_data_logs",
			Columns: []model.SelectColumn{{
				Expression: model.NewWildcardExpr,
			},
			},
			WhereClause: &where_clause.Function{
				Name: where_clause.Literal{Name: isIPAddressInRangePrimitive},
				Args: []where_clause.Statement{
					&where_clause.Function{
						Name: where_clause.Literal{Name: CASTPrimitive},
						Args: []where_clause.Statement{
							&where_clause.Literal{Name: IpFieldName},
							&where_clause.Literal{Name: StringLiteral},
						},
					},
					&where_clause.Literal{Name: IpFieldContent},
				},
			},
		},
		{
			FromClause: model.NewSelectColumnFromString("kibana_sample_data_logs"),
			TableName:  "kibana_sample_data_logs",
			Columns: []model.SelectColumn{{
				Expression: model.NewWildcardExpr,
			},
			},
			WhereClause: &where_clause.InfixOp{
				Left:  &where_clause.Literal{Name: IpFieldName},
				Op:    "<",
				Right: &where_clause.Literal{Name: IpFieldContent},
			},
		},
		{
			FromClause: model.NewSelectColumnFromString("kibana_sample_data_logs"),
			TableName:  "kibana_sample_data_logs",
			Columns: []model.SelectColumn{{
				Expression: model.NewWildcardExpr,
			},
			},
			WhereClause: &where_clause.Function{
				Name: where_clause.Literal{Name: isIPAddressInRangePrimitive},
				Args: []where_clause.Statement{
					&where_clause.Function{
						Name: where_clause.Literal{Name: CASTPrimitive},
						Args: []where_clause.Statement{
							&where_clause.Literal{Name: IpFieldName},
							&where_clause.Literal{Name: StringLiteral},
						},
					},
					&where_clause.Literal{Name: IpFieldContent},
				},
			},
		},
		//SELECT * FROM "default"."kibana_sample_data_logs" WHERE
		//(("@timestamp">=parseDateTime64BestEffort('2024-06-06T09:58:50.387Z') AND
		//"@timestamp"<=parseDateTime64BestEffort('2024-06-10T09:58:50.387Z')) AND
		//isIPAddressInRange(CAST(clientip,'String'),'32.208.36.11/16'))
		{
			FromClause: model.NewSelectColumnFromString("kibana_sample_data_logs"),
			TableName:  "kibana_sample_data_logs",
			Columns: []model.SelectColumn{{
				Expression: model.NewWildcardExpr,
			},
			},
			WhereClause: &where_clause.InfixOp{
				Left: &where_clause.InfixOp{
					Left: &where_clause.InfixOp{
						Left: &where_clause.Literal{Name: strconv.Quote("@timestamp")},
						Op:   ">=",
						Right: &where_clause.Function{
							Name: where_clause.Literal{Name: "parseDateTime64BestEffort"},
							Args: []where_clause.Statement{&where_clause.Literal{Name: "'2024-06-06T09:58:50.387Z'"}}},
					},
					Op: "AND",
					Right: &where_clause.InfixOp{
						Left: &where_clause.Literal{Name: strconv.Quote("@timestamp")},
						Op:   "<=",
						Right: &where_clause.Function{
							Name: where_clause.Literal{Name: "parseDateTime64BestEffort"},
							Args: []where_clause.Statement{&where_clause.Literal{Name: "'2024-06-10T09:58:50.387Z'"}}},
					},
				},
				Op: "AND",
				Right: &where_clause.Function{
					Name: where_clause.Literal{Name: isIPAddressInRangePrimitive},
					Args: []where_clause.Statement{
						&where_clause.Function{
							Name: where_clause.Literal{Name: CASTPrimitive},
							Args: []where_clause.Statement{
								&where_clause.Literal{Name: IpFieldName},
								&where_clause.Literal{Name: StringLiteral},
							},
						},
						&where_clause.Literal{Name: IpFieldContent},
					},
				},
			},
		},
	}
	queries := [][]*model.Query{
		{
			{
				FromClause: model.NewSelectColumnFromString("kibana_sample_data_logs"),
				TableName:  "kibana_sample_data_logs",
				Columns: []model.SelectColumn{{
					Expression: model.NewWildcardExpr,
				},
				},
				WhereClause: &where_clause.InfixOp{
					Left:  &where_clause.Literal{Name: IpFieldName},
					Op:    "=",
					Right: &where_clause.Literal{Name: IpFieldContent},
				},
			},
		},
		{
			{
				FromClause: model.NewSelectColumnFromString("kibana_sample_data_logs"),
				TableName:  "kibana_sample_data_logs",
				Columns: []model.SelectColumn{{
					Expression: model.NewWildcardExpr,
				},
				},
				WhereClause: &where_clause.InfixOp{
					Left:  &where_clause.Literal{Name: IpFieldName},
					Op:    "<",
					Right: &where_clause.Literal{Name: IpFieldContent},
				},
			},
		},
		{
			{
				FromClause: model.NewSelectColumnFromString("kibana_sample_data_logs"),
				TableName:  "kibana_sample_data_logs",
				Columns: []model.SelectColumn{{
					Expression: model.NewWildcardExpr,
				},
				},
				WhereClause: &where_clause.InfixOp{
					Left:  &where_clause.Literal{Name: IpFieldName},
					Op:    "iLIKE",
					Right: &where_clause.Literal{Name: IpFieldContent},
				},
			},
		},
		//SELECT * FROM "default"."kibana_sample_data_logs" WHERE
		//(("@timestamp">=parseDateTime64BestEffort('2024-06-06T09:58:50.387Z') AND
		//"@timestamp"<=parseDateTime64BestEffort('2024-06-10T09:58:50.387Z')) AND
		//"clientip" iLIKE '%32.208.36.11/16%')
		{
			{
				FromClause: model.NewSelectColumnFromString("kibana_sample_data_logs"),
				TableName:  "kibana_sample_data_logs",
				Columns: []model.SelectColumn{{
					Expression: model.NewWildcardExpr,
				},
				},
				WhereClause: &where_clause.InfixOp{
					Left: &where_clause.InfixOp{
						Left: &where_clause.InfixOp{
							Left: &where_clause.Literal{Name: strconv.Quote("@timestamp")},
							Op:   ">=",
							Right: &where_clause.Function{
								Name: where_clause.Literal{Name: "parseDateTime64BestEffort"},
								Args: []where_clause.Statement{&where_clause.Literal{Name: "'2024-06-06T09:58:50.387Z'"}}},
						},
						Op: "AND",
						Right: &where_clause.InfixOp{
							Left: &where_clause.Literal{Name: strconv.Quote("@timestamp")},
							Op:   "<=",
							Right: &where_clause.Function{
								Name: where_clause.Literal{Name: "parseDateTime64BestEffort"},
								Args: []where_clause.Statement{&where_clause.Literal{Name: "'2024-06-10T09:58:50.387Z'"}}},
						},
					},
					Op: "AND",
					Right: &where_clause.InfixOp{
						Left:  &where_clause.Literal{Name: strconv.Quote("clientip")},
						Op:    "iLIKE",
						Right: &where_clause.Literal{Name: IpFieldContent},
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
