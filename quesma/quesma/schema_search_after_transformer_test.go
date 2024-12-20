// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package quesma

import (
	"fmt"
	"quesma/model"
	"quesma/schema"
	"strconv"
	"testing"
)

func Test_validateAndParse(t *testing.T) {
	fields := map[schema.FieldName]schema.Field{
		"message":    {PropertyName: "message", InternalPropertyName: "message", Type: schema.QuesmaTypeText},
		"@timestamp": {PropertyName: "@timestamp", InternalPropertyName: "@timestamp", Type: schema.QuesmaTypeDate},
	}
	Schema := schema.NewSchema(fields, true, "")

	var testcases = []struct {
		searchAfter                     any
		isInputFineBulletproofStrategy  bool
		isInputFineBasicAndFastStrategy bool
	}{
		{nil, true, true},
		{[]any{}, false, false},
		{[]any{1}, true, true},
		{[]any{1.0}, true, true},
		{[]any{1.1}, false, false},
		{[]any{-1}, false, false},
		{[]any{1, 3}, true, false},
		{"string is bad", false, false},
	}

	strategy := searchAfterStrategyFactory(basicAndFast)
	for i, tc := range testcases {
		t.Run(fmt.Sprintf("%v (testNr:%d)", tc.searchAfter, i), func(t *testing.T) {
			query := &model.Query{}
			query.SelectCommand.OrderBy = []model.OrderByExpr{model.NewOrderByExprWithoutOrder(model.NewColumnRef("@timestamp"))}
			query.SearchAfter = tc.searchAfter
			_, err := strategy.validateAndParse(query, Schema)
			if (err == nil) != tc.isInputFineBasicAndFastStrategy {
				t.Errorf("BasicAndFast strategy failed to validate the input: %v, err: %v", tc.searchAfter, err)
			}
		})
	}
}

func TestApplyStrategyAndTransformQuery(t *testing.T) {
	emptyQuery := func() *model.Query { return &model.Query{} }
	withWhere := func(query *model.Query, timestamp any) *model.Query {
		additionalWhere := model.NewInfixExpr(model.ColumnRef{}, "<", model.NewFunction("fromUnixTimestamp64Milli", model.NewLiteral(timestamp)))
		query.SelectCommand.WhereClause = model.And([]model.Expr{query.SelectCommand.WhereClause, additionalWhere})
		return query
	}
	oneRealQuery := func() *model.Query {
		return &model.Query{
			TableName: "kibana_sample_data_logs",
			SelectCommand: model.SelectCommand{
				FromClause: model.NewTableRef("kibana_sample_data_logs"),
				Columns:    []model.Expr{model.NewColumnRef("message")},
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
						Name: "a",
						Args: []model.Expr{
							&model.FunctionExpr{
								Name: "b",
								Args: []model.Expr{
									&model.AliasedExpr{
										Expr: &model.FunctionExpr{
											Name: "c",
											Args: []model.Expr{
												&model.LiteralExpr{Value: 8},
												&model.LiteralExpr{Value: "'0.0.0.0'"},
											},
										},
										Alias: "happy alias",
									},
								},
							},
							&model.LiteralExpr{Value: "happy literal"},
						},
					},
				},
			},
		}
	}

	var testcases = []struct {
		searchAfter      any
		query            *model.Query
		transformedQuery *model.Query
	}{
		{nil, emptyQuery(), emptyQuery()},
		{[]any{}, emptyQuery(), emptyQuery()},
		{[]any{1}, emptyQuery(), withWhere(emptyQuery(), 1)},
		{[]any{1.0}, emptyQuery(), withWhere(emptyQuery(), 1)},
		{[]any{1.1}, emptyQuery(), emptyQuery()},
		{[]any{5, 10}, emptyQuery(), emptyQuery()},
		{[]any{-1}, emptyQuery(), emptyQuery()},
		{"string is bad", emptyQuery(), emptyQuery()},
		{[]any{int64(1)}, oneRealQuery(), withWhere(oneRealQuery(), 1)},
	}

	//strategy := searchAfterStrategyFactory(basicAndFast)
	for i, tc := range testcases {
		t.Run(fmt.Sprintf("%v (testNr:%d)", tc.searchAfter, i), func(t *testing.T) {
			//basicAndFast.transform(tc.query, tc.searchAfter)
			//assert.Equal(t, AsString(tc.query.SelectCommand), AsString(tc.transformedQuery.SelectCommand))
		})
	}
}
