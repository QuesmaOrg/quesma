// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package model

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"strconv"
	"testing"
)

func Test_validateAndParse(t *testing.T) {
	var testcases = []struct {
		searchAfter                     any
		isInputFineFoolproofStrategy    bool
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

	foolproof := SearchAfterStrategyFactory(Foolproof, ColumnRef{})
	basicAndFast := SearchAfterStrategyFactory(BasicAndFast, ColumnRef{})
	for i, tc := range testcases {
		t.Run(fmt.Sprintf("%v (testNr:%d)", tc.searchAfter, i), func(t *testing.T) {
			err := foolproof.Validate(tc.searchAfter)
			if (err == nil) != tc.isInputFineFoolproofStrategy {
				t.Errorf("Foolproof strategy failed to validate the input: %v", tc.searchAfter)
			}

			err = basicAndFast.Validate(tc.searchAfter)
			if (err == nil) != tc.isInputFineBasicAndFastStrategy {
				t.Errorf("BasicAndFast strategy failed to validate the input: %v", tc.searchAfter)
			}
		})
	}
}

func TestApplyStrategyAndTransformQuery(t *testing.T) {
	emptyQuery := func() *Query { return &Query{} }
	withWhere := func(query *Query, timestamp any) *Query {
		additionalWhere := NewInfixExpr(ColumnRef{}, "<", NewFunction("fromUnixTimestamp64Milli", NewLiteral(timestamp)))
		query.SelectCommand.WhereClause = And([]Expr{query.SelectCommand.WhereClause, additionalWhere})
		return query
	}
	oneRealQuery := func() *Query {
		return &Query{
			TableName: "kibana_sample_data_logs",
			SelectCommand: SelectCommand{
				FromClause: NewTableRef("kibana_sample_data_logs"),
				Columns:    []Expr{NewColumnRef("message")},
				WhereClause: &InfixExpr{
					Left: &InfixExpr{
						Left: &InfixExpr{
							Left: &LiteralExpr{Value: strconv.Quote("@timestamp")},
							Op:   ">=",
							Right: &FunctionExpr{
								Name: "parseDateTime64BestEffort",
								Args: []Expr{&LiteralExpr{Value: "'2024-06-06T09:58:50.387Z'"}}},
						},
						Op: "AND",
						Right: &InfixExpr{
							Left: &LiteralExpr{Value: strconv.Quote("@timestamp")},
							Op:   "<=",
							Right: &FunctionExpr{
								Name: "parseDateTime64BestEffort",
								Args: []Expr{&LiteralExpr{Value: "'2024-06-10T09:58:50.387Z'"}}},
						},
					},
					Op: "AND",
					Right: &FunctionExpr{
						Name: "a",
						Args: []Expr{
							&FunctionExpr{
								Name: "b",
								Args: []Expr{
									&AliasedExpr{
										Expr: &FunctionExpr{
											Name: "c",
											Args: []Expr{
												&LiteralExpr{Value: 8},
												&LiteralExpr{Value: "'0.0.0.0'"},
											},
										},
										Alias: "happy alias",
									},
								},
							},
							&LiteralExpr{Value: "happy literal"},
						},
					},
				},
			},
		}
	}

	var testcases = []struct {
		searchAfter      any
		query            *Query
		transformedQuery *Query
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

	//foolproof := SearchAfterStrategyFactory(Foolproof, ColumnRef{})
	basicAndFast := SearchAfterStrategyFactory(BasicAndFast, ColumnRef{})
	for i, tc := range testcases {
		t.Run(fmt.Sprintf("%v (testNr:%d)", tc.searchAfter, i), func(t *testing.T) {
			basicAndFast.ApplyStrategyAndTransformQuery(tc.query, tc.searchAfter)
			assert.Equal(t, AsString(tc.query.SelectCommand), AsString(tc.transformedQuery.SelectCommand))
		})
	}
}
