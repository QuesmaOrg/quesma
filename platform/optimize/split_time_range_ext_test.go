// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package optimize

import (
	"github.com/QuesmaOrg/quesma/platform/model"
	"github.com/k0kubun/pp"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestSplitTimeRange_no_change_15m(t *testing.T) {
	transformer := &splitTimeRangeExt{}
	plan := model.NewExecutionPlan([]*model.Query{{
		SelectCommand: model.SelectCommand{
			Columns:    []model.Expr{model.NewColumnRef("*")},
			FromClause: model.NewTableRef("foo"),
			Limit:      500,
			OrderBy:    []model.OrderByExpr{model.NewOrderByExpr(model.NewColumnRef("@timestamp"), model.DescOrder)},
			WhereClause: model.And([]model.Expr{
				model.NewInfixExpr(model.NewColumnRef("@timestamp"), ">=", model.NewFunction("fromUnixTimestamp64Milli", model.NewLiteral(int64(1749549192480)))),
				model.NewInfixExpr(model.NewColumnRef("@timestamp"), "<=", model.NewFunction("fromUnixTimestamp64Milli", model.NewLiteral(int64(1749550092480)))),
				model.NewInfixExpr(model.NewColumnRef("status"), "=", model.NewLiteral("active")),
			}),
		},
	}}, nil)

	newPlan, err := transformer.Transform(plan, make(map[string]string))

	assert.NoError(t, err)

	assert.Equal(t, 1, len(newPlan.Queries))
	assert.Equal(t, plan.Queries[0].SelectCommand, newPlan.Queries[0].SelectCommand)
}

func TestSplitTimeRange_split_1h(t *testing.T) {
	transformer := &splitTimeRangeExt{}
	timestamp := 1749550092480
	timestamp15MinutesAgo := timestamp - (15 * 60 * 1000) // 15 minutes in milliseconds
	timestampHourAgo := timestamp - (60 * 60 * 1000)      // 1 hour in milliseconds

	newTimestampWhereExpr := func(operator string, timestampArg int64) model.Expr {
		return model.NewInfixExpr(
			model.NewColumnRef("@timestamp"),
			operator,
			model.NewFunction("fromUnixTimestamp64Milli", model.NewLiteral(timestampArg)),
		)
	}

	plan := model.NewExecutionPlan([]*model.Query{{
		SelectCommand: model.SelectCommand{
			Columns:    []model.Expr{model.NewColumnRef("*")},
			FromClause: model.NewTableRef("foo"),
			Limit:      500,
			OrderBy:    []model.OrderByExpr{model.NewOrderByExpr(model.NewColumnRef("@timestamp"), model.DescOrder)},
			WhereClause: model.And([]model.Expr{
				newTimestampWhereExpr(">=", int64(timestampHourAgo)),
				newTimestampWhereExpr("<=", int64(timestamp)),
				model.NewInfixExpr(model.NewColumnRef("status"), "=", model.NewLiteral("active")),
			}),
		},
	}}, nil)

	newPlan, err := transformer.Transform(plan, make(map[string]string))

	assert.NoError(t, err)

	assert.Equal(t, 2, len(newPlan.Queries))
	for q := range newPlan.Queries {
		assert.Equal(t, plan.Queries[0].SelectCommand.Columns, newPlan.Queries[q].SelectCommand.Columns)
		assert.Equal(t, plan.Queries[0].SelectCommand.FromClause, newPlan.Queries[q].SelectCommand.FromClause)
		assert.Equal(t, plan.Queries[0].SelectCommand.Limit, newPlan.Queries[q].SelectCommand.Limit)
		assert.Equal(t, plan.Queries[0].SelectCommand.OrderBy, newPlan.Queries[q].SelectCommand.OrderBy)
	}

	expectedWhereA := model.And([]model.Expr{
		model.And([]model.Expr{
			newTimestampWhereExpr(">=", int64(timestampHourAgo)),
			newTimestampWhereExpr("<=", int64(timestamp)),
			model.NewInfixExpr(model.NewColumnRef("status"), "=", model.NewLiteral("active")),
		}),
		model.And([]model.Expr{
			newTimestampWhereExpr(">=", int64(timestampHourAgo)),
			newTimestampWhereExpr("<=", int64(timestamp15MinutesAgo)),
		}),
	})
	expectedWhereB := model.And([]model.Expr{
		model.And([]model.Expr{
			newTimestampWhereExpr(">=", int64(timestampHourAgo)),
			newTimestampWhereExpr("<=", int64(timestamp)),
			model.NewInfixExpr(model.NewColumnRef("status"), "=", model.NewLiteral("active")),
		}),
		model.And([]model.Expr{
			newTimestampWhereExpr(">=", int64(timestamp15MinutesAgo)),
			newTimestampWhereExpr("<=", int64(timestamp)),
		}),
	})

	pp.Println("Expected Where A: ", model.AsString(expectedWhereA))
	pp.Println("Actual Where A:   ", model.AsString(newPlan.Queries[0].SelectCommand.WhereClause))
	pp.Println("Expected Where B: ", model.AsString(expectedWhereB))
	pp.Println("Actual Where B:   ", model.AsString(newPlan.Queries[1].SelectCommand.WhereClause))

	assert.Equal(t, expectedWhereA, newPlan.Queries[0].SelectCommand.WhereClause)
	assert.Equal(t, expectedWhereB, newPlan.Queries[1].SelectCommand.WhereClause)
}
