// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package optimize

import (
	"github.com/QuesmaOrg/quesma/platform/model"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestSplitTimeRange_no_change(t *testing.T) {
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
