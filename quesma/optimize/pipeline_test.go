// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package optimize

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"quesma/model"
	"testing"
)

func Test_cacheGroupBy(t *testing.T) {

	tests := []struct {
		name        string
		shouldCache bool
		query       model.SelectCommand
	}{
		{
			"select all",
			false,
			model.SelectCommand{
				Columns:    []model.Expr{model.NewColumnRef("*")},
				FromClause: model.NewTableRef("foo"),
			},
		},

		{
			"select a, count() from foo  group by 1",
			true,
			model.SelectCommand{
				Columns:    []model.Expr{model.NewColumnRef("a"), model.NewFunction("count", model.NewColumnRef("*"))},
				FromClause: model.NewTableRef("foo"),
				GroupBy:    []model.Expr{model.NewLiteral(1)},
			},
		},
		// Add CTE here
	}

	for _, tt := range tests {

		t.Run(tt.name, func(t *testing.T) {

			queries := []*model.Query{
				{
					SelectCommand: tt.query,
				},
			}
			pipeline := NewOptimizePipeline()
			optimized, err := pipeline.Transform(queries)
			if err != nil {
				t.Fatalf("error optimizing query: %v", err)
			}

			if len(optimized) != 1 {
				t.Fatalf("expected 1 query, got %d", len(optimized))
			}

			var enabled bool
			if optimized[0].OptimizeHints.Settings["use_query_cache"] != nil {
				enabled = optimized[0].OptimizeHints.Settings["use_query_cache"].(bool)
			}

			assert.Truef(t, enabled == tt.shouldCache, "expected use_query_cache to be %v, got %v", tt.shouldCache, enabled)

		})

	}
}

func Test_dateTrunc(t *testing.T) {

	date := func(s string) model.Expr {
		return model.NewFunction("parseDateTime64BestEffort", model.NewLiteral(fmt.Sprintf("'%s'", s)))
	}

	and := func(a, b model.Expr) model.Expr {
		return model.NewInfixExpr(a, "and", b)
	}

	lt := func(a, b model.Expr) model.Expr {
		return model.NewInfixExpr(a, "<", b)
	}

	gt := func(a, b model.Expr) model.Expr {
		return model.NewInfixExpr(a, ">", b)
	}

	col := func(s string) model.Expr {
		return model.NewColumnRef(s)
	}

	tests := []struct {
		name     string
		query    model.SelectCommand
		expected model.SelectCommand
	}{
		{
			"select all",
			model.SelectCommand{
				Columns:    []model.Expr{model.NewColumnRef("*")},
				FromClause: model.NewTableRef("foo"),
			},
			model.SelectCommand{
				Columns:    []model.Expr{model.NewColumnRef("*")},
				FromClause: model.NewTableRef("foo"),
			},
		},

		{
			"select all where date ",
			model.SelectCommand{
				Columns:     []model.Expr{model.NewColumnRef("*")},
				FromClause:  model.NewTableRef("foo"),
				WhereClause: date("2024-06-04T13:08:53.675Z"),
			},
			model.SelectCommand{
				Columns:     []model.Expr{model.NewColumnRef("*")},
				FromClause:  model.NewTableRef("foo"),
				WhereClause: date("2024-06-04T13:08:53.675Z"),
			},
		},

		{
			"select all where and between dates (>24h)",
			model.SelectCommand{
				Columns:     []model.Expr{model.NewColumnRef("*")},
				FromClause:  model.NewTableRef("foo"),
				WhereClause: and(gt(col("a"), date("2024-06-04T13:08:53.675Z")), lt(col("a"), date("2024-06-06T13:10:53.675Z"))),
			},
			model.SelectCommand{
				Columns:     []model.Expr{model.NewColumnRef("*")},
				FromClause:  model.NewTableRef("foo"),
				WhereClause: and(gt(col("a"), date("2024-06-04T13:05:00Z")), lt(col("a"), date("2024-06-06T13:15:00Z"))),
			},
		},

		{
			"select all where and between dates (<24h)",
			model.SelectCommand{
				Columns:     []model.Expr{model.NewColumnRef("*")},
				FromClause:  model.NewTableRef("foo"),
				WhereClause: and(gt(col("a"), date("2024-06-06T10:08:53.675Z")), lt(col("a"), date("2024-06-06T13:10:53.675Z"))),
			},
			model.SelectCommand{
				Columns:     []model.Expr{model.NewColumnRef("*")},
				FromClause:  model.NewTableRef("foo"),
				WhereClause: and(gt(col("a"), date("2024-06-06T10:08:53.675Z")), lt(col("a"), date("2024-06-06T13:10:53.675Z"))),
			},
		},

		{
			"select a, count() from foo  group by 1",
			model.SelectCommand{
				Columns:    []model.Expr{model.NewColumnRef("a"), model.NewFunction("count", model.NewColumnRef("*"))},
				FromClause: model.NewTableRef("foo"),
				GroupBy:    []model.Expr{model.NewLiteral(1)},
			},
			model.SelectCommand{
				Columns:    []model.Expr{model.NewColumnRef("a"), model.NewFunction("count", model.NewColumnRef("*"))},
				FromClause: model.NewTableRef("foo"),
				GroupBy:    []model.Expr{model.NewLiteral(1)},
			},
		},
		// Add CTE here
	}

	for _, tt := range tests {

		t.Run(tt.name, func(t *testing.T) {

			queries := []*model.Query{
				{
					SelectCommand: tt.query,
				},
			}
			pipeline := NewOptimizePipeline()
			optimized, err := pipeline.Transform(queries)

			if err != nil {
				t.Fatalf("error optimizing query: %v", err)
			}

			if len(optimized) != 1 {
				t.Fatalf("expected 1 query, got %d", len(optimized))
			}

			assert.Equal(t, tt.expected, optimized[0].SelectCommand)

		})

	}
}
