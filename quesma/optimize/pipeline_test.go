// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package optimize

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"quesma/model"
	"quesma/quesma/config"
	"testing"
)

func Test_cacheGroupBy(t *testing.T) {

	tests := []struct {
		name        string
		shouldCache bool
		tableName   string
		query       model.SelectCommand
	}{
		{
			"select all",
			false,
			"foo",
			model.SelectCommand{
				Columns:    []model.Expr{model.NewColumnRef("*")},
				FromClause: model.NewTableRef("foo"),
			},
		},

		{
			"select a, count() from foo  group by 1",
			true,
			"foo",
			model.SelectCommand{
				Columns:    []model.Expr{model.NewColumnRef("a"), model.NewFunction("count", model.NewColumnRef("*"))},
				FromClause: model.NewTableRef("foo"),
				GroupBy:    []model.Expr{model.NewLiteral(1)},
			},
		},
		// Add CTE here
	}

	cfg := config.QuesmaConfiguration{}
	cfg.IndexConfig = make(map[string]config.IndexConfiguration)
	cfg.IndexConfig["foo"] = config.IndexConfiguration{
		EnabledOptimizers: map[string]config.OptimizerConfiguration{"cache_group_by_queries": {Enabled: true}},
	}

	for _, tt := range tests {

		t.Run(tt.name, func(t *testing.T) {

			queries := []*model.Query{
				{
					SelectCommand: tt.query,
					TableName:     tt.tableName,
				},
			}
			pipeline := NewOptimizePipeline(cfg)
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
		name      string
		tableName string
		query     model.SelectCommand
		expected  model.SelectCommand
	}{
		{
			"select all",
			"foo",
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
			"foo",
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
			"foo",
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
			"foo",
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
			"foo",
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
		{
			"select all where and between dates (>24h), disabled index ",
			"foo2",
			model.SelectCommand{
				Columns:     []model.Expr{model.NewColumnRef("*")},
				FromClause:  model.NewTableRef("foo2"),
				WhereClause: and(gt(col("a"), date("2024-01-06T10:08:53.675Z")), lt(col("a"), date("2024-06-06T13:10:53.675Z"))),
			},
			model.SelectCommand{
				Columns:     []model.Expr{model.NewColumnRef("*")},
				FromClause:  model.NewTableRef("foo2"),
				WhereClause: and(gt(col("a"), date("2024-01-06T10:08:53.675Z")), lt(col("a"), date("2024-06-06T13:10:53.675Z"))),
			},
		},
		// Add CTE here
	}

	cfg := config.QuesmaConfiguration{}
	cfg.IndexConfig = make(map[string]config.IndexConfiguration)
	cfg.IndexConfig["foo"] = config.IndexConfiguration{
		EnabledOptimizers: map[string]config.OptimizerConfiguration{"truncate_date": {Enabled: true}},
	}

	for _, tt := range tests {

		t.Run(tt.name, func(t *testing.T) {

			queries := []*model.Query{
				{
					TableName:     tt.tableName,
					SelectCommand: tt.query,
				},
			}
			pipeline := NewOptimizePipeline(cfg)
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

func Test_materialized_view_replace(t *testing.T) {

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
	literal := func(a any) model.Expr { return model.NewLiteral(a) }

	tests := []struct {
		name      string
		tableName string
		query     model.SelectCommand
		expected  model.SelectCommand
	}{
		{
			"select all where date ",
			"foo",
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
			"select all with condition",
			"foo",
			model.SelectCommand{
				Columns:     []model.Expr{model.NewColumnRef("*")},
				FromClause:  model.NewTableRef("foo"),
				WhereClause: gt(col("a"), literal(10)),
			},
			model.SelectCommand{
				Columns:     []model.Expr{model.NewColumnRef("*")},
				FromClause:  model.NewTableRef("foo_view"),
				WhereClause: literal("TRUE"),
			},
		},

		{
			"select all with condition 2",
			"foo",
			model.SelectCommand{
				Columns:     []model.Expr{model.NewColumnRef("*")},
				FromClause:  model.NewTableRef("foo"),
				WhereClause: and(lt(col("c"), literal(1)), gt(col("a"), literal(10))),
			},
			model.SelectCommand{
				Columns:     []model.Expr{model.NewColumnRef("*")},
				FromClause:  model.NewTableRef("foo_view"),
				WhereClause: and(lt(col("c"), literal(1)), literal("TRUE")),
			},
		},

		{
			"select all with condition 3",
			"foo",
			model.SelectCommand{
				Columns:     []model.Expr{model.NewColumnRef("*")},
				FromClause:  model.NewTableRef("foo"),
				WhereClause: and(gt(col("a"), literal(10)), and(lt(col("c"), literal(1)), gt(col("a"), literal(10)))),
			},
			model.SelectCommand{
				Columns:     []model.Expr{model.NewColumnRef("*")},
				FromClause:  model.NewTableRef("foo_view"),
				WhereClause: and(literal("TRUE"), and(lt(col("c"), literal(1)), literal("TRUE"))),
			},
		},

		{
			"select all without condition",
			"foo",
			model.SelectCommand{
				Columns:     []model.Expr{model.NewColumnRef("*")},
				FromClause:  model.NewTableRef("foo"),
				WhereClause: lt(col("a"), literal(10)),
			},
			model.SelectCommand{
				Columns:     []model.Expr{model.NewColumnRef("*")},
				FromClause:  model.NewTableRef("foo"),
				WhereClause: lt(col("a"), literal(10)),
			},
		},

		{
			"select all from other table with condition",
			"foo",
			model.SelectCommand{
				Columns:     []model.Expr{model.NewColumnRef("*")},
				FromClause:  model.NewTableRef("foo1"),
				WhereClause: gt(col("a"), literal(10)),
			},
			model.SelectCommand{
				Columns:     []model.Expr{model.NewColumnRef("*")},
				FromClause:  model.NewTableRef("foo1"),
				WhereClause: gt(col("a"), literal(10)),
			},
		},
	}

	cfg := config.QuesmaConfiguration{}
	cfg.IndexConfig = make(map[string]config.IndexConfiguration)
	cfg.IndexConfig["foo"] = config.IndexConfiguration{
		EnabledOptimizers: map[string]config.OptimizerConfiguration{
			"materialized_view_replace": {
				Enabled: true,
				Properties: map[string]string{
					"table":     "foo",
					"condition": `"a">10`,
					"view":      "foo_view",
				},
			},
		},
	}

	for _, tt := range tests {

		t.Run(tt.name, func(t *testing.T) {

			queries := []*model.Query{
				{
					TableName:     tt.tableName,
					SelectCommand: tt.query,
				},
			}
			pipeline := NewOptimizePipeline(cfg)
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
