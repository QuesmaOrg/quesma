// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package optimize

import "github.com/QuesmaOrg/quesma/platform/model"

// cacheQueries - a transformer that suggests db to cache the query results
//
// It's done by adding settings to the query
//
// https://clickhouse.com/docs/en/operations/query-cache
//
// Cached queries can be examined with:
//
// select * from system.query_cache
//
// Cache can be dropped with
//
//  SYSTEM DROP QUERY CACHE
//

type cacheQueries struct {
}

func (s *cacheQueries) Name() string {
	return "cache_queries"
}

func (s *cacheQueries) IsEnabledByDefault() bool {
	// this transformer can use a lot of memory on database side
	return false
}

func (s *cacheQueries) Transform(queries []*model.Query, properties map[string]string) ([]*model.Query, error) {

	for _, query := range queries {

		var hasGroupBy bool
		var hasWindowFunction bool
		var hasCount bool
		visitor := model.NewBaseVisitor()

		visitor.OverrideVisitSelectCommand = func(v *model.BaseExprVisitor, query model.SelectCommand) interface{} {

			if len(query.GroupBy) > 0 {
				hasGroupBy = true
			}

			for _, expr := range query.Columns {
				expr.Accept(v)
			}

			if query.FromClause != nil {
				query.FromClause.Accept(v)
			}
			if query.WhereClause != nil {
				query.WhereClause.Accept(v)
			}

			return query
		}

		// we use window functions in  aggregation queries
		visitor.OverrideVisitWindowFunction = func(v *model.BaseExprVisitor, f model.WindowFunction) interface{} {
			hasWindowFunction = true
			return f
		}

		visitor.OverrideVisitFunction = func(v *model.BaseExprVisitor, f model.FunctionExpr) interface{} {

			if f.Name == "count" {
				hasCount = true
			}
			return f

		}

		query.SelectCommand.Accept(visitor)

		if hasGroupBy || hasWindowFunction || hasCount {
			query.OptimizeHints.ClickhouseQuerySettings["use_query_cache"] = true
			query.OptimizeHints.OptimizationsPerformed = append(query.OptimizeHints.OptimizationsPerformed, s.Name())
		}

	}
	return queries, nil
}
