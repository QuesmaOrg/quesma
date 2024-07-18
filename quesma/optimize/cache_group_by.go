// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package optimize

import "quesma/model"

// cacheGroupByQueries - a transformer that suggests db to cache the query results
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

type cacheGroupByQueries struct {
}

func (s *cacheGroupByQueries) Name() string {
	return "cache_group_by_queries"
}

func (s *cacheGroupByQueries) IsEnabledByDefault() bool {
	// this transformer can use a lot of memory on database side
	return false
}

func (s *cacheGroupByQueries) Transform(queries []*model.Query, properties map[string]string) ([]*model.Query, error) {

	for _, query := range queries {

		// TODO add better detection
		// TODO add CTE here
		if len(query.SelectCommand.GroupBy) > 0 {
			query.OptimizeHints.Settings["use_query_cache"] = true
			query.OptimizeHints.OptimizationsPerformed = append(query.OptimizeHints.OptimizationsPerformed, s.Name())
		}
	}
	return queries, nil
}
