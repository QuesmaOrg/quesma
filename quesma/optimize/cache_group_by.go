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

func (s *cacheGroupByQueries) Transform(queries []*model.Query) ([]*model.Query, error) {

	for _, query := range queries {

		// TODO add better detection
		// TODO add CTE here
		if len(query.SelectCommand.GroupBy) > 0 {
			query.OptimizeHints.Settings["use_query_cache"] = true
			query.OptimizeHints.OptimizationsPerformed = append(query.OptimizeHints.OptimizationsPerformed, "cacheGroupByQueries")
		}
	}
	return queries, nil
}
