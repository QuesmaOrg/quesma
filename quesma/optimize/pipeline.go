// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package optimize

import (
	"quesma/model"
	"quesma/plugins"
	"time"
)

// OptimizePipeline - a transformer that optimizes queries
type OptimizePipeline struct {
	optimizations []plugins.QueryTransformer
}

func NewOptimizePipeline() plugins.QueryTransformer {

	return &OptimizePipeline{
		optimizations: []plugins.QueryTransformer{
			&truncateDate{truncateTo: 5 * time.Minute},
			&cacheGroupByQueries{},
		},
	}
}

func (s *OptimizePipeline) Transform(queries []*model.Query) ([]*model.Query, error) {

	// add  hints if not present
	for _, query := range queries {
		if query.OptimizeHints == nil {
			query.OptimizeHints = model.NewQueryExecutionHints()
		}
	}

	// run optimizations on queries
	for _, optimization := range s.optimizations {
		var err error
		queries, err = optimization.Transform(queries)
		if err != nil {
			return nil, err
		}
	}

	return queries, nil
}
