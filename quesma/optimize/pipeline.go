// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package optimize

import (
	"quesma/model"
	"quesma/plugins"
	"quesma/quesma/config"
	"time"
)

// OptimizeTransformer - an interface for query transformers that have a name.
type OptimizeTransformer interface {
	plugins.QueryTransformer
	Name() string             // this name is used to enable/disable the transformer in the configuration
	IsEnabledByDefault() bool // should return true for "not aggressive" transformers only
}

// OptimizePipeline - a transformer that optimizes queries
type OptimizePipeline struct {
	config        config.QuesmaConfiguration
	optimizations []OptimizeTransformer
}

func NewOptimizePipeline(config config.QuesmaConfiguration) plugins.QueryTransformer {

	return &OptimizePipeline{
		config: config,
		optimizations: []OptimizeTransformer{
			&truncateDate{truncateTo: 5 * time.Minute},
			&cacheGroupByQueries{},
		},
	}
}

func (s *OptimizePipeline) getIndexName(queries []*model.Query) string {

	// HACK - this is a temporary solution
	// We should have struct:
	// sth like this:
	// type ExecutionPlan struct {
	//  IndexName string
	// 	Queries []*model.Query
	//  ...
	// }

	return queries[0].TableName
}

func (s *OptimizePipeline) isEnabledFor(transformer OptimizeTransformer, queries []*model.Query) bool {

	indexName := s.getIndexName(queries)

	// first we check index specific settings
	if indexCfg, ok := s.config.IndexConfig[indexName]; ok {
		if enabled, ok := indexCfg.EnabledOptimizers[transformer.Name()]; ok {
			return enabled
		}
	}

	// then we check global settings
	if enabled, ok := s.config.EnabledOptimizers[transformer.Name()]; ok {
		return enabled
	}

	// default is not enabled
	return transformer.IsEnabledByDefault()
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

		if !s.isEnabledFor(optimization, queries) {
			continue
		}

		var err error
		queries, err = optimization.Transform(queries)
		if err != nil {
			return nil, err
		}
	}

	return queries, nil
}
