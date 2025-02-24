// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package optimize

import (
	"context"
	"github.com/QuesmaOrg/quesma/quesma/model"
	"github.com/QuesmaOrg/quesma/quesma/quesma/config"
	"strings"
	"time"
)

// OptimizeTransformer - an interface for query transformers that have a name.
type OptimizeTransformer interface {
	Transform(queries []*model.Query, properties map[string]string) ([]*model.Query, error)

	Name() string             // this name is used to enable/disable the transformer in the configuration
	IsEnabledByDefault() bool // should return true for "not aggressive" transformers only
}

// OptimizePipeline - a transformer that optimizes queries
type OptimizePipeline struct {
	config        *config.QuesmaConfiguration
	optimizations []OptimizeTransformer
}

func NewOptimizePipeline(config *config.QuesmaConfiguration) model.QueryTransformer {

	return &OptimizePipeline{
		config: config,
		optimizations: []OptimizeTransformer{
			&truncateDate{truncateTo: 5 * time.Minute},
			&cacheQueries{},
			&materializedViewReplace{},
			&splitTimeRange{},
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

	// we assume here that  table_name is the index name
	tableName := queries[0].TableName
	res := strings.Replace(tableName, `"`, "", -1)
	if strings.Contains(res, ".") {
		parts := strings.Split(res, ".")
		if len(parts) == 2 {
			return parts[1]
		}
	}
	return res
}

func (s *OptimizePipeline) findConfig(transformer OptimizeTransformer, queries []*model.Query) (disabled bool, props map[string]string) {

	indexName := s.getIndexName(queries)

	// first we check index specific settings
	if indexCfg, ok := s.config.IndexConfig[indexName]; ok {
		if optimizerCfg, ok := indexCfg.Optimizers[transformer.Name()]; ok {
			return optimizerCfg.Disabled, optimizerCfg.Properties
		}
	}

	// default is not enabled
	return !transformer.IsEnabledByDefault(), make(map[string]string)
}

func (s *OptimizePipeline) Transform(ctx context.Context, queries []*model.Query) ([]*model.Query, error) {

	if len(queries) == 0 {
		return queries, nil
	}

	// add  hints if not present
	for _, query := range queries {
		if query.OptimizeHints == nil {
			query.OptimizeHints = model.NewQueryExecutionHints()
		}
	}

	// run optimizations on queries
	for _, optimization := range s.optimizations {

		disabled, properties := s.findConfig(optimization, queries)

		if disabled {
			continue
		}

		var err error
		queries, err = optimization.Transform(queries, properties)
		if err != nil {
			return nil, err
		}
	}

	return queries, nil
}
