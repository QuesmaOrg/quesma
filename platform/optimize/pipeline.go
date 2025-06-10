// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package optimize

import (
	"github.com/QuesmaOrg/quesma/platform/config"
	"github.com/QuesmaOrg/quesma/platform/model"
	"log"
	"strings"
	"time"
)

// OptimizeTransformer - an interface for query transformers that have a name.
type OptimizeTransformer interface {
	Transform(plan *model.ExecutionPlan, properties map[string]string) (*model.ExecutionPlan, error)

	Name() string             // this name is used to enable/disable the transformer in the configuration
	IsEnabledByDefault() bool // should return true for "not aggressive" transformers only
}

// OptimizePipeline - a transformer that optimizes queries
type OptimizePipeline struct {
	config        *config.QuesmaConfiguration
	optimizations []OptimizeTransformer
}

func checkIfOptimizerIsEnabled(config *config.QuesmaConfiguration, name string) bool {

	if c, ok := config.DefaultQueryOptimizers[name]; ok {
		return !c.Disabled
	}
	return true // default is enabled

}

func NewOptimizePipeline(config *config.QuesmaConfiguration) model.QueryTransformer {

	var optimizations []OptimizeTransformer

	if checkIfOptimizerIsEnabled(config, "truncateDate") {
		optimizations = append(optimizations, &truncateDate{truncateTo: 5 * time.Minute})
	}

	if checkIfOptimizerIsEnabled(config, "cacheQueries") {
		optimizations = append(optimizations, &cacheQueries{})
	}

	if checkIfOptimizerIsEnabled(config, "materializedViewReplace") {
		optimizations = append(optimizations, &materializedViewReplace{})
	}

	if checkIfOptimizerIsEnabled(config, "splitTimeRangeExt") {
		optimizations = append(optimizations, &splitTimeRangeExt{})
	}

	log.Println("OptimizePipeline: enabled optimizations:", optimizations)

	return &OptimizePipeline{
		config:        config,
		optimizations: optimizations,
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

func (s *OptimizePipeline) Transform(plan *model.ExecutionPlan) (*model.ExecutionPlan, error) {

	if len(plan.Queries) == 0 {
		return plan, nil
	}

	// add  hints if not present
	for _, query := range plan.Queries {
		if query.OptimizeHints == nil {
			query.OptimizeHints = model.NewQueryExecutionHints()
		}
	}

	// run optimizations on queries
	for _, optimization := range s.optimizations {

		disabled, properties := s.findConfig(optimization, plan.Queries)

		if disabled {
			continue
		}

		var err error
		plan, err = optimization.Transform(plan, properties)
		if err != nil {
			return nil, err
		}
	}

	return plan, nil
}
