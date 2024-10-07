// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package index_registry

import (
	"quesma/clickhouse"
	"quesma/elasticsearch"
	"quesma/logger"
	"quesma/quesma/config"
	"quesma/quesma/recovery"
	"sort"
	"sync"
	"time"
)

// HACK: we should have separate config for each pipeline
// maybe we should pass a pipeline name here
func getTargets(indexConf config.IndexConfiguration) []string {
	if len(indexConf.IngestTarget) > 0 {
		return indexConf.IngestTarget
	}
	return indexConf.QueryTarget
}

type clickhouseIndex struct {
	TableName      string
	IsVirtualTable bool
}

type elasticIndex struct {
	IndexName string
}

type pipelineResolver struct {
	pipelineName string

	cfg             map[string]config.IndexConfiguration
	resolver        IndexResolver
	recentDecisions map[string]*Decision
}

type indexRegistryImpl struct {
	m sync.Mutex

	tableDiscovery       clickhouse.TableDiscovery
	elasticIndexResolver elasticsearch.IndexResolver

	elasticIndexes    map[string]elasticIndex
	clickhouseIndexes map[string]clickhouseIndex

	pipelineResolvers map[string]*pipelineResolver
}

func (r *indexRegistryImpl) Resolve(pipeline string, indexPattern string) *Decision {

	r.m.Lock()
	defer r.m.Unlock()

	res, exists := r.pipelineResolvers[pipeline]
	if !exists {
		// proper error handling
		return nil
	}

	if decision, ok := res.recentDecisions[indexPattern]; ok {
		return decision
	}

	decision := res.resolver.Resolve(indexPattern)
	res.recentDecisions[indexPattern] = decision
	return decision
}

func (r *indexRegistryImpl) updateIndexes() {

	r.m.Lock()
	defer r.m.Unlock()

	defer func() {
		for _, res := range r.pipelineResolvers {
			res.recentDecisions = make(map[string]*Decision)
		}

	}()

	// TODO how to interact with the table discovery ?
	r.tableDiscovery.ReloadTableDefinitions()
	tableMap := r.tableDiscovery.TableDefinitions()
	clickhouseIndexes := make(map[string]clickhouseIndex)

	tableMap.Range(func(name string, tableDef *clickhouse.Table) bool {

		clickhouseIndexes[name] = clickhouseIndex{
			TableName: name,
		}
		return true
	})

	r.clickhouseIndexes = clickhouseIndexes
	logger.Info().Msgf("Clickhouse indexes updated: %v", clickhouseIndexes)

	elasticIndexes := make(map[string]elasticIndex)
	sources, ok, err := r.elasticIndexResolver.Resolve("*")
	if err != nil {
		logger.Error().Msgf("Could not resolve indexes from Elastic: %v", err)
		return
	}
	if !ok {
		logger.Error().Msg("Could not resolve indexes from Elastic")
		return
	}

	for _, index := range sources.Indices {
		elasticIndexes[index.Name] = elasticIndex{
			IndexName: index.Name,
		}
	}

	logger.Info().Msgf("Elastic indexes updated: %v", elasticIndexes)
	r.elasticIndexes = elasticIndexes
}

// for demo and debugging purposes
func (r *indexRegistryImpl) typicalDecisions() {

	fill := func(pattern string) {
		for name := range r.pipelineResolvers {
			r.Resolve(name, pattern)
		}
	}

	for _, index := range r.pipelineResolvers {

		for pattern := range index.cfg {
			fill(pattern)
		}
	}

	for _, index := range r.elasticIndexes {
		fill(index.IndexName)
	}

	for _, index := range r.clickhouseIndexes {
		fill(index.TableName)
	}
	fill("*")
	fill("logs-*")
}

func (r *indexRegistryImpl) RecentDecisions() []PatternDecision {

	r.m.Lock()
	defer r.m.Unlock()

	var patternsMap = make(map[string]bool)

	for _, res := range r.pipelineResolvers {
		for pattern := range res.recentDecisions {
			patternsMap[pattern] = true
		}
	}

	var patterns []string
	for pattern := range patternsMap {
		patterns = append(patterns, pattern)
	}

	sort.Strings(patterns)

	var res []PatternDecision
	for _, pattern := range patterns {

		pd := PatternDecision{
			Pattern:   pattern,
			Decisions: make(map[string]*Decision),
		}
		for _, resolver := range r.pipelineResolvers {
			if decision, ok := resolver.recentDecisions[pattern]; ok {
				pd.Decisions[resolver.pipelineName] = decision
			}
		}

		res = append(res, pd)
	}

	return res
}

func (r *indexRegistryImpl) Pipelines() []string {

	r.m.Lock()
	defer r.m.Unlock()

	var res []string

	for name := range r.pipelineResolvers {
		res = append(res, name)
	}
	sort.Strings(res)

	return res
}

func NewIndexRegistry(ingestConf map[string]config.IndexConfiguration, queryConf map[string]config.IndexConfiguration, discovery clickhouse.TableDiscovery, elasticResolver elasticsearch.IndexResolver) IndexRegistry {

	res := &indexRegistryImpl{

		tableDiscovery:       discovery,
		elasticIndexResolver: elasticResolver,
		pipelineResolvers:    make(map[string]*pipelineResolver),
	}

	ingestResolver := &pipelineResolver{
		pipelineName: IngestPipeline,
		cfg:          ingestConf,
		resolver: &composedIndexResolver{
			decisionLadder: []namedResolver{
				{"patternIsNotAllowed", patternIsNotAllowed},
				{"kibanaInternal", resolveInternalElasticName},
				{"disabled", makeIsDisabledInConfig(ingestConf)},
				//{"dockerIndexes", resolveDockerIndexes},
				{"autodiscovery", res.makeResolveAutodiscovery(ingestConf)},
				{"singleIndex", res.singleIndex(ingestConf)},
				{"commonTable", res.makeClickhouseCommonTableResolver(ingestConf)},
				{"elasticAsDefault", makeElasticIsDefault(ingestConf)},
				{"commonTableAsDefault", makeCommonTableAsDefault(ingestConf)},
			},
		},
		recentDecisions: make(map[string]*Decision),
	}

	res.pipelineResolvers[IngestPipeline] = ingestResolver

	queryResolver := &pipelineResolver{
		pipelineName: QueryPipeline,
		cfg:          ingestConf,
		resolver: &composedIndexResolver{
			decisionLadder: []namedResolver{
				// checking if we can handle the pattern
				{"kibanaInternal", resolveInternalElasticName},
				{"searchAcrossConnectors", res.makeCheckIfPatternMatchesAllConnectors()},
				{"disabled", makeIsDisabledInConfig(queryConf)},

				// resolving how we can handle the pattern
				//{"dockerIndexes", resolveDockerIndexes},
				{"autodiscovery", res.makeResolveAutodiscovery(queryConf)},
				{"singleIndex", res.singleIndex(queryConf)},
				{"commonTable", res.makeClickhouseCommonTableResolver(queryConf)},

				// default action
				{"elasticAsDefault", makeElasticIsDefault(queryConf)},
				{"commonTableAsDefault", makeCommonTableAsDefault(queryConf)},
			},
		},
		recentDecisions: make(map[string]*Decision),
	}

	res.pipelineResolvers[QueryPipeline] = queryResolver

	go func() {
		defer recovery.LogPanic()

		for {
			logger.Info().Msgf("Updating indexes")
			res.updateIndexes()
			res.typicalDecisions()
			time.Sleep(1 * time.Minute)
		}

	}()

	return res

}
