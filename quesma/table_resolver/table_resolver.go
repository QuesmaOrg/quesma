// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package table_resolver

import (
	"context"
	"fmt"
	"quesma/clickhouse"
	"quesma/elasticsearch"
	"quesma/logger"
	"quesma/quesma/config"
	"quesma/quesma/recovery"
	"quesma_v2/core/mux"
	"sort"
	"sync"
	"time"
)

type tableResolver interface {
	resolve(indexPattern string) *mux.Decision
}

// parsedPattern stores the parsed index pattern
type parsedPattern struct {
	// source
	source string

	// parsed data
	isPattern bool
	parts     []string
}

type patternSplitter struct {
	name     string
	resolver func(pattern string) (parsedPattern, *mux.Decision)
}

type basicResolver struct {
	name     string
	resolver func(part string) *mux.Decision
}

type decisionMerger struct {
	name   string
	merger func(decisions []*mux.Decision) *mux.Decision
}

// Compound resolver works in the following way:
// 1. patternSplitter splits a pattern, for example: logs* into concrete single indexes (e.g. logs1, logs2)
// 2. decisionLadder rules are evaluated on each index separately, resulting in a decision for each index
// 3. decisionMerger merges those decisions, making sure that the decisions are compatible. It yields a single decision.
type compoundResolver struct {
	patternSplitter patternSplitter
	decisionLadder  []basicResolver
	decisionMerger  decisionMerger
}

func (ir *compoundResolver) resolve(indexName string) *mux.Decision {
	input, decision := ir.patternSplitter.resolver(indexName)
	if decision != nil {
		decision.ResolverName = ir.patternSplitter.name
		return decision
	}

	var decisions []*mux.Decision
	for _, part := range input.parts {
		for _, resolver := range ir.decisionLadder {
			decision := resolver.resolver(part)

			if decision != nil {
				decision.ResolverName = resolver.name
				decisions = append(decisions, decision)
				break
			}
		}
	}

	return ir.decisionMerger.merger(decisions)
}

// HACK: we should have separate config for each pipeline
// now we have a single config for both, but with different fields
func getTargets(indexConf config.IndexConfiguration, pipeline string) []string {
	switch pipeline {
	case mux.IngestPipeline:
		return indexConf.IngestTarget
	case mux.QueryPipeline:
		return indexConf.QueryTarget
	default:
		return []string{}
	}
}

// table represents a table or an index discovered in the connector (clickhouse or elastic or ...)
type table struct {
	name      string
	isVirtual bool
}

type pipelineResolver struct {
	pipelineName string

	resolver        tableResolver
	recentDecisions map[string]*mux.Decision
}

type tableRegistryImpl struct {
	m      sync.Mutex
	ctx    context.Context
	cancel context.CancelFunc

	tableDiscovery clickhouse.TableDiscovery
	indexManager   elasticsearch.IndexManagement

	elasticIndexes    map[string]table
	clickhouseIndexes map[string]table

	pipelineResolvers map[string]*pipelineResolver
	conf              config.QuesmaConfiguration
}

func (r *tableRegistryImpl) Resolve(pipeline string, indexPattern string) *mux.Decision {
	r.m.Lock()
	defer r.m.Unlock()

	res, exists := r.pipelineResolvers[pipeline]
	if !exists {
		return &mux.Decision{
			IndexPattern: indexPattern,
			Err:          fmt.Errorf("pipeline '%s' not found", pipeline),
			Reason:       "Pipeline not found. This is a bug.",
			ResolverName: "tableRegistryImpl",
		}
	}

	if decision, ok := res.recentDecisions[indexPattern]; ok {
		return decision
	}

	decision := res.resolver.resolve(indexPattern)
	decision.IndexPattern = indexPattern
	res.recentDecisions[indexPattern] = decision

	logger.Debug().Msgf("Decision for pipeline '%s', pattern '%s':  %s", pipeline, indexPattern, decision.String())

	return decision
}

func (r *tableRegistryImpl) updateIndexes() {

	logger.Info().Msgf("Index registry updating state.")

	// TODO how to interact with the table discovery ?
	// right now we enforce the reload of the table definitions
	// schema registry is doing the same
	// we should inject list of tables into the resolver
	r.tableDiscovery.ReloadTableDefinitions()

	tableMap := r.tableDiscovery.TableDefinitions()
	clickhouseIndexes := make(map[string]table)

	tableMap.Range(func(name string, tableDef *clickhouse.Table) bool {
		clickhouseIndexes[name] = table{
			name:      name,
			isVirtual: tableDef.VirtualTable,
		}
		return true
	})

	logger.Info().Msgf("Clickhouse tables updated: %v", clickhouseIndexes)

	elasticIndexes := make(map[string]table)
	r.indexManager.ReloadIndices()
	sources := r.indexManager.GetSources()

	for _, index := range sources.Indices {
		elasticIndexes[index.Name] = table{
			name: index.Name,
		}
	}
	for _, index := range sources.DataStreams {
		elasticIndexes[index.Name] = table{
			name: index.Name,
		}
	}

	logger.Info().Msgf("Elastic tables updated: %v", elasticIndexes)

	// Let's update the state

	r.m.Lock()
	defer r.m.Unlock()

	// this is a critical section
	r.elasticIndexes = elasticIndexes
	r.clickhouseIndexes = clickhouseIndexes
	for _, res := range r.pipelineResolvers {
		res.recentDecisions = make(map[string]*mux.Decision)
	}
}

func (r *tableRegistryImpl) updateState() {
	r.updateIndexes()
}

func (r *tableRegistryImpl) Stop() {
	r.cancel()
	logger.Info().Msg("Table resolver stopped.")
}

func (r *tableRegistryImpl) Start() {
	go func() {
		defer recovery.LogPanic()
		logger.Info().Msg("Table resolve started.")

		for {
			select {
			case <-r.ctx.Done():
				return
			case <-time.After(1 * time.Minute):
				r.updateState()
			}
		}
	}()
}

func (r *tableRegistryImpl) RecentDecisions() []mux.PatternDecisions {
	r.m.Lock()
	defer r.m.Unlock()

	var patternsMap = make(map[string]bool)

	for _, res := range r.pipelineResolvers {
		for p := range res.recentDecisions {
			patternsMap[p] = true
		}
	}

	var patterns []string
	for p := range patternsMap {
		patterns = append(patterns, p)
	}

	sort.Strings(patterns)

	var res []mux.PatternDecisions
	for _, p := range patterns {

		pd := mux.PatternDecisions{
			Pattern:   p,
			Decisions: make(map[string]*mux.Decision),
		}
		for _, resolver := range r.pipelineResolvers {
			if decision, ok := resolver.recentDecisions[p]; ok {
				pd.Decisions[resolver.pipelineName] = decision
			}
		}
		res = append(res, pd)
	}

	return res
}

func (r *tableRegistryImpl) Pipelines() []string {

	r.m.Lock()
	defer r.m.Unlock()

	var res []string

	for name := range r.pipelineResolvers {
		res = append(res, name)
	}
	sort.Strings(res)

	return res
}

func NewTableResolver(quesmaConf config.QuesmaConfiguration, discovery clickhouse.TableDiscovery, elasticResolver elasticsearch.IndexManagement) TableResolver {
	ctx, cancel := context.WithCancel(context.Background())

	indexConf := quesmaConf.IndexConfig

	res := &tableRegistryImpl{
		ctx:    ctx,
		cancel: cancel,

		conf: quesmaConf,

		tableDiscovery:    discovery,
		indexManager:      elasticResolver,
		pipelineResolvers: make(map[string]*pipelineResolver),
	}

	// TODO Here we should read the config and create resolver for each pipeline defined.
	// TODO We should use the pipeline name as a key in the map.

	ingestResolver := &pipelineResolver{
		pipelineName: mux.IngestPipeline,

		resolver: &compoundResolver{
			patternSplitter: patternSplitter{
				name:     "singleIndexSplitter",
				resolver: singleIndexSplitter,
			},
			decisionLadder: []basicResolver{
				{"kibanaInternal", resolveInternalElasticName},
				{"disabled", makeIsDisabledInConfig(indexConf, mux.IngestPipeline)},

				{"singleIndex", res.singleIndex(indexConf, mux.IngestPipeline)},
				{"commonTable", res.makeCommonTableResolver(indexConf, mux.IngestPipeline)},

				{"defaultWildcard", makeDefaultWildcard(quesmaConf, mux.IngestPipeline)},
			},
			decisionMerger: decisionMerger{
				name:   "basicDecisionMerger",
				merger: basicDecisionMerger,
			},
		},
		recentDecisions: make(map[string]*mux.Decision),
	}

	res.pipelineResolvers[mux.IngestPipeline] = ingestResolver

	queryResolver := &pipelineResolver{
		pipelineName: mux.QueryPipeline,

		resolver: &compoundResolver{
			patternSplitter: patternSplitter{
				name:     "wildcardPatternSplitter",
				resolver: res.wildcardPatternSplitter,
			},
			decisionLadder: []basicResolver{
				// checking if we can handle the parsedPattern
				{"kibanaInternal", resolveInternalElasticName},
				{"disabled", makeIsDisabledInConfig(indexConf, mux.QueryPipeline)},

				{"singleIndex", res.singleIndex(indexConf, mux.QueryPipeline)},
				{"commonTable", res.makeCommonTableResolver(indexConf, mux.QueryPipeline)},

				// default action
				{"defaultWildcard", makeDefaultWildcard(quesmaConf, mux.QueryPipeline)},
			},
			decisionMerger: decisionMerger{
				name:   "basicDecisionMerger",
				merger: basicDecisionMerger,
			},
		},
		recentDecisions: make(map[string]*mux.Decision),
	}

	res.pipelineResolvers[mux.QueryPipeline] = queryResolver
	// update the state ASAP
	res.updateState()
	return res
}
