// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package table_resolver

import (
	"context"
	"fmt"
	"github.com/QuesmaOrg/quesma/quesma/clickhouse"
	"github.com/QuesmaOrg/quesma/quesma/elasticsearch"
	"github.com/QuesmaOrg/quesma/quesma/logger"
	"github.com/QuesmaOrg/quesma/quesma/quesma/config"
	"github.com/QuesmaOrg/quesma/quesma/quesma/recovery"
	"github.com/QuesmaOrg/quesma/quesma/quesma/types"
	"quesma_v2/core"
	"sort"
	"sync"
)

type tableResolver interface {
	resolve(indexPattern string) *quesma_api.Decision
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
	resolver func(pattern string) (parsedPattern, *quesma_api.Decision)
}

type basicResolver struct {
	name     string
	resolver func(part string) *quesma_api.Decision
}

type decisionMerger struct {
	name   string
	merger func(decisions []*quesma_api.Decision) *quesma_api.Decision
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

func (ir *compoundResolver) resolve(indexName string) *quesma_api.Decision {
	input, decision := ir.patternSplitter.resolver(indexName)
	if decision != nil {
		decision.ResolverName = ir.patternSplitter.name
		return decision
	}

	var decisions []*quesma_api.Decision
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
	case quesma_api.IngestPipeline:
		return indexConf.IngestTarget
	case quesma_api.QueryPipeline:
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
	recentDecisions map[string]*quesma_api.Decision
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

func (r *tableRegistryImpl) Resolve(pipeline string, indexPattern string) *quesma_api.Decision {
	r.m.Lock()
	defer r.m.Unlock()

	res, exists := r.pipelineResolvers[pipeline]
	if !exists {
		return &quesma_api.Decision{
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
		res.recentDecisions = make(map[string]*quesma_api.Decision)
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

	notificationChannel := make(chan types.ReloadMessage, 100)
	r.tableDiscovery.AddListener(notificationChannel)

	go func() {
		defer recovery.LogPanic()
		logger.Info().Msg("Table resolve started.")

		for {
			select {
			case <-r.ctx.Done():
				return
			case <-notificationChannel:
				fmt.Println("XXX Notification received")
				r.updateState()
			}
		}
	}()
}

func (r *tableRegistryImpl) RecentDecisions() []quesma_api.PatternDecisions {
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

	var res []quesma_api.PatternDecisions
	for _, p := range patterns {

		pd := quesma_api.PatternDecisions{
			Pattern:   p,
			Decisions: make(map[string]*quesma_api.Decision),
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
		pipelineName: quesma_api.IngestPipeline,

		resolver: &compoundResolver{
			patternSplitter: patternSplitter{
				name:     "singleIndexSplitter",
				resolver: singleIndexSplitter,
			},
			decisionLadder: []basicResolver{
				{"kibanaInternal", resolveInternalElasticName},
				{"disabled", makeIsDisabledInConfig(indexConf, quesma_api.IngestPipeline)},

				{"singleIndex", res.singleIndex(indexConf, quesma_api.IngestPipeline)},
				{"commonTable", res.makeCommonTableResolver(indexConf, quesma_api.IngestPipeline)},

				{"defaultWildcard", makeDefaultWildcard(quesmaConf, quesma_api.IngestPipeline)},
			},
			decisionMerger: decisionMerger{
				name:   "basicDecisionMerger",
				merger: basicDecisionMerger,
			},
		},
		recentDecisions: make(map[string]*quesma_api.Decision),
	}

	res.pipelineResolvers[quesma_api.IngestPipeline] = ingestResolver

	queryResolver := &pipelineResolver{
		pipelineName: quesma_api.QueryPipeline,

		resolver: &compoundResolver{
			patternSplitter: patternSplitter{
				name:     "wildcardPatternSplitter",
				resolver: res.wildcardPatternSplitter,
			},
			decisionLadder: []basicResolver{
				// checking if we can handle the parsedPattern
				{"kibanaInternal", resolveInternalElasticName},
				{"disabled", makeIsDisabledInConfig(indexConf, quesma_api.QueryPipeline)},

				{"singleIndex", res.singleIndex(indexConf, quesma_api.QueryPipeline)},
				{"commonTable", res.makeCommonTableResolver(indexConf, quesma_api.QueryPipeline)},

				// default action
				{"defaultWildcard", makeDefaultWildcard(quesmaConf, quesma_api.QueryPipeline)},
			},
			decisionMerger: decisionMerger{
				name:   "basicDecisionMerger",
				merger: basicDecisionMerger,
			},
		},
		recentDecisions: make(map[string]*quesma_api.Decision),
	}

	res.pipelineResolvers[quesma_api.QueryPipeline] = queryResolver
	// update the state ASAP
	res.updateState()
	return res
}
