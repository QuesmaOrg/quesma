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
	"sort"
	"strings"
	"sync"
	"time"
)

type tableResolver interface {
	resolve(indexPattern string) *Decision
}

// parsedPattern stores the parsed index pattern
type parsedPattern struct {
	// source
	source string

	// parsed data
	isPattern bool
	parts     []string
}

type basicResolver struct {
	name     string
	resolver func(pattern parsedPattern) *Decision
}

type compoundResolver struct {
	decisionLadder []basicResolver
}

func (ir *compoundResolver) resolve(indexName string) *Decision {
	patterns := strings.Split(indexName, ",")

	input := parsedPattern{
		source:    indexName,
		isPattern: len(patterns) > 1 || strings.Contains(indexName, "*"),
		parts:     patterns,
	}

	for _, resolver := range ir.decisionLadder {
		decision := resolver.resolver(input)

		if decision != nil {
			decision.ResolverName = resolver.name
			return decision
		}
	}
	return &Decision{
		Reason: "Could not resolve pattern. This is a bug.",
		Err:    fmt.Errorf("could not resolve index"), // TODO better error
	}
}

// HACK: we should have separate config for each pipeline
// now we have a single config for both, but with different fields
func getTargets(indexConf config.IndexConfiguration, pipeline string) []string {
	switch pipeline {
	case IngestPipeline:
		return indexConf.IngestTarget
	case QueryPipeline:
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
	recentDecisions map[string]*Decision
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

func (r *tableRegistryImpl) Resolve(pipeline string, indexPattern string) *Decision {
	r.m.Lock()
	defer r.m.Unlock()

	res, exists := r.pipelineResolvers[pipeline]
	if !exists {
		return &Decision{
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
	r.m.Lock()
	defer r.m.Unlock()

	defer func() {
		for _, res := range r.pipelineResolvers {
			res.recentDecisions = make(map[string]*Decision)
		}
	}()

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

	r.clickhouseIndexes = clickhouseIndexes
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
	r.elasticIndexes = elasticIndexes
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

func (r *tableRegistryImpl) RecentDecisions() []PatternDecisions {
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

	var res []PatternDecisions
	for _, p := range patterns {

		pd := PatternDecisions{
			Pattern:   p,
			Decisions: make(map[string]*Decision),
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
		pipelineName: IngestPipeline,

		resolver: &compoundResolver{
			decisionLadder: []basicResolver{
				{"patternIsNotAllowed", patternIsNotAllowed},
				{"kibanaInternal", resolveInternalElasticName},
				{"disabled", makeIsDisabledInConfig(indexConf, IngestPipeline)},

				{"singleIndex", res.singleIndex(indexConf, IngestPipeline)},
				{"commonTable", res.makeCommonTableResolver(indexConf, IngestPipeline)},

				{"defaultWildcard", makeDefaultWildcard(quesmaConf, IngestPipeline)},
			},
		},
		recentDecisions: make(map[string]*Decision),
	}

	res.pipelineResolvers[IngestPipeline] = ingestResolver

	queryResolver := &pipelineResolver{
		pipelineName: QueryPipeline,

		resolver: &compoundResolver{
			decisionLadder: []basicResolver{
				// checking if we can handle the parsedPattern
				{"kibanaInternal", resolveInternalElasticName},
				{"searchAcrossConnectors", res.makeCheckIfPatternMatchesAllConnectors(QueryPipeline)},
				{"disabled", makeIsDisabledInConfig(indexConf, QueryPipeline)},

				{"singleIndex", res.singleIndex(indexConf, QueryPipeline)},
				{"commonTable", res.makeCommonTableResolver(indexConf, QueryPipeline)},

				// default action
				{"defaultWildcard", makeDefaultWildcard(quesmaConf, QueryPipeline)},
			},
		},
		recentDecisions: make(map[string]*Decision),
	}

	res.pipelineResolvers[QueryPipeline] = queryResolver
	// update the state ASAP
	res.updateState()
	return res
}
