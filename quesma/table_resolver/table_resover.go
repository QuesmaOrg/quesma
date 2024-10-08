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

type pattern struct {
	// source
	pattern string

	// parsed data
	isPattern bool
	patterns  []string
}

type namedResolver struct {
	name     string
	resolver func(pattern pattern) *Decision
}

type composedResolver struct {
	decisionLadder []namedResolver
}

func (ir *composedResolver) resolve(indexName string) *Decision {

	patterns := strings.Split(indexName, ",")

	input := pattern{
		pattern:   indexName,
		isPattern: len(patterns) > 1 || strings.Contains(indexName, "*"),
		patterns:  patterns,
	}

	for _, resolver := range ir.decisionLadder {
		decision := resolver.resolver(input)

		if decision != nil {
			decision.ResolverName = resolver.name
			return decision
		}
	}
	return &Decision{
		Message: "Could not resolve pattern. This is a bug.",
		Err:     fmt.Errorf("could not resolve index"), // TODO better error
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

type table struct {
	name      string
	isVirtual bool
}

type pipelineResolver struct {
	pipelineName string

	cfg             map[string]config.IndexConfiguration
	resolver        tableResolver
	recentDecisions map[string]*Decision
}

type tableRegistryImpl struct {
	m      sync.Mutex
	ctx    context.Context
	cancel context.CancelFunc

	tableDiscovery       clickhouse.TableDiscovery
	elasticIndexResolver elasticsearch.IndexResolver

	elasticIndexes    map[string]table
	clickhouseIndexes map[string]table

	pipelineResolvers map[string]*pipelineResolver
}

func (r *tableRegistryImpl) Resolve(pipeline string, indexPattern string) *Decision {

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

	decision := res.resolver.resolve(indexPattern)
	res.recentDecisions[indexPattern] = decision
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
	r.tableDiscovery.ReloadTableDefinitions()

	tableMap := r.tableDiscovery.TableDefinitions()
	clickhouseIndexes := make(map[string]table)

	tableMap.Range(func(name string, tableDef *clickhouse.Table) bool {

		clickhouseIndexes[name] = table{
			name: name,
		}
		return true
	})

	r.clickhouseIndexes = clickhouseIndexes
	logger.Info().Msgf("Clickhouse indexes updated: %v", clickhouseIndexes)

	elasticIndexes := make(map[string]table)
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
		elasticIndexes[index.Name] = table{
			name: index.Name,
		}
	}

	logger.Info().Msgf("Elastic indexes updated: %v", elasticIndexes)
	r.elasticIndexes = elasticIndexes
}

func (r *tableRegistryImpl) updateState() {
	r.updateIndexes()
}

func (r *tableRegistryImpl) Stop() {
	r.cancel()
	logger.Info().Msg("Index registry stopped.")
}

func (r *tableRegistryImpl) Start() {
	go func() {
		defer recovery.LogPanic()
		logger.Info().Msg("Index registry started.")

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

func (r *tableRegistryImpl) RecentDecisions() []PatternDecision {

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

func NewTableResolver(quesmaConf config.QuesmaConfiguration, discovery clickhouse.TableDiscovery, elasticResolver elasticsearch.IndexResolver) TableResolver {

	ctx, cancel := context.WithCancel(context.Background())

	ingestConf := quesmaConf.IndexConfig
	queryConf := quesmaConf.IndexConfig

	res := &tableRegistryImpl{
		ctx:    ctx,
		cancel: cancel,

		tableDiscovery:       discovery,
		elasticIndexResolver: elasticResolver,
		pipelineResolvers:    make(map[string]*pipelineResolver),
	}

	ingestResolver := &pipelineResolver{
		pipelineName: IngestPipeline,
		cfg:          ingestConf,
		resolver: &composedResolver{
			decisionLadder: []namedResolver{
				{"patternIsNotAllowed", patternIsNotAllowed},
				{"kibanaInternal", resolveInternalElasticName},
				{"disabled", makeIsDisabledInConfig(ingestConf)},
				//{"dockerIndexes", resolveDockerIndexes},
				{"autodiscovery", res.makeResolveAutodiscovery(ingestConf)},
				{"singleIndex", res.singleIndex(ingestConf, IngestPipeline)},
				{"commonTable", res.makeClickhouseCommonTableResolver(ingestConf)},
				{"elasticAsDefault", makeElasticIsDefault(ingestConf)},
				{"commonTableAsDefault", makeCommonTableAsDefault(ingestConf, IngestPipeline)},
			},
		},
		recentDecisions: make(map[string]*Decision),
	}

	res.pipelineResolvers[IngestPipeline] = ingestResolver

	queryResolver := &pipelineResolver{
		pipelineName: QueryPipeline,
		cfg:          ingestConf,
		resolver: &composedResolver{
			decisionLadder: []namedResolver{
				// checking if we can handle the pattern
				{"kibanaInternal", resolveInternalElasticName},
				{"searchAcrossConnectors", res.makeCheckIfPatternMatchesAllConnectors()},
				{"disabled", makeIsDisabledInConfig(queryConf)},

				// resolving how we can handle the pattern
				//{"dockerIndexes", resolveDockerIndexes},
				{"autodiscovery", res.makeResolveAutodiscovery(queryConf)},
				{"singleIndex", res.singleIndex(queryConf, IngestPipeline)},
				{"commonTable", res.makeClickhouseCommonTableResolver(queryConf)},

				// default action
				{"elasticAsDefault", makeElasticIsDefault(queryConf)},
				{"commonTableAsDefault", makeCommonTableAsDefault(queryConf, IngestPipeline)},
			},
		},
		recentDecisions: make(map[string]*Decision),
	}

	res.pipelineResolvers[QueryPipeline] = queryResolver

	res.updateState()

	return res
}
