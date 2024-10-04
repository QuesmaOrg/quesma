// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package index_registry

import (
	"fmt"
	"quesma/clickhouse"
	"quesma/common_table"
	"quesma/elasticsearch"
	"quesma/end_user_errors"
	"quesma/logger"
	"quesma/quesma/config"
	"quesma/util"
	"sort"
	"sync"
	"time"
)

func patternIsNotAllowed(input indexPattern) *Decision {
	if !input.isPattern {
		return nil
	}
	return &Decision{
		Message: "Pattern is not allowed.",
		Err:     fmt.Errorf("pattern is not allowed"), // TODO better error
	}
}

func makeIsDisabledInConfig(cfg map[string]config.IndexConfiguration) func(input indexPattern) *Decision {

	return func(input indexPattern) *Decision {

		if input.isPattern {

			// pass to the next resolver

			return nil
		} else {
			idx, ok := cfg[input.pattern]
			if ok {
				if len(idx.Target) == 0 {
					return &Decision{
						IsClosed: true,
						Message:  "Index is disabled in config.",
					}
				}
			}
		}

		return nil
	}
}

func resolveInternalElasticName(pattern indexPattern) *Decision {

	if elasticsearch.IsInternalIndex(pattern.pattern) {
		return &Decision{
			PassToElastic: true,
			Message:       "It's kibana internals",
		}
	}

	return nil
}

func elasticAsDefault(indexName indexPattern) *Decision {

	// TODO check "*" config pattern

	return &Decision{
		PassToElastic: true,
		Message:       "Elastic is default.",
	}
}

func commonTableAsDefault(indexName indexPattern) *Decision {

	// TODO check "*" config pattern

	return &Decision{
		PassToClickhouse: true,
		IsCommonTable:    true,
		Message:          "Use common table.",
	}
}

type clickhouseIndex struct {
	TableName      string
	IsVirtualTable bool
}

type elasticIndex struct {
	IndexName string
}

type indexRegistryImpl struct {
	m sync.Mutex

	tableDiscovery       clickhouse.TableDiscovery
	elasticIndexResolver elasticsearch.IndexResolver

	ingestIndexConfig map[string]config.IndexConfiguration
	queryIndexConfig  map[string]config.IndexConfiguration

	elasticIndexes    map[string]elasticIndex
	clickhouseIndexes map[string]clickhouseIndex

	ingestResolver IndexResolver
	queryResolver  IndexResolver

	// recent decisions and the cache
	ingestResolved map[string]*Decision
	queryResolved  map[string]*Decision
}

func (r *indexRegistryImpl) ResolveIngest(indexName string) *Decision {

	r.m.Lock()
	defer r.m.Unlock()

	if decision, exists := r.ingestResolved[indexName]; exists {
		return decision
	}

	decision := r.ingestResolver.Resolve(indexName)
	r.ingestResolved[indexName] = decision
	return decision
}

func (r *indexRegistryImpl) ResolveQuery(indexName string) *Decision {

	r.m.Lock()
	defer r.m.Unlock()

	if decision, exists := r.queryResolved[indexName]; exists {
		return decision
	}

	decision := r.queryResolver.Resolve(indexName)
	r.queryResolved[indexName] = decision
	return decision
}

func (r *indexRegistryImpl) updateIndexes() {

	r.m.Lock()
	defer r.m.Unlock()

	defer func() {
		r.ingestResolved = make(map[string]*Decision)
		r.queryResolved = make(map[string]*Decision)
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
		r.ResolveIngest(pattern)
		r.ResolveQuery(pattern)
	}

	for _, index := range r.ingestIndexConfig {
		fill(index.Name)
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

	for pattern := range r.ingestResolved {
		patternsMap[pattern] = true
	}

	for pattern := range r.queryResolved {
		patternsMap[pattern] = true
	}

	var patterns []string
	for pattern := range patternsMap {
		patterns = append(patterns, pattern)
	}

	sort.Strings(patterns)

	var res []PatternDecision
	for _, pattern := range patterns {

		pd := PatternDecision{
			Pattern: pattern,
			Ingest:  r.ingestResolved[pattern],
			Query:   r.queryResolved[pattern],
		}

		res = append(res, pd)
	}

	return res
}

func (r *indexRegistryImpl) resolveAutodiscovery(input indexPattern) *Decision {

	if input.isPattern {
		return nil
	}

	// TODO what is autodiscovery ?

	// we should expose all tables ASIS ??

	var enabledAutodiscovery bool

	if !enabledAutodiscovery {
		return nil
	}

	if table, ok := r.clickhouseIndexes[input.pattern]; ok && !table.IsVirtualTable {
		return &Decision{
			PassToClickhouse:    true,
			ClickhouseTableName: table.TableName,
			Message:             "Found the physical table. Autodiscovery.",
		}
	}

	return nil
}

func (r *indexRegistryImpl) makeClickhouseSingleTableResolver(indexConfig map[string]config.IndexConfiguration) func(input indexPattern) *Decision {

	return func(input indexPattern) *Decision {

		if input.isPattern {
			return nil
		}

		if cfg, ok := indexConfig[input.pattern]; ok {
			if !cfg.UseCommonTable {

				switch len(cfg.Target) {

				case 0:
					return &Decision{
						Message:  "Disabled in the config.",
						IsClosed: true,
					}

				case 1:

					// TODO check if we query clickhouse or elastic

					return &Decision{
						PassToClickhouse:    true,
						ClickhouseTableName: input.pattern,
						Indexes:             []string{input.pattern},
						Message:             "Enabled in the config. Physical table will be used.",
					}

				case 2:

					// check targets and decide
					// TODO what about A/B testing ?

					return &Decision{
						PassToClickhouse:    true,
						ClickhouseTableName: input.pattern,
						Indexes:             []string{input.pattern},
						Message:             "Enabled in the config. Physical table will be used.",
					}

				default:
					return &Decision{
						Message: "Unsupported configuration",
						Err:     end_user_errors.ErrSearchCondition.New(fmt.Errorf("There are too many backend connectors ", input.pattern)),
					}
				}
			}
		}

		// TODO autodiscovery ?

		return nil
	}
}

func (r *indexRegistryImpl) makeCheckIfPatternMatchesAllConnectors() func(input indexPattern) *Decision {

	return func(input indexPattern) *Decision {
		if input.isPattern {

			matchedElastic := []string{}
			matchedClickhouse := []string{}

			for _, pattern := range input.patterns {

				for indexName := range r.elasticIndexes {
					if util.IndexPatternMatches(pattern, indexName) {
						matchedElastic = append(matchedElastic, indexName)
					}
				}

				for tableName := range r.clickhouseIndexes {
					if util.IndexPatternMatches(pattern, tableName) {
						matchedClickhouse = append(matchedClickhouse, tableName)
					}
				}
			}

			nElastic := len(matchedElastic)
			nClickhouse := len(matchedClickhouse)

			switch {

			case nElastic > 0 && nClickhouse > 0:
				return &Decision{
					Err:     end_user_errors.ErrSearchCondition.New(fmt.Errorf("index pattern [%s] resolved to both elasticsearch indices: [%s] and clickhouse tables: [%s]", input.patterns, matchedElastic, matchedClickhouse)),
					Message: "Both Elastic and Clickhouse matched.",
				}

			case nElastic > 0 && nClickhouse == 0:

				return &Decision{
					PassToElastic: true,
					Message:       "Only Elastic matched.",
				}

			case nElastic == 0 && nClickhouse > 0:
				// it will be resolved by sth else later
				return nil

			case nElastic == 0 && nClickhouse == 0:

				// TODO we should return emtpy result here
				// or pass to another resolver
				return &Decision{
					IsEmpty: true,
					Message: "No indexes matched. Checked both connectors.",
				}
			}
		}

		return nil
	}

}

func (r *indexRegistryImpl) makeClickhouseCommonTableResolver() func(input indexPattern) *Decision {

	return func(input indexPattern) *Decision {

		if input.isPattern {

			// TODO at this point we should'nt have elastic indexes?
			for _, pattern := range input.patterns {
				for indexName := range r.elasticIndexes {
					if util.IndexPatternMatches(pattern, indexName) {

						// TODO what about config ?
						// TODO ?
						return &Decision{
							Err:     end_user_errors.ErrSearchCondition.New(fmt.Errorf("index pattern [%s] resolved to elasticsearch indices", input.patterns)),
							Message: "We're not supporting common tables for Elastic.",
						}
					}
				}
			}

			matchedIndexes := []string{}

			for _, pattern := range input.patterns {
				for indexName, index := range r.clickhouseIndexes {

					// TODO what about config ?
					if util.IndexPatternMatches(pattern, indexName) && index.IsVirtualTable {
						matchedIndexes = append(matchedIndexes, indexName)
					}
				}
			}

			if len(matchedIndexes) == 0 {
				return &Decision{
					IsEmpty: true,
					Message: "No indexes found.",
				}
			}

			// HERE
			return &Decision{
				PassToClickhouse: true,
				IsCommonTable:    true,
				Indexes:          matchedIndexes,
				Message:          "Common table will be used. Querying multiple indexes.",
			}
		}

		if input.pattern == common_table.TableName {
			return &Decision{
				Err:     fmt.Errorf("common table is not allowed to be queried directly"),
				Message: "It's internal table. Not allowed to be queried directly.",
			}
		}

		if idxConfig, ok := r.ingestIndexConfig[input.pattern]; ok && idxConfig.UseCommonTable {
			return &Decision{
				PassToClickhouse:    true,
				ClickhouseTableName: common_table.TableName,
				Indexes:             []string{input.pattern},
				Message:             "Common table will be used.",
			}
		}

		return nil
	}
}

func NewIndexRegistry(indexConf map[string]config.IndexConfiguration, discovery clickhouse.TableDiscovery, elasticResolver elasticsearch.IndexResolver) IndexRegistry {

	res := &indexRegistryImpl{

		tableDiscovery:       discovery,
		elasticIndexResolver: elasticResolver,

		ingestIndexConfig: indexConf,
		queryIndexConfig:  indexConf,

		ingestResolved: make(map[string]*Decision),
		queryResolved:  make(map[string]*Decision),
	}

	res.ingestResolver = &composedIndexResolver{
		decisionLadder: []namedResolver{
			{"patternIsNotAllowed", patternIsNotAllowed},
			{"kibanaInternal", resolveInternalElasticName},
			{"disabled", makeIsDisabledInConfig(indexConf)},
			{"autodiscovery", res.resolveAutodiscovery},
			{"singleTable", res.makeClickhouseSingleTableResolver(indexConf)},
			{"commonTable", res.makeClickhouseCommonTableResolver()},
			{"elasticAsDefault", elasticAsDefault},
			{"commonTableAsDefault", commonTableAsDefault},
		},
	}

	res.queryResolver = &composedIndexResolver{
		decisionLadder: []namedResolver{
			// checking if we can handle the pattern
			{"kibanaInternal", resolveInternalElasticName},
			{"searchAcrossConnectors", res.makeCheckIfPatternMatchesAllConnectors()},
			{"disabled", makeIsDisabledInConfig(indexConf)},

			// resolving how we can handle the pattern
			{"singleTable", res.makeClickhouseSingleTableResolver(indexConf)},
			{"commonTable", res.makeClickhouseCommonTableResolver()},

			// default action
			{"elasticAsDefault", elasticAsDefault},
			{"commonTableAsDefault", commonTableAsDefault},
		},
	}

	go func() {

		for {
			logger.Info().Msgf("Updating indexes")
			res.updateIndexes()
			res.typicalDecisions()
			time.Sleep(1 * time.Minute)
		}

	}()

	return res

}
