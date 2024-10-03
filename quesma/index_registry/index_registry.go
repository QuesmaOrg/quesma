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

			// todo ??

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

func fallbackToElastic(indexName indexPattern) *Decision {
	return &Decision{
		PassToElastic: true,
		Message:       "It's fallback.",
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

func (r *indexRegistryImpl) makeClickhouseSingleTableResolver() func(input indexPattern) *Decision {

	return func(input indexPattern) *Decision {

		if input.isPattern {
			return nil
		}

		if table, ok := r.clickhouseIndexes[input.pattern]; ok && !table.IsVirtualTable {
			return &Decision{
				PassToClickhouse:    true,
				ClickhouseTableName: table.TableName,
				Message:             "Found the physical table.",
			}
		}

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
				return nil
			}
		}

		return nil
	}

}

func (r *indexRegistryImpl) makeClickhouseCommonTableResolver() func(input indexPattern) *Decision {

	return func(input indexPattern) *Decision {

		if input.isPattern {

			// TODO
			/*
				for _, pattern := range input.patterns {

				}
			*/
			// HERE
			return nil
		}

		if table, ok := r.clickhouseIndexes[input.pattern]; ok && table.IsVirtualTable {
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
			{"isDisabledInConfig", makeIsDisabledInConfig(indexConf)},
			{"resolveSingleIndexPerTable", res.makeClickhouseSingleTableResolver()},
			{"resolveCommonTable", res.makeClickhouseCommonTableResolver()},
			{"fallbackToElastic", fallbackToElastic},
		},
	}

	res.queryResolver = &composedIndexResolver{
		decisionLadder: []namedResolver{
			{"kibanaInternal", resolveInternalElasticName},
			{"bothConnectors", res.makeCheckIfPatternMatchesAllConnectors()},
			{"isDisabledInConfig", makeIsDisabledInConfig(indexConf)},
			{"resolveSingleIndexPerTable", res.makeClickhouseSingleTableResolver()},
			{"resolveCommonTable", res.makeClickhouseCommonTableResolver()},
			{"fallbackToElastic", fallbackToElastic},
		},
	}

	// TODO
	go func() {

		for {
			logger.Info().Msgf("Updating indexes")
			res.updateIndexes()

			time.Sleep(1 * time.Minute)
		}

	}()

	return res

}
