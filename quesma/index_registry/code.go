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
	"strings"
	"sync"
	"time"
)

type Decision struct {
	IsClosed bool

	PassToElastic bool

	PassToClickhouse    bool
	ClickhouseTableName string
	Indexes             []string
	IsCommonTable       bool

	Err error

	// internal
	Message      string
	ResolverName string
}

func (d *Decision) String() string {

	var lines []string

	if d.IsClosed {
		lines = append(lines, "Returns an closed index message.")
	}

	if d.PassToElastic {
		lines = append(lines, "Pass to Elastic.")
	}

	if d.PassToClickhouse {

		lines = append(lines, "Will query clickhouse.")
		if len(d.ClickhouseTableName) > 0 {
			lines = append(lines, fmt.Sprintf("Table: '%s' .", d.ClickhouseTableName))
		}
		if d.IsCommonTable {

			lines = append(lines, "Common table.")

		}
		if len(d.Indexes) > 0 {
			lines = append(lines, fmt.Sprintf("Indexes: %v.", d.Indexes))
		}
	}

	if d.Err != nil {
		lines = append(lines, fmt.Sprintf("Returns error: '%v'.", d.Err))
	}

	lines = append(lines, fmt.Sprintf("%s (%s).", d.Message, d.ResolverName))

	return strings.Join(lines, " ")

}

// API for the customers (router, ingest processor and query processor)
type IndexResolver interface {
	Resolve(indexPattern string) *Decision
}

// ---

type indexPattern struct {
	pattern   string
	isPattern bool
	patterns  []string
}

type namedResolver struct {
	name     string
	resolver func(pattern indexPattern) *Decision
}

type composedIndexResolver struct {
	resolvers []namedResolver
}

func (ir *composedIndexResolver) Resolve(indexName string) *Decision {

	patterns := strings.Split(indexName, ",")

	input := indexPattern{
		pattern:   indexName,
		isPattern: len(patterns) > 1 || strings.Contains(indexName, "*"),
		patterns:  patterns,
	}

	for _, resolver := range ir.resolvers {
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

type IndexRegistry struct {
	m sync.Mutex

	tableDiscovery       clickhouse.TableDiscovery
	elasticIndexResolver elasticsearch.IndexResolver

	IngestIndexConfig map[string]config.IndexConfiguration
	QueryIndexConfig  map[string]config.IndexConfiguration

	elasticIndexes    map[string]elasticIndex
	clickhouseIndexes map[string]clickhouseIndex

	ingestResolver IndexResolver
	queryResolver  IndexResolver

	ingestResolved map[string]*Decision
	queryResolved  map[string]*Decision
}

func (ir *IndexRegistry) ResolveIngest(indexName string) *Decision {

	ir.m.Lock()
	defer ir.m.Unlock()

	if decision, exists := ir.ingestResolved[indexName]; exists {
		return decision
	}

	decision := ir.ingestResolver.Resolve(indexName)
	ir.ingestResolved[indexName] = decision
	return decision
}

func (ir *IndexRegistry) ResolveQuery(indexName string) *Decision {

	ir.m.Lock()
	defer ir.m.Unlock()

	if decision, exists := ir.queryResolved[indexName]; exists {
		return decision
	}

	decision := ir.queryResolver.Resolve(indexName)
	ir.queryResolved[indexName] = decision
	return decision
}

func (ir *IndexRegistry) updateIndexes() {

	ir.m.Lock()
	defer ir.m.Unlock()

	defer func() {
		ir.ingestResolved = make(map[string]*Decision)
		ir.queryResolved = make(map[string]*Decision)
	}()

	// TODO how to interact with the table discovery ?
	ir.tableDiscovery.ReloadTableDefinitions()
	tableMap := ir.tableDiscovery.TableDefinitions()
	clickhouseIndexes := make(map[string]clickhouseIndex)

	tableMap.Range(func(name string, tableDef *clickhouse.Table) bool {

		clickhouseIndexes[name] = clickhouseIndex{
			TableName: name,
		}
		return true
	})

	ir.clickhouseIndexes = clickhouseIndexes
	logger.Info().Msgf("Clickhouse indexes updated: %v", clickhouseIndexes)

	elasticIndexes := make(map[string]elasticIndex)
	sources, ok, err := ir.elasticIndexResolver.Resolve("*")
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
	ir.elasticIndexes = elasticIndexes
}

type PatternDecision struct {
	Pattern string
	Ingest  *Decision
	Query   *Decision
}

func (ir *IndexRegistry) RecentDecisions() []PatternDecision {

	ir.m.Lock()
	defer ir.m.Unlock()

	var patternsMap = make(map[string]bool)

	for pattern := range ir.ingestResolved {
		patternsMap[pattern] = true
	}

	for pattern := range ir.queryResolved {
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
			Ingest:  ir.ingestResolved[pattern],
			Query:   ir.queryResolved[pattern],
		}

		res = append(res, pd)
	}

	return res
}

func (ir *IndexRegistry) makeClickhouseSingleTableResolver() func(input indexPattern) *Decision {

	return func(input indexPattern) *Decision {

		if input.isPattern {
			return nil
		}

		if table, ok := ir.clickhouseIndexes[input.pattern]; ok && !table.IsVirtualTable {
			return &Decision{
				PassToClickhouse:    true,
				ClickhouseTableName: table.TableName,
				Message:             "Found the physical table.",
			}
		}

		return nil
	}
}

func (ir *IndexRegistry) makeCheckIfPatternMatchesAllConnectors() func(input indexPattern) *Decision {

	return func(input indexPattern) *Decision {
		if input.isPattern {

			matchedElastic := []string{}
			matchedClickhouse := []string{}

			for _, pattern := range input.patterns {

				for indexName := range ir.elasticIndexes {
					if util.IndexPatternMatches(pattern, indexName) {
						matchedElastic = append(matchedElastic, indexName)
					}
				}

				for tableName := range ir.clickhouseIndexes {
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

func (ir *IndexRegistry) makeClickhouseCommonTableResolver() func(input indexPattern) *Decision {

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

		if table, ok := ir.clickhouseIndexes[input.pattern]; ok && table.IsVirtualTable {
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

func NewIndexRegistry(indexConf map[string]config.IndexConfiguration, discovery clickhouse.TableDiscovery, elasticResolver elasticsearch.IndexResolver) *IndexRegistry {

	res := &IndexRegistry{

		tableDiscovery:       discovery,
		elasticIndexResolver: elasticResolver,

		IngestIndexConfig: indexConf,
		QueryIndexConfig:  indexConf,

		ingestResolved: make(map[string]*Decision),
		queryResolved:  make(map[string]*Decision),
	}

	res.ingestResolver = &composedIndexResolver{
		resolvers: []namedResolver{
			{"patternIsNotAllowed", patternIsNotAllowed},
			{"kibanaInternal", resolveInternalElasticName},
			{"isDisabledInConfig", makeIsDisabledInConfig(indexConf)},
			{"resolveSingleIndexPerTable", res.makeClickhouseSingleTableResolver()},
			{"resolveCommonTable", res.makeClickhouseCommonTableResolver()},
			{"fallbackToElastic", fallbackToElastic},
		},
	}

	res.queryResolver = &composedIndexResolver{
		resolvers: []namedResolver{
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
