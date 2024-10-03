// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package index_registry

import (
	"fmt"
	"quesma/clickhouse"
	"quesma/elasticsearch"
	"quesma/logger"
	"quesma/quesma/config"
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

	ReturnError bool
	Err         error

	// internal
	Message      string
	ResolverName string
}

func (d *Decision) String() string {

	var builder strings.Builder

	if d.IsClosed {
		builder.WriteString("Closed.")
	}

	if d.PassToElastic {
		builder.WriteString("Pass to Elastic.")
	}

	if d.PassToClickhouse {
		builder.WriteString("Pass to Clickhouse.")
		if len(d.ClickhouseTableName) > 0 {
			builder.WriteString(fmt.Sprintf("Table: '%s' .", d.ClickhouseTableName))
		}
		if d.IsCommonTable {
			builder.WriteString("Common table.")
		}
		if len(d.Indexes) > 0 {
			builder.WriteString(fmt.Sprintf("Indexes: %v.", d.Indexes))
		}
	}

	if d.ReturnError {
		builder.WriteString(fmt.Sprintf("Error: '%v'.", d.Err))
	}

	builder.WriteString(fmt.Sprintf("Resolved by '%s', message '%s'  ", d.ResolverName, d.Message))
	return builder.String()

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
		Message:     "Could not resolve index. This is a bug.",
		ReturnError: true,
		Err:         fmt.Errorf("could not resolve index"),
	}
}

func patternIsNotAllowed(input indexPattern) *Decision {
	if !input.isPattern {
		return nil
	}
	return &Decision{
		Message:     "Pattern is not allowed",
		ReturnError: true,
		Err:         fmt.Errorf("pattern is not allowed"),
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
						Message:  "Index is disabled in config",
					}
				}
			}
		}

		return nil
	}
}

func resolveSingleIndexPerTable(input indexPattern) *Decision {

	if input.isPattern {
		return nil
	}

	if strings.HasPrefix(input.pattern, "kibana") {
		return &Decision{
			PassToElastic:       false,
			ClickhouseTableName: input.pattern,
			Message:             "Clickhouse table",
		}
	}
	return nil
}

func resolveInternalElasticName(pattern indexPattern) *Decision {

	if elasticsearch.IsInternalIndex(pattern.pattern) {
		return &Decision{
			PassToElastic: true,
			Message:       "Internal Elastic index",
		}
	}

	return nil
}

func fallbackToElastic(indexName indexPattern) *Decision {
	return &Decision{
		PassToElastic: true,
		Message:       "Fallback to Elastic",
	}
}

type clickhouseIndex struct {
	TableName string
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

	tableMap := ir.tableDiscovery.TableDefinitions()
	clickhouseIndexes := make(map[string]clickhouseIndex)

	tableMap.Range(func(name string, tableDef *clickhouse.Table) bool {

		clickhouseIndexes[name] = clickhouseIndex{
			TableName: name,
		}
		return true
	})

	ir.clickhouseIndexes = clickhouseIndexes

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

	ir.elasticIndexes = elasticIndexes
}

func NewIndexRegistry(indexConf map[string]config.IndexConfiguration, discovery clickhouse.TableDiscovery, elasticResolver elasticsearch.IndexResolver) *IndexRegistry {

	res := &IndexRegistry{

		tableDiscovery:       discovery,
		elasticIndexResolver: elasticResolver,

		IngestIndexConfig: indexConf,
		QueryIndexConfig:  indexConf,

		ingestResolved: make(map[string]*Decision),
		queryResolved:  make(map[string]*Decision),

		ingestResolver: &composedIndexResolver{
			resolvers: []namedResolver{
				{"patternIsNotAllowed", patternIsNotAllowed},
				{"kibanaInternal", resolveInternalElasticName},
				{"isDisabledInConfig", makeIsDisabledInConfig(indexConf)},
				{"resolveSingleIndexPerTable", resolveSingleIndexPerTable},
				{"fallbackToElastic", fallbackToElastic},
			},
		},
		queryResolver: &composedIndexResolver{
			resolvers: []namedResolver{
				{"kibanaInternal", resolveInternalElasticName},
				{"isDisabledInConfig", makeIsDisabledInConfig(indexConf)},
				{"resolveSingleIndexPerTable", resolveSingleIndexPerTable},
				{"fallbackToElastic", fallbackToElastic},
			},
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
