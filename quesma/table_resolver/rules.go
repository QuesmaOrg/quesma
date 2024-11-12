// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package table_resolver

import (
	"fmt"
	"quesma/common_table"
	"quesma/elasticsearch"
	"quesma/end_user_errors"
	"quesma/quesma/config"
	"quesma/util"
	"reflect"
	"strings"
)

// TODO these rules may be incorrect and incomplete
// They will be fixed int the next iteration.

func (r *tableRegistryImpl) wildcardPatternSplitter(pattern string) (parsedPattern, *Decision) {
	patterns := strings.Split(pattern, ",")

	// Given a (potentially wildcard) pattern, find all non-wildcard index names that match the pattern
	var matchingSingleNames []string
	for _, pattern := range patterns {
		// If pattern is not an actual pattern (so it's a single index), just add it to the list
		// and skip further processing.
		// If pattern is an internal Kibana index, add it to the list without any processing - resolveInternalElasticName
		// will take care of it.
		if !elasticsearch.IsIndexPattern(pattern) || elasticsearch.IsInternalIndex(pattern) {
			matchingSingleNames = append(matchingSingleNames, pattern)
			continue
		}

		for indexName := range r.conf.IndexConfig {
			if util.IndexPatternMatches(pattern, indexName) {
				matchingSingleNames = append(matchingSingleNames, indexName)
			}
		}

		// but maybe we should also check against the actual indexes ??
		for indexName := range r.elasticIndexes {
			if util.IndexPatternMatches(pattern, indexName) {
				matchingSingleNames = append(matchingSingleNames, indexName)
			}
		}
		if r.conf.AutodiscoveryEnabled {
			for tableName := range r.clickhouseIndexes {
				if util.IndexPatternMatches(pattern, tableName) {
					matchingSingleNames = append(matchingSingleNames, tableName)
				}
			}
		}
	}

	matchingSingleNames = util.Distinct(matchingSingleNames)

	return parsedPattern{
		source:    pattern,
		isPattern: len(patterns) > 1 || strings.Contains(pattern, "*"),
		parts:     matchingSingleNames,
	}, nil
}

func singleIndexSplitter(pattern string) (parsedPattern, *Decision) {
	patterns := strings.Split(pattern, ",")
	if len(patterns) > 1 || strings.Contains(pattern, "*") {
		return parsedPattern{}, &Decision{
			Reason: "Pattern is not allowed.",
			Err:    fmt.Errorf("pattern is not allowed"),
		}
	}

	return parsedPattern{
		source:    pattern,
		isPattern: false,
		parts:     patterns,
	}, nil
}

func makeIsDisabledInConfig(cfg map[string]config.IndexConfiguration, pipeline string) func(part string) *Decision {

	return func(part string) *Decision {
		idx, ok := cfg[part]
		if ok {
			if len(getTargets(idx, pipeline)) == 0 {
				return &Decision{
					IsClosed: true,
					Reason:   "Index is disabled in config.",
				}
			}
		}

		return nil
	}
}

func resolveInternalElasticName(part string) *Decision {

	if elasticsearch.IsInternalIndex(part) {
		return &Decision{
			UseConnectors: []ConnectorDecision{&ConnectorDecisionElastic{ManagementCall: true}},
			Reason:        "It's kibana internals",
		}
	}

	return nil
}

func makeDefaultWildcard(quesmaConf config.QuesmaConfiguration, pipeline string) func(part string) *Decision {
	return func(part string) *Decision {
		var targets []string
		var useConnectors []ConnectorDecision

		switch pipeline {
		case IngestPipeline:
			targets = quesmaConf.DefaultIngestTarget
		case QueryPipeline:
			targets = quesmaConf.DefaultQueryTarget
		default:
			return &Decision{
				Reason: "Unsupported configuration",
				Err:    end_user_errors.ErrSearchCondition.New(fmt.Errorf("unsupported pipeline: %s", pipeline)),
			}
		}

		for _, target := range targets {
			switch target {
			case config.ClickhouseTarget:
				useConnectors = append(useConnectors, &ConnectorDecisionClickhouse{
					ClickhouseTableName: part,
					IsCommonTable:       quesmaConf.UseCommonTableForWildcard,
					ClickhouseTables:    []string{part},
				})
			case config.ElasticsearchTarget:
				useConnectors = append(useConnectors, &ConnectorDecisionElastic{})
			default:
				return &Decision{
					Reason: "Unsupported configuration",
					Err:    end_user_errors.ErrSearchCondition.New(fmt.Errorf("unsupported target: %s", target)),
				}
			}
		}

		return &Decision{
			UseConnectors: useConnectors,
			IsClosed:      len(useConnectors) == 0,
			Reason:        fmt.Sprintf("Using default wildcard ('%s') configuration for %s processor", config.DefaultWildcardIndexName, pipeline),
		}
	}
}

func (r *tableRegistryImpl) singleIndex(indexConfig map[string]config.IndexConfiguration, pipeline string) func(part string) *Decision {

	return func(part string) *Decision {
		if cfg, ok := indexConfig[part]; ok {
			if !cfg.UseCommonTable {

				targets := getTargets(cfg, pipeline)

				switch len(targets) {

				// case 0 is handled before (makeIsDisabledInConfig)

				case 1:

					decision := &Decision{
						Reason: "Enabled in the config. ",
					}

					var targetDecision ConnectorDecision

					switch targets[0] {

					case config.ElasticsearchTarget:
						targetDecision = &ConnectorDecisionElastic{}
					case config.ClickhouseTarget:
						targetDecision = &ConnectorDecisionClickhouse{
							ClickhouseTableName: part,
							ClickhouseTables:    []string{part},
						}
					default:
						return &Decision{
							Reason: "Unsupported configuration",
							Err:    end_user_errors.ErrSearchCondition.New(fmt.Errorf("unsupported target: %s", targets[0])),
						}
					}
					decision.UseConnectors = append(decision.UseConnectors, targetDecision)

					return decision
				case 2:

					switch pipeline {

					case IngestPipeline:
						return &Decision{
							Reason: "Enabled in the config. Dual write is enabled.",

							UseConnectors: []ConnectorDecision{&ConnectorDecisionClickhouse{
								ClickhouseTableName: part,
								ClickhouseTables:    []string{part}},
								&ConnectorDecisionElastic{}},
						}

					case QueryPipeline:

						if targets[0] == config.ClickhouseTarget && targets[1] == config.ElasticsearchTarget {

							return &Decision{
								Reason:          "Enabled in the config. A/B testing.",
								EnableABTesting: true,
								UseConnectors: []ConnectorDecision{&ConnectorDecisionClickhouse{
									ClickhouseTableName: part,
									ClickhouseTables:    []string{part}},
									&ConnectorDecisionElastic{}},
							}
						} else if targets[0] == config.ElasticsearchTarget && targets[1] == config.ClickhouseTarget {

							return &Decision{
								Reason:          "Enabled in the config. A/B testing.",
								EnableABTesting: true,
								UseConnectors: []ConnectorDecision{
									&ConnectorDecisionElastic{},
									&ConnectorDecisionClickhouse{
										ClickhouseTableName: part,
										ClickhouseTables:    []string{part}},
								},
							}

						}

					default:
						return &Decision{
							Reason: "Unsupported configuration",
							Err:    end_user_errors.ErrSearchCondition.New(fmt.Errorf("unsupported pipeline: %s", pipeline)),
						}
					}

					return &Decision{
						Reason: "Unsupported configuration",
						Err:    end_user_errors.ErrSearchCondition.New(fmt.Errorf("unsupported configuration for pipeline %s, targets: %v", pipeline, targets)),
					}

				default:
					return &Decision{
						Reason: "Unsupported configuration",
						Err:    end_user_errors.ErrSearchCondition.New(fmt.Errorf("too many backend connector")),
					}
				}
			}
		}

		return nil
	}
}

func (r *tableRegistryImpl) makeCommonTableResolver(cfg map[string]config.IndexConfiguration, pipeline string) func(part string) *Decision {

	return func(part string) *Decision {
		if part == common_table.TableName {
			return &Decision{
				Err:    fmt.Errorf("common table is not allowed to be queried directly"),
				Reason: "It's internal table. Not allowed to be queried directly.",
			}
		}

		var virtualTableExists bool

		if r.conf.AutodiscoveryEnabled {
			for indexName, index := range r.clickhouseIndexes {
				if index.isVirtual && indexName == part {
					virtualTableExists = true
					break
				}
			}
		}

		if idxConfig, ok := cfg[part]; (ok && idxConfig.UseCommonTable) || (virtualTableExists) {
			return &Decision{
				UseConnectors: []ConnectorDecision{&ConnectorDecisionClickhouse{
					ClickhouseTableName: common_table.TableName,
					ClickhouseTables:    []string{part},
					IsCommonTable:       true,
				}},
				Reason: "Common table will be used.",
			}
		}

		return nil
	}
}

func mergeUseConnectors(lhs []ConnectorDecision, rhs []ConnectorDecision, rhsIndexName string) ([]ConnectorDecision, *Decision) {
	for _, connDecisionRhs := range rhs {
		foundMatching := false
		for _, connDecisionLhs := range lhs {
			if _, ok := connDecisionRhs.(*ConnectorDecisionElastic); ok {
				if _, ok := connDecisionLhs.(*ConnectorDecisionElastic); ok {
					foundMatching = true
				}
			}
			if rhsClickhouse, ok := connDecisionRhs.(*ConnectorDecisionClickhouse); ok {
				if lhsClickhouse, ok := connDecisionLhs.(*ConnectorDecisionClickhouse); ok {
					if lhsClickhouse.ClickhouseTableName != rhsClickhouse.ClickhouseTableName {
						return nil, &Decision{
							Reason: "Incompatible decisions for two indexes - they use a different ClickHouse table",
							Err:    fmt.Errorf("incompatible decisions for two indexes (different ClickHouse table) - %s and %s", connDecisionRhs, connDecisionLhs),
						}
					}
					if lhsClickhouse.IsCommonTable {
						if !rhsClickhouse.IsCommonTable {
							return nil, &Decision{
								Reason: "Incompatible decisions for two indexes - one uses the common table, the other does not",
								Err:    fmt.Errorf("incompatible decisions for two indexes (common table usage) - %s and %s", connDecisionRhs, connDecisionLhs),
							}
						}
						lhsClickhouse.ClickhouseTables = append(lhsClickhouse.ClickhouseTables, rhsClickhouse.ClickhouseTables...)
						lhsClickhouse.ClickhouseTables = util.Distinct(lhsClickhouse.ClickhouseTables)
					} else {
						if !reflect.DeepEqual(lhsClickhouse, rhsClickhouse) {
							return nil, &Decision{
								Reason: "Incompatible decisions for two indexes - they use ClickHouse tables differently",
								Err:    fmt.Errorf("incompatible decisions for two indexes (different usage of ClickHouse) - %s and %s", connDecisionRhs, connDecisionLhs),
							}
						}
					}
					foundMatching = true
				}
			}
		}
		if !foundMatching {
			return nil, &Decision{
				Reason: "Incompatible decisions for two indexes - they use different connectors",
				Err:    fmt.Errorf("incompatible decisions for two indexes - they use different connectors: could not find connector %s used for index %s in decisions: %s", connDecisionRhs, rhsIndexName, lhs),
			}
		}
	}

	return lhs, nil
}

func basicDecisionMerger(decisions []*Decision) *Decision {
	if len(decisions) == 0 {
		return &Decision{
			IsEmpty: true,
			Reason:  "No indexes matched, no decisions made.",
		}
	}
	if len(decisions) == 1 {
		return decisions[0]
	}

	for _, decision := range decisions {
		if decision == nil {
			return &Decision{
				Reason: "Got a nil decision. This is a bug.",
				Err:    fmt.Errorf("could not resolve index"),
			}
		}

		if decision.Err != nil {
			return decision
		}

		if decision.IsEmpty {
			return &Decision{
				Reason: "Got an empty decision. This is a bug.",
				Err:    fmt.Errorf("could not resolve index, empty index: %s", decision.IndexPattern),
			}
		}

		if decision.EnableABTesting != decisions[0].EnableABTesting {
			return &Decision{
				Reason: "One of the indexes matching the pattern does A/B testing, while another index does not - inconsistency.",
				Err:    fmt.Errorf("inconsistent A/B testing configuration - index %s (A/B testing: %v) and index %s (A/B testing: %v)", decision.IndexPattern, decision.EnableABTesting, decisions[0].IndexPattern, decisions[0].EnableABTesting),
			}
		}
	}

	var nonClosedDecisions []*Decision
	for _, decision := range decisions {
		if !decision.IsClosed {
			nonClosedDecisions = append(nonClosedDecisions, decision)
		}
	}
	if len(nonClosedDecisions) == 0 {
		// All indexes are closed
		return &Decision{
			IsClosed: true,
			Reason:   "All indexes matching the pattern are closed.",
		}
	}
	// Discard all closed indexes
	decisions = nonClosedDecisions

	useConnectors := decisions[0].UseConnectors

	for i, decision := range decisions {
		if i == 0 {
			continue
		}
		if len(decision.UseConnectors) != len(decisions[0].UseConnectors) {
			return &Decision{
				Reason: "Inconsistent number of connectors",
				Err:    fmt.Errorf("inconsistent number of connectors - index %s (%d connectors) and index %s (%d connectors)", decision.IndexPattern, len(decision.UseConnectors), decisions[0].IndexPattern, len(decisions[0].UseConnectors)),
			}
		}

		newUseConnectors, mergeDecision := mergeUseConnectors(useConnectors, decision.UseConnectors, decision.IndexPattern)
		if mergeDecision != nil {
			return mergeDecision
		}
		useConnectors = newUseConnectors
	}

	return &Decision{
		UseConnectors:   useConnectors,
		EnableABTesting: decisions[0].EnableABTesting,
		Reason:          "Merged decisions",
	}
}
