// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package table_resolver

import (
	"fmt"
	"github.com/QuesmaOrg/quesma/platform/common_table"
	"github.com/QuesmaOrg/quesma/platform/config"
	"github.com/QuesmaOrg/quesma/platform/elasticsearch"
	"github.com/QuesmaOrg/quesma/platform/end_user_errors"
	"github.com/QuesmaOrg/quesma/platform/util"
	"github.com/QuesmaOrg/quesma/platform/v2/core"
	"reflect"
	"strings"
)

// TODO these rules may be incorrect and incomplete
// They will be fixed int the next iteration.

func (r *tableRegistryImpl) wildcardPatternSplitter(pattern string) (parsedPattern, *quesma_api.Decision) {
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
			if matches, _ := util.IndexPatternMatches(pattern, indexName); matches {
				matchingSingleNames = append(matchingSingleNames, indexName)
			}
		}

		// but maybe we should also check against the actual indexes ??
		for indexName := range r.elasticIndexes {
			if matches, _ := util.IndexPatternMatches(pattern, indexName); matches {
				matchingSingleNames = append(matchingSingleNames, indexName)
			}
		}
		if r.conf.AutodiscoveryEnabled {
			for tableName := range r.clickhouseIndexes {
				if matches, _ := util.IndexPatternMatches(pattern, tableName); matches {
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

func singleIndexSplitter(pattern string) (parsedPattern, *quesma_api.Decision) {
	patterns := strings.Split(pattern, ",")
	if len(patterns) > 1 || strings.Contains(pattern, "*") {
		return parsedPattern{}, &quesma_api.Decision{
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

func makeIsDisabledInConfig(cfg map[string]config.IndexConfiguration, pipeline string) func(part string) *quesma_api.Decision {

	return func(part string) *quesma_api.Decision {
		idx, ok := cfg[part]
		if ok {
			if len(getTargets(idx, pipeline)) == 0 {
				return &quesma_api.Decision{
					IsClosed: true,
					Reason:   "Index is disabled in config.",
				}
			}
		}

		return nil
	}
}

func resolveInternalElasticName(part string) *quesma_api.Decision {

	if elasticsearch.IsInternalIndex(part) {
		return &quesma_api.Decision{
			UseConnectors: []quesma_api.ConnectorDecision{&quesma_api.ConnectorDecisionElastic{ManagementCall: true}},
			Reason:        "It's kibana internals",
		}
	}

	return nil
}

func resolveTableName(quesmaConf config.QuesmaConfiguration, originalName string) string {
	if indexCfg, ok := quesmaConf.IndexConfig[originalName]; ok {
		return indexCfg.TableName(originalName)
	}
	return originalName
}

func makeDefaultWildcard(quesmaConf config.QuesmaConfiguration, pipeline string) func(part string) *quesma_api.Decision {
	return func(part string) *quesma_api.Decision {
		var targets []string
		var useConnectors []quesma_api.ConnectorDecision

		switch pipeline {
		case quesma_api.IngestPipeline:
			targets = quesmaConf.DefaultIngestTarget
		case quesma_api.QueryPipeline:
			targets = quesmaConf.DefaultQueryTarget
		default:
			return &quesma_api.Decision{
				Reason: "Unsupported configuration",
				Err:    end_user_errors.ErrSearchCondition.New(fmt.Errorf("unsupported pipeline: %s", pipeline)),
			}
		}

		for _, target := range targets {
			switch target {
			case config.ClickhouseTarget:
				var tableName string
				if quesmaConf.UseCommonTableForWildcard {
					tableName = common_table.TableName
				} else {
					tableName = resolveTableName(quesmaConf, part)
				}
				useConnectors = append(useConnectors, &quesma_api.ConnectorDecisionClickhouse{
					ClickhouseTableName: tableName,
					IsCommonTable:       quesmaConf.UseCommonTableForWildcard,
					ClickhouseIndexes:   []string{part},
				})
			case config.ElasticsearchTarget:
				useConnectors = append(useConnectors, &quesma_api.ConnectorDecisionElastic{})
			default:
				return &quesma_api.Decision{
					Reason: "Unsupported configuration",
					Err:    end_user_errors.ErrSearchCondition.New(fmt.Errorf("unsupported target: %s", target)),
				}
			}
		}

		return &quesma_api.Decision{
			UseConnectors: useConnectors,
			IsClosed:      len(useConnectors) == 0,
			Reason:        fmt.Sprintf("Using default wildcard ('%s') configuration for %s processor", config.DefaultWildcardIndexName, pipeline),
		}
	}
}

func (r *tableRegistryImpl) singleIndex(indexConfig map[string]config.IndexConfiguration, pipeline string) func(part string) *quesma_api.Decision {

	return func(part string) *quesma_api.Decision {
		if cfg, ok := indexConfig[part]; ok {
			if !cfg.UseCommonTable {

				targets := getTargets(cfg, pipeline)

				switch len(targets) {

				// case 0 is handled before (makeIsDisabledInConfig)

				case 1:

					decision := &quesma_api.Decision{
						Reason: "Enabled in the config. ",
					}

					var targetDecision quesma_api.ConnectorDecision

					switch targets[0] {

					case config.ElasticsearchTarget:
						targetDecision = &quesma_api.ConnectorDecisionElastic{}
					case config.ClickhouseTarget:
						targetDecision = &quesma_api.ConnectorDecisionClickhouse{
							ClickhouseTableName: cfg.TableName(part),
							ClickhouseIndexes:   []string{part},
						}
					default:
						return &quesma_api.Decision{
							Reason: "Unsupported configuration",
							Err:    end_user_errors.ErrSearchCondition.New(fmt.Errorf("unsupported target: %s", targets[0])),
						}
					}
					decision.UseConnectors = append(decision.UseConnectors, targetDecision)

					return decision
				case 2:

					switch pipeline {

					case quesma_api.IngestPipeline:
						return &quesma_api.Decision{
							Reason: "Enabled in the config. Dual write is enabled.",

							UseConnectors: []quesma_api.ConnectorDecision{&quesma_api.ConnectorDecisionClickhouse{
								ClickhouseTableName: cfg.TableName(part),
								ClickhouseIndexes:   []string{part}},
								&quesma_api.ConnectorDecisionElastic{}},
						}

					case quesma_api.QueryPipeline:

						if targets[0] == config.ClickhouseTarget && targets[1] == config.ElasticsearchTarget {

							return &quesma_api.Decision{
								Reason:          "Enabled in the config. A/B testing.",
								EnableABTesting: true,
								UseConnectors: []quesma_api.ConnectorDecision{&quesma_api.ConnectorDecisionClickhouse{
									ClickhouseTableName: cfg.TableName(part),
									ClickhouseIndexes:   []string{part}},
									&quesma_api.ConnectorDecisionElastic{}},
							}
						} else if targets[0] == config.ElasticsearchTarget && targets[1] == config.ClickhouseTarget {

							return &quesma_api.Decision{
								Reason:          "Enabled in the config. A/B testing.",
								EnableABTesting: true,
								UseConnectors: []quesma_api.ConnectorDecision{
									&quesma_api.ConnectorDecisionElastic{},
									&quesma_api.ConnectorDecisionClickhouse{
										ClickhouseTableName: cfg.TableName(part),
										ClickhouseIndexes:   []string{part}},
								},
							}

						}

					default:
						return &quesma_api.Decision{
							Reason: "Unsupported configuration",
							Err:    end_user_errors.ErrSearchCondition.New(fmt.Errorf("unsupported pipeline: %s", pipeline)),
						}
					}

					return &quesma_api.Decision{
						Reason: "Unsupported configuration",
						Err:    end_user_errors.ErrSearchCondition.New(fmt.Errorf("unsupported configuration for pipeline %s, targets: %v", pipeline, targets)),
					}

				default:
					return &quesma_api.Decision{
						Reason: "Unsupported configuration",
						Err:    end_user_errors.ErrSearchCondition.New(fmt.Errorf("too many backend connector")),
					}
				}
			}
		}

		return nil
	}
}

func (r *tableRegistryImpl) makeCommonTableResolver(cfg map[string]config.IndexConfiguration, pipeline string) func(part string) *quesma_api.Decision {

	return func(part string) *quesma_api.Decision {
		if part == common_table.TableName {
			return &quesma_api.Decision{
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

		var connectors []quesma_api.ConnectorDecision

		connectors = append(connectors, &quesma_api.ConnectorDecisionClickhouse{
			ClickhouseTableName: common_table.TableName,
			ClickhouseIndexes:   []string{part},
			IsCommonTable:       true,
		})

		enableABTesting := false
		if optCfg, ok := r.conf.DefaultQueryOptimizers[config.ElasticABOptimizerName]; ok {
			if !optCfg.Disabled {
				enableABTesting = true
				// TODO uncomment this in order to enable A/B testing
				//connectors = append(connectors, &quesma_api.ConnectorDecisionElastic{})
			}
		}

		if idxConfig, ok := cfg[part]; (ok && idxConfig.UseCommonTable) || (virtualTableExists) {
			return &quesma_api.Decision{
				EnableABTesting: enableABTesting,
				UseConnectors:   connectors,
				Reason:          "Common table will be used.",
			}
		}

		return nil
	}
}

func mergeUseConnectors(lhs []quesma_api.ConnectorDecision, rhs []quesma_api.ConnectorDecision, rhsIndexName string) ([]quesma_api.ConnectorDecision, *quesma_api.Decision) {
	for _, connDecisionRhs := range rhs {
		foundMatching := false
		for _, connDecisionLhs := range lhs {
			if _, ok := connDecisionRhs.(*quesma_api.ConnectorDecisionElastic); ok {
				if _, ok := connDecisionLhs.(*quesma_api.ConnectorDecisionElastic); ok {
					foundMatching = true
				}
			}
			if rhsClickhouse, ok := connDecisionRhs.(*quesma_api.ConnectorDecisionClickhouse); ok {
				if lhsClickhouse, ok := connDecisionLhs.(*quesma_api.ConnectorDecisionClickhouse); ok {
					if lhsClickhouse.ClickhouseTableName != rhsClickhouse.ClickhouseTableName {
						return nil, &quesma_api.Decision{
							Reason: "Incompatible decisions for two indexes - they use a different ClickHouse table",
							Err:    fmt.Errorf("incompatible decisions for two indexes (different ClickHouse table) - %s and %s", connDecisionRhs, connDecisionLhs),
						}
					}
					if lhsClickhouse.IsCommonTable {
						if !rhsClickhouse.IsCommonTable {
							return nil, &quesma_api.Decision{
								Reason: "Incompatible decisions for two indexes - one uses the common table, the other does not",
								Err:    fmt.Errorf("incompatible decisions for two indexes (common table usage) - %s and %s", connDecisionRhs, connDecisionLhs),
							}
						}
						lhsClickhouse.ClickhouseIndexes = append(lhsClickhouse.ClickhouseIndexes, rhsClickhouse.ClickhouseIndexes...)
						lhsClickhouse.ClickhouseIndexes = util.Distinct(lhsClickhouse.ClickhouseIndexes)
					} else {
						if !reflect.DeepEqual(lhsClickhouse, rhsClickhouse) {
							return nil, &quesma_api.Decision{
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
			return nil, &quesma_api.Decision{
				Reason: "Incompatible decisions for two indexes - they use different connectors",
				Err:    fmt.Errorf("incompatible decisions for two indexes - they use different connectors: could not find connector %s used for index %s in decisions: %s", connDecisionRhs, rhsIndexName, lhs),
			}
		}
	}

	return lhs, nil
}

func basicDecisionMerger(decisions []*quesma_api.Decision) *quesma_api.Decision {
	if len(decisions) == 0 {
		return &quesma_api.Decision{
			IsEmpty: true,
			Reason:  "No indexes matched, no decisions made.",
		}
	}
	if len(decisions) == 1 {
		return decisions[0]
	}

	for _, decision := range decisions {
		if decision == nil {
			return &quesma_api.Decision{
				Reason: "Got a nil decision. This is a bug.",
				Err:    fmt.Errorf("could not resolve index"),
			}
		}

		if decision.Err != nil {
			return decision
		}

		if decision.IsEmpty {
			return &quesma_api.Decision{
				Reason: "Got an empty decision. This is a bug.",
				Err:    fmt.Errorf("could not resolve index, empty index: %s", decision.IndexPattern),
			}
		}

		if decision.EnableABTesting != decisions[0].EnableABTesting {
			return &quesma_api.Decision{
				Reason: "One of the indexes matching the pattern does A/B testing, while another index does not - inconsistency.",
				Err:    fmt.Errorf("inconsistent A/B testing configuration - index %s (A/B testing: %v) and index %s (A/B testing: %v)", decision.IndexPattern, decision.EnableABTesting, decisions[0].IndexPattern, decisions[0].EnableABTesting),
			}
		}
	}

	var nonClosedDecisions []*quesma_api.Decision
	for _, decision := range decisions {
		if !decision.IsClosed {
			nonClosedDecisions = append(nonClosedDecisions, decision)
		}
	}
	if len(nonClosedDecisions) == 0 {
		// All indexes are closed
		return &quesma_api.Decision{
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
			return &quesma_api.Decision{
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

	return &quesma_api.Decision{
		UseConnectors:   useConnectors,
		EnableABTesting: decisions[0].EnableABTesting,
		Reason:          "Merged decisions",
	}
}
