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
	"slices"
)

// TODO these rules may be incorrect and incomplete
// They will be fixed int the next iteration.

func patternIsNotAllowed(input parsedPattern) *Decision {
	if !input.isPattern {
		return nil
	}
	return &Decision{
		Reason: "Pattern is not allowed.",
		Err:    fmt.Errorf("pattern is not allowed"),
	}
}

func makeIsDisabledInConfig(cfg map[string]config.IndexConfiguration, pipeline string) func(input parsedPattern) *Decision {

	return func(input parsedPattern) *Decision {

		if !input.isPattern {
			idx, ok := cfg[input.source]
			if ok {
				if len(getTargets(idx, pipeline)) == 0 {
					return &Decision{
						IsClosed: true,
						Reason:   "Index is disabled in config.",
					}
				}
			}
		}

		return nil
	}
}

func resolveInternalElasticName(pattern parsedPattern) *Decision {

	if elasticsearch.IsInternalIndex(pattern.source) {
		return &Decision{
			UseConnectors: []ConnectorDecision{&ConnectorDecisionElastic{ManagementCall: true}},
			Reason:        "It's kibana internals",
		}
	}

	return nil
}

func makeDefaultWildcard(quesmaConf config.QuesmaConfiguration, pipeline string) func(input parsedPattern) *Decision {
	return func(input parsedPattern) *Decision {
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
					ClickhouseTableName: input.source,
					IsCommonTable:       quesmaConf.UseCommonTableForWildcard,
					ClickhouseTables:    []string{input.source},
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

func (r *tableRegistryImpl) singleIndex(indexConfig map[string]config.IndexConfiguration, pipeline string) func(input parsedPattern) *Decision {

	return func(input parsedPattern) *Decision {

		if input.isPattern {
			return nil
		}

		if cfg, ok := indexConfig[input.source]; ok {
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
							ClickhouseTableName: input.source,
							ClickhouseTables:    []string{input.source},
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
								ClickhouseTableName: input.source,
								ClickhouseTables:    []string{input.source}},
								&ConnectorDecisionElastic{}},
						}

					case QueryPipeline:

						if targets[0] == config.ClickhouseTarget && targets[1] == config.ElasticsearchTarget {

							return &Decision{
								Reason:          "Enabled in the config. A/B testing.",
								EnableABTesting: true,
								UseConnectors: []ConnectorDecision{&ConnectorDecisionClickhouse{
									ClickhouseTableName: input.source,
									ClickhouseTables:    []string{input.source}},
									&ConnectorDecisionElastic{}},
							}
						} else if targets[0] == config.ElasticsearchTarget && targets[1] == config.ClickhouseTarget {

							return &Decision{
								Reason:          "Enabled in the config. A/B testing.",
								EnableABTesting: true,
								UseConnectors: []ConnectorDecision{
									&ConnectorDecisionElastic{},
									&ConnectorDecisionClickhouse{
										ClickhouseTableName: input.source,
										ClickhouseTables:    []string{input.source}},
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

func (r *tableRegistryImpl) makeCheckIfPatternMatchesAllConnectors(pipeline string) func(input parsedPattern) *Decision {

	return func(input parsedPattern) *Decision {
		if input.isPattern {

			var matchedElastic []string
			var matchedClickhouse []string

			for _, pattern := range input.parts {

				// here we check against the config

				for indexName, index := range r.conf.IndexConfig {
					targets := getTargets(index, pipeline)

					if util.IndexPatternMatches(pattern, indexName) {

						for _, target := range targets {
							switch target {
							case config.ElasticsearchTarget:
								matchedElastic = append(matchedElastic, indexName)
							case config.ClickhouseTarget:
								matchedClickhouse = append(matchedClickhouse, indexName)
							default:
								return &Decision{
									Err:    end_user_errors.ErrSearchCondition.New(fmt.Errorf("unsupported target: %s", target)),
									Reason: "Unsupported target.",
								}
							}
						}
					}
				}

				// but maybe we should also check against the actual indexes ??
				for indexName := range r.elasticIndexes {
					if util.IndexPatternMatches(pattern, indexName) {
						matchedElastic = append(matchedElastic, indexName)
					}
				}
				if r.conf.AutodiscoveryEnabled {
					for tableName := range r.clickhouseIndexes {
						if util.IndexPatternMatches(pattern, tableName) {
							matchedClickhouse = append(matchedClickhouse, tableName)
						}
					}
				}

			}

			matchedElastic = util.Distinct(matchedElastic)
			matchedClickhouse = util.Distinct(matchedClickhouse)

			nElastic := len(matchedElastic)
			nClickhouse := len(matchedClickhouse)

			switch {

			case nElastic > 0 && nClickhouse > 0:
				return &Decision{
					Err:    end_user_errors.ErrSearchCondition.New(fmt.Errorf("index pattern [%s] resolved to both elasticsearch indices: [%s] and clickhouse tables: [%s]", input.parts, matchedElastic, matchedClickhouse)),
					Reason: "Both Elastic and Clickhouse matched.",
				}

			case nElastic > 0 && nClickhouse == 0:

				return &Decision{
					UseConnectors: []ConnectorDecision{&ConnectorDecisionElastic{}},
					Reason:        "Only Elastic matched.",
				}

			case nElastic == 0 && nClickhouse > 0:
				// it will be resolved by sth else later
				return nil

			case nElastic == 0 && nClickhouse == 0:
				return &Decision{
					IsEmpty: true,
					Reason:  "No indexes matched. Checked both connectors.",
				}
			}
		}

		return nil
	}

}

func (r *tableRegistryImpl) makeCommonTableResolver(cfg map[string]config.IndexConfiguration, pipeline string) func(input parsedPattern) *Decision {

	return func(input parsedPattern) *Decision {

		if input.isPattern {

			// At this point we should do not have any elastic indexes.
			// This is because we have already checked if the pattern matches any elastic indexes.
			for _, pattern := range input.parts {
				for indexName := range r.elasticIndexes {
					if util.IndexPatternMatches(pattern, indexName) {
						return &Decision{
							Err:    end_user_errors.ErrSearchCondition.New(fmt.Errorf("index parsedPattern [%s] resolved to elasticsearch indices", input.parts)),
							Reason: "We're not supporting common tables for Elastic.",
						}
					}
				}
			}

			var matchedVirtualTables []string
			var matchedTables []string
			for _, pattern := range input.parts {

				// here we check against the config

				for indexName, index := range r.conf.IndexConfig {
					if util.IndexPatternMatches(pattern, indexName) {

						targets := getTargets(index, pipeline)

						if slices.Contains(targets, config.ClickhouseTarget) {
							if index.UseCommonTable {
								matchedVirtualTables = append(matchedVirtualTables, indexName)
							} else {
								matchedTables = append(matchedTables, indexName)
							}
						}
					}
				}

				// but maybe we should also check against the actual indexes ??
				if r.conf.AutodiscoveryEnabled {
					for indexName, index := range r.clickhouseIndexes {
						if util.IndexPatternMatches(pattern, indexName) {
							if index.isVirtual {
								matchedVirtualTables = append(matchedVirtualTables, indexName)
							} else {
								matchedTables = append(matchedTables, indexName)
							}
						}
					}
				}
			}

			matchedTables = util.Distinct(matchedTables)
			matchedVirtualTables = util.Distinct(matchedVirtualTables)

			switch {

			case len(matchedTables) == 0 && len(matchedVirtualTables) == 0:
				return &Decision{
					IsEmpty: true,
					Reason:  "No indexes found.",
				}

			case len(matchedTables) == 1 && len(matchedVirtualTables) == 0:
				return &Decision{
					UseConnectors: []ConnectorDecision{&ConnectorDecisionClickhouse{
						ClickhouseTableName: matchedTables[0],
						ClickhouseTables:    []string{matchedTables[0]},
					}},
					Reason: "Pattern matches single standalone table.",
				}

			case len(matchedTables) == 0 && len(matchedVirtualTables) > 0:
				return &Decision{
					UseConnectors: []ConnectorDecision{&ConnectorDecisionClickhouse{
						IsCommonTable:       true,
						ClickhouseTableName: common_table.TableName,
						ClickhouseTables:    matchedVirtualTables,
					}},
					Reason: "Common table will be used. Querying multiple indexes.",
				}

			default:
				return &Decision{
					Err:    end_user_errors.ErrSearchCondition.New(fmt.Errorf("index pattern [%s] resolved to both standalone table indices: [%s] and common table indices: [%s]", input.source, matchedTables, matchedVirtualTables)),
					Reason: "Both standalone table and common table indexes matches the pattern",
				}
			}
		}

		if input.source == common_table.TableName {
			return &Decision{
				Err:    fmt.Errorf("common table is not allowed to be queried directly"),
				Reason: "It's internal table. Not allowed to be queried directly.",
			}
		}

		var virtualTableExists bool

		if r.conf.AutodiscoveryEnabled {
			for indexName, index := range r.clickhouseIndexes {
				if index.isVirtual && indexName == input.source {
					virtualTableExists = true
					break
				}
			}
		}

		if idxConfig, ok := cfg[input.source]; (ok && idxConfig.UseCommonTable) || (virtualTableExists) {
			return &Decision{
				UseConnectors: []ConnectorDecision{&ConnectorDecisionClickhouse{
					ClickhouseTableName: common_table.TableName,
					ClickhouseTables:    []string{input.source},
					IsCommonTable:       true,
				}},
				Reason: "Common table will be used.",
			}
		}

		return nil
	}
}
