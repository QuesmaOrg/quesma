// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package index_registry

import (
	"fmt"
	"quesma/common_table"
	"quesma/elasticsearch"
	"quesma/end_user_errors"
	"quesma/quesma/config"
	"quesma/util"
	"strings"
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
			UseConnectors: []ConnectorDecision{&ConnectorDecisionElastic{}},
			Message:       "It's kibana internals",
		}
	}

	return nil
}

func makeElasticIsDefault(cfg map[string]config.IndexConfiguration) func(input indexPattern) *Decision {

	return func(input indexPattern) *Decision {

		/*
			wildcard, ok := cfg["*"]
			if !ok {
				return &Decision{
					Message: "wildcard is not defined in the config",
					Err:     end_user_errors.ErrSearchCondition.New(fmt.Errorf("Wildcard is not defined in the config ", input.pattern)),
				}
			}

			targets := getTargets(wildcard)
			fmt.Println("XXXX ", input, targets[0])

			if len(targets) == 0 {
				return &Decision{
					Message:  "Disabled in the config.",
					IsClosed: true,
				}
			}

			if len(targets) != 1 {
				return &Decision{
					Message: "Unsupported configuration",
					Err:     end_user_errors.ErrSearchCondition.New(fmt.Errorf("There are too many backend connectors ", input.pattern)),
				}
			}

		*/

		// TODO
		// read the wildcard configuration, and check if it's enabled
		// rwildcard is not passed in the config

		return &Decision{
			UseConnectors: []ConnectorDecision{&ConnectorDecisionElastic{}},
			Message:       "Elastic is default.",
		}

	}
}

func makeCommonTableAsDefault(cfg map[string]config.IndexConfiguration) func(input indexPattern) *Decision {

	return func(input indexPattern) *Decision {

		// it will not work
		// default configuration is not passed in the config
		//

		wildcard, ok := cfg["*"]
		if !ok {
			return &Decision{
				Message: "Wildcard is not defined in the config.",
				Err:     end_user_errors.ErrSearchCondition.New(fmt.Errorf("wildcard is not defined in the config")),
			}
		}

		targets := getTargets(wildcard)

		if len(targets) == 0 {
			return &Decision{
				Message:  "Disabled in the config.",
				IsClosed: true,
			}
		}

		if len(targets) != 1 {
			return &Decision{
				Message: "Unsupported configuration",
				Err:     end_user_errors.ErrSearchCondition.New(fmt.Errorf("there are too many backend connectors ")),
			}
		}

		if targets[0] == "clickhouse" {

			return &Decision{
				UseConnectors: []ConnectorDecision{&ConnectorDecisionClickhouse{
					IsCommonTable: true,
				}},
				Message: "Use common table.",
			}
		}
		return nil
	}
}

func (r *indexRegistryImpl) makeResolveAutodiscovery(cfg map[string]config.IndexConfiguration) func(input indexPattern) *Decision {

	return func(input indexPattern) *Decision {

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
				UseConnectors: []ConnectorDecision{&ConnectorDecisionClickhouse{
					ClickhouseTableName: table.TableName,
				}},
				Message: "Found the physical table. Autodiscovery.",
			}
		}

		return nil
	}
}

func (r *indexRegistryImpl) singleIndex(indexConfig map[string]config.IndexConfiguration) func(input indexPattern) *Decision {

	return func(input indexPattern) *Decision {

		if input.isPattern {
			return nil
		}

		if cfg, ok := indexConfig[input.pattern]; ok {
			if !cfg.UseCommonTable {

				targets := getTargets(cfg)

				switch len(targets) {

				case 0:
					return &Decision{
						Message:  "Disabled in the config.",
						IsClosed: true,
					}

				case 1:

					decision := &Decision{
						Message: "Enabled in the config. ",
					}

					var targetDecision ConnectorDecision

					// FIXME this
					switch targets[0] {

					case "elasticsearch":
						targetDecision = &ConnectorDecisionElastic{}
					case "clickhouse":
						targetDecision = &ConnectorDecisionClickhouse{
							ClickhouseTableName: input.pattern,
							Indexes:             []string{input.pattern},
						}
					default:
						return &Decision{
							Message: "Unsupported configuration",
							Err:     end_user_errors.ErrSearchCondition.New(fmt.Errorf("unsupported target: %s", targets[0])),
						}
					}
					decision.UseConnectors = append(decision.UseConnectors, targetDecision)

					return decision
				case 2:

					// check targets and decide
					// TODO what about A/B testing ?

					return &Decision{
						Message: "Enabled in the config. Physical table will be used.",

						UseConnectors: []ConnectorDecision{&ConnectorDecisionClickhouse{
							ClickhouseTableName: input.pattern,
							Indexes:             []string{input.pattern},
						}},
					}

				default:
					return &Decision{
						Message: "Unsupported configuration",
						Err:     end_user_errors.ErrSearchCondition.New(fmt.Errorf("too many backend connector")),
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
					UseConnectors: []ConnectorDecision{&ConnectorDecisionElastic{}},
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

func (r *indexRegistryImpl) makeClickhouseCommonTableResolver(cfg map[string]config.IndexConfiguration) func(input indexPattern) *Decision {

	return func(input indexPattern) *Decision {

		if input.isPattern {

			// TODO at this point we shouldn't have elastic indexes?
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
					// what if index uses common table but is't
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
				UseConnectors: []ConnectorDecision{&ConnectorDecisionClickhouse{
					IsCommonTable: true,
					Indexes:       matchedIndexes,
				}},
				Message: "Common table will be used. Querying multiple indexes.",
			}
		}

		if input.pattern == common_table.TableName {
			return &Decision{
				Err:     fmt.Errorf("common table is not allowed to be queried directly"),
				Message: "It's internal table. Not allowed to be queried directly.",
			}
		}

		if idxConfig, ok := cfg[input.pattern]; ok && idxConfig.UseCommonTable {
			return &Decision{
				UseConnectors: []ConnectorDecision{&ConnectorDecisionClickhouse{
					ClickhouseTableName: common_table.TableName,
					Indexes:             []string{input.pattern},
				}},
				Message: "Common table will be used.",
			}
		}

		return nil
	}
}

func resolveDockerIndexes(pattern indexPattern) *Decision {

	if strings.Contains(pattern.pattern, "logs-elastic_agent") ||
		strings.Contains(pattern.pattern, "metrics-docker") ||
		strings.Contains(pattern.pattern, "metrics-system") {
		return &Decision{
			UseConnectors: []ConnectorDecision{&ConnectorDecisionClickhouse{
				IsCommonTable: true,
				Indexes:       pattern.patterns,
			}},
			Message: "Docker index.",
		}
	}

	return nil
}

var _ = resolveDockerIndexes
