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
)

// TODO these rules may be incorrect and incomplete
// They will be fixed int the next iteration.

func patternIsNotAllowed(input parsedPattern) *Decision {
	if !input.isPattern {
		return nil
	}
	return &Decision{
		Message: "Pattern is not allowed.",
		Err:     fmt.Errorf("pattern is not allowed"),
	}
}

func makeIsDisabledInConfig(cfg map[string]config.IndexConfiguration, pipeline string) func(input parsedPattern) *Decision {

	return func(input parsedPattern) *Decision {

		if input.isPattern {

			// pass to the next resolver

			return nil
		} else {
			idx, ok := cfg[input.source]
			if ok {
				if len(getTargets(idx, pipeline)) == 0 {
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

func resolveInternalElasticName(pattern parsedPattern) *Decision {

	if elasticsearch.IsInternalIndex(pattern.source) {
		return &Decision{
			UseConnectors: []ConnectorDecision{&ConnectorDecisionElastic{}},
			Message:       "It's kibana internals",
		}
	}

	return nil
}

func makeElasticIsDefault(cfg map[string]config.IndexConfiguration) func(input parsedPattern) *Decision {

	return func(input parsedPattern) *Decision {
		return &Decision{
			UseConnectors: []ConnectorDecision{&ConnectorDecisionElastic{}},
			Message:       "Elastic is default.",
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
							ClickhouseTableName: input.source,
							Indexes:             []string{input.source},
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
							ClickhouseTableName: input.source,
							Indexes:             []string{input.source},
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

func (r *tableRegistryImpl) makeCheckIfPatternMatchesAllConnectors() func(input parsedPattern) *Decision {

	return func(input parsedPattern) *Decision {
		if input.isPattern {

			matchedElastic := []string{}
			matchedClickhouse := []string{}

			for _, pattern := range input.parts {

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
					Err:     end_user_errors.ErrSearchCondition.New(fmt.Errorf("index pattern [%s] resolved to both elasticsearch indices: [%s] and clickhouse tables: [%s]", input.parts, matchedElastic, matchedClickhouse)),
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
				// or pass to another tableResolver
				return &Decision{
					IsEmpty: true,
					Message: "No indexes matched. Checked both connectors.",
				}
			}
		}

		return nil
	}

}

func (r *tableRegistryImpl) makeCommonTableResolver(cfg map[string]config.IndexConfiguration) func(input parsedPattern) *Decision {

	return func(input parsedPattern) *Decision {

		if input.isPattern {

			// TODO at this point we shouldn't have elastic indexes?
			for _, pattern := range input.parts {
				for indexName := range r.elasticIndexes {
					if util.IndexPatternMatches(pattern, indexName) {

						// TODO what about config ?
						// TODO ?
						return &Decision{
							Err:     end_user_errors.ErrSearchCondition.New(fmt.Errorf("index parsedPattern [%s] resolved to elasticsearch indices", input.parts)),
							Message: "We're not supporting common tables for Elastic.",
						}
					}
				}
			}

			matchedIndexes := []string{}

			for _, pattern := range input.parts {
				for indexName, index := range r.clickhouseIndexes {

					// TODO what about config ?
					// what if index uses common table but is't
					if util.IndexPatternMatches(pattern, indexName) && index.isVirtual {
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
					IsCommonTable:       true,
					ClickhouseTableName: common_table.TableName,
					Indexes:             matchedIndexes,
				}},
				Message: "Common table will be used. Querying multiple indexes.",
			}
		}

		if input.source == common_table.TableName {
			return &Decision{
				Err:     fmt.Errorf("common table is not allowed to be queried directly"),
				Message: "It's internal table. Not allowed to be queried directly.",
			}
		}

		if idxConfig, ok := cfg[input.source]; ok && idxConfig.UseCommonTable {
			return &Decision{
				UseConnectors: []ConnectorDecision{&ConnectorDecisionClickhouse{
					ClickhouseTableName: common_table.TableName,
					Indexes:             []string{input.source},
					IsCommonTable:       true,
				}},
				Message: "Common table will be used.",
			}
		}

		return nil
	}
}
