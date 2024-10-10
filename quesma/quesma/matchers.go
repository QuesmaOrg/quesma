// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package quesma

import (
	"quesma/elasticsearch"
	"quesma/logger"
	"quesma/quesma/config"
	"quesma/quesma/mux"
	"quesma/quesma/types"
	"quesma/schema"
	"quesma/table_resolver"
	"quesma/tracing"
	"strings"
)

func matchedAgainstAsyncId() mux.RequestMatcher {
	return mux.RequestMatcherFunc(func(req *mux.Request) bool {
		if !strings.HasPrefix(req.Params["id"], tracing.AsyncIdPrefix) {
			logger.Debug().Msgf("async query id %s is forwarded to Elasticsearch", req.Params["id"])
			return false
		}
		return true
	})
}

func matchedAgainstBulkBody(configuration *config.QuesmaConfiguration, tableResolver table_resolver.TableResolver) mux.RequestMatcher {
	return mux.RequestMatcherFunc(func(req *mux.Request) bool {
		idx := 0
		for _, s := range strings.Split(req.Body, "\n") {
			if len(s) == 0 {
				// ElasticSearch Agent sends empty lines between some JSONs, ignore them.
				continue
			}
			if idx%2 == 0 {
				name := extractIndexName(s)

				decision := tableResolver.Resolve(table_resolver.IngestPipeline, name)

				// if have any enabled Clickhouse connector, then return true
				for _, connector := range decision.UseConnectors {
					if _, ok := connector.(*table_resolver.ConnectorDecisionClickhouse); ok {
						return true
					}
				}
			}
			idx += 1
		}

		// All indexes are disabled, the whole bulk can go to Elastic
		return false
	})
}

// Query path only (looks at QueryTarget)
func matchedAgainstPattern(configuration *config.QuesmaConfiguration, sr schema.Registry, indexRegistry table_resolver.TableResolver) mux.RequestMatcher {
	return mux.RequestMatcherFunc(func(req *mux.Request) bool {
		indexPattern := elasticsearch.NormalizePattern(req.Params["index"])

		decision := indexRegistry.Resolve(table_resolver.QueryPipeline, indexPattern)
		for _, connector := range decision.UseConnectors {
			if _, ok := connector.(*table_resolver.ConnectorDecisionClickhouse); ok {
				return true
			}
		}

		return false
	})
}

// check whether exact index name is enabled
func matchedExact(cfg *config.QuesmaConfiguration, queryPath bool, indexRegistry table_resolver.TableResolver, pipelineName string) mux.RequestMatcher {
	return mux.RequestMatcherFunc(func(req *mux.Request) bool {

		indexName := req.Params["index"]

		decision := indexRegistry.Resolve(pipelineName, indexName)
		for _, connector := range decision.UseConnectors {
			if _, ok := connector.(*table_resolver.ConnectorDecisionClickhouse); ok {
				return true
			}
		}
		return false
	})
}

func matchedExactQueryPath(cfg *config.QuesmaConfiguration, indexRegistry table_resolver.TableResolver) mux.RequestMatcher {
	return matchedExact(cfg, true, indexRegistry, table_resolver.QueryPipeline)
}

func matchedExactIngestPath(cfg *config.QuesmaConfiguration, indexRegistry table_resolver.TableResolver) mux.RequestMatcher {
	return matchedExact(cfg, false, indexRegistry, table_resolver.IngestPipeline)
}

// Returns false if the body contains a Kibana internal search.
// Kibana does several /_search where you can identify it only by field
func matchAgainstKibanaInternal() mux.RequestMatcher {
	return mux.RequestMatcherFunc(func(req *mux.Request) bool {

		var query types.JSON

		switch req.ParsedBody.(type) {

		case types.JSON:
			query = req.ParsedBody.(types.JSON)

		default:
			return true
		}

		hasJsonKey := func(keyFrag string, node interface{}) bool {
			keyFrag = strings.ToLower(keyFrag)

			var hasJsonKeyRec func(node interface{}) bool

			hasJsonKeyRec = func(node interface{}) bool {
				if node == nil {
					return false
				}

				switch nodeValue := node.(type) {
				case map[string]interface{}:
					for k, v := range nodeValue {
						if strings.Contains(strings.ToLower(k), keyFrag) {
							return true
						}

						if hasJsonKeyRec(v) {
							return true
						}
					}
				case []interface{}:
					for _, i := range nodeValue {
						if hasJsonKeyRec(i) {
							return true
						}
					}
				}
				return false
			}

			return hasJsonKeyRec(node)
		}

		q := query["query"].(map[string]interface{})

		// 1. https://www.elastic.co/guide/en/security/current/alert-schema.html
		// 2. migrationVersion
		// 3., 4., 5. related to Kibana Fleet
		return !hasJsonKey("kibana.alert.", q) && !hasJsonKey("migrationVersion", q) && !hasJsonKey("idleTimeoutExpiration", q) && !strings.Contains(req.Body, "fleet-message-signing-keys") && !strings.Contains(req.Body, "fleet-uninstall-tokens")
	})
}
