// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package quesma

import (
	"github.com/goccy/go-json"
	"quesma/logger"
	"quesma/painful"
	"quesma/quesma/config"
	"quesma/quesma/types"
	"quesma/table_resolver"
	"quesma_v2/core"
	tracing "quesma_v2/core/tracing"
	"strings"
)

func matchedAgainstAsyncId() quesma_api.RequestMatcher {
	return quesma_api.RequestMatcherFunc(func(req *quesma_api.Request) quesma_api.MatchResult {
		if !strings.HasPrefix(req.Params["id"], tracing.AsyncIdPrefix) {
			logger.Debug().Msgf("async query id %s is forwarded to Elasticsearch", req.Params["id"])
			return quesma_api.MatchResult{Matched: false}
		}
		return quesma_api.MatchResult{Matched: true}
	})
}

func matchedAgainstBulkBody(configuration *config.QuesmaConfiguration, tableResolver table_resolver.TableResolver) quesma_api.RequestMatcher {
	return quesma_api.RequestMatcherFunc(func(req *quesma_api.Request) quesma_api.MatchResult {
		idx := 0
		for _, s := range strings.Split(req.Body, "\n") {
			if len(s) == 0 {
				// ElasticSearch Agent sends empty lines between some JSONs, ignore them.
				continue
			}
			if idx%2 == 0 {
				name := extractIndexName(s)

				decision := tableResolver.Resolve(quesma_api.IngestPipeline, name)

				if decision.IsClosed {
					return quesma_api.MatchResult{Matched: true, Decision: decision}
				}

				// if have any enabled Clickhouse connector, then return true
				for _, connector := range decision.UseConnectors {
					if _, ok := connector.(*quesma_api.ConnectorDecisionClickhouse); ok {
						return quesma_api.MatchResult{Matched: true, Decision: decision}
					}
				}
			}
			idx += 1
		}

		// All indexes are disabled, the whole bulk can go to Elastic
		return quesma_api.MatchResult{Matched: false}
	})
}

// Query path only (looks at QueryTarget)
func matchedAgainstPattern(indexRegistry table_resolver.TableResolver) quesma_api.RequestMatcher {
	return matchAgainstTableResolver(indexRegistry, quesma_api.QueryPipeline)
}

// check whether exact index name is enabled
func matchAgainstTableResolver(indexRegistry table_resolver.TableResolver, pipelineName string) quesma_api.RequestMatcher {
	return quesma_api.RequestMatcherFunc(func(req *quesma_api.Request) quesma_api.MatchResult {

		indexName := req.Params["index"]

		decision := indexRegistry.Resolve(pipelineName, indexName)
		if decision.Err != nil {
			return quesma_api.MatchResult{Matched: false, Decision: decision}
		}
		for _, connector := range decision.UseConnectors {
			if _, ok := connector.(*quesma_api.ConnectorDecisionClickhouse); ok {
				return quesma_api.MatchResult{Matched: true, Decision: decision}
			}
		}
		return quesma_api.MatchResult{Matched: false, Decision: decision}
	})
}

func matchedExactQueryPath(indexRegistry table_resolver.TableResolver) quesma_api.RequestMatcher {
	return matchAgainstTableResolver(indexRegistry, quesma_api.QueryPipeline)
}

func matchedExactIngestPath(indexRegistry table_resolver.TableResolver) quesma_api.RequestMatcher {
	return matchAgainstTableResolver(indexRegistry, quesma_api.IngestPipeline)
}

// Returns false if the body contains a Kibana internal search.
// Kibana does several /_search where you can identify it only by field
func matchAgainstKibanaInternal() quesma_api.RequestMatcher {
	return quesma_api.RequestMatcherFunc(func(req *quesma_api.Request) quesma_api.MatchResult {

		var query types.JSON

		switch req.ParsedBody.(type) {

		case types.JSON:
			query = req.ParsedBody.(types.JSON)

		default:
			return quesma_api.MatchResult{Matched: true}
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
		matched := !hasJsonKey("kibana.alert.", q) && !hasJsonKey("migrationVersion", q) && !hasJsonKey("idleTimeoutExpiration", q) && !strings.Contains(req.Body, "fleet-message-signing-keys") && !strings.Contains(req.Body, "fleet-uninstall-tokens")
		return quesma_api.MatchResult{Matched: matched}
	})
}

func matchAgainstIndexNameInScriptRequestBody(tableResolver table_resolver.TableResolver) quesma_api.RequestMatcher {
	return quesma_api.RequestMatcherFunc(func(req *quesma_api.Request) quesma_api.MatchResult {

		var scriptRequest painful.ScriptRequest

		err := json.Unmarshal([]byte(req.Body), &scriptRequest)
		if err != nil {
			return quesma_api.MatchResult{Matched: false}
		}

		decision := tableResolver.Resolve(quesma_api.QueryPipeline, scriptRequest.ContextSetup.IndexName)

		if decision.Err != nil {
			return quesma_api.MatchResult{Matched: false, Decision: decision}
		}
		for _, connector := range decision.UseConnectors {
			if _, ok := connector.(*quesma_api.ConnectorDecisionClickhouse); ok {
				return quesma_api.MatchResult{Matched: true, Decision: decision}
			}
		}

		return quesma_api.MatchResult{Matched: false, Decision: nil}
	})
}
