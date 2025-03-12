// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package frontend_connectors

import (
	"github.com/QuesmaOrg/quesma/platform/parsers/painful"
	"github.com/QuesmaOrg/quesma/platform/table_resolver"
	"github.com/QuesmaOrg/quesma/platform/types"
	"github.com/QuesmaOrg/quesma/platform/v2/core"
	"github.com/QuesmaOrg/quesma/platform/v2/core/tracing"
	"github.com/goccy/go-json"
	"strings"
)

func matchedAgainstAsyncId() quesma_api.RequestMatcher {
	return quesma_api.RequestMatcherFunc(func(req *quesma_api.Request) quesma_api.MatchResult {
		if !strings.HasPrefix(req.Params["id"], tracing.AsyncIdPrefix) {
			return quesma_api.MatchResult{Matched: false}
		}
		return quesma_api.MatchResult{Matched: true}
	})
}

func matchedAgainstPattern(indexRegistry table_resolver.TableResolver) quesma_api.RequestMatcher {
	return matchAgainstTableResolver(indexRegistry, quesma_api.QueryPipeline)
}

func matchAgainstTableResolver(indexRegistry table_resolver.TableResolver, pipelineName string) quesma_api.RequestMatcher {
	return quesma_api.RequestMatcherFunc(func(req *quesma_api.Request) quesma_api.MatchResult {
		indexName := req.Params["index"]

		if strings.HasPrefix(indexName, ".") { // Skip internal indices
			return quesma_api.MatchResult{Matched: false}
		}

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

// getPitIdFromRequest gets the PIT ID from the request body,
// depending on request kind it can be either at root or under `pit` key, e.g.:
// {"id": "pit_id"} or {"pit": {"id": "pit_id"}}
func getPitIdFromRequest(req *quesma_api.Request, pitAtRoot bool) string {
	var payload struct {
		ID  string `json:"id,omitempty"`
		Pit struct {
			ID string `json:"id"`
		} `json:"pit,omitempty"`
	}
	if err := json.Unmarshal([]byte(req.Body), &payload); err != nil {
		return ""
	}
	if pitAtRoot {
		return payload.ID
	}
	return payload.Pit.ID
}

func matchPitId(getPitFn func(*quesma_api.Request) string) quesma_api.RequestMatcher {
	return quesma_api.RequestMatcherFunc(func(req *quesma_api.Request) quesma_api.MatchResult {
		if strings.HasPrefix(getPitFn(req), quesmaPitPrefix) {
			return quesma_api.MatchResult{Matched: true}
		}
		return quesma_api.MatchResult{Matched: false}
	})
}

func isSearchRequestWithQuesmaPit() quesma_api.RequestMatcher {
	return matchPitId(func(req *quesma_api.Request) string {
		return getPitIdFromRequest(req, false)
	})
}

func hasQuesmaPitId() quesma_api.RequestMatcher {
	return matchPitId(func(req *quesma_api.Request) string {
		return getPitIdFromRequest(req, true)
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
