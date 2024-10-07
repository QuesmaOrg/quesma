// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package quesma

import (
	"quesma/elasticsearch"
	"quesma/index_registry"
	"quesma/logger"
	"quesma/quesma/config"
	"quesma/quesma/mux"
	"quesma/quesma/types"
	"quesma/schema"
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

func matchedAgainstBulkBody(configuration *config.QuesmaConfiguration, indexRegistry index_registry.IndexRegistry) mux.RequestMatcher {
	return mux.RequestMatcherFunc(func(req *mux.Request) bool {
		idx := 0
		for _, s := range strings.Split(req.Body, "\n") {
			if len(s) == 0 {
				// ElasticSearch Agent sends empty lines between some JSONs, ignore them.
				continue
			}
			if idx%2 == 0 {
				name := extractIndexName(s)

				decision := indexRegistry.ResolveIngest(name)
				index_registry.TODO("matchedAgainstBulkBody", name, " -> ", decision)

				indexConfig, found := configuration.IndexConfig[name]
				if found && (indexConfig.IsClickhouseIngestEnabled() || indexConfig.IsIngestDisabled()) {
					return true
				}
			}
			idx += 1
		}

		// All indexes are disabled, the whole bulk can go to Elastic
		return false
	})
}

// Query path only (looks at QueryTarget)
func matchedAgainstPattern(configuration *config.QuesmaConfiguration, sr schema.Registry, indexRegistry index_registry.IndexRegistry) mux.RequestMatcher {
	return mux.RequestMatcherFunc(func(req *mux.Request) bool {
		indexPattern := elasticsearch.NormalizePattern(req.Params["index"])

		decision := indexRegistry.ResolveQuery(indexPattern)
		index_registry.TODO("matchedAgainstPattern", indexPattern, " -> ", decision)

		if elasticsearch.IsInternalIndex(indexPattern) {
			logger.Debug().Msgf("index %s is an internal Elasticsearch index, skipping", indexPattern)
			return false
		}

		indexPatterns := strings.Split(indexPattern, ",")

		if elasticsearch.IsIndexPattern(indexPattern) {
			for _, pattern := range indexPatterns {
				if elasticsearch.IsInternalIndex(pattern) {
					logger.Debug().Msgf("index %s is an internal Elasticsearch index, skipping", indexPattern)
					return false
				}
			}
			if configuration.IndexAutodiscoveryEnabled() {
				for tableName := range sr.AllSchemas() {
					if config.MatchName(elasticsearch.NormalizePattern(indexPattern), string(tableName)) {
						return true
					}
				}
			}
			for _, pattern := range indexPatterns {
				for _, indexName := range configuration.IndexConfig {
					if config.MatchName(elasticsearch.NormalizePattern(pattern), indexName.Name) {
						if configuration.IndexConfig[indexName.Name].IsClickhouseQueryEnabled() {
							return true
						}
					}
				}
			}
			return false
		} else {
			if configuration.IndexAutodiscoveryEnabled() {
				for tableName := range sr.AllSchemas() {
					if config.MatchName(elasticsearch.NormalizePattern(indexPattern), string(tableName)) {
						return true
					}
				}
			}
			for _, index := range configuration.IndexConfig {
				pattern := elasticsearch.NormalizePattern(indexPattern)
				if config.MatchName(pattern, index.Name) {
					if indexConfig, exists := configuration.IndexConfig[index.Name]; exists {
						return indexConfig.IsClickhouseQueryEnabled()
					}
				}
			}
			logger.Debug().Msgf("no index found for pattern %s", indexPattern)
			return false
		}
	})
}

// check whether exact index name is enabled
func matchedExact(cfg *config.QuesmaConfiguration, queryPath bool, indexRegistry index_registry.IndexRegistry) mux.RequestMatcher {
	return mux.RequestMatcherFunc(func(req *mux.Request) bool {

		indexName := req.Params["index"]

		decision := indexRegistry.ResolveIngest(indexName)
		index_registry.TODO("XXX matchedExact", indexName, " -> ", decision)

		if elasticsearch.IsInternalIndex(req.Params["index"]) {
			logger.Debug().Msgf("index %s is an internal Elasticsearch index, skipping", req.Params["index"])
			return false
		}

		indexConfig, exists := cfg.IndexConfig[req.Params["index"]]
		if queryPath {
			return exists && indexConfig.IsClickhouseQueryEnabled()
		} else {
			return exists && (indexConfig.IsClickhouseIngestEnabled() || indexConfig.IsIngestDisabled())
		}
	})
}

func matchedExactQueryPath(cfg *config.QuesmaConfiguration, indexRegistry index_registry.IndexRegistry) mux.RequestMatcher {
	return matchedExact(cfg, true, indexRegistry)
}

func matchedExactIngestPath(cfg *config.QuesmaConfiguration, indexRegistry index_registry.IndexRegistry) mux.RequestMatcher {
	return matchedExact(cfg, false, indexRegistry)
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
