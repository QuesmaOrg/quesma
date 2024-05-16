package quesma

import (
	"mitmproxy/quesma/elasticsearch"
	"mitmproxy/quesma/logger"
	"mitmproxy/quesma/quesma/config"
	"mitmproxy/quesma/quesma/mux"
	"strings"
)

func matchedAgainstAsyncId() mux.MatchPredicate {
	return func(m map[string]string, _ string) bool {
		if !strings.HasPrefix(m["id"], quesmaAsyncIdPrefix) {
			logger.Debug().Msgf("async query id %s is forwarded to Elasticsearch", m["id"])
			return false
		}
		return true
	}
}

func matchedAgainstBulkBody(configuration config.QuesmaConfiguration) func(m map[string]string, body string) bool {
	return func(m map[string]string, body string) bool {
		for idx, s := range strings.Split(body, "\n") {
			if idx%2 == 0 && len(s) > 0 {
				indexConfig, found := configuration.IndexConfig[extractIndexName(s)]
				if !found || !indexConfig.Enabled {
					return false
				}
			}
		}
		return true
	}
}

func matchedAgainstPattern(configuration config.QuesmaConfiguration) mux.MatchPredicate {
	return func(m map[string]string, _ string) bool {
		indexPattern := elasticsearch.NormalizePattern(m["index"])
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

			for _, pattern := range indexPatterns {
				for _, indexName := range configuration.IndexConfig {
					if config.MatchName(elasticsearch.NormalizePattern(pattern), indexName.Name) {
						if configuration.IndexConfig[indexName.Name].Enabled {
							return true
						}
					}
				}
			}
			return false
		} else {
			for _, index := range configuration.IndexConfig {
				pattern := elasticsearch.NormalizePattern(indexPattern)
				if config.MatchName(pattern, index.Name) {
					if indexConfig, exists := configuration.IndexConfig[index.Name]; exists {
						return indexConfig.Enabled
					}
				}
			}
			logger.Debug().Msgf("no index found for pattern %s", indexPattern)
			return false
		}
	}
}

func matchAgainstKibanaAlerts() mux.MatchPredicate {
	return func(m map[string]string, body string) bool {

		if body == "" {
			return true
		}

		// https://www.elastic.co/guide/en/security/current/alert-schema.html

		// We parse query here and check if it contains references to fields that
		// are alert related.
		// But it's complex to do so. We just check if the body contains some keywords.

		if strings.Contains(body, `"kibana.alert.rule.`) {
			logger.Warn().Msg("kibana alert rule detected")
			return false
		}

		if strings.Contains(body, `"kibana.alert.workflow.`) {
			logger.Warn().Msg("kibana alert workflow detected")
			return false
		}

		if strings.Contains(body, "kibana") {
			logger.Warn().Msgf("kibana message detected %v", body)
			return false
		}

		return true
	}
}
