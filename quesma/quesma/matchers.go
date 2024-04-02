package quesma

import (
	"mitmproxy/quesma/logger"
	"mitmproxy/quesma/quesma/config"
	"mitmproxy/quesma/quesma/mux"
	"slices"
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
				indexConfig, found := configuration.GetIndexConfig(extractIndexName(s))
				if !found || !indexConfig.Enabled {
					return false
				}
			}
		}
		return true
	}
}

func matchedAgainstPattern(configuration config.QuesmaConfiguration, tables func() []string) mux.MatchPredicate {
	return func(m map[string]string, _ string) bool {
		indexPattern := m["index"]
		if strings.HasPrefix(indexPattern, elasticIndexPrefix) {
			logger.Debug().Msgf("index %s is an internal Elasticsearch index, skipping", indexPattern)
			return false
		}

		var candidates []string

		if strings.ContainsAny(indexPattern, "*,") {
			for _, pattern := range strings.Split(indexPattern, ",") {
				for _, tableName := range tables() {
					if config.MatchName(preprocessPattern(pattern), tableName) {
						candidates = append(candidates, tableName)
					}
				}
			}

			slices.Sort(candidates)
			candidates = slices.Compact(candidates)

			for _, candidate := range candidates {
				indexConfig, exists := configuration.GetIndexConfig(candidate)
				if !exists || !indexConfig.Enabled {
					return false
				}

				if exists && indexConfig.Enabled {
					return true
				}
			}
			return false
		} else {
			for _, tableName := range tables() {
				pattern := preprocessPattern(indexPattern)
				if config.MatchName(pattern, tableName) {
					candidates = append(candidates, tableName)
				}
			}

			for _, candidate := range candidates {
				indexConfig, exists := configuration.GetIndexConfig(candidate)
				if exists && indexConfig.Enabled {
					return true
				}
			}
			logger.Debug().Msgf("no index found for pattern %s", indexPattern)
			return false
		}
	}
}

func preprocessPattern(p string) string {
	if p == "_all" {
		return "*"
	}
	return p
}
