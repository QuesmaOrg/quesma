package quesma

import (
	"mitmproxy/quesma/elasticsearch"
	"mitmproxy/quesma/logger"
	"mitmproxy/quesma/quesma/config"
	"mitmproxy/quesma/quesma/mux"
	"strings"
)

func matchedAgainstAsyncId() mux.RequestMatcher {
	return mux.RequestMatcherFunc(func(req *mux.Request) bool {
		if !strings.HasPrefix(req.Params["id"], quesmaAsyncIdPrefix) {
			logger.Debug().Msgf("async query id %s is forwarded to Elasticsearch", req.Params["id"])
			return false
		}
		return true
	})
}

func matchedAgainstBulkBody(configuration config.QuesmaConfiguration) mux.RequestMatcher {
	return mux.RequestMatcherFunc(func(req *mux.Request) bool {
		for idx, s := range strings.Split(req.Body, "\n") {
			if idx%2 == 0 && len(s) > 0 {
				indexConfig, found := configuration.IndexConfig[extractIndexName(s)]
				if !found || !indexConfig.Enabled {
					return false
				}
			}
		}
		return true
	})
}

func matchedAgainstPattern(configuration config.QuesmaConfiguration) mux.RequestMatcher {
	return mux.RequestMatcherFunc(func(req *mux.Request) bool {
		indexPattern := elasticsearch.NormalizePattern(req.Params["index"])
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
	})
}

// Returns false if the body contains a Kibana alert related field.
func matchAgainstKibanaAlerts() mux.RequestMatcher {
	return mux.RequestMatcherFunc(func(req *mux.Request) bool {

		var query mux.JSON

		switch req.ParsedBody.(type) {

		case mux.JSON:
			query = req.ParsedBody.(mux.JSON)

		default:
			return true
		}

		// https://www.elastic.co/guide/en/security/current/alert-schema.html

		var findKibanaAlertField func(node interface{}) bool

		findKibanaAlertField = func(node interface{}) bool {

			if node == nil {
				return false
			}

			switch nodeValue := node.(type) {

			case map[string]interface{}:

				for k, v := range nodeValue {

					if strings.Contains(k, "kibana.alert.") {
						return true
					}

					if findKibanaAlertField(v) {
						return true
					}
				}

			case []interface{}:

				for _, i := range nodeValue {
					if findKibanaAlertField(i) {
						return true
					}
				}

			}
			return false
		}

		q := query["query"].(map[string]interface{})

		return !findKibanaAlertField(q)
	})
}
