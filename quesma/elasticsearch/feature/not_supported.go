// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package feature

import (
	"context"
	"fmt"
	"github.com/QuesmaOrg/quesma/quesma/elasticsearch"
	"regexp"
	"strings"
)

const (
	apmTelemetryOpaqueId = "apm-telemetry-task"
	kibanaFleetOpaqueId  = "fleet-usage-sender"
)

// https://www.elastic.co/guide/en/elasticsearch/reference/current/search-template.html
var searchTemplatePathRegexps = compileRegexes([]string{"^/_scripts/(.*)", "^/_render/template$", "^/_render/template$"})
var searchTemplatePathWithIndexRegexps = compileRegexes([]string{"^/(.*)/_search/template$", "^/(.*)/_msearch/template$"})
var indexPathRegexp = regexp.MustCompile("^/(.*?)/(.*)$")

func compileRegexes(path []string) []*regexp.Regexp {
	var result []*regexp.Regexp

	for _, p := range path {
		result = append(result, regexp.MustCompile(p))
	}
	return result
}

var NotSupportedLogger *ThrottledLogger

func init() {
	NotSupportedLogger = NewThrottledLogger()
}

func logMessage(message string, args ...interface{}) {
	NotSupportedLogger.Log(fmt.Sprintf(message, args...))
}

func AnalyzeUnsupportedCalls(ctx context.Context, method, path string, opaqueId string, indexResolver func(ctx context.Context, pattern string) (indexes []string, err error)) (result bool) {
	return checkSearchTemplate(ctx, method, path) || checkSearchTemplateWithIndex(ctx, method, path, indexResolver) || (!checkIfKibanaInternalOpaqueId(opaqueId) && checkIfOurIndex(ctx, method, path, indexResolver))
}

func checkSearchTemplate(ctx context.Context, method string, path string) bool {

	for _, rx := range searchTemplatePathRegexps {
		if rx.MatchString(path) {
			logMessage("Not supported feature detected. Request  '%v %v'", method, path)
			return true
		}
	}

	return false
}

func checkSearchTemplateWithIndex(ctx context.Context, method string, path string, indexResolver func(context.Context, string) ([]string, error)) bool {

	for _, rx := range searchTemplatePathWithIndexRegexps {
		if rx.MatchString(path) {

			match := rx.FindStringSubmatch(path)
			if len(match) > 1 {
				indexes, err := indexResolver(ctx, match[1])
				if err != nil {
					logMessage("Error resolving index: %v", err)
					return false
				}
				for _, indexName := range indexes {
					logMessage("Not supported feature detected.  index: %v, request: '%v %v'", indexName, method, path)
				}
				return true
			}
		}
	}
	return false
}

func checkIfKibanaInternalOpaqueId(opaqueId string) bool {
	opaqueId = strings.ToLower(opaqueId)
	return strings.Contains(opaqueId, apmTelemetryOpaqueId) || strings.Contains(opaqueId, kibanaFleetOpaqueId)
}

func checkIfOurIndex(ctx context.Context, method string, path string, indexResolver func(context.Context, string) ([]string, error)) bool {

	// Check if the request matches /:index/:whatever pattern
	// We assume here that the first part is the index (indexes)
	// If it is our index, we log a warning.

	match := indexPathRegexp.FindStringSubmatch(path)
	if len(match) > 1 {
		indexNamePart := match[1]
		var matched bool

		if strings.HasPrefix(indexNamePart, "_") {
			// This is not actually an index, this is some endpoint (for example /_monitoring/bulk)
			return false
		}
		// Optimization: avoid using indexResolver for internal indexes
		if elasticsearch.IsInternalIndex(indexNamePart) {
			return false
		}

		indexes, err := indexResolver(ctx, indexNamePart)
		if err != nil {
			logMessage("Error resolving index: %v", err)
			return false
		}
		for _, indexName := range indexes {
			matched = true
			logMessage("Not supported feature detected.  index: '%s' request: '%s %s''", indexName, method, path)
		}

		return matched
	}
	return false

}
