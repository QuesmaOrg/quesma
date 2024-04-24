package feature

import (
	"context"
	"fmt"
	"regexp"
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

func AnalyzeUnsupportedCalls(ctx context.Context, method, path string, indexResolver func(ctx context.Context, pattern string) (indexes []string)) (result bool) {
	return checkSearchTemplate(ctx, method, path) || checkSearchTemplateWithIndex(ctx, method, path, indexResolver) || checkIfOurIndex(ctx, method, path, indexResolver)
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

func checkSearchTemplateWithIndex(ctx context.Context, method string, path string, indexResolver func(context.Context, string) []string) bool {

	for _, rx := range searchTemplatePathWithIndexRegexps {
		if rx.MatchString(path) {

			match := rx.FindStringSubmatch(path)
			if len(match) > 1 {
				for _, indexName := range indexResolver(ctx, match[1]) {
					logMessage("Not supported feature detected.  index: %v, request: '%v %v'", indexName, method, path)
				}
				return true
			}
		}
	}
	return false
}

func checkIfOurIndex(ctx context.Context, method string, path string, indexResolver func(context.Context, string) []string) bool {

	// Check if the request matches /:index/:whatever pattern
	// We assume here that the first part is the index (indexes)
	// If it is our index, we log a warning.

	match := indexPathRegexp.FindStringSubmatch(path)
	if len(match) > 1 {
		indexNamePart := match[1]
		var matched bool
		for _, indexName := range indexResolver(ctx, indexNamePart) {
			matched = true
			logMessage("Not supported feature detected.  index: '%s' request: '%s %s''", indexName, method, path)
		}

		return matched
	}
	return false

}
