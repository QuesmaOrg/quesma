// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package regex

import (
	"quesma/model"
	"strings"
)

// ToClickhouseExpr converts a regex pattern to a Clickhouse expression.
// It's our old heuristic, maybe it'll need to be improved.
func ToClickhouseExpr(pattern string) (clickhouseFuncName string, patternExpr model.Expr) {
	// really simple == (out of all special characters, only . and .* may be present)
	isPatternReallySimple := func(pattern string) bool {
		// any special characters excluding . and * not allowed. Also (not the most important check) * can't be first character.
		if strings.ContainsAny(pattern, `?+|{}[]()"\`) || (len(pattern) > 0 && pattern[0] == '*') {
			return false
		}
		// .* allowed, but [any other char]* - not
		for i, char := range pattern[1:] {
			prevChar := pattern[i]
			if char == '*' && prevChar != '.' {
				return false
			}
		}
		return true
	}

	var funcName string
	if isPatternReallySimple(pattern) {
		pattern = strings.ReplaceAll(pattern, "_", `\_`)
		pattern = strings.ReplaceAll(pattern, ".*", "%")
		pattern = strings.ReplaceAll(pattern, ".", "_")
		funcName = "LIKE"
	} else { // this Clickhouse function is much slower, so we use it only for complex regexps
		funcName = "REGEXP"
	}

	return funcName, model.NewLiteral("'" + pattern + "'")
}
