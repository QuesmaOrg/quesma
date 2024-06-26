// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package index

import (
	"fmt"
	"regexp"
	"strings"
)

func TableNamePatternRegexp(indexPattern string) *regexp.Regexp {
	var builder strings.Builder

	for _, char := range indexPattern {
		switch char {
		case '*':
			builder.WriteString(".*")
		case '[', ']', '\\', '^', '$', '.', '|', '?', '+', '(', ')':
			builder.WriteRune('\\')
			builder.WriteRune(char)
		default:
			builder.WriteRune(char)
		}
	}

	return regexp.MustCompile(fmt.Sprintf("^%s$", builder.String()))
}
