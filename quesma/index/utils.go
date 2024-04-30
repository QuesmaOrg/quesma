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
