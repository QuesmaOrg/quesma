package index

import (
	"fmt"
	"regexp"
	"strings"
)

func TableNamePatternRegexp(indexPattern string) *regexp.Regexp {
	return regexp.MustCompile(fmt.Sprintf("^%s$", strings.Replace(indexPattern, "*", ".*", -1)))
}
