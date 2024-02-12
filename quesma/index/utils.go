package index

import (
	"regexp"
	"strings"
)

var regexpCache = map[string]*regexp.Regexp{}

func TableNamePatternRegexp(indexPattern string) *regexp.Regexp {
	if reg, ok := regexpCache[indexPattern]; ok {
		return reg
	}
	compiled := regexp.MustCompile(strings.Replace(indexPattern, "*", ".*", -1))
	regexpCache[indexPattern] = compiled
	return compiled
}
