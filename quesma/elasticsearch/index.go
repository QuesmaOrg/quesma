package elasticsearch

import (
	"quesma/logger"
	"regexp"
	"strings"
)

const (
	AllIndexesAliasIndexName = "_all"
	internalIndexPrefix      = "."
)

func IsIndexPattern(index string) bool {
	return strings.ContainsAny(index, "*,")
}

func IsInternalIndex(index string) bool {
	return strings.HasPrefix(index, internalIndexPrefix)
}

func IndexMatches(indexNamePattern, indexName string) bool {
	r, err := regexp.Compile("^" + strings.Replace(indexNamePattern, "*", ".*", -1) + "$")
	if err != nil {
		logger.Error().Msgf("invalid index name pattern [%s]: %s", indexNamePattern, err)
		return false
	}
	return r.MatchString(indexName)
}
