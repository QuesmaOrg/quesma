package elasticsearch

import "strings"

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
