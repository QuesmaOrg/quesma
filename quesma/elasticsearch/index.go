package elasticsearch

import "strings"

func IsIndexPattern(index string) bool {
	return strings.ContainsAny(index, "*,")
}
