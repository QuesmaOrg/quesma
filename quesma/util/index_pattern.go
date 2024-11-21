package util

import (
	"fmt"
	"regexp"
	"strings"
)

func IndexPatternMatches(indexNamePattern, indexName string) (bool, error) {
	r, err := regexp.Compile("^" + strings.Replace(indexNamePattern, "*", ".*", -1) + "$")
	if err != nil {
		return false, fmt.Errorf("invalid index name pattern [%s]: %s", indexNamePattern, err)
	}
	return r.MatchString(indexName), nil
}
