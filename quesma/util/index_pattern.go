// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

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
