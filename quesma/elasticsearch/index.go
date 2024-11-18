// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package elasticsearch

import (
	"strings"
)

const (
	internalIndexPrefix = "."
)

func IsIndexPattern(index string) bool {
	return strings.ContainsAny(index, "*,")
}

func IsInternalIndex(index string) bool {
	return strings.HasPrefix(index, internalIndexPrefix)
}

// InternalPaths is a list of paths that are considered internal and should not handled by Quesma
var InternalPaths = []string{"/_nodes", "/_xpack"}
