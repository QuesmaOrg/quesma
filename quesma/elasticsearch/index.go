// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package elasticsearch

import (
	"fmt"
	"github.com/QuesmaOrg/quesma/quesma/end_user_errors"
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

func IsValidIndexName(name string) error {
	const maxIndexNameLength = 256

	if len(name) > maxIndexNameLength {
		return end_user_errors.ErrIndexNameTooLong.New(fmt.Errorf("index name is too long: %d, max length: %d", len(name), maxIndexNameLength))
	}

	// TODO add more checks, elasticsearch is quite strict about index names

	return nil
}
