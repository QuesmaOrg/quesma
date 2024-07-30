// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package clickhouse

import (
	"fmt"
	"strings"
)

type tableVerifier struct {
	// TODO enable various verification strategies
}

func (t tableVerifier) verify(table discoveredTable) (bool, []string) {
	var violations = make([]string, 0)
	for columnName := range table.columnTypes {
		if strings.Contains(columnName, ".") {
			violations = append(violations, fmt.Sprintf("Column name %s in a table %s contains a dot, which is not allowed and might produce undefined behaviour", columnName, table.name))
		}
	}
	return len(violations) == 0, violations
}
