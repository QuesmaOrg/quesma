// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package clickhouse

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_tableVerifier_verify(t1 *testing.T) {
	tests := []struct {
		name       string
		table      discoveredTable
		isValid    bool
		violations []string
	}{
		{
			name: "valid table",
			table: discoveredTable{
				name: "table",
				columnTypes: map[string]columnMetadata{
					"column1": columnMetadata{colType: "String"},
					"column2": columnMetadata{colType: "Int32"},
				},
			},
			isValid:    true,
			violations: []string{},
		},
		{
			name: "table with invalid column names",
			table: discoveredTable{
				name: "table",
				columnTypes: map[string]columnMetadata{
					"column1": columnMetadata{colType: "String"},
					"foo.bar": columnMetadata{colType: "Int32"},
				},
			},
			isValid:    false,
			violations: []string{"Column name foo.bar in a table table contains a dot, which is not allowed and might produce undefined behaviour"},
		},
	}
	for _, tt := range tests {
		t1.Run(tt.name, func(t1 *testing.T) {
			t := tableVerifier{}
			isValid, violations := t.verify(tt.table)
			assert.Equalf(t1, tt.isValid, isValid, "verify(%v)", tt.table)
			assert.Equalf(t1, tt.violations, violations, "verify(%v)", tt.table)
		})
	}
}
