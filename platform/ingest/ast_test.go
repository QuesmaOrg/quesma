// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package ingest

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestCreateTableStatement_ToSQL(t *testing.T) {
	tests := []struct {
		name     string
		stmt     CreateTableStatement
		expected string
	}{
		{
			name: "basic table",
			stmt: CreateTableStatement{
				Name: "my_table",
				Columns: []ColumnStatement{
					{ColumnName: "id", ColumnType: "Int64"},
					{ColumnName: "@timestamp", ColumnType: "DateTime64(3)", AdditionalMetadata: "DEFAULT now64()"},
					{ColumnName: "name", ColumnType: "String", Comment: "user name"},
				},
				Comment:    "created by Quesma",
				PostClause: "ENGINE = MergeTree() ORDER BY (\"@timestamp\")",
			},
			expected: `CREATE TABLE IF NOT EXISTS "my_table" 
(

	"id" Int64,
	"@timestamp" DateTime64(3) DEFAULT now64(),
	"name" String COMMENT 'user name'
)
ENGINE = MergeTree() ORDER BY ("@timestamp")
COMMENT 'created by Quesma'`,
		},
		{
			name: "basic table with cluster",
			stmt: CreateTableStatement{
				Name:    "my_table",
				Cluster: "quesma_cluster",
				Columns: []ColumnStatement{
					{ColumnName: "id", ColumnType: "Int64"},
				},
				Comment:    "created by Quesma",
				PostClause: "ENGINE = MergeTree() ORDER BY (\"id\")",
			},
			expected: `CREATE TABLE IF NOT EXISTS "my_table" ON CLUSTER "quesma_cluster" 
 
(

	"id" Int64
)
ENGINE = MergeTree() ORDER BY ("id")
COMMENT 'created by Quesma'`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.stmt.ToSQL()
			assert.Equal(t, tt.expected, got)
		})
	}
}
