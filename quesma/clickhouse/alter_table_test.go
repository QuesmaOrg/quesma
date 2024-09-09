// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package clickhouse

import (
	"github.com/stretchr/testify/assert"
	"quesma/concurrent"
	"quesma/quesma/config"
	"quesma/quesma/types"
	"strconv"
	"testing"
)

func TestAlterTable(t *testing.T) {
	chConfig := &ChTableConfig{
		hasTimestamp:         true,
		timestampDefaultsNow: true,
		engine:               "MergeTree",
		orderBy:              "(timestamp)",
		partitionBy:          "",
		primaryKey:           "",
		ttl:                  "",
		attributes: []Attribute{
			NewDefaultStringAttribute(),
		},
		castUnsupportedAttrValueTypesToString: true,
		preferCastingToOthers:                 true,
	}
	rowsToInsert := []string{
		`{"Test1":1}`,
		`{"Test1":1,"Test2":2}`,
	}
	expectedInsert := []string{
		"{\"Test1\":1}",
		"{\"Test1\":1,\"Test2\":2}",
	}
	alters := []string{
		"ALTER TABLE \"tableName\" ADD COLUMN IF NOT EXISTS \"Test1\" Nullable(Int64)",
		"ALTER TABLE \"tableName\" ADD COLUMN IF NOT EXISTS \"Test2\" Nullable(Int64)",
	}
	columns := []string{"Test1", "Test2"}
	table := &Table{
		Name: "tableName",
		Cols: map[string]*Column{},
	}
	fieldsMap := concurrent.NewMapWith("tableName", table)

	lm := NewLogManager(fieldsMap, &config.QuesmaConfiguration{})
	for i := range rowsToInsert {
		_, alter, onlySchemaFields, nonSchemaFields, err := lm.BuildIngestSQLStatements(table, types.MustJSON(rowsToInsert[i]), nil, chConfig)
		insert := generateInsertJson(nonSchemaFields, onlySchemaFields)
		assert.Equal(t, expectedInsert[i], insert)
		assert.Equal(t, alters[i], alter[0])
		// Table will grow with each iteration
		assert.Equal(t, i+1, len(table.Cols))
		for _, col := range columns[:i+1] {
			_, ok := table.Cols[col]
			assert.True(t, ok)
		}
		for k, col := range table.Cols {
			assert.Equal(t, k, col.Name)
			assert.Equal(t, "Nullable", col.Modifiers)
		}

		assert.NoError(t, err)
	}
}

func TestAlterTableHeuristic(t *testing.T) {
	chConfig := &ChTableConfig{
		hasTimestamp:         true,
		timestampDefaultsNow: true,
		engine:               "MergeTree",
		orderBy:              "(timestamp)",
		partitionBy:          "",
		primaryKey:           "",
		ttl:                  "",
		attributes: []Attribute{
			NewDefaultStringAttribute(),
		},
		castUnsupportedAttrValueTypesToString: true,
		preferCastingToOthers:                 true,
	}

	var testcases = []struct {
		numberOfInserts               int
		numberOfFieldUpdatesFrequency int
		expected                      int
	}{
		{1000, 1, 991},
		{1000, 2, 496},
		{1000, 3, 331},
		{1000, 10, 100},
		{1000, 100, 10},
		{1000, 1000, 1},
	}
	for _, tc := range testcases {
		const tableName = "tableName"
		table := &Table{
			Name: tableName,
			Cols: map[string]*Column{},
		}
		fieldsMap := concurrent.NewMapWith(tableName, table)
		lm := NewLogManager(fieldsMap, &config.QuesmaConfiguration{})

		rowsToInsert := make([]string, 0)
		previousRow := ``
		comma := ``
		fieldIndex := 0
		for i := range tc.numberOfInserts {
			if i > 0 {
				comma = ","
			}

			if i%tc.numberOfFieldUpdatesFrequency == 0 {
				fieldIndex += 1
			}
			currentRow := previousRow + comma + `"Test` + strconv.Itoa(fieldIndex) + `":` + strconv.Itoa(fieldIndex)
			rowsToInsert = append(rowsToInsert, `{`+currentRow+`}`)
			previousRow = currentRow
		}

		assert.Equal(t, int64(0), lm.ingestCounter)
		for i := range rowsToInsert {
			_, _, _, _, err := lm.BuildIngestSQLStatements(table, types.MustJSON(rowsToInsert[i]), nil, chConfig)
			assert.NoError(t, err)
		}
		assert.Equal(t, tc.expected, len(table.Cols))
	}
}
