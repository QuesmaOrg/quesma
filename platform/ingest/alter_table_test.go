// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package ingest

import (
	"github.com/QuesmaOrg/quesma/platform/clickhouse"
	"github.com/QuesmaOrg/quesma/platform/config"
	"github.com/QuesmaOrg/quesma/platform/schema"
	"github.com/QuesmaOrg/quesma/platform/types"
	"github.com/QuesmaOrg/quesma/platform/util"
	"github.com/stretchr/testify/assert"
	"strconv"
	"testing"
)

func TestAlterTable(t *testing.T) {
	chConfig := &clickhouse.ChTableConfig{
		HasTimestamp:         true,
		TimestampDefaultsNow: true,
		Engine:               "MergeTree",
		OrderBy:              "(timestamp)",
		PrimaryKey:           "",
		Ttl:                  "",
		Attributes: []clickhouse.Attribute{
			clickhouse.NewDefaultStringAttribute(),
		},
		CastUnsupportedAttrValueTypesToString: true,
		PreferCastingToOthers:                 true,
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
	table := &clickhouse.Table{
		Name:   "tableName",
		Cols:   map[string]*clickhouse.Column{},
		Config: chConfig,
	}
	fieldsMap := util.NewSyncMapWith("tableName", table)

	encodings := make(map[schema.FieldEncodingKey]schema.EncodedFieldName)

	ip := newIngestProcessorWithEmptyTableMap(fieldsMap, &config.QuesmaConfiguration{})
	for i := range rowsToInsert {
		alter, onlySchemaFields, nonSchemaFields, err := ip.GenerateIngestContent(table, types.MustJSON(rowsToInsert[i]), nil, encodings)
		assert.NoError(t, err)
		insert, err := generateInsertJson(nonSchemaFields, onlySchemaFields)
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
	chConfig := &clickhouse.ChTableConfig{
		HasTimestamp:         true,
		TimestampDefaultsNow: true,
		Engine:               "MergeTree",
		OrderBy:              "(timestamp)",
		PrimaryKey:           "",
		Ttl:                  "",
		Attributes: []clickhouse.Attribute{
			clickhouse.NewDefaultStringAttribute(),
		},
		CastUnsupportedAttrValueTypesToString: true,
		PreferCastingToOthers:                 true,
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

	encodings := make(map[schema.FieldEncodingKey]schema.EncodedFieldName)

	for _, tc := range testcases {
		const tableName = "tableName"
		table := &clickhouse.Table{
			Name:   tableName,
			Cols:   map[string]*clickhouse.Column{},
			Config: chConfig,
		}
		fieldsMap := util.NewSyncMapWith(tableName, table)
		ip := newIngestProcessorWithEmptyTableMap(fieldsMap, &config.QuesmaConfiguration{})

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

		assert.Equal(t, int64(0), ip.ingestCounter)
		for i := range rowsToInsert {
			_, _, _, err := ip.GenerateIngestContent(table, types.MustJSON(rowsToInsert[i]), nil, encodings)
			assert.NoError(t, err)
		}
		assert.Equal(t, tc.expected, len(table.Cols))
	}
}
