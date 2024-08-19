// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package clickhouse

import (
	"github.com/stretchr/testify/assert"
	"quesma/concurrent"
	"quesma/quesma/config"
	"quesma/quesma/types"
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
	rowsToInsert := []string{`{"Test1":1}`}
	expectedInsert := []string{"{\"attributes_string_key\":[],\"attributes_string_type\":[],\"attributes_string_value\":[],\"Test1\":1}"}
	alters := []string{"ALTER TABLE \"\" ADD COLUMN IF NOT EXISTS \"Test1\" Int64"}
	fieldsMap := concurrent.NewMapWith("tableName", &Table{
		Cols: map[string]*Column{},
	})

	lm := NewLogManager(fieldsMap, config.QuesmaConfiguration{})
	for i := range rowsToInsert {
		insert, alter, err := lm.BuildIngestSQLStatements("tableName", types.MustJSON(rowsToInsert[0]), nil, chConfig, true)
		assert.Equal(t, expectedInsert[i], insert)
		assert.Equal(t, alters, alter)
		assert.NoError(t, err)
	}
}
