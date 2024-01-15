package clickhouse

import (
	"encoding/json"
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
)

var hasOthersTrueConfig = ChTableConfig{
	false,
	false,
	"MergeTree",
	"",
	"",
	"",
	"",
	true,
}

// inserting row with 2 non-schema fields
// they are added to "others" column as JSON (one is nested)
func TestInsertNonSchemaFields_1(t *testing.T) {
	rowToInsert := `{"host_name":"hermes","message":"User password reset requested","service_name":"queue","non-schema2":"2","severity":"info","source":"azure","timestamp":"2024-01-08T18:56:08.454Z","non-schema1":{"a":"b"}}`
	var emptyMap map[string]SchemaMap
	fieldsMap := map[string]SchemaMap{
		"tableName": {
			"host_name":    nil,
			"message":      nil,
			"service_name": nil,
			"severity":     nil,
			"timestamp":    nil,
			"source":       nil,
		},
	}

	f := func(m1, m2 map[string]SchemaMap) {
		lm := NewLogManagerNoConnection(emptyMap, fieldsMap)
		j, err := lm.BuildInsertJson("tableName", rowToInsert, hasOthersTrueConfig)
		assert.NoError(t, err)
		m := make(SchemaMap)
		err = json.Unmarshal([]byte(j), &m)
		assert.NoError(t, err)
		nestedJson, ok := m["others"].(SchemaMap)
		assert.True(t, ok)
		assert.Equal(t, 2, len(nestedJson))
		_, ok = nestedJson["non-schema1"]
		assert.True(t, ok)
		_, ok = nestedJson["non-schema2"]
		assert.True(t, ok)
	}

	// both cases need to be OK
	f(emptyMap, fieldsMap)
	f(fieldsMap, emptyMap)
}

// inserting row with 0 non-schema fields, but support for it
func TestInsertNonSchemaFields_2(t *testing.T) {
	rowToInsert := `{"host_name":"hermes","message":"User password reset requested","service_name":"queue","severity":"info","source":"azure","timestamp":"2024-01-08T18:56:08.454Z"}`
	var emptyMap map[string]SchemaMap
	fieldsMap := map[string]SchemaMap{
		"tableName": {
			"host_name":    nil,
			"message":      nil,
			"service_name": nil,
			"severity":     nil,
			"timestamp":    nil,
			"source":       nil,
		},
	}

	f := func(m1, m2 map[string]SchemaMap) {
		lm := NewLogManagerNoConnection(emptyMap, fieldsMap)
		j, err := lm.BuildInsertJson("tableName", rowToInsert, hasOthersTrueConfig)
		assert.NoError(t, err)

		m := make(SchemaMap)
		err = json.Unmarshal([]byte(j), &m)
		assert.NoError(t, err)
		nestedJson, ok := m["others"].(SchemaMap)
		assert.True(t, ok)
		assert.Equal(t, 0, len(nestedJson))
	}

	// both cases need to be OK
	f(emptyMap, fieldsMap)
	f(fieldsMap, emptyMap)
}

func TestAddTimestamp(t *testing.T) {
	c := ChTableConfig{
		true,
		true,
		"MergeTree",
		"(timestamp)",
		"",
		"",
		"",
		false,
	}
	query, err := buildCreateTableQuery("tableName", `{"host_name":"hermes","message":"User password reset requested","service_name":"queue","severity":"info","source":"azure"}`, c)
	assert.NoError(t, err)
	assert.True(t, strings.Contains(query, timestampFieldName))
}

func TestJsonToFieldsMap(t *testing.T) {
	mExpected := SchemaMap{
		"host_name":    "hermes",
		"message":      "User password reset requested",
		"service_name": "queue",
		"severity":     "info",
		"source":       "azure",
		"timestamp":    "2024-01-08T18:56:08.454Z",
	}
	j := `{"host_name":"hermes","message":"User password reset requested","service_name":"queue","severity":"info","source":"azure","timestamp":"2024-01-08T18:56:08.454Z"}`
	m, err := JsonToFieldsMap(j)
	assert.NoError(t, err)
	assert.Equal(t, len(mExpected), len(m))
	for k, vExpected := range mExpected {
		v, ok := m[k]
		assert.True(t, ok)
		assert.Equal(t, vExpected, v)
	}
}

// When tests grow bigger, it's probably better to sparse those maps in following tests.
// But for now I leave it as is, as it makes it easier to debug.
func TestDifferenceMapSimple_1(t *testing.T) {
	m := SchemaMap{
		"message":      nil,
		"service_name": nil,
		"severity":     nil,
		"source":       nil,
		"timestamp":    nil,
	}
	mExpected := SchemaMap{
		"host_name":    nil,
		"message":      nil,
		"service_name": nil,
		"severity":     nil,
		"source":       nil,
		"timestamp":    nil,
	}
	mDiff := DifferenceMap(mExpected, m)
	assert.Equal(t, 0, len(mDiff))
}

// one extra field
func TestDifferenceMapSimple_2(t *testing.T) {
	m := SchemaMap{
		"host_name":    "a",
		"message":      "b",
		"service_name": "c",
		"severity":     "d",
		"source":       "e",
		"timestamp":    "f",
	}
	mExpected := SchemaMap{
		"message":      nil,
		"service_name": nil,
		"severity":     nil,
		"source":       nil,
		"timestamp":    nil,
	}
	mDiff := DifferenceMap(mExpected, m)
	assert.Equal(t, 1, len(mDiff))
	_, ok := mDiff["host_name"]
	assert.True(t, ok)
}

func TestDifferenceMapNested(t *testing.T) {
	m := SchemaMap{
		"host_name": SchemaMap{
			"a": nil,
		},
		"message":      nil,
		"service_name": nil,
		"severity":     nil,
		"source":       nil,
		"timestamp":    nil,
	}
	mExpected := SchemaMap{
		"message":      nil,
		"service_name": nil,
		"severity":     nil,
		"source":       nil,
		"timestamp":    nil,
	}
	mDiff := DifferenceMap(mExpected, m)
	assert.Equal(t, 1, len(mDiff))
	mNested := mDiff["host_name"].(SchemaMap)
	_, ok := mNested["a"]
	assert.True(t, ok)
	assert.Equal(t, 1, len(mNested))
}

func TestDifferenceMapSimpleAndNested_1(t *testing.T) {
	m := SchemaMap{
		"host_name": SchemaMap{
			"a": SchemaMap{
				"b": nil,
			},
		},
		"message":      nil,
		"service_name": nil,
		"severity":     nil,
		"source":       nil,
		"timestamp":    nil,
		"non-schema":   nil,
	}
	mExpected := SchemaMap{
		"message":      nil,
		"service_name": nil,
		"severity":     nil,
		"source":       nil,
		"timestamp":    nil,
	}
	mDiff := DifferenceMap(mExpected, m)
	assert.Equal(t, 2, len(mDiff))
	mNested := mDiff["host_name"].(SchemaMap)
	assert.Equal(t, 1, len(mNested))
	mNestedLvl2, ok := mNested["a"].(SchemaMap)
	assert.True(t, ok)
	_, ok = mNestedLvl2["b"]
	assert.True(t, ok)
	assert.Equal(t, 1, len(mNestedLvl2))
}

func TestDifferenceMapSimpleAndNested_2(t *testing.T) {
	m := SchemaMap{
		"host_name": SchemaMap{
			"a": SchemaMap{
				"b": nil,
			},
			"b": nil,
		},
		"message":      nil,
		"service_name": nil,
		"severity":     nil,
		"source":       nil,
		"timestamp":    nil,
		"non-schema":   nil,
	}
	mExpected := SchemaMap{
		"host_name": SchemaMap{
			"b": nil,
		},
		"message":      nil,
		"service_name": nil,
		"severity":     nil,
		"source":       nil,
		"timestamp":    nil,
	}
	mDiff := DifferenceMap(mExpected, m)
	assert.Equal(t, 2, len(mDiff))
	mNested := mDiff["host_name"].(SchemaMap)
	assert.Equal(t, 1, len(mNested))
	mNestedLvl2, ok := mNested["a"].(SchemaMap)
	assert.True(t, ok)
	_, ok = mNestedLvl2["b"]
	assert.True(t, ok)
	assert.Equal(t, 1, len(mNestedLvl2))
}

func TestDifferenceMapBig(t *testing.T) {
	m := SchemaMap{
		"host_name": SchemaMap{
			"a": SchemaMap{
				"b": nil,
			},
			"b": nil,
			"c": nil,
		},
		"message":      nil,
		"service_name": nil,
		"severity":     nil,
		"source":       nil,
		"timestamp":    nil,
		"non-schema": SchemaMap{
			"a": SchemaMap{
				"b": nil,
			},
			"c": nil,
		},
		"nested": SchemaMap{
			"n1": SchemaMap{
				"n11": SchemaMap{
					"n111": nil,
					"n112": nil,
				},
				"n12": nil,
			},
			"n2": SchemaMap{
				"n21": nil,
				"n22": SchemaMap{
					"n221": SchemaMap{
						"m2221": nil,
					},
				},
			},
		},
	}
	mExpected := SchemaMap{
		"host_name": SchemaMap{
			"b": nil,
		},
		"message": SchemaMap{
			"m": nil,
		},
		"service_name": nil,
		"severity":     nil,
		"source":       nil,
		"timestamp":    nil,
		"nested": SchemaMap{
			"n1": SchemaMap{
				"n11": SchemaMap{
					"n111": nil,
				},
				"n12": nil,
			},
			"n2": SchemaMap{
				"n21": nil,
			},
		},
	}
	mDiff := DifferenceMap(mExpected, m)

	assert.Equal(t, 3, len(mDiff))
	mNested := mDiff["host_name"].(SchemaMap)
	assert.Equal(t, 2, len(mNested))
	mNestedLvl2, ok := mNested["a"].(SchemaMap)
	assert.True(t, ok)
	_, ok = mNestedLvl2["b"]
	assert.True(t, ok)
	assert.Equal(t, 1, len(mNestedLvl2))

	mNested = mDiff["non-schema"].(SchemaMap)
	assert.Equal(t, 2, len(mNested))
	assert.Nil(t, mNested["c"])
	mNested = mNested["a"].(SchemaMap)
	assert.Equal(t, 1, len(mNested))
	assert.Nil(t, mNested["b"])

	mNested = mDiff["nested"].(SchemaMap)
	assert.Equal(t, 2, len(mNested))
	mNested2 := mNested["n1"].(SchemaMap)
	assert.Equal(t, 1, len(mNested2))
	mNested3 := mNested2["n11"].(SchemaMap)
	assert.Equal(t, 1, len(mNested3))
	assert.Nil(t, mNested3["n112"])
	mNested2 = mNested["n2"].(SchemaMap)
	assert.Equal(t, 1, len(mNested2))
	mNested3 = mNested2["n22"].(SchemaMap)
	assert.Equal(t, 1, len(mNested3))
	mNested4 := mNested3["n221"].(SchemaMap)
	assert.Equal(t, 1, len(mNested4))
	assert.Nil(t, mNested4["2221"])
}

func TestRemovingNonSchemaFields(t *testing.T) {
	insertQueryMap := SchemaMap{
		"schema1": SchemaMap{
			"schema11": SchemaMap{
				"schema111": nil,
			},
			"non-schema12": SchemaMap{
				"non-schema111": nil,
			},
		},
		"schema2": SchemaMap{
			"schema21": SchemaMap{
				"non-schema211": nil,
				"non-schema212": SchemaMap{
					"non-schema2121": nil,
				},
				"schema211": nil,
				"schema212": SchemaMap{
					"non-schema2121": nil,
					"schema2121":     nil,
				},
			},
			"schema22": SchemaMap{
				"schema221":     nil,
				"non-schema221": nil,
			},
		},
		"non-schema1": SchemaMap{
			"non-schema11": SchemaMap{
				"non-schema111": nil,
			},
		},
		"non-schema2": nil,
	}
	schemaMap := SchemaMap{
		"schema1": SchemaMap{
			"schema11": SchemaMap{
				"schema111": nil,
			},
		},
		"schema2": SchemaMap{
			"schema21": SchemaMap{
				"schema211": nil,
				"schema212": SchemaMap{
					"schema2121": nil,
				},
			},
			"schema22": SchemaMap{
				"schema221": nil,
			},
		},
	}

	afterRemovalMap := RemoveNonSchemaFields(schemaMap, insertQueryMap)
	assert.Equal(t, 2, len(afterRemovalMap))
	nestedMap, ok := afterRemovalMap["schema1"].(SchemaMap)
	assert.True(t, ok)
	assert.Equal(t, 1, len(nestedMap))
	nestedMap2, ok := nestedMap["schema11"].(SchemaMap)
	assert.True(t, ok)
	assert.Equal(t, 1, len(nestedMap2))
	assert.Nil(t, nestedMap2["schema111"])

	nestedMap, ok = afterRemovalMap["schema2"].(SchemaMap)
	assert.True(t, ok)
	assert.Equal(t, 2, len(nestedMap))
	nestedMap2, ok = nestedMap["schema21"].(SchemaMap)
	assert.True(t, ok)
	assert.Equal(t, 2, len(nestedMap2))
	assert.Nil(t, nestedMap2["211"])
	nestedMap3, ok := nestedMap2["schema212"].(SchemaMap)
	assert.True(t, ok)
	assert.Equal(t, 1, len(nestedMap3))
	assert.Nil(t, nestedMap3["schema2121"])

	nestedMap2, ok = nestedMap["schema22"].(SchemaMap)
	assert.True(t, ok)
	assert.Equal(t, 1, len(nestedMap2))
	assert.Nil(t, nestedMap2["schema221"])
}

/*
Some manual testcases:
You can send those JSONs the same way it's done in log-generator/logger.go
'{"schema1":{"schema11":{"schema111":"1"}},"schema2":{"schema21":{"schema211":"2","schema212":{"schema2121":"3"}},"schema22":{"schema221":"4"}}}'
'{"schema1":{"schema11":{"schema111":"2"},"non-schema12":{"non-schema111":"2"}},"schema2":{"schema21":{"non-schema211":"3","non-schema212":{"non-schema2121":"4"},"schema211":"5","schema212":{"non-schema2121":"6","schema2121":"7"}},"schema22":{"schema221":"8","non-schema221":"9"}},"non-schema1":{"non-schema11":{"non-schema111":"10"}},"non-schema2":"11"}'

'{"message":"m","service_name":"s","severity":"s","source":"s"}'
'{"message":"m","service_name":"s","host_name":"h","os":"o","severity":"s","source":"s"}'
*/
