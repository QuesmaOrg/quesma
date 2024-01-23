package clickhouse

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

var hasOthersConfig = &ChTableConfig{
	hasTimestamp:                          false,
	timestampDefaultsNow:                  false,
	engine:                                "MergeTree",
	orderBy:                               "(timestamp)",
	partitionBy:                           "",
	primaryKey:                            "",
	ttl:                                   "",
	hasOthers:                             true,
	attributes:                            []Attribute{},
	castUnsupportedAttrValueTypesToString: false,
	preferCastingToOthers:                 false,
}

// inserting row with 2 non-schema fields
// they are added to "others" column as JSON (one is nested)
func TestInsertNonSchemaFieldsToOthers_1(t *testing.T) {
	rowToInsert := `{"host.name":"hermes","message":"User password reset requested","service.name":"queue","non-schema2":"2","severity":"info","source":"azure","timestamp":"2024-01-08T18:56:08.454Z","non-schema1":{"a":"b"}}`
	var emptyMap TableMap
	// TODO fix columns
	fieldsMap := TableMap{
		"tableName": &Table{
			Cols: map[string]*Column{
				"host.name":    nil,
				"message":      nil,
				"service.name": nil,
				"severity":     nil,
				"timestamp":    nil,
				"source":       nil,
			},
		},
	}

	f := func(t1, t2 TableMap) {
		lm := NewLogManagerNoConnection(emptyMap, fieldsMap)
		j, err := lm.BuildInsertJson("tableName", rowToInsert, hasOthersConfig)
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

// TODO update this test now it doesn't do many useful things
/*
// inserting row with 0 non-schema fields, but support for it
func TestInsertNonSchemaFields_2(t *testing.T) {
	rowToInsert := `{"host.name":"hermes","message":"User password reset requested","service.name":"queue","severity":"info","source":"azure","timestamp":"2024-01-08T18:56:08.454Z"}`
	var emptyMap TableMap
	// TODO fix columns
	fieldsMap := TableMap{
		"tableName": &Table{
			Cols: map[string]*Column{
				"host.name":    nil,
				"message":      nil,
				"service.name": nil,
				"severity":     nil,
				"timestamp":    nil,
				"source":       nil,
			},
		},
	}

	f := func(t1, t2 TableMap) {
		lm := NewLogManagerNoConnection(emptyMap, fieldsMap)
		j, err := lm.BuildInsertJson("tableName", rowToInsert, hasOthersConfig)
		assert.NoError(t, err)
		fmt.Println(j)
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
*/

func TestAddTimestamp(t *testing.T) {
	config := &ChTableConfig{
		hasTimestamp:                          true,
		timestampDefaultsNow:                  true,
		engine:                                "MergeTree",
		orderBy:                               "(@timestamp)",
		partitionBy:                           "",
		primaryKey:                            "",
		ttl:                                   "",
		hasOthers:                             false,
		attributes:                            []Attribute{},
		castUnsupportedAttrValueTypesToString: false,
		preferCastingToOthers:                 false,
	}
	query, err := buildCreateTableQueryNoOurFields("tableName", `{"host.name":"hermes","message":"User password reset requested","service.name":"queue","severity":"info","source":"azure"}`, config)
	assert.NoError(t, err)
	assert.True(t, strings.Contains(query, timestampFieldName))
}

func TestJsonToFieldsMap(t *testing.T) {
	mExpected := SchemaMap{
		"host.name":    "hermes",
		"message":      "User password reset requested",
		"service.name": "queue",
		"severity":     "info",
		"source":       "azure",
		"timestamp":    "2024-01-08T18:56:08.454Z",
	}
	j := `{"host.name":"hermes","message":"User password reset requested","service.name":"queue","severity":"info","source":"azure","timestamp":"2024-01-08T18:56:08.454Z"}`
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
		"service.name": nil,
		"severity":     nil,
		"source":       nil,
		"timestamp":    nil,
	}
	table := &Table{
		Cols: map[string]*Column{
			"host.name":    nil,
			"message":      nil,
			"service.name": nil,
			"severity":     nil,
			"timestamp":    nil,
			"source":       nil,
		},
	}

	mDiff := DifferenceMap(m, table)
	assert.Equal(t, 0, len(mDiff))
}

// one extra field
func TestDifferenceMapSimple_2(t *testing.T) {
	m := SchemaMap{
		"host.name":    "a",
		"message":      "b",
		"service.name": "c",
		"severity":     "d",
		"source":       "e",
		"timestamp":    "f",
	}
	table := &Table{
		Cols: map[string]*Column{
			"message":      nil,
			"service.name": nil,
			"severity":     nil,
			"timestamp":    nil,
			"source":       nil,
		},
	}

	mDiff := DifferenceMap(m, table)
	assert.Equal(t, 1, len(mDiff))
	_, ok := mDiff["host.name"]
	assert.True(t, ok)
}

func TestDifferenceMapNested(t *testing.T) {
	m := SchemaMap{
		"host.name": SchemaMap{
			"a": nil,
		},
		"message":      nil,
		"service.name": nil,
		"severity":     nil,
		"source":       nil,
		"timestamp":    nil,
	}
	table := &Table{
		Cols: map[string]*Column{
			"message":      nil,
			"service.name": nil,
			"severity":     nil,
			"timestamp":    nil,
			"source":       nil,
		},
	}

	mDiff := DifferenceMap(m, table)
	assert.Equal(t, 1, len(mDiff))
	mNested := mDiff["host.name"].(SchemaMap)
	_, ok := mNested["a"]
	assert.True(t, ok)
	assert.Equal(t, 1, len(mNested))
}

func TestDifferenceMapSimpleAndNested_1(t *testing.T) {
	m := SchemaMap{
		"host.name": SchemaMap{
			"a": SchemaMap{
				"b": nil,
			},
		},
		"message":      nil,
		"service.name": nil,
		"severity":     nil,
		"source":       nil,
		"timestamp":    nil,
		"non-schema":   nil,
	}
	table := &Table{
		Cols: map[string]*Column{
			"message":      nil,
			"service.name": nil,
			"severity":     nil,
			"timestamp":    nil,
			"source":       nil,
		},
	}

	mDiff := DifferenceMap(m, table)
	assert.Equal(t, 2, len(mDiff))
	mNested := mDiff["host.name"].(SchemaMap)
	assert.Equal(t, 1, len(mNested))
	mNestedLvl2, ok := mNested["a"].(SchemaMap)
	assert.True(t, ok)
	_, ok = mNestedLvl2["b"]
	assert.True(t, ok)
	assert.Equal(t, 1, len(mNestedLvl2))
}

func TestDifferenceMapSimpleAndNested_2(t *testing.T) {
	m := SchemaMap{
		"host.name": SchemaMap{
			"a": SchemaMap{
				"b": nil,
			},
			"b": nil,
		},
		"message":      nil,
		"service.name": nil,
		"severity":     nil,
		"source":       nil,
		"timestamp":    nil,
		"non-schema":   nil,
	}
	table := &Table{
		Cols: map[string]*Column{
			"host.name": {Name: "host.name", Codec: Codec{Name: ""}, Type: MultiValueType{
				Name: "Tuple", Cols: []*Column{
					{Name: "b", Type: NewBaseType("String")},
				},
			}},
			"message":      nil,
			"service.name": nil,
			"severity":     nil,
			"timestamp":    nil,
			"source":       nil,
		},
	}

	mDiff := DifferenceMap(m, table)
	assert.Equal(t, 2, len(mDiff))
	mNested := mDiff["host.name"].(SchemaMap)
	assert.Equal(t, 1, len(mNested))
	mNestedLvl2, ok := mNested["a"].(SchemaMap)
	assert.True(t, ok)
	_, ok = mNestedLvl2["b"]
	assert.True(t, ok)
	assert.Equal(t, 1, len(mNestedLvl2))
}

func TestDifferenceMapBig(t *testing.T) {
	m := SchemaMap{
		"host.name": SchemaMap{
			"a": SchemaMap{
				"b": nil,
			},
			"b": nil,
			"c": nil,
		},
		"message":      nil,
		"service.name": nil,
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
	table := &Table{
		Cols: map[string]*Column{
			"host.name": {Name: "host.name", Type: MultiValueType{
				Name: "Tuple", Cols: []*Column{
					{Name: "b", Type: NewBaseType("String")},
				},
			}},
			"message": {Name: "message", Type: MultiValueType{
				Name: "Tuple", Cols: []*Column{
					{Name: "m", Type: NewBaseType("String")},
				},
			}},
			"service.name": nil,
			"severity":     nil,
			"timestamp":    nil,
			"source":       nil,
			"nested": {Name: "nested", Type: MultiValueType{
				Name: "Tuple", Cols: []*Column{
					{Name: "n1", Type: MultiValueType{
						Name: "Tuple", Cols: []*Column{
							{Name: "n11", Type: MultiValueType{
								Name: "Tuple", Cols: []*Column{
									{Name: "n111", Type: NewBaseType("String")},
								},
							},
							},
							{Name: "n12", Type: NewBaseType("String")},
						},
					}},
					{Name: "n2", Type: MultiValueType{
						Name: "Tuple", Cols: []*Column{
							{Name: "n21", Type: NewBaseType("String")},
						},
					}},
				},
			}},
		},
	}

	mDiff := DifferenceMap(m, table)

	assert.Equal(t, 3, len(mDiff))
	mNested := mDiff["host.name"].(SchemaMap)
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
	table := &Table{
		Cols: map[string]*Column{
			"schema1": {Name: "schema1", Type: MultiValueType{
				Name: "Tuple", Cols: []*Column{
					{Name: "schema11", Type: MultiValueType{
						Name: "Tuple", Cols: []*Column{
							{Name: "schema111", Type: NewBaseType("String")},
						},
					}},
				},
			}},
			"schema2": {Name: "schema2", Type: MultiValueType{
				Name: "Tuple", Cols: []*Column{
					{Name: "schema21", Type: MultiValueType{
						Name: "Tuple", Cols: []*Column{
							{Name: "schema212", Type: MultiValueType{
								Name: "Tuple", Cols: []*Column{
									{Name: "schema2121", Type: NewBaseType("String")},
								},
							}},
							{Name: "schema211", Type: NewBaseType("String")},
						},
					}},
					{Name: "schema22", Type: MultiValueType{
						Name: "Tuple", Cols: []*Column{
							{Name: "schema221", Type: NewBaseType("String")},
						},
					}},
				},
			}},
		},
	}

	afterRemovalMap := RemoveNonSchemaFields(insertQueryMap, table)
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

func TestJsonFlatteningToStringAttr(t *testing.T) {
	config := &ChTableConfig{
		hasTimestamp:         true,
		timestampDefaultsNow: true,
		engine:               "MergeTree",
		orderBy:              "(timestamp)",
		partitionBy:          "",
		primaryKey:           "",
		ttl:                  "",
		hasOthers:            false,
		attributes: []Attribute{
			NewDefaultInt64Attribute(),
			NewDefaultFloat64Attribute(),
			NewDefaultBoolAttribute(),
			NewDefaultStringAttribute(),
		},
		castUnsupportedAttrValueTypesToString: true,
		preferCastingToOthers:                 true,
	}
	m := SchemaMap{
		"host.name": SchemaMap{
			"a": SchemaMap{
				"b": nil,
			},
			"b": nil,
			"c": nil,
		},
	}
	attrs, others, err := BuildAttrsMapAndOthers(m, config)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(others))
	assert.Equal(t, 2, len(attrs))
	for k := range attrs {
		assert.Contains(t, k, "string")
	}
}

func TestJsonConvertingBoolToStringAttr(t *testing.T) {
	config := &ChTableConfig{
		hasTimestamp:         true,
		timestampDefaultsNow: true,
		engine:               "MergeTree",
		orderBy:              "(timestamp)",
		partitionBy:          "",
		primaryKey:           "",
		ttl:                  "",
		hasOthers:            false,
		attributes: []Attribute{
			NewDefaultStringAttribute(),
		},
		castUnsupportedAttrValueTypesToString: true,
		preferCastingToOthers:                 true,
	}
	m := SchemaMap{
		"b1": true,
		"b2": false,
		"b3": SchemaMap{
			"a": SchemaMap{
				"b": nil,
			},
			"b": nil,
			"c": nil,
		},
	}

	attrs, others, err := BuildAttrsMapAndOthers(m, config)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(others))
	assert.Equal(t, 2, len(attrs))
	for k := range attrs {
		assert.Contains(t, k, "string")
	}
}

func TestCreateTableString_1(t *testing.T) {
	table := Table{
		Created:  false,
		Name:     "abc",
		Database: "",
		Cluster:  "",
		Cols:     map[string]*Column{},
		Config: &ChTableConfig{
			hasTimestamp:                          false,
			timestampDefaultsNow:                  false,
			engine:                                "MergeTree",
			orderBy:                               "",
			partitionBy:                           "",
			primaryKey:                            "",
			ttl:                                   "toDateTime(epoch_time_original / 1000000000) + toIntervalSecond(1296000)",
			settings:                              "index_granularity = 8192, ttl_only_drop_parts = 1",
			hasOthers:                             true,
			attributes:                            nil,
			castUnsupportedAttrValueTypesToString: true,
			preferCastingToOthers:                 true,
		},
	}
	expected := `CREATE TABLE IF NOT EXISTS "abc" (
	"others" JSON
)
ENGINE = MergeTree
TTL toDateTime(epoch_time_original / 1000000000) + toIntervalSecond(1296000)
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1
`
	assert.Equal(t, expected, table.CreateTableString())
}

// Doesn't test for 100% equality, as map iteration order isn't deterministic, but should definitely be good enough.
func TestCreateTableString_2(t *testing.T) {
	table := Table{
		Created:  false,
		Name:     "/_bulk?refresh=false&_source_includes=originId&require_alias=true_16",
		Database: "",
		Cluster:  "",
		Cols: map[string]*Column{
			"doc": {
				Name: "doc",
				Type: MultiValueType{
					Name: "Tuple",
					Cols: []*Column{
						{
							Name: "Tuple",
							Type: MultiValueType{
								Name: "Tuple",
								Cols: []*Column{
									{
										Name: "runAt",
										Type: NewBaseType("DateTime64"),
									},
									{
										Name: "startedAt",
										Type: NewBaseType("DateTime64"),
									},
									{
										Name: "Tuple",
										Type: NewBaseType("String"),
									},
									{
										Name: "status",
										Type: NewBaseType("String"),
									},
								},
							},
						},
						{
							Name: "updated_at",
							Type: NewBaseType("DateTime64"),
						},
					},
				},
			},
			"@timestamp": {
				Name: "@timestamp",
				Type: NewBaseType("DateTime64"),
			},
		},
		Config: &ChTableConfig{
			hasTimestamp:         true,
			timestampDefaultsNow: true,
			engine:               "MergeTree",
			orderBy:              "(@timestamp)",
			partitionBy:          "",
			primaryKey:           "",
			ttl:                  "",
			hasOthers:            false,
			attributes: []Attribute{
				NewDefaultInt64Attribute(),
				NewDefaultStringAttribute(),
				NewDefaultBoolAttribute(),
			},
			castUnsupportedAttrValueTypesToString: false,
			preferCastingToOthers:                 false,
		},
		indexes: []IndexStatement{
			getIndexStatement("body"),
			getIndexStatement("severity"),
		},
	}
	expectedRows := []string{
		`CREATE TABLE IF NOT EXISTS "/_bulk?refresh=false&_source_includes=originId&require_alias=true_16" (`,
		`"doc" Tuple`,
		`(`,
		`"Tuple" Tuple`,
		`(`,
		`"runAt" DateTime64,`,
		`"startedAt" DateTime64,`,
		`"Tuple" String,`,
		`"status" String`,
		`),`,
		`"updated_at" DateTime64`,
		`),`,
		`"@timestamp" DateTime64,`,
		`"attributes_int64_key" Array(String),`,
		`"attributes_int64_value" Array(Int64),`,
		`"attributes_string_key" Array(String),`,
		`"attributes_string_value" Array(String),`,
		`"attributes_bool_key" Array(String),`,
		`"attributes_bool_value" Array(Bool),`,
		`INDEX body_idx body TYPE tokenbf_v1(10240, 3, 0) GRANULARITY 4,`,
		`INDEX severity_idx severity TYPE set(25) GRANULARITY 4`,
		`)`,
		`ENGINE = MergeTree`,
		`ORDER BY (@timestamp)`,
		"",
	}
	createTableString := table.CreateTableString()
	for _, row := range strings.Split(createTableString, "\n") {
		assert.Contains(t, expectedRows, strings.TrimSpace(row))
	}
}

// Doesn't test for 100% equality, as map iteration order isn't deterministic, but should definitely be good enough.
func TestCreateTableString_NewDateTypes(t *testing.T) {
	table := Table{
		Created:  false,
		Name:     "abc",
		Database: "",
		Cluster:  "",
		Cols: map[string]*Column{
			"low_card_string": {
				Name: "low_card_string",
				Type: NewBaseType("LowCardinality(String)"),
			},
			"uuid": {
				Name: "uuid",
				Type: NewBaseType("UUID"),
			},
			"int32": {
				Name: "int32",
				Type: NewBaseType("Int32"),
			},
			"epoch_time": {
				Name:      "epoch_time",
				Type:      NewBaseType("DateTime('Asia/Kolkata')"),
				Modifiers: "CODEC(DoubleDelta, LZ4)",
			},
			"estimated_connection_speedinkbps": {
				Name:      "estimated_connection_speedinkbps",
				Type:      NewBaseType("Float64"),
				Modifiers: "CODEC(DoubleDelta, LZ4)",
			},
		},
		Config: &ChTableConfig{
			hasTimestamp:         true,
			timestampDefaultsNow: true,
			engine:               "MergeTree",
			orderBy:              "(@timestamp)",
			partitionBy:          "",
			primaryKey:           "",
			ttl:                  "",
			hasOthers:            true,
			attributes: []Attribute{
				NewDefaultInt64Attribute(),
			},
			castUnsupportedAttrValueTypesToString: true,
			preferCastingToOthers:                 true,
		},
	}
	expectedRows := []string{
		`CREATE TABLE IF NOT EXISTS "abc" (`,
		`"int32" Int32,`,
		`"low_card_string" LowCardinality(String),`,
		`"uuid" UUID,`,
		`"others" JSON,`,
		`"attributes_int64_key" Array(String),`,
		`"attributes_int64_value" Array(Int64)`,
		`"@timestamp" DateTime64(3) DEFAULT now64(),`,
		`"epoch_time" DateTime('Asia/Kolkata') CODEC(DoubleDelta, LZ4),`,
		`"estimated_connection_speedinkbps" Float64 CODEC(DoubleDelta, LZ4),`,
		`ENGINE = MergeTree`,
		`)`,
		`ORDER BY (@timestamp)`,
		"",
	}
	createTableString := table.CreateTableString()
	for _, row := range strings.Split(createTableString, "\n") {
		assert.Contains(t, expectedRows, strings.TrimSpace(row))
	}
}

/*
Some manual testcases:
You can send those JSONs the same way it's done in log-generator/logger.go
'{"schema1":{"schema11":{"schema111":"1"}},"schema2":{"schema21":{"schema211":"2","schema212":{"schema2121":"3"}},"schema22":{"schema221":"4"}}}'
'{"schema1":{"schema11":{"schema111":"2"},"non-schema12":{"non-schema111":"2"}},"schema2":{"schema21":{"non-schema211":"3","non-schema212":{"non-schema2121":"4"},"schema211":"5","schema212":{"non-schema2121":"6","schema2121":"7"}},"schema22":{"schema221":"8","non-schema221":"9"}},"non-schema1":{"non-schema11":{"non-schema111":"10"}},"non-schema2":"11"}'

'{"message":"m","service.name":"s","severity":"s","source":"s"}'
'{"message":"m","service.name":"s","host.name":"h","os":"o","severity":"s","source":"s"}'
*/
