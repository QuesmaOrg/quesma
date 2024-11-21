// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package ingest

import (
	"encoding/json"
	"quesma/clickhouse"
	"quesma/persistence"
	"quesma/quesma/config"
	"quesma/quesma/types"
	"quesma/schema"
	"quesma/telemetry"
	"quesma/util"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
)

func newIngestProcessorWithEmptyTableMap(tables *TableMap, cfg *config.QuesmaConfiguration) *IngestProcessor {
	var tableDefinitions = atomic.Pointer[TableMap]{}
	tableDefinitions.Store(tables)
	return &IngestProcessor{chDb: nil, tableDiscovery: clickhouse.NewTableDiscoveryWith(cfg, nil, *tables),
		cfg: cfg, phoneHomeAgent: telemetry.NewPhoneHomeEmptyAgent(),
		ingestFieldStatistics: make(IngestFieldStatistics),
		virtualTableStorage:   persistence.NewStaticJSONDatabase(),
	}
}

func newIngestProcessorEmpty() *IngestProcessor {
	var tableDefinitions = atomic.Pointer[TableMap]{}
	tableDefinitions.Store(NewTableMap())
	cfg := &config.QuesmaConfiguration{}
	return &IngestProcessor{tableDiscovery: clickhouse.NewTableDiscovery(cfg, nil, persistence.NewStaticJSONDatabase()), cfg: cfg,
		phoneHomeAgent: telemetry.NewPhoneHomeEmptyAgent(), ingestFieldStatistics: make(IngestFieldStatistics)}
}

var hasOthersConfig = &clickhouse.ChTableConfig{
	HasTimestamp:                          false,
	TimestampDefaultsNow:                  false,
	Engine:                                "MergeTree",
	OrderBy:                               "(timestamp)",
	PartitionBy:                           "",
	PrimaryKey:                            "",
	Ttl:                                   "",
	Attributes:                            []clickhouse.Attribute{},
	CastUnsupportedAttrValueTypesToString: false,
	PreferCastingToOthers:                 false,
}

// inserting row with 2 non-schema fields
// they are added to "others" clickhouse.Column as JSON (one is nested)
func TestInsertNonSchemaFieldsToOthers_1(t *testing.T) {
	rowToInsert := `{"host.name":"hermes","message":"User password reset requested","service.name":"queue","non-schema2":"2","severity":"info","source":"azure","timestamp":"2024-01-08T18:56:08.454Z","non-schema1":{"a":"b"}}`
	var emptyMap TableMap
	// TODO fix clickhouse.Columns
	fieldsMap := util.NewSyncMapWith("tableName", &clickhouse.Table{
		Cols: map[string]*clickhouse.Column{
			"host::name":    nil,
			"message":       nil,
			"service::name": nil,
			"severity":      nil,
			"timestamp":     nil,
			"source":        nil,
		},
	})

	encodings := make(map[schema.FieldEncodingKey]schema.EncodedFieldName)

	tableName, exists := fieldsMap.Load("tableName")
	assert.True(t, exists)
	f := func(t1, t2 TableMap) {
		ip := newIngestProcessorWithEmptyTableMap(fieldsMap, &config.QuesmaConfiguration{})
		alter, onlySchemaFields, nonSchemaFields, err := ip.GenerateIngestContent(tableName, types.MustJSON(rowToInsert), nil, hasOthersConfig, encodings)
		assert.NoError(t, err)
		j, err := generateInsertJson(nonSchemaFields, onlySchemaFields)
		assert.NoError(t, err)
		assert.Equal(t, 0, len(alter))
		m := make(SchemaMap)
		err = json.Unmarshal([]byte(j), &m)
		assert.NoError(t, err)
	}

	// both cases need to be OK
	f(emptyMap, *fieldsMap)
	f(*fieldsMap, emptyMap)
}

// TODO update this test now it doesn't do many useful things
/*
// inserting row with 0 non-schema fields, but support for it
func TestInsertNonSchemaFields_2(t *testing.T) {
	rowToInsert := `{"host.name":"hermes","message":"User password reset requested","service.name":"queue","severity":"info","source":"azure","timestamp":"2024-01-08T18:56:08.454Z"}`
	var emptyMap TableMap
	// TODO fix clickhouse.Columns
	fieldsMap := TableMap{
		"tableName": &clickhouse.Table{
			Cols: map[string]*clickhouse.Column{
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
	tableConfig := &clickhouse.ChTableConfig{
		HasTimestamp:                          true,
		TimestampDefaultsNow:                  true,
		Engine:                                "MergeTree",
		OrderBy:                               "(@timestamp)",
		PartitionBy:                           "",
		PrimaryKey:                            "",
		Ttl:                                   "",
		Attributes:                            []clickhouse.Attribute{},
		CastUnsupportedAttrValueTypesToString: false,
		PreferCastingToOthers:                 false,
	}
	nameFormatter := DefaultColumnNameFormatter()
	ip := newIngestProcessorEmpty()
	ip.schemaRegistry = &schema.StaticRegistry{}
	jsonData := types.MustJSON(`{"host.name":"hermes","message":"User password reset requested","service.name":"queue","severity":"info","source":"azure"}`)
	encodings := populateFieldEncodings([]types.JSON{jsonData}, tableName)

	ignoredFields := ip.getIgnoredFields(tableName)
	columnsFromJson := JsonToColumns("", jsonData, 1,
		tableConfig, nameFormatter, ignoredFields)

	columnsFromSchema := SchemaToColumns(findSchemaPointer(ip.schemaRegistry, tableName), nameFormatter, tableName, encodings)
	columns := columnsWithIndexes(columnsToString(columnsFromJson, columnsFromSchema, encodings, tableName), Indexes(jsonData))
	query := createTableQuery(tableName, columns, tableConfig)
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
	m, err := types.ParseJSON(j)
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
	table := &clickhouse.Table{
		Cols: map[string]*clickhouse.Column{
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
	table := &clickhouse.Table{
		Cols: map[string]*clickhouse.Column{
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
	table := &clickhouse.Table{
		Cols: map[string]*clickhouse.Column{
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
	table := &clickhouse.Table{
		Cols: map[string]*clickhouse.Column{
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
	table := &clickhouse.Table{
		Cols: map[string]*clickhouse.Column{
			"host.name": {Name: "host.name", Codec: clickhouse.Codec{Name: ""}, Type: clickhouse.MultiValueType{
				Name: "Tuple", Cols: []*clickhouse.Column{
					{Name: "b", Type: clickhouse.NewBaseType("String")},
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
	table := &clickhouse.Table{
		Cols: map[string]*clickhouse.Column{
			"host.name": {Name: "host.name", Type: clickhouse.MultiValueType{
				Name: "Tuple", Cols: []*clickhouse.Column{
					{Name: "b", Type: clickhouse.NewBaseType("String")},
				},
			}},
			"message": {Name: "message", Type: clickhouse.MultiValueType{
				Name: "Tuple", Cols: []*clickhouse.Column{
					{Name: "m", Type: clickhouse.NewBaseType("String")},
				},
			}},
			"service.name": nil,
			"severity":     nil,
			"timestamp":    nil,
			"source":       nil,
			"nested": {Name: "nested", Type: clickhouse.MultiValueType{
				Name: "Tuple", Cols: []*clickhouse.Column{
					{Name: "n1", Type: clickhouse.MultiValueType{
						Name: "Tuple", Cols: []*clickhouse.Column{
							{Name: "n11", Type: clickhouse.MultiValueType{
								Name: "Tuple", Cols: []*clickhouse.Column{
									{Name: "n111", Type: clickhouse.NewBaseType("String")},
								},
							},
							},
							{Name: "n12", Type: clickhouse.NewBaseType("String")},
						},
					}},
					{Name: "n2", Type: clickhouse.MultiValueType{
						Name: "Tuple", Cols: []*clickhouse.Column{
							{Name: "n21", Type: clickhouse.NewBaseType("String")},
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
	table := &clickhouse.Table{
		Cols: map[string]*clickhouse.Column{
			"schema1": {Name: "schema1", Type: clickhouse.MultiValueType{
				Name: "Tuple", Cols: []*clickhouse.Column{
					{Name: "schema11", Type: clickhouse.MultiValueType{
						Name: "Tuple", Cols: []*clickhouse.Column{
							{Name: "schema111", Type: clickhouse.NewBaseType("String")},
						},
					}},
				},
			}},
			"schema2": {Name: "schema2", Type: clickhouse.MultiValueType{
				Name: "Tuple", Cols: []*clickhouse.Column{
					{Name: "schema21", Type: clickhouse.MultiValueType{
						Name: "Tuple", Cols: []*clickhouse.Column{
							{Name: "schema212", Type: clickhouse.MultiValueType{
								Name: "Tuple", Cols: []*clickhouse.Column{
									{Name: "schema2121", Type: clickhouse.NewBaseType("String")},
								},
							}},
							{Name: "schema211", Type: clickhouse.NewBaseType("String")},
						},
					}},
					{Name: "schema22", Type: clickhouse.MultiValueType{
						Name: "Tuple", Cols: []*clickhouse.Column{
							{Name: "schema221", Type: clickhouse.NewBaseType("String")},
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
	config := &clickhouse.ChTableConfig{
		HasTimestamp:         true,
		TimestampDefaultsNow: true,
		Engine:               "MergeTree",
		OrderBy:              "(@timestamp)",
		PartitionBy:          "",
		PrimaryKey:           "",
		Ttl:                  "",
		Attributes: []clickhouse.Attribute{
			clickhouse.NewDefaultInt64Attribute(),
			clickhouse.NewDefaultFloat64Attribute(),
			clickhouse.NewDefaultBoolAttribute(),
			clickhouse.NewDefaultStringAttribute(),
		},
		CastUnsupportedAttrValueTypesToString: true,
		PreferCastingToOthers:                 true,
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
	attrs, err := BuildAttrsMap(m, config)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(attrs))
	for k := range attrs {
		assert.Contains(t, k, "string")
	}
}

func TestJsonConvertingBoolToStringAttr(t *testing.T) {
	config := &clickhouse.ChTableConfig{
		HasTimestamp:         true,
		TimestampDefaultsNow: true,
		Engine:               "MergeTree",
		OrderBy:              "(@timestamp)",
		PartitionBy:          "",
		PrimaryKey:           "",
		Ttl:                  "",
		Attributes: []clickhouse.Attribute{
			clickhouse.NewDefaultStringAttribute(),
		},
		CastUnsupportedAttrValueTypesToString: true,
		PreferCastingToOthers:                 true,
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

	attrs, err := BuildAttrsMap(m, config)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(attrs))
	for k := range attrs {
		assert.Contains(t, k, "string")
	}
}

// Doesn't test for 100% equality, as map iteration order isn't deterministic, but should definitely be good enough.
func TestCreateTableString_1(t *testing.T) {
	table := clickhouse.Table{
		Created: false,
		Name:    "/_bulk?refresh=false&_source_includes=originId&require_alias=true_16",
		Cols: map[string]*clickhouse.Column{
			"doc": {
				Name: "doc",
				Type: clickhouse.MultiValueType{
					Name: "Tuple",
					Cols: []*clickhouse.Column{
						{
							Name: "Tuple",
							Type: clickhouse.MultiValueType{
								Name: "Tuple",
								Cols: []*clickhouse.Column{
									{
										Name: "runAt",
										Type: clickhouse.NewBaseType("DateTime64"),
									},
									{
										Name: "startedAt",
										Type: clickhouse.NewBaseType("DateTime64"),
									},
									{
										Name: "Tuple",
										Type: clickhouse.NewBaseType("String"),
									},
									{
										Name: "status",
										Type: clickhouse.NewBaseType("String"),
									},
								},
							},
						},
						{
							Name: "updated_at",
							Type: clickhouse.NewBaseType("DateTime64"),
						},
					},
				},
			},
			"@timestamp": {
				Name: "@timestamp",
				Type: clickhouse.NewBaseType("DateTime64"),
			},
		},
		Config: &clickhouse.ChTableConfig{
			HasTimestamp:         true,
			TimestampDefaultsNow: true,
			Engine:               "MergeTree",
			OrderBy:              "(@timestamp)",
			PartitionBy:          "",
			PrimaryKey:           "",
			Ttl:                  "",
			Attributes: []clickhouse.Attribute{
				clickhouse.NewDefaultInt64Attribute(),
				clickhouse.NewDefaultStringAttribute(),
				clickhouse.NewDefaultBoolAttribute(),
			},
			CastUnsupportedAttrValueTypesToString: false,
			PreferCastingToOthers:                 false,
		},
		Indexes: []clickhouse.IndexStatement{
			clickhouse.GetIndexStatement("body"),
			clickhouse.GetIndexStatement("severity"),
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
		`"attributes_values" Map(String,String),`,
		`"attributes_metadata" Map(String,String),`,
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
	table := clickhouse.Table{
		Created: false,
		Name:    "abc",
		Cols: map[string]*clickhouse.Column{
			"low_card_string": {
				Name: "low_card_string",
				Type: clickhouse.NewBaseType("LowCardinality(String)"),
			},
			"uuid": {
				Name: "uuid",
				Type: clickhouse.NewBaseType("UUID"),
			},
			"int32": {
				Name: "int32",
				Type: clickhouse.NewBaseType("Int32"),
			},
			"epoch_time": {
				Name:      "epoch_time",
				Type:      clickhouse.NewBaseType("DateTime('Asia/Kolkata')"),
				Modifiers: "CODEC(DoubleDelta, LZ4)",
			},
			"estimated_connection_speedinkbps": {
				Name:      "estimated_connection_speedinkbps",
				Type:      clickhouse.NewBaseType("Float64"),
				Modifiers: "CODEC(DoubleDelta, LZ4)",
			},
		},
		Config: &clickhouse.ChTableConfig{
			HasTimestamp:         true,
			TimestampDefaultsNow: true,
			Engine:               "MergeTree",
			OrderBy:              "(@timestamp)",
			PartitionBy:          "",
			PrimaryKey:           "",
			Ttl:                  "",
			Attributes: []clickhouse.Attribute{
				clickhouse.NewDefaultInt64Attribute(),
			},
			CastUnsupportedAttrValueTypesToString: true,
			PreferCastingToOthers:                 true,
		},
	}
	expectedRows := []string{
		`CREATE TABLE IF NOT EXISTS "abc" (`,
		`"int32" Int32,`,
		`"low_card_string" LowCardinality(String),`,
		`"uuid" UUID,`,
		`"others" JSON,`,
		`"attributes_int64_key" Array(String),`,
		`"attributes_int64_value" Array(Int64),`,
		`"attributes_values" Map(String,String),`,
		`"attributes_metadata" Map(String,String)`,
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

func TestLogManager_GetTable(t *testing.T) {
	tests := []struct {
		name             string
		predefinedTables TableMap
		tableNamePattern string
		found            bool
	}{
		{
			name:             "empty",
			predefinedTables: *util.NewSyncMap[string, *clickhouse.Table](),
			tableNamePattern: "table",
			found:            false,
		},
		{
			name:             "should find by name",
			predefinedTables: *util.NewSyncMapWith("table1", &clickhouse.Table{Name: "table1"}),
			tableNamePattern: "table1",
			found:            true,
		},
		{
			name:             "should not find by name",
			predefinedTables: *util.NewSyncMapWith("table1", &clickhouse.Table{Name: "table1"}),
			tableNamePattern: "foo",
			found:            false,
		},
		{
			name:             "should find by pattern",
			predefinedTables: *util.NewSyncMapWith("logs-generic-default", &clickhouse.Table{Name: "logs-generic-default"}),
			tableNamePattern: "logs-generic-*",
			found:            true,
		},
		{
			name:             "should find by pattern",
			predefinedTables: *util.NewSyncMapWith("logs-generic-default", &clickhouse.Table{Name: "logs-generic-default"}),
			tableNamePattern: "*-*-*",
			found:            true,
		},
		{
			name:             "should find by pattern",
			predefinedTables: *util.NewSyncMapWith("logs-generic-default", &clickhouse.Table{Name: "logs-generic-default"}),
			tableNamePattern: "logs-*-default",
			found:            true,
		},
		{
			name:             "should find by pattern",
			predefinedTables: *util.NewSyncMapWith("logs-generic-default", &clickhouse.Table{Name: "logs-generic-default"}),
			tableNamePattern: "*",
			found:            true,
		},
		{
			name:             "should not find by pattern",
			predefinedTables: *util.NewSyncMapWith("logs-generic-default", &clickhouse.Table{Name: "logs-generic-default"}),
			tableNamePattern: "foo-*",
			found:            false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var tableDefinitions = atomic.Pointer[TableMap]{}
			tableDefinitions.Store(&tt.predefinedTables)
			ip := newIngestProcessorWithEmptyTableMap(&tt.predefinedTables, &config.QuesmaConfiguration{})
			assert.Equalf(t, tt.found, ip.FindTable(tt.tableNamePattern) != nil, "GetTable(%v)", tt.tableNamePattern)
		})
	}
}
