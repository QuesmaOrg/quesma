// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package ingest

import (
	"github.com/QuesmaOrg/quesma/platform/backend_connectors"
	"github.com/QuesmaOrg/quesma/platform/config"
	"github.com/QuesmaOrg/quesma/platform/database_common"
	"github.com/QuesmaOrg/quesma/platform/persistence"
	"github.com/QuesmaOrg/quesma/platform/schema"
	"github.com/QuesmaOrg/quesma/platform/types"
	"github.com/QuesmaOrg/quesma/platform/util"
	quesma_api "github.com/QuesmaOrg/quesma/platform/v2/core"
	"github.com/QuesmaOrg/quesma/platform/v2/core/diag"
	"github.com/goccy/go-json"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
)

func newIngestProcessorWithEmptyTableMap(tables *TableMap, cfg *config.QuesmaConfiguration) *IngestProcessor {
	var tableDefinitions = atomic.Pointer[TableMap]{}
	tableDefinitions.Store(tables)
	lowerer := NewSqlLowerer(persistence.NewStaticJSONDatabase())
	processor := &IngestProcessor{chDb: nil, tableDiscovery: database_common.NewTableDiscoveryWith(cfg, nil, *tables),
		cfg: cfg, phoneHomeClient: diag.NewPhoneHomeEmptyAgent(),
		lowerers: make(map[quesma_api.BackendConnectorType]Lowerer),
		lowerer:  lowerer,
	}
	processor.RegisterLowerer(lowerer, quesma_api.ClickHouseSQLBackend)
	return processor
}

func newIngestProcessorWithHydrolixLowerer(tables *TableMap, cfg *config.QuesmaConfiguration) *IngestProcessor {
	var tableDefinitions = atomic.Pointer[TableMap]{}
	tableDefinitions.Store(tables)
	lowerer := NewHydrolixLowerer(persistence.NewStaticJSONDatabase())
	processor := &IngestProcessor{chDb: backend_connectors.NewHydrolixBackendConnector(&cfg.Hydrolix), tableDiscovery: database_common.NewTableDiscoveryWith(cfg, nil, *tables),
		cfg: cfg, phoneHomeClient: diag.NewPhoneHomeEmptyAgent(),
		lowerers: make(map[quesma_api.BackendConnectorType]Lowerer),
	}
	processor.RegisterLowerer(lowerer, quesma_api.HydrolixSQLBackend)
	return processor
}

func newIngestProcessorEmpty() *IngestProcessor {
	var tableDefinitions = atomic.Pointer[TableMap]{}
	tableDefinitions.Store(NewTableMap())
	cfg := &config.QuesmaConfiguration{}
	lowerer := NewSqlLowerer(persistence.NewStaticJSONDatabase())
	processor := &IngestProcessor{tableDiscovery: database_common.NewTableDiscovery(cfg, nil, persistence.NewStaticJSONDatabase()), cfg: cfg,
		phoneHomeClient: diag.NewPhoneHomeEmptyAgent(), lowerers: make(map[quesma_api.BackendConnectorType]Lowerer), lowerer: lowerer}
	processor.RegisterLowerer(lowerer, quesma_api.ClickHouseSQLBackend)
	return processor
}

var hasOthersConfig = &database_common.ChTableConfig{
	HasTimestamp:                          false,
	TimestampDefaultsNow:                  false,
	Engine:                                "MergeTree",
	OrderBy:                               "(timestamp)",
	PrimaryKey:                            "",
	Ttl:                                   "",
	Attributes:                            []database_common.Attribute{},
	CastUnsupportedAttrValueTypesToString: false,
	PreferCastingToOthers:                 false,
}

// inserting row with 2 non-schema fields
// they are added to "others" database_common.Column as JSON (one is nested)
func TestInsertNonSchemaFieldsToOthers_1(t *testing.T) {
	rowToInsert := `{"host.name":"hermes","message":"User password reset requested","service.name":"queue","non-schema2":"2","severity":"info","source":"azure","timestamp":"2024-01-08T18:56:08.454Z","non-schema1":{"a":"b"}}`
	var emptyMap TableMap
	// TODO fix database_common.Columns
	fieldsMap := util.NewSyncMapWith("tableName", &database_common.Table{
		Cols: map[string]*database_common.Column{
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
	tableName.Config = hasOthersConfig
	assert.True(t, exists)
	f := func(t1, t2 TableMap) {
		ip := newIngestProcessorWithEmptyTableMap(fieldsMap, &config.QuesmaConfiguration{})
		alter, onlySchemaFields, nonSchemaFields, err := ip.lowerer.GenerateIngestContent(tableName, types.MustJSON(rowToInsert), nil, encodings)
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
	// TODO fix database_common.Columns
	fieldsMap := TableMap{
		"tableName": &database_common.Table{
			Cols: map[string]*database_common.Column{
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
	tableConfig := &database_common.ChTableConfig{
		HasTimestamp:                          true,
		TimestampDefaultsNow:                  true,
		Engine:                                "MergeTree",
		OrderBy:                               "(@timestamp)",
		PrimaryKey:                            "",
		Ttl:                                   "",
		Attributes:                            []database_common.Attribute{},
		CastUnsupportedAttrValueTypesToString: false,
		PreferCastingToOthers:                 false,
	}
	nameFormatter := DefaultColumnNameFormatter()
	ip := newIngestProcessorEmpty()
	ip.schemaRegistry = &schema.StaticRegistry{}
	jsonData := types.MustJSON(`{"host.name":"hermes","message":"User password reset requested","service.name":"queue","severity":"info","source":"azure"}`)
	encodings := populateFieldEncodings([]types.JSON{jsonData}, tableName)

	columnsFromJson := JsonToColumns(jsonData, tableConfig)

	columnsFromSchema := SchemaToColumns(findSchemaPointer(ip.schemaRegistry, tableName), nameFormatter, tableName, encodings)
	columns := columnsWithIndexes(columnPropertiesToString(columnsToProperties(columnsFromJson, columnsFromSchema, encodings, tableName)), Indexes(jsonData))
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
	table := &database_common.Table{
		Cols: map[string]*database_common.Column{
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
	table := &database_common.Table{
		Cols: map[string]*database_common.Column{
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
	table := &database_common.Table{
		Cols: map[string]*database_common.Column{
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
	table := &database_common.Table{
		Cols: map[string]*database_common.Column{
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
	table := &database_common.Table{
		Cols: map[string]*database_common.Column{
			"host.name": {Name: "host.name", Codec: database_common.Codec{Name: ""}, Type: database_common.MultiValueType{
				Name: "Tuple", Cols: []*database_common.Column{
					{Name: "b", Type: database_common.NewBaseType("String")},
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
	table := &database_common.Table{
		Cols: map[string]*database_common.Column{
			"host.name": {Name: "host.name", Type: database_common.MultiValueType{
				Name: "Tuple", Cols: []*database_common.Column{
					{Name: "b", Type: database_common.NewBaseType("String")},
				},
			}},
			"message": {Name: "message", Type: database_common.MultiValueType{
				Name: "Tuple", Cols: []*database_common.Column{
					{Name: "m", Type: database_common.NewBaseType("String")},
				},
			}},
			"service.name": nil,
			"severity":     nil,
			"timestamp":    nil,
			"source":       nil,
			"nested": {Name: "nested", Type: database_common.MultiValueType{
				Name: "Tuple", Cols: []*database_common.Column{
					{Name: "n1", Type: database_common.MultiValueType{
						Name: "Tuple", Cols: []*database_common.Column{
							{Name: "n11", Type: database_common.MultiValueType{
								Name: "Tuple", Cols: []*database_common.Column{
									{Name: "n111", Type: database_common.NewBaseType("String")},
								},
							},
							},
							{Name: "n12", Type: database_common.NewBaseType("String")},
						},
					}},
					{Name: "n2", Type: database_common.MultiValueType{
						Name: "Tuple", Cols: []*database_common.Column{
							{Name: "n21", Type: database_common.NewBaseType("String")},
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
	table := &database_common.Table{
		Cols: map[string]*database_common.Column{
			"schema1": {Name: "schema1", Type: database_common.MultiValueType{
				Name: "Tuple", Cols: []*database_common.Column{
					{Name: "schema11", Type: database_common.MultiValueType{
						Name: "Tuple", Cols: []*database_common.Column{
							{Name: "schema111", Type: database_common.NewBaseType("String")},
						},
					}},
				},
			}},
			"schema2": {Name: "schema2", Type: database_common.MultiValueType{
				Name: "Tuple", Cols: []*database_common.Column{
					{Name: "schema21", Type: database_common.MultiValueType{
						Name: "Tuple", Cols: []*database_common.Column{
							{Name: "schema212", Type: database_common.MultiValueType{
								Name: "Tuple", Cols: []*database_common.Column{
									{Name: "schema2121", Type: database_common.NewBaseType("String")},
								},
							}},
							{Name: "schema211", Type: database_common.NewBaseType("String")},
						},
					}},
					{Name: "schema22", Type: database_common.MultiValueType{
						Name: "Tuple", Cols: []*database_common.Column{
							{Name: "schema221", Type: database_common.NewBaseType("String")},
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
	config := &database_common.ChTableConfig{
		HasTimestamp:         true,
		TimestampDefaultsNow: true,
		Engine:               "MergeTree",
		OrderBy:              "(@timestamp)",
		PrimaryKey:           "",
		Ttl:                  "",
		Attributes: []database_common.Attribute{
			database_common.NewDefaultInt64Attribute(),
			database_common.NewDefaultFloat64Attribute(),
			database_common.NewDefaultBoolAttribute(),
			database_common.NewDefaultStringAttribute(),
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
	config := &database_common.ChTableConfig{
		HasTimestamp:         true,
		TimestampDefaultsNow: true,
		Engine:               "MergeTree",
		OrderBy:              "(@timestamp)",
		PrimaryKey:           "",
		Ttl:                  "",
		Attributes: []database_common.Attribute{
			database_common.NewDefaultStringAttribute(),
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
	table := database_common.Table{
		Name: "/_bulk?refresh=false&_source_includes=originId&require_alias=true_16",
		Cols: map[string]*database_common.Column{
			"doc": {
				Name: "doc",
				Type: database_common.MultiValueType{
					Name: "Tuple",
					Cols: []*database_common.Column{
						{
							Name: "Tuple",
							Type: database_common.MultiValueType{
								Name: "Tuple",
								Cols: []*database_common.Column{
									{
										Name: "runAt",
										Type: database_common.NewBaseType("DateTime64"),
									},
									{
										Name: "startedAt",
										Type: database_common.NewBaseType("DateTime64"),
									},
									{
										Name: "Tuple",
										Type: database_common.NewBaseType("String"),
									},
									{
										Name: "status",
										Type: database_common.NewBaseType("String"),
									},
								},
							},
						},
						{
							Name: "updated_at",
							Type: database_common.NewBaseType("DateTime64"),
						},
					},
				},
			},
			"@timestamp": {
				Name: "@timestamp",
				Type: database_common.NewBaseType("DateTime64"),
			},
		},
		Config: &database_common.ChTableConfig{
			HasTimestamp:         true,
			TimestampDefaultsNow: true,
			Engine:               "MergeTree",
			OrderBy:              "(@timestamp)",
			PrimaryKey:           "",
			Ttl:                  "",
			Attributes: []database_common.Attribute{
				database_common.NewDefaultStringAttribute(),
			},
			CastUnsupportedAttrValueTypesToString: false,
			PreferCastingToOthers:                 false,
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
		`"attributes_values" Map(String,String),`,
		`"attributes_metadata" Map(String,String)`,
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
	table := database_common.Table{
		Name: "abc",
		Cols: map[string]*database_common.Column{
			"low_card_string": {
				Name: "low_card_string",
				Type: database_common.NewBaseType("LowCardinality(String)"),
			},
			"uuid": {
				Name: "uuid",
				Type: database_common.NewBaseType("UUID"),
			},
			"int32": {
				Name: "int32",
				Type: database_common.NewBaseType("Int32"),
			},
			"epoch_time": {
				Name:      "epoch_time",
				Type:      database_common.NewBaseType("DateTime('Asia/Kolkata')"),
				Modifiers: "CODEC(DoubleDelta, LZ4)",
			},
			"estimated_connection_speedinkbps": {
				Name:      "estimated_connection_speedinkbps",
				Type:      database_common.NewBaseType("Float64"),
				Modifiers: "CODEC(DoubleDelta, LZ4)",
			},
		},
		Config: &database_common.ChTableConfig{
			HasTimestamp:         true,
			TimestampDefaultsNow: true,
			Engine:               "MergeTree",
			OrderBy:              "(@timestamp)",
			PrimaryKey:           "",
			Ttl:                  "",
			Attributes: []database_common.Attribute{
				database_common.NewDefaultInt64Attribute(),
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
			predefinedTables: *util.NewSyncMap[string, *database_common.Table](),
			tableNamePattern: "table",
			found:            false,
		},
		{
			name:             "should find by name",
			predefinedTables: *util.NewSyncMapWith("table1", &database_common.Table{Name: "table1"}),
			tableNamePattern: "table1",
			found:            true,
		},
		{
			name:             "should not find by name",
			predefinedTables: *util.NewSyncMapWith("table1", &database_common.Table{Name: "table1"}),
			tableNamePattern: "foo",
			found:            false,
		},
		{
			name:             "should find by pattern",
			predefinedTables: *util.NewSyncMapWith("logs-generic-default", &database_common.Table{Name: "logs-generic-default"}),
			tableNamePattern: "logs-generic-*",
			found:            true,
		},
		{
			name:             "should find by pattern",
			predefinedTables: *util.NewSyncMapWith("logs-generic-default", &database_common.Table{Name: "logs-generic-default"}),
			tableNamePattern: "*-*-*",
			found:            true,
		},
		{
			name:             "should find by pattern",
			predefinedTables: *util.NewSyncMapWith("logs-generic-default", &database_common.Table{Name: "logs-generic-default"}),
			tableNamePattern: "logs-*-default",
			found:            true,
		},
		{
			name:             "should find by pattern",
			predefinedTables: *util.NewSyncMapWith("logs-generic-default", &database_common.Table{Name: "logs-generic-default"}),
			tableNamePattern: "*",
			found:            true,
		},
		{
			name:             "should not find by pattern",
			predefinedTables: *util.NewSyncMapWith("logs-generic-default", &database_common.Table{Name: "logs-generic-default"}),
			tableNamePattern: "foo-*",
			found:            false,
		},
	}
	for i, tt := range tests {
		t.Run(util.PrettyTestName(tt.name, i), func(t *testing.T) {
			var tableDefinitions = atomic.Pointer[TableMap]{}
			tableDefinitions.Store(&tt.predefinedTables)
			ip := newIngestProcessorWithEmptyTableMap(&tt.predefinedTables, &config.QuesmaConfiguration{})
			assert.Equalf(t, tt.found, ip.FindTable(tt.tableNamePattern) != nil, "GetTable(%v)", tt.tableNamePattern)
		})
	}
}
