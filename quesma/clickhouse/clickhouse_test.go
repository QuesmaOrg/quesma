// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package clickhouse

import (
	"context"
	"github.com/QuesmaOrg/quesma/quesma/config"
	schema2 "github.com/QuesmaOrg/quesma/quesma/schema"
	"github.com/QuesmaOrg/quesma/quesma/types"
	"github.com/QuesmaOrg/quesma/quesma/util"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
)

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
		HasTimestamp:         true,
		TimestampDefaultsNow: true,
		Engine:               "MergeTree",
		OrderBy:              "(timestamp)",
		PartitionBy:          "",
		PrimaryKey:           "",
		Ttl:                  "",
		Attributes: []Attribute{
			NewDefaultInt64Attribute(),
			NewDefaultFloat64Attribute(),
			NewDefaultBoolAttribute(),
			NewDefaultStringAttribute(),
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
	config := &ChTableConfig{
		HasTimestamp:         true,
		TimestampDefaultsNow: true,
		Engine:               "MergeTree",
		OrderBy:              "(timestamp)",
		PartitionBy:          "",
		PrimaryKey:           "",
		Ttl:                  "",
		Attributes: []Attribute{
			NewDefaultStringAttribute(),
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
	table := Table{
		Created: false,
		Name:    "/_bulk?refresh=false&_source_includes=originId&require_alias=true_16",
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
			HasTimestamp:         true,
			TimestampDefaultsNow: true,
			Engine:               "MergeTree",
			OrderBy:              "(@timestamp)",
			PartitionBy:          "",
			PrimaryKey:           "",
			Ttl:                  "",
			Attributes: []Attribute{
				NewDefaultInt64Attribute(),
				NewDefaultStringAttribute(),
				NewDefaultBoolAttribute(),
			},
			CastUnsupportedAttrValueTypesToString: false,
			PreferCastingToOthers:                 false,
		},
		Indexes: []IndexStatement{
			GetIndexStatement("body"),
			GetIndexStatement("severity"),
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
	table := Table{
		Created: false,
		Name:    "abc",
		Cols: map[string]*Column{
			"low_card_string": {
				Name:    "low_card_string",
				Type:    NewBaseType("LowCardinality(String)"),
				Comment: "some comment 1",
			},
			"uuid": {
				Name: "uuid",
				Type: NewBaseType("UUID"),
			},
			"int32": {
				Name:    "int32",
				Type:    NewBaseType("Int32"),
				Comment: "some comment 2",
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
			HasTimestamp:         true,
			TimestampDefaultsNow: true,
			Engine:               "MergeTree",
			OrderBy:              "(@timestamp)",
			PartitionBy:          "",
			PrimaryKey:           "",
			Ttl:                  "",
			Attributes: []Attribute{
				NewDefaultInt64Attribute(),
			},
			CastUnsupportedAttrValueTypesToString: true,
			PreferCastingToOthers:                 true,
		},
	}
	expectedRows := []string{
		`CREATE TABLE IF NOT EXISTS "abc" (`,
		`"int32" Int32 COMMENT 'some comment 2',`,
		`"low_card_string" LowCardinality(String) COMMENT 'some comment 1',`,
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
			predefinedTables: *util.NewSyncMap[string, *Table](),
			tableNamePattern: "table",
			found:            false,
		},
		{
			name:             "should find by name",
			predefinedTables: *util.NewSyncMapWith("table1", &Table{Name: "table1"}),
			tableNamePattern: "table1",
			found:            true,
		},
		{
			name:             "should not find by name",
			predefinedTables: *util.NewSyncMapWith("table1", &Table{Name: "table1"}),
			tableNamePattern: "foo",
			found:            false,
		},
		{
			name:             "should find by pattern",
			predefinedTables: *util.NewSyncMapWith("logs-generic-default", &Table{Name: "logs-generic-default"}),
			tableNamePattern: "logs-generic-*",
			found:            true,
		},
		{
			name:             "should find by pattern",
			predefinedTables: *util.NewSyncMapWith("logs-generic-default", &Table{Name: "logs-generic-default"}),
			tableNamePattern: "*-*-*",
			found:            true,
		},
		{
			name:             "should find by pattern",
			predefinedTables: *util.NewSyncMapWith("logs-generic-default", &Table{Name: "logs-generic-default"}),
			tableNamePattern: "logs-*-default",
			found:            true,
		},
		{
			name:             "should find by pattern",
			predefinedTables: *util.NewSyncMapWith("logs-generic-default", &Table{Name: "logs-generic-default"}),
			tableNamePattern: "*",
			found:            true,
		},
		{
			name:             "should not find by pattern",
			predefinedTables: *util.NewSyncMapWith("logs-generic-default", &Table{Name: "logs-generic-default"}),
			tableNamePattern: "foo-*",
			found:            false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var tableDefinitions = atomic.Pointer[TableMap]{}
			tableDefinitions.Store(&tt.predefinedTables)
			lm := NewLogManager(&tt.predefinedTables, &config.QuesmaConfiguration{})
			assert.Equalf(t, tt.found, lm.FindTable(tt.tableNamePattern) != nil, "GetTable(%v)", tt.tableNamePattern)
		})
	}
}

func TestLogManager_ResolveIndexes(t *testing.T) {
	tests := []struct {
		name     string
		tables   *TableMap
		patterns string
		resolved []string
	}{
		{
			name:     "empty table map, non-empty pattern",
			tables:   NewTableMap(),
			patterns: "table",
			resolved: []string{},
		},
		{
			name:     "empty table map, empty pattern",
			tables:   NewTableMap(),
			patterns: "table",
			resolved: []string{},
		},
		{
			name:     "non-empty table map, empty pattern",
			tables:   newTableMap("table1", "table2"),
			patterns: "",
			resolved: []string{"table1", "table2"},
		},
		{
			name:     "non-empty table map, _all pattern",
			tables:   newTableMap("table1", "table2"),
			patterns: "_all",
			resolved: []string{"table1", "table2"},
		},
		{
			name:     "non-empty table map, * pattern",
			tables:   newTableMap("table1", "table2"),
			patterns: "*",
			resolved: []string{"table1", "table2"},
		},
		{
			name:     "non-empty table map, *,* pattern",
			tables:   newTableMap("table1", "table2"),
			patterns: "*,*",
			resolved: []string{"table1", "table2"},
		},
		{
			name:     "non-empty table map, table* pattern",
			tables:   newTableMap("table1", "table2"),
			patterns: "table*",
			resolved: []string{"table1", "table2"},
		},
		{
			name:     "non-empty table map, table1,table2 pattern",
			tables:   newTableMap("table1", "table2"),
			patterns: "table1,table2",
			resolved: []string{"table1", "table2"},
		},
		{
			name:     "non-empty table map, table1 pattern",
			tables:   newTableMap("table1", "table2"),
			patterns: "table1",
			resolved: []string{"table1"},
		},
		{
			name:     "non-empty table map, table2 pattern",
			tables:   newTableMap("table1", "table2"),
			patterns: "table2",
			resolved: []string{"table2"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var tableDefinitions = atomic.Pointer[TableMap]{}
			schemaTables := make(map[schema2.IndexName]schema2.Schema)

			for _, name := range tt.tables.Keys() {
				schemaTables[schema2.IndexName(name)] = schema2.Schema{}
			}
			schemaRegistry := schema2.StaticRegistry{
				Tables: schemaTables,
			}
			tableDefinitions.Store(tt.tables)
			lm := &LogManager{tableDiscovery: NewTableDiscoveryWith(&config.QuesmaConfiguration{}, nil, *tt.tables)}
			indexes, err := lm.ResolveIndexPattern(context.Background(), &schemaRegistry, tt.patterns)
			assert.NoError(t, err)
			assert.Equalf(t, tt.resolved, indexes, tt.patterns, "ResolveIndexPattern(%v)", tt.patterns)
		})
	}
}

func newTableMap(tables ...string) *TableMap {
	newMap := util.NewSyncMap[string, *Table]()
	for _, table := range tables {
		newMap.Store(table, &Table{Name: table})
	}
	return newMap
}
