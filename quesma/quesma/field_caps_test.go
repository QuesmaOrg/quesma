package quesma

import (
	"encoding/json"
	"github.com/stretchr/testify/assert"
	"mitmproxy/quesma/clickhouse"
	"mitmproxy/quesma/concurrent"
	"mitmproxy/quesma/elasticsearch/elasticsearch_field_types"
	"mitmproxy/quesma/model"
	"mitmproxy/quesma/quesma/config"
	"mitmproxy/quesma/schema"
	"mitmproxy/quesma/util"
	"strconv"
	"testing"
)

func TestFieldCaps(t *testing.T) {
	table := &clickhouse.Table{
		Name: tableName,
		Cols: map[string]*clickhouse.Column{
			"service.name": {Name: "service.name", Type: clickhouse.BaseType{Name: "String"}},
			"host.name": {Name: "host.name", Type: clickhouse.MultiValueType{
				Name: "Tuple", Cols: []*clickhouse.Column{
					{Name: "b", Type: clickhouse.NewBaseType("String")},
				},
			}},
			"arrayOfTuples": {Name: "arrayOfTuples", Type: clickhouse.CompoundType{
				Name: "arrayOfTuples", BaseType: clickhouse.MultiValueType{
					Name: "Tuple", Cols: []*clickhouse.Column{
						{Name: "b", Type: clickhouse.NewBaseType("String")},
					},
				},
			}},
			"arrayOfArraysOfStrings": {Name: "arrayOfArraysOfStrings", Type: clickhouse.CompoundType{
				Name: "arrayOfStringsOfStrings", BaseType: clickhouse.CompoundType{
					Name: "b", BaseType: clickhouse.NewBaseType("String"),
				},
			}},
		},
	}
	expected := []byte(`{
  "fields": {
    "QUESMA_CLICKHOUSE_RESPONSE": {
      "text": {
        "aggregatable": false,
        "metadata_field": false,
        "searchable": true,
        "type": "text",
		"indices": ["logs-generic-default"]
      }
    },
    "arrayOfArraysOfStrings": {
      "keyword": {
        "aggregatable": true,
        "searchable": true,
        "type": "keyword",
		"indices": ["logs-generic-default"]
      }
    },
    "arrayOfTuples": {
      "object": {
        "aggregatable": false,
        "metadata_field": false,
        "searchable": true,
        "type": "object",
		"indices": ["logs-generic-default"]
      }
    },
    "host.name": {
      "object": {
        "aggregatable": false,
        "metadata_field": false,
        "searchable": true,
        "type": "object",
		"indices": ["logs-generic-default"]
      }
    },
    "service.name": {
      "keyword": {
        "aggregatable": true,
        "searchable": true,
        "type": "keyword",
		"indices": ["logs-generic-default"]
      }
    }
  },
  "indices": [
    "logs-generic-default"
  ]
}
`)
	tableMap := concurrent.NewMapWith("logs-generic-default", table)
	resp, err := handleFieldCapsIndex(ctx, config.QuesmaConfiguration{}, emptyRegistry{}, []string{"logs-generic-default"}, *tableMap)
	assert.NoError(t, err)
	expectedResp, err := json.MarshalIndent(expected, "", "  ")
	assert.NoError(t, err)
	err = json.Unmarshal(expectedResp, &expectedResp)
	assert.NoError(t, err)

	difference1, difference2, err := util.JsonDifference(
		string(resp),
		string(expectedResp),
	)

	assert.NoError(t, err)
	assert.Empty(t, difference1)
	assert.Empty(t, difference2)
}

func TestFieldCapsWithStaticSchema(t *testing.T) {
	table := &clickhouse.Table{
		Name: tableName,
		Cols: map[string]*clickhouse.Column{
			"service.name": {Name: "service.name", Type: clickhouse.BaseType{Name: "String"}},
		},
	}
	expected := []byte(`{
  "fields": {
    "QUESMA_CLICKHOUSE_RESPONSE": {
      "text": {
        "aggregatable": false,
        "metadata_field": false,
        "searchable": true,
        "type": "text",
		"indices": ["logs-generic-default"]
      }
    },
    "service.name": {
      "match_only_text": {
        "aggregatable": true,
        "searchable": true,
        "metadata_field": false,
        "type": "match_only_text",
		"indices": ["logs-generic-default"]
      }
    }
  },
  "indices": [
    "logs-generic-default"
  ]
}
`)
	tableMap := concurrent.NewMapWith("logs-generic-default", table)
	resp, err := handleFieldCapsIndex(ctx, config.QuesmaConfiguration{
		IndexConfig: map[string]config.IndexConfiguration{
			"logs-generic-default": {
				TypeMappings: map[string]string{
					"service.name": elasticsearch_field_types.FieldTypeMatchOnlyText,
				},
			},
		},
	}, emptyRegistry{}, []string{"logs-generic-default"}, *tableMap)
	assert.NoError(t, err)
	expectedResp, err := json.MarshalIndent(expected, "", "  ")
	assert.NoError(t, err)
	err = json.Unmarshal(expectedResp, &expectedResp)
	assert.NoError(t, err)

	difference1, difference2, err := util.JsonDifference(
		string(resp),
		string(expectedResp),
	)

	assert.NoError(t, err)
	assert.Empty(t, difference1)
	assert.Empty(t, difference2)
}

func TestFieldCapsMultipleIndexes(t *testing.T) {
	tableMap := clickhouse.NewTableMap()
	tableMap.Store("logs-1", &clickhouse.Table{
		Name: tableName,
		Cols: map[string]*clickhouse.Column{
			"foo.bar1": {Name: "foo.bar1", Type: clickhouse.BaseType{Name: "String"}},
		},
	})
	tableMap.Store("logs-2", &clickhouse.Table{
		Name: tableName,
		Cols: map[string]*clickhouse.Column{
			"foo.bar2": {Name: "foo.bar2", Type: clickhouse.BaseType{Name: "String"}},
		},
	})
	resp, err := handleFieldCapsIndex(ctx, config.QuesmaConfiguration{}, emptyRegistry{}, []string{"logs-1", "logs-2"}, *tableMap)
	assert.NoError(t, err)
	expectedResp, err := json.MarshalIndent([]byte(`{
  "fields": {
    "QUESMA_CLICKHOUSE_RESPONSE": {
      "text": {
        "aggregatable": false,
        "metadata_field": false,
        "searchable": true,
        "type": "text",
		"indices": ["logs-1", "logs-2"]
      }
    },
    "foo.bar1": {
      "keyword": {
        "aggregatable": true,
        "searchable": true,
        "type": "keyword",
		"indices": ["logs-1"]
      }
    },
    "foo.bar2": {
      "keyword": {
        "aggregatable": true,
        "searchable": true,
        "type": "keyword",
		"indices": ["logs-2"]
      }
    }
  },
  "indices": [
    "logs-1",
	"logs-2"
  ]
}
`), "", "  ")
	assert.NoError(t, err)
	err = json.Unmarshal(expectedResp, &expectedResp)
	assert.NoError(t, err)

	difference1, difference2, err := util.JsonDifference(
		string(resp),
		string(expectedResp),
	)

	assert.NoError(t, err)
	assert.Empty(t, difference1)
	assert.Empty(t, difference2)
}

func TestFieldCapsMultipleIndexesConflictingEntries(t *testing.T) {
	tableMap := clickhouse.NewTableMap()
	tableMap.Store("logs-1", &clickhouse.Table{
		Name: "logs-1",
		Cols: map[string]*clickhouse.Column{
			"foo.bar": {Name: "foo.bar", Type: clickhouse.BaseType{Name: "String"}},
		},
	})
	tableMap.Store("logs-2", &clickhouse.Table{
		Name: "logs-2",
		Cols: map[string]*clickhouse.Column{
			"foo.bar": {Name: "foo.bar", Type: clickhouse.BaseType{Name: "Boolean"}},
		},
	})
	tableMap.Store("logs-3", &clickhouse.Table{
		Name: "logs-3",
		Cols: map[string]*clickhouse.Column{
			"foo.bar": {Name: "foo.bar", Type: clickhouse.BaseType{Name: "Boolean"}},
		},
	})
	resp, err := handleFieldCapsIndex(ctx, config.QuesmaConfiguration{}, emptyRegistry{}, []string{"logs-1", "logs-2", "logs-3"}, *tableMap)
	assert.NoError(t, err)
	expectedResp, err := json.MarshalIndent([]byte(`{
  "fields": {
    "QUESMA_CLICKHOUSE_RESPONSE": {
      "text": {
        "aggregatable": false,
        "metadata_field": false,
        "searchable": true,
        "type": "text",
		"indices": ["logs-1", "logs-2", "logs-3"]
      }
    },
    "foo.bar": {
      "keyword": {
        "aggregatable": true,
        "searchable": true,
        "type": "keyword",
		"indices": ["logs-1"]
      },
		"boolean": {
		  "aggregatable": false,
		  "searchable": true,
          "metadata_field": false,
          "type": "boolean",
		  "indices": ["logs-2", "logs-3"]
      }
    }
  },
  "indices": [
    "logs-1",
	"logs-2",
	"logs-3"
  ]
}
`), "", "  ")
	assert.NoError(t, err)
	err = json.Unmarshal(expectedResp, &expectedResp)
	assert.NoError(t, err)

	difference1, difference2, err := util.JsonDifference(
		string(resp),
		string(expectedResp),
	)

	assert.NoError(t, err)
	assert.Empty(t, difference1)
	assert.Empty(t, difference2)
}

func TestAddNewFieldCapability(t *testing.T) {
	Cols := map[string]*clickhouse.Column{
		"service.name": {Name: "service.name", Type: clickhouse.BaseType{Name: "String"}}}

	numericTypes := []clickhouse.BaseType{
		{Name: "DateTime"},
		{Name: "Int64"},
		{Name: "Int32"},
		{Name: "Int16"},
		{Name: "Int8"},
		{Name: "UInt8"},
		{Name: "Float32"},
		{Name: "Float64"}}

	for index, clickhouseType := range numericTypes {
		Cols["col"+strconv.Itoa(index)] = &clickhouse.Column{Name: mapPrimitiveType(clickhouseType.Name), Type: clickhouseType}
	}

	fields := make(map[string]map[string]model.FieldCapability)
	for _, col := range Cols {
		addNewDefaultFieldCapability(fields, col, "foo")
	}

	// Check aggregatable property
	assert.Equal(t, false, fields["service.name"]["text"].Aggregatable)
	for _, clickhouseType := range numericTypes {
		assert.Equal(t, true, fields[mapPrimitiveType(clickhouseType.Name)][mapPrimitiveType(clickhouseType.Name)].Aggregatable)
	}
}

func Test_merge(t *testing.T) {
	type args struct {
		cap1 model.FieldCapability
		cap2 model.FieldCapability
	}
	tests := []struct {
		name   string
		args   args
		want   model.FieldCapability
		merged bool
	}{
		{
			name: "different types",
			args: args{
				cap1: model.FieldCapability{Type: "text"},
				cap2: model.FieldCapability{Type: "keyword"},
			},
			want:   model.FieldCapability{},
			merged: false,
		},
		{
			name: "same types, different indices",
			args: args{
				cap1: model.FieldCapability{Type: "keyword", Aggregatable: true, MetadataField: util.Pointer(false), Indices: []string{"b", "a"}},
				cap2: model.FieldCapability{Type: "keyword", Aggregatable: true, MetadataField: util.Pointer(false), Indices: []string{"b"}},
			},
			want:   model.FieldCapability{Type: "keyword", Aggregatable: true, MetadataField: util.Pointer(false), Indices: []string{"a", "b"}},
			merged: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1 := merge(tt.args.cap1, tt.args.cap2)
			assert.Equalf(t, tt.want, got, "merge(%v, %v)", tt.args.cap1, tt.args.cap2)
			assert.Equalf(t, tt.merged, got1, "merge(%v, %v)", tt.args.cap1, tt.args.cap2)
		})
	}
}

type emptyRegistry struct {
}

func (e emptyRegistry) AllSchemas() map[schema.TableName]schema.Schema {
	return map[schema.TableName]schema.Schema{}
}

func (e emptyRegistry) FindSchema(schema.TableName) (schema.Schema, bool) {
	return schema.Schema{}, false
}

func (e emptyRegistry) Start() {
}
