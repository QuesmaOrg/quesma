package quesma

import (
	"encoding/json"
	"github.com/stretchr/testify/assert"
	"mitmproxy/quesma/clickhouse"
	"mitmproxy/quesma/concurrent"
	"mitmproxy/quesma/model"
	"mitmproxy/quesma/util"
	"strconv"
	"testing"
)

const testTableName = "logs-generic-default"

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
        "type": "text"
      }
    },
    "arrayOfArraysOfStrings": {
      "keyword": {
        "aggregatable": true,
        "searchable": true,
        "type": "keyword"
      }
    },
    "arrayOfTuples": {
      "object": {
        "aggregatable": false,
        "metadata_field": false,
        "searchable": true,
        "type": "object"
      }
    },
    "host.name": {
      "object": {
        "aggregatable": false,
        "metadata_field": false,
        "searchable": true,
        "type": "object"
      }
    },
    "service.name": {
      "keyword": {
        "aggregatable": true,
        "searchable": true,
        "type": "keyword"
      }
    }
  },
  "Indices": [
    "logs-generic-default"
  ]
}
`)
	tableMap := concurrent.NewMapWith(testTableName, table)
	resp, err := handleFieldCapsIndex(ctx, []string{testTableName}, *tableMap)
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
	resp, err := handleFieldCapsIndex(ctx, []string{"logs-1", "logs-2"}, *tableMap)
	assert.NoError(t, err)
	expectedResp, err := json.MarshalIndent([]byte(`{
  "fields": {
    "QUESMA_CLICKHOUSE_RESPONSE": {
      "text": {
        "aggregatable": false,
        "metadata_field": false,
        "searchable": true,
        "type": "text"
      }
    },
    "foo.bar1": {
      "keyword": {
        "aggregatable": true,
        "searchable": true,
        "type": "keyword"
      }
    },
    "foo.bar2": {
      "keyword": {
        "aggregatable": true,
        "searchable": true,
        "type": "keyword"
      }
    }
  },
  "Indices": [
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
		addNewDefaultFieldCapability(fields, col)
	}

	// Check aggregatable property
	assert.Equal(t, false, fields["service.name"]["text"].Aggregatable)
	for _, clickhouseType := range numericTypes {
		assert.Equal(t, true, fields[mapPrimitiveType(clickhouseType.Name)][mapPrimitiveType(clickhouseType.Name)].Aggregatable)
	}
}
