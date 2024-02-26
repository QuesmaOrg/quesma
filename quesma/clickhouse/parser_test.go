package clickhouse

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestRemoveTypeMismatchSchemaField_ArrayOfTuples(t *testing.T) {
	createTableString := `CREATE TABLE kibana_sample_data_ecommerce
		(
			"@timestamp" DateTime64(3) DEFAULT now64(),
			"attributes_string_key" Array(String),
			"attributes_string_value" Array(String),
			"manufacturer" Array(String),
			"products" Array(Tuple(discount_percentage Nullable(Int64), category Nullable(String), unit_discount_amount Nullable(Int64), created_on DateTime64(3) )),
			"total_quantity" Nullable(Int64),
		)
		ENGINE = MergeTree
		ORDER BY "@timestamp"
		SETTINGS index_granularity = 8192`

	table, err := NewTable(createTableString, NewNoTimestampOnlyStringAttrCHConfig())
	assert.NoError(t, err)

	incomingInsert := SchemaMap{
		"products": []SchemaMap{
			{
				"discount_percentage":           5,
				"category":                      "good",
				"unit_discount_amount":          "5", // invalid type
				"non-schema-field-invalid-type": true,
				"non-schema-field-valid-type":   "good",
				"created_on": SchemaMap{ // invalid type
					"invalid-nesting": true,
				},
			},
		},
	}

	removedMismatch := RemoveTypeMismatchSchemaFields(incomingInsert, table)
	assert.Len(t, removedMismatch, 1)
	products := removedMismatch["products"].([]map[string]interface{})
	assert.Len(t, products, 1)
	product := products[0]
	assert.True(t, product["discount_percentage"] == 5)
	assert.True(t, product["category"] == "good")
	assert.Len(t, products, 1)
}

func TestRemoveTypeMismatchSchemaField_ArrayOfTuples_2(t *testing.T) {
	createTableString := `CREATE TABLE kibana_sample_data_ecommerce
		(
		    "array-ints" Array(Int64),
			"array-strs" Array(String),
		)
		ENGINE = MergeTree
		ORDER BY "@timestamp"
		SETTINGS index_granularity = 8192`

	table, err := NewTable(createTableString, NewChTableConfigNoAttrs())
	assert.NoError(t, err)

	incomingInsert := SchemaMap{
		"array-ints": []interface{}{"1"},
		"array-strs": []interface{}{1},
	}

	removedMismatch := RemoveTypeMismatchSchemaFields(incomingInsert, table)
	assert.Len(t, removedMismatch, 0)
}

func TestRemoveTypeMismatchSchemaField_BaseTypes(t *testing.T) {
	createTableString := `CREATE TABLE kibana_sample_data_ecommerce
		(
		    "int1" Int64,
		    "int2" Int64,
		    "str1" String,
		    "str2" String
		)
		ENGINE = MergeTree
		ORDER BY "@timestamp"
		SETTINGS index_granularity = 8192`

	table, err := NewTable(createTableString, NewChTableConfigNoAttrs())
	assert.NoError(t, err)

	incomingInsert := SchemaMap{
		"int1": "1",
		"int2": "1.5",
		"str1": 1,
		"str2": 1.5,
	}

	removedMismatch := RemoveTypeMismatchSchemaFields(incomingInsert, table)
	assert.Len(t, removedMismatch, 0)
}

func TestRemoveTypeMismatchSchemaField_intsAndFloats(t *testing.T) {
	createTableString := `CREATE TABLE kibana_sample_data_ecommerce
		(
		    "int" Int64
		)
		ENGINE = MergeTree
		ORDER BY "@timestamp"
		SETTINGS index_granularity = 8192`
	table, err := NewTable(createTableString, NewChTableConfigNoAttrs())
	assert.NoError(t, err)

	incomingInsert := SchemaMap{
		"int": 1.0,
	}
	removedMismatch := RemoveTypeMismatchSchemaFields(incomingInsert, table)
	assert.Len(t, removedMismatch, 1)
}
