// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package ingest

import (
	"context"
	"fmt"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"quesma/clickhouse"
	"quesma/concurrent"
	"quesma/index_registry"
	"quesma/quesma/config"
	"quesma/quesma/types"
	"quesma/util"
	"strings"
	"testing"
)

func TestGetTypeName(t *testing.T) {
	values := make(map[string][]interface{})
	values["UInt64"] = []interface{}{1}
	values["Float64"] = []interface{}{1.1}
	values["Int64"] = []interface{}{-1}
	values["String"] = []interface{}{"string"}
	values["Bool"] = []interface{}{true}
	values["Array(UInt64)"] = []interface{}{[]interface{}{1}}
	values["Array(Int64)"] = []interface{}{[]interface{}{-1}}
	values["Array(Array(Int64))"] = []interface{}{[][]interface{}{{-1}}}
	values["Array(Array(Array(Int64)))"] = []interface{}{[][][]interface{}{{{-1}}}}
	for typeName, values := range values {
		for _, value := range values {
			t.Run(typeName, func(t *testing.T) {
				assert.NotNil(t, value)
				assert.Equal(t, typeName, getTypeName(value))
			})
		}
	}
}

func TestValidateIngest(t *testing.T) {
	floatCol := &clickhouse.Column{Name: "float_field", Type: clickhouse.BaseType{
		Name:   "Float64",
		GoType: clickhouse.NewBaseType("float64").GoType,
	}}

	invalidJson := validateValueAgainstType("float", 1, floatCol)
	assert.Equal(t, 0, len(invalidJson))
	StringCol := &clickhouse.Column{Name: "float_field", Type: clickhouse.BaseType{
		Name:   "String",
		GoType: clickhouse.NewBaseType("string").GoType,
	}}

	invalidJson = validateValueAgainstType("string", 1, StringCol)
	assert.Equal(t, 1, len(invalidJson))

}

func EscapeBrackets(s string) string {
	s = strings.ReplaceAll(s, `(`, `\(`)
	s = strings.ReplaceAll(s, `)`, `\)`)
	s = strings.ReplaceAll(s, `[`, `\[`)
	s = strings.ReplaceAll(s, `]`, `\]`)
	return s
}

func TestIngestValidation(t *testing.T) {
	// Trying to ingest a field with a different type than the one defined in the table
	// will end with populating attributes_string_key with the field name and attributes_string_value with the field value
	inputJson := []string{
		`{"string_field":10}`,
		`{"string_field":"10"}`,

		`{"int_field":15}`,
		`{"int_field":15.0}`,

		`{"int_field":"15"}`,
		`{"int_field":"1.5"}`,
		`{"string_field":15}`,
		`{"string_field":1.5}`,

		`{"int_array_field":[81,85,69.0,83,77,65]}`,
		`{"string_array_field":["DHRFZN","HLVJDR"]}`,

		`{"int_array_field":[81,"oops",69,83,77,65]}`,
		`{"string_array_field":["DHRFZN",15,"HLVJDR"]}`,

		`{"int32_field":15}`,
		`{"float_field":7.5}`,
		`{"float_field":15}`,

		`{"int32_field":2147483647}`,
		`{"int32_field":2147483648}`,

		`{"uint8_field":-1}`,
		`{"uint8_field":255}`,
		`{"uint8_field":1000}`,
	}
	expectedInsertJsons := []string{
		fmt.Sprintf(`INSERT INTO "%s" FORMAT JSONEachRow {"attributes_values":{"string_field":"10"},"attributes_metadata":{"string_field":"v1;Int64"}}`, tableName),
		fmt.Sprintf(`INSERT INTO "%s" FORMAT JSONEachRow {"string_field":"10"}`, tableName),

		fmt.Sprintf(`INSERT INTO "%s" FORMAT JSONEachRow {"int_field":15}`, tableName),
		fmt.Sprintf(`INSERT INTO "%s" FORMAT JSONEachRow {"int_field":15}`, tableName),

		fmt.Sprintf(`INSERT INTO "%s" FORMAT JSONEachRow {"attributes_values":{"int_field":"15"},"attributes_metadata":{"int_field":"v1;String"}}`, tableName),
		fmt.Sprintf(`INSERT INTO "%s" FORMAT JSONEachRow {"attributes_values":{"int_field":"1.5"},"attributes_metadata":{"int_field":"v1;String"}}`, tableName),
		fmt.Sprintf(`INSERT INTO "%s" FORMAT JSONEachRow {"attributes_values":{"string_field":"15"},"attributes_metadata":{"string_field":"v1;Int64"}}`, tableName),
		fmt.Sprintf(`INSERT INTO "%s" FORMAT JSONEachRow {"attributes_values":{"string_field":"1.5"},"attributes_metadata":{"string_field":"v1;Float64"}}`, tableName),
		fmt.Sprintf(`INSERT INTO "%s" FORMAT JSONEachRow {"int_array_field":[81,85,69,83,77,65]}`, tableName),
		fmt.Sprintf(`INSERT INTO "%s" FORMAT JSONEachRow {"string_array_field":["DHRFZN","HLVJDR"]}`, tableName),

		fmt.Sprintf(`INSERT INTO "%s" FORMAT JSONEachRow {"attributes_values":{"int_array_field":"[81,\\"oops\\",69,83,77,65]"},"attributes_metadata":{"int_array_field":"v1;Array(Int64)"}}`, tableName),
		fmt.Sprintf(`INSERT INTO "%s" FORMAT JSONEachRow {"attributes_values":{"string_array_field":"[\\"DHRFZN\\",15,\\"HLVJDR\\"]"},"attributes_metadata":{"string_array_field":"v1;Array(String)"}}`, tableName),
		fmt.Sprintf(`INSERT INTO "%s" FORMAT JSONEachRow {"int32_field":15}`, tableName),
		fmt.Sprintf(`INSERT INTO "%s" FORMAT JSONEachRow {"float_field":7.5}`, tableName),
		fmt.Sprintf(`INSERT INTO "%s" FORMAT JSONEachRow {"float_field":15}`, tableName),

		fmt.Sprintf(`INSERT INTO "%s" FORMAT JSONEachRow {"int32_field":2147483647}`, tableName),
		fmt.Sprintf(`INSERT INTO "%s" FORMAT JSONEachRow {"attributes_values":{"int32_field":"2147483648"},"attributes_metadata":{"int32_field":"v1;Int64"}}`, tableName),

		fmt.Sprintf(`INSERT INTO "%s" FORMAT JSONEachRow {"attributes_values":{"uint8_field":"-1"},"attributes_metadata":{"uint8_field":"v1;Int64"}}`, tableName),
		fmt.Sprintf(`INSERT INTO "%s" FORMAT JSONEachRow {"uint8_field":255}`, tableName),
		fmt.Sprintf(`INSERT INTO "%s" FORMAT JSONEachRow {"attributes_values":{"uint8_field":"1000"},"attributes_metadata":{"uint8_field":"v1;Int64"}}`, tableName),
	}
	tableMap := concurrent.NewMapWith(tableName, &clickhouse.Table{
		Name:   tableName,
		Config: NewChTableConfigFourAttrs(),
		Cols: map[string]*clickhouse.Column{
			"string_field": {Name: "string_field", Type: clickhouse.BaseType{
				Name:   "String",
				GoType: clickhouse.NewBaseType("String").GoType,
			}},
			"int_field": {Name: "int_field", Type: clickhouse.BaseType{
				Name:   "Int64",
				GoType: clickhouse.NewBaseType("Int64").GoType,
			}},
			"int32_field": {Name: "int32_field", Type: clickhouse.BaseType{
				Name:   "Int32",
				GoType: clickhouse.NewBaseType("Int32").GoType,
			}},
			"uint8_field": {Name: "uint8_field", Type: clickhouse.BaseType{
				Name:   "UInt8",
				GoType: clickhouse.NewBaseType("UInt8").GoType,
			}},
			"float_field": {Name: "float_field", Type: clickhouse.BaseType{
				Name:   "Float32",
				GoType: clickhouse.NewBaseType("Float32").GoType,
			}},
			"string_array_field": {Name: "string_array_field", Type: clickhouse.CompoundType{
				Name: "Array",
				BaseType: clickhouse.BaseType{
					Name:   "String",
					GoType: clickhouse.NewBaseType("String").GoType,
				},
			}},
			"int_array_field": {Name: "int_array_field", Type: clickhouse.CompoundType{
				Name: "Array",
				BaseType: clickhouse.BaseType{
					Name:   "Int64",
					GoType: clickhouse.NewBaseType("Int64").GoType,
				},
			}},
		},
		Created: true,
	})
	for i := range inputJson {
		db, mock := util.InitSqlMockWithPrettyPrint(t, true)
		ip := NewIngestProcessorEmpty()
		ip.chDb = db
		ip.tableDiscovery = clickhouse.NewTableDiscoveryWith(&config.QuesmaConfiguration{}, nil, *tableMap)
		ip.indexRegistry = index_registry.NewEmptyIndexRegistry()
		defer db.Close()

		mock.ExpectExec(EscapeBrackets(expectedInsertJsons[i])).WithoutArgs().WillReturnResult(sqlmock.NewResult(0, 0))
		err := ip.ProcessInsertQuery(context.Background(), tableName, []types.JSON{types.MustJSON((inputJson[i]))}, &IngestTransformer{}, &columNameFormatter{separator: "::"})
		assert.NoError(t, err)
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal("there were unfulfilled expections:", err)
		}
	}
}
