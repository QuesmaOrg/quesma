// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package ingest

import (
	"context"
	"fmt"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/QuesmaOrg/quesma/platform/backend_connectors"
	"github.com/QuesmaOrg/quesma/platform/clickhouse"
	"github.com/QuesmaOrg/quesma/platform/config"
	"github.com/QuesmaOrg/quesma/platform/table_resolver"
	mux "github.com/QuesmaOrg/quesma/platform/v2/core"
	"github.com/QuesmaOrg/quesma/platform/v2/core/types"
	"github.com/QuesmaOrg/quesma/platform/v2/core/util"
	"github.com/stretchr/testify/assert"
	"reflect"
	"strings"
	"testing"
)

func TestValidateIngest(t *testing.T) {
	floatCol := &clickhouse.Column{Name: "float_field", Type: clickhouse.BaseType{
		Name:   "Float64",
		GoType: clickhouse.NewBaseType("float64").GoType,
	}}

	invalidJson := validateValueAgainstType("float", 1, floatCol.Type)
	assert.True(t, invalidJson)
	StringCol := &clickhouse.Column{Name: "float_field", Type: clickhouse.BaseType{
		Name:   "String",
		GoType: clickhouse.NewBaseType("string").GoType,
	}}

	invalidJson = validateValueAgainstType("string", 1, StringCol.Type)
	assert.False(t, invalidJson)

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
		`{"float_field":"15"}`,
		`{"float_field":"15.55"}`,

		`{"int32_field":2147483647}`,
		`{"int32_field":2147483648}`,

		`{"uint8_field":-1}`,
		`{"uint8_field":255}`,
		`{"uint8_field":1000}`,

		`{"float_array_field":[3.14, 6.28, 0.99]}`,
		`{"float_array_field":[1, 2, 3]}`,

		`{"nested_array_map_field": [
			[
				[
					{"field1": "value1", "field2": [1, 2, 3]},
					{"field1": "value2", "field2": [4, 5, 6]}
				],
				[
					{"field1": "value3", "field2": [7, 8, 9]},
					{"field1": "value4", "field2": [10, 11, 12]}
				]
			],
			[
				[
					{"field1": "value1", "field2": [1, 2, 3]}
				]
			],
			[]
		]}`,
		`{"nested_array_map_field": [
			[],
			[
				[],
				[{}],
				[
					{"field1": "value1", "field2": [1, 2, 3]},
					{"field1": "value2", "field2": [4, 5, 6]}
				],
				[
					{"field1": "value3", "field2": [7, 8, 9]},
					{"field1": "value4", "field2": [10, 11, 12]}
				]
			],
			[
				[
					{"field1": "value1", "field2": [1, 2, 3]}
				]
			],
		]}`,
	}
	expectedInsertJsons := []string{
		fmt.Sprintf(`INSERT INTO "%s" FORMAT JSONEachRow {"attributes_metadata":{"string_field":"v1;Int64"},"attributes_values":{"string_field":"10"}}`, tableName),
		fmt.Sprintf(`INSERT INTO "%s" FORMAT JSONEachRow {"string_field":"10"}`, tableName),

		fmt.Sprintf(`INSERT INTO "%s" FORMAT JSONEachRow {"int_field":15}`, tableName),
		fmt.Sprintf(`INSERT INTO "%s" FORMAT JSONEachRow {"int_field":15}`, tableName),

		fmt.Sprintf(`INSERT INTO "%s" FORMAT JSONEachRow {"int_field":"15"}`, tableName),
		fmt.Sprintf(`INSERT INTO "%s" FORMAT JSONEachRow {"attributes_metadata":{"int_field":"v1;String"},"attributes_values":{"int_field":"1.5"}}`, tableName),
		fmt.Sprintf(`INSERT INTO "%s" FORMAT JSONEachRow {"attributes_metadata":{"string_field":"v1;Int64"},"attributes_values":{"string_field":"15"}}`, tableName),
		fmt.Sprintf(`INSERT INTO "%s" FORMAT JSONEachRow {"attributes_metadata":{"string_field":"v1;Float64"},"attributes_values":{"string_field":"1.5"}}`, tableName),
		fmt.Sprintf(`INSERT INTO "%s" FORMAT JSONEachRow {"int_array_field":[81,85,69,83,77,65]}`, tableName),
		fmt.Sprintf(`INSERT INTO "%s" FORMAT JSONEachRow {"string_array_field":["DHRFZN","HLVJDR"]}`, tableName),
		fmt.Sprintf(`INSERT INTO "%s" FORMAT JSONEachRow {"attributes_metadata":{"int_array_field":"v1;Array(Int64)"},"attributes_values":{"int_array_field":"[81,\\\"oops\\\",69,83,77,65]"}}`, tableName),
		fmt.Sprintf(`INSERT INTO "%s" FORMAT JSONEachRow {"attributes_metadata":{"string_array_field":"v1;Array(String)"},"attributes_values":{"string_array_field":"[\\\"DHRFZN\\\",15,\\\"HLVJDR\\\"]"}}`, tableName),
		fmt.Sprintf(`INSERT INTO "%s" FORMAT JSONEachRow {"int32_field":15}`, tableName),
		fmt.Sprintf(`INSERT INTO "%s" FORMAT JSONEachRow {"float_field":7.5}`, tableName),
		fmt.Sprintf(`INSERT INTO "%s" FORMAT JSONEachRow {"float_field":15}`, tableName),
		fmt.Sprintf(`INSERT INTO "%s" FORMAT JSONEachRow {"float_field":"15"}`, tableName),
		fmt.Sprintf(`INSERT INTO "%s" FORMAT JSONEachRow {"float_field":"15.55"}`, tableName),

		fmt.Sprintf(`INSERT INTO "%s" FORMAT JSONEachRow {"int32_field":2147483647}`, tableName),
		fmt.Sprintf(`INSERT INTO "%s" FORMAT JSONEachRow {"attributes_metadata":{"int32_field":"v1;Int64"},"attributes_values":{"int32_field":"2147483648"}}`, tableName),

		fmt.Sprintf(`INSERT INTO "%s" FORMAT JSONEachRow {"attributes_metadata":{"uint8_field":"v1;Int64"},"attributes_values":{"uint8_field":"-1"}}`, tableName),
		fmt.Sprintf(`INSERT INTO "%s" FORMAT JSONEachRow {"uint8_field":255}`, tableName),
		fmt.Sprintf(`INSERT INTO "%s" FORMAT JSONEachRow {"attributes_metadata":{"uint8_field":"v1;Int64"},"attributes_values":{"uint8_field":"1000"}}`, tableName),

		fmt.Sprintf(`INSERT INTO "%s" FORMAT JSONEachRow {"float_array_field":[3.14,6.28,0.99]}`, tableName),
		fmt.Sprintf(`INSERT INTO "%s" FORMAT JSONEachRow {"float_array_field":[1,2,3]}`, tableName),

		fmt.Sprintf(`INSERT INTO "%s" FORMAT JSONEachRow {"nested_array_map_field":[[[{"field1":"value1","field2":[1,2,3]},{"field1":"value2","field2":[4,5,6]}],[{"field1":"value3","field2":[7,8,9]},{"field1":"value4","field2":[10,11,12]}]],[[{"field1":"value1","field2":[1,2,3]}]],[]]}`, tableName),
		fmt.Sprintf(`INSERT INTO "%s" FORMAT JSONEachRow {"nested_array_map_field":[[],[[],[{}],[{"field1":"value1","field2":[1,2,3]},{"field1":"value2","field2":[4,5,6]}],[{"field1":"value3","field2":[7,8,9]},{"field1":"value4","field2":[10,11,12]}]],[[{"field1":"value1","field2":[1,2,3]}]]]}`, tableName),
	}
	tableMap := util.NewSyncMapWith(tableName, &clickhouse.Table{
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
			"float_array_field": {Name: "float_array_field", Type: clickhouse.CompoundType{
				Name: "Array",
				BaseType: clickhouse.BaseType{
					Name:   "Float64",
					GoType: clickhouse.NewBaseType("Float64").GoType,
				},
			}},
			// Array(Array(Array(Tuple(field1 String, field2 Array(Int64)))))
			"nested_array_map_field": {Name: "nested_array_map_field", Type: clickhouse.CompoundType{
				Name: "Array",
				BaseType: clickhouse.CompoundType{
					Name: "Array",
					BaseType: clickhouse.CompoundType{
						Name: "Array",
						BaseType: clickhouse.MultiValueType{
							Name: "Tuple",
							Cols: []*clickhouse.Column{
								{
									Name: "field1",
									Type: clickhouse.BaseType{
										Name:   "String",
										GoType: clickhouse.NewBaseType("String").GoType,
									},
								},
								{
									Name: "field2",
									Type: clickhouse.CompoundType{
										Name: "Array",
										BaseType: clickhouse.BaseType{
											Name:   "Int64",
											GoType: clickhouse.NewBaseType("Int64").GoType,
										},
									},
								},
							},
						},
					},
				},
			}},
		},
	})

	splitInsertJSONEachRow := func(sql string) (prefix, jsonPart string, ok bool) {
		idx := strings.Index(sql, "{")
		if idx == -1 {
			return "", "", false // not a JSONEachRow insert or malformed
		}
		return sql[:idx], sql[idx:], true
	}

	for i := range inputJson {

		queryMatcher := sqlmock.QueryMatcherFunc(func(expectedSQL, actualSQL string) error {

			dumpState := func() {
				fmt.Println("Expected SQL:", expectedSQL)
				fmt.Println("Actual SQL:  ", actualSQL)
				fmt.Println("---")
			}

			expectedInsert, expectedJson, ok := splitInsertJSONEachRow(expectedSQL)
			if !ok {
				dumpState()
				return fmt.Errorf("expected SQL does not match JSONEachRow format: %s", expectedSQL)
			}

			actualInsert, actualJson, ok := splitInsertJSONEachRow(actualSQL)
			if !ok {
				dumpState()
				return fmt.Errorf("actual SQL does not match JSONEachRow format: %s", actualSQL)
			}

			if expectedInsert != actualInsert {
				dumpState()
				return fmt.Errorf("expected insert prefix '%s' does not match actual '%s'", expectedInsert, actualInsert)
			}

			expectedMap, err := types.ParseJSON(expectedJson)
			if err != nil {
				dumpState()
				return fmt.Errorf("failed to parse expected JSON: %s, error: %v", expectedJson, err)
			}
			actualMap, err := types.ParseJSON(actualJson)
			if err != nil {
				dumpState()
				return fmt.Errorf("failed to parse actual JSON: %s, error: %v", actualJson, err)
			}

			if !reflect.DeepEqual(expectedMap, actualMap) {
				dumpState()
				return fmt.Errorf("expected JSON %s does not match actual JSON %s", expectedJson, actualJson)
			}

			return nil
		})

		conn, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(queryMatcher))
		if err != nil {
			t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
		}

		db := backend_connectors.NewClickHouseBackendConnectorWithConnection("", conn)
		ip := newIngestProcessorEmpty()
		ip.chDb = db
		ip.tableDiscovery = clickhouse.NewTableDiscoveryWith(&config.QuesmaConfiguration{}, nil, *tableMap)

		resolver := table_resolver.NewEmptyTableResolver()
		decision := &mux.Decision{
			UseConnectors: []mux.ConnectorDecision{&mux.ConnectorDecisionClickhouse{
				ClickhouseTableName: "test_table",
			}}}
		resolver.Decisions["test_table"] = decision

		ip.tableResolver = resolver

		defer db.Close()

		mock.ExpectExec(expectedInsertJsons[i]).WithoutArgs().WillReturnResult(sqlmock.NewResult(0, 0))
		err = ip.ProcessInsertQuery(context.Background(), tableName, []types.JSON{types.MustJSON((inputJson[i]))}, &IngestTransformerTest{}, &columNameFormatter{separator: "::"})
		assert.NoError(t, err)

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal("there were unfulfilled expections:", err)
		}
	}
}
