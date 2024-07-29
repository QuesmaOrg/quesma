// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package clickhouse

import (
	"context"
	"fmt"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"quesma/concurrent"
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

func EscapeBrackets(s string) string {
	s = strings.ReplaceAll(s, `(`, `\(`)
	s = strings.ReplaceAll(s, `)`, `\)`)
	s = strings.ReplaceAll(s, `[`, `\[`)
	s = strings.ReplaceAll(s, `]`, `\]`)
	return s
}

func TestIngestValidation(t *testing.T) {
	expectedInsertJsons := []string{
		fmt.Sprintf(`INSERT INTO "%s" FORMAT JSONEachRow {"attributes_string_key":["non_insert_field"],"attributes_string_value":[10]}`, tableName),
	}
	tableMap := concurrent.NewMapWith(tableName, &Table{
		Name:   tableName,
		Config: NewChTableConfigFourAttrs(),
		Cols: map[string]*Column{
			"non_insert_field": {Name: "non_insert_field", Type: BaseType{
				Name:   "String",
				goType: NewBaseType("String").goType,
			}},
		},
		Created: true,
	})
	db, mock := util.InitSqlMockWithPrettyPrint(t, true)
	lm := NewLogManagerEmpty()
	lm.chDb = db
	lm.schemaLoader = newTableDiscoveryWith(config.QuesmaConfiguration{}, nil, *tableMap)
	defer db.Close()
	mock.ExpectExec(EscapeBrackets(expectedInsertJsons[0])).WithoutArgs().WillReturnResult(sqlmock.NewResult(0, 0))

	err := lm.ProcessInsertQuery(context.Background(), tableName, []types.JSON{types.MustJSON((`{"non_insert_field":10}`))}, &IngestTransformer{}, &columNameFormatter{separator: "::"})
	assert.NoError(t, err)
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal("there were unfulfilled expections:", err)
	}
}
