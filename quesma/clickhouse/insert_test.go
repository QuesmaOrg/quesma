package clickhouse

import (
	"context"
	"fmt"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"mitmproxy/quesma/concurrent"
	"mitmproxy/quesma/quesma/config"
	"mitmproxy/quesma/quesma/types"
	"mitmproxy/quesma/util"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
)

// So far this file tests:
// 1) creating table through insert in 3 cases:
//   a) schema doesn't exist
//   b) schema exists in our memory (e.g. is predefined), but isn't created in ClickHouse, so CREATE TABLE needs to be sent
//   c) schema exists both in our memory and in ClickHouse
// 2) inserting into table (building insert query string with/without attrs)
// 3) that predefined schema trumps (is more important) schema from insert's JSON

const tableName = "test_table"

var insertTests = []struct {
	name                  string
	insertJson            string
	createTableLines      []string // those and only those lines should be in create table query
	createTableLinesAttrs []string
}{
	{
		"insert fields agree with schema",
		`{"@timestamp":"2024-01-27T16:11:19.94Z","host.name":"hermes","message":"User password reset failed","service.name":"frontend","severity":"debug","source":"rhel"}`,
		[]string{
			`CREATE TABLE IF NOT EXISTS "test_table"`,
			`(`,
			`	`,
			`	"@timestamp" DateTime64`,
			`	"host.name" String`,
			`	"message" String`,
			`	"service.name" String`,
			`	"severity" String`,
			`	"source" String`,
			`	INDEX severity_idx severity TYPE set(25) GRANULARITY 4`,
			`	`,
			``,
			`)`,
			`ENGINE = MergeTree`,
			`ORDER BY ("@timestamp")`,
			`COMMENT 'created by Quesma'`,
		},
		[]string{
			`"attributes_float64_value" Array(Float64),`,
			`"attributes_float64_key" Array(String),`,
			`"attributes_string_value" Array(Float64),`,
			`"attributes_string_key" Array(String),`,
			`"attributes_int64_value" Array(Float64),`,
			`"attributes_int64_key" Array(String),`,
			`"attributes_bool_value" Array(Float64),`,
			`"attributes_bool_key" Array(String),`,
			``,
		},
	},
	{
		"insert fields disagree with schema",
		`{"@timestamp":"2024-01-27T16:11:19.94Z","host.name":"hermes","message":"User password reset failed","random1":["debug"],"random2":"random-string","severity":"frontend"}`,
		[]string{
			`CREATE TABLE IF NOT EXISTS "test_table"`,
			`(`,
			`	`,
			`	"@timestamp" DateTime64`,
			`	"host.name" String`,
			`	"message" String`,
			`	"random1" Array(String)`,
			`	"random2" string`,
			`	"severity" String`,
			`	INDEX severity_idx severity TYPE set(25) GRANULARITY 4`,
			`	`,
			``,
			`)`,
			`ENGINE = MergeTree`,
			`ORDER BY ("@timestamp")`,
			`COMMENT 'created by Quesma'`,
		},
		[]string{
			`"attributes_float64_value" Array(Float64),`,
			`"attributes_float64_key" Array(String),`,
			`"attributes_string_value" Array(Float64),`,
			`"attributes_string_key" Array(String),`,
			`"attributes_int64_value" Array(Float64),`,
			`"attributes_int64_key" Array(String),`,
			`"attributes_bool_value" Array(Float64),`,
			`"attributes_bool_key" Array(String),`,
			``,
		},
	},
}

var configs = []*ChTableConfig{
	NewChTableConfigNoAttrs(),
	NewDefaultCHConfig(),
}

var expectedInserts = []string{
	`INSERT INTO "` + tableName + `" FORMAT JSONEachRow ` + insertTests[0].insertJson,
	`INSERT INTO "` + tableName + `" FORMAT JSONEachRow {"attributes_string_key":\["service.name","severity","source"\],"attributes_string_value":\["frontend","debug","rhel"\],"@timestamp":"2024-01-27T16:11:19.94Z","host.name":"hermes","message":"User password reset failed"}`,
	`INSERT INTO "` + tableName + `" FORMAT JSONEachRow ` + strings.Replace(strings.Replace(insertTests[1].insertJson, "[", `\[`, 1), "]", `\]`, 1),
	`INSERT INTO "` + tableName + `" FORMAT JSONEachRow {"attributes_string_key":\["random1","random2","severity"\],"attributes_string_value":\["\[debug\]","random-string","frontend"\],"@timestamp":"2024-01-27T16:11:19.94Z","host.name":"hermes","message":"User password reset failed"}`,
}

type logManagerHelper struct {
	lm                  *LogManager
	tableAlreadyCreated bool
}

func logManagersNonEmpty(cfg *ChTableConfig) []logManagerHelper {
	lms := make([]logManagerHelper, 0, 4)
	for _, created := range []bool{true, false} {
		full := concurrent.NewMapWith(tableName, &Table{
			Name:   tableName,
			Config: cfg,
			Cols: map[string]*Column{
				"@timestamp":       dateTime("@timestamp"),
				"host.name":        genericString("host.name"),
				"message":          lowCardinalityString("message"),
				"non-insert-field": genericString("non-insert-field"),
			},
			Created: created,
		})
		lms = append(lms, logManagerHelper{NewLogManager(full, config.QuesmaConfiguration{}), created})
	}
	return lms
}

func logManagers(config *ChTableConfig) []logManagerHelper {
	return append([]logManagerHelper{{NewLogManagerEmpty(), false}}, logManagersNonEmpty(config)...)
}

func TestAutomaticTableCreationAtInsert(t *testing.T) {
	for index1, tt := range insertTests {
		for index2, config := range configs {
			for index3, lm := range logManagers(config) {
				t.Run("case insertTest["+strconv.Itoa(index1)+"], config["+strconv.Itoa(index2)+"], logManager["+strconv.Itoa(index3)+"]", func(t *testing.T) {

					query, err := buildCreateTableQueryNoOurFields(context.Background(), tableName, types.MustJSON(tt.insertJson), config)
					assert.NoError(t, err)
					table, err := NewTable(query, config)
					assert.NoError(t, err)
					query = addOurFieldsToCreateTableQuery(query, config, table)

					// check if CREATE TABLE string is OK
					queryByLine := strings.Split(query, "\n")
					if len(config.attributes) > 0 {
						assert.Equal(t, len(tt.createTableLines)+2*len(config.attributes)+1, len(queryByLine))
						for _, line := range tt.createTableLines {
							assert.True(t, slices.Contains(tt.createTableLines, line) || slices.Contains(tt.createTableLinesAttrs, line))
						}
					} else {
						assert.Equal(t, len(tt.createTableLines), len(queryByLine))
						for _, line := range tt.createTableLines {
							assert.Contains(t, tt.createTableLines, line)
						}
					}
					logManagerEmpty := lm.lm.schemaLoader.TableDefinitions().Size() == 0

					// check if we properly create table in our tables table :) (:) suggested by Copilot) if needed
					tableInMemory := lm.lm.FindTable(tableName)
					needCreate := true
					if tableInMemory != nil && tableInMemory.Created {
						needCreate = false
					}
					noSuchTable := lm.lm.AddTableIfDoesntExist(table)
					assert.Equal(t, needCreate, noSuchTable)

					// and Created is set to true
					tableInMemory = lm.lm.FindTable(tableName)
					assert.NotNil(t, tableInMemory)
					assert.True(t, tableInMemory.Created)

					// and we have a schema in memory in every case
					assert.Equal(t, 1, lm.lm.schemaLoader.TableDefinitions().Size())

					// and that schema in memory is what it should be (predefined, if it was predefined, new if it was new)
					resolvedTable, _ := lm.lm.schemaLoader.TableDefinitions().Load(tableName)
					if logManagerEmpty {
						assert.Equal(t, 6+2*len(config.attributes), len(resolvedTable.Cols))
					} else if lm.lm.schemaLoader.TableDefinitions().Size() > 0 {
						assert.Equal(t, 4, len(resolvedTable.Cols))
					} else {
						assert.Equal(t, 4, len(resolvedTable.Cols))
					}
				})
			}
		}
	}
}

func TestProcessInsertQuery(t *testing.T) {
	ctx := context.Background()
	for index1, tt := range insertTests {
		for index2, config := range configs {
			for index3, lm := range logManagers(config) {
				t.Run("case insertTest["+strconv.Itoa(index1)+"], config["+strconv.Itoa(index2)+"], logManager["+strconv.Itoa(index3)+"]", func(t *testing.T) {
					db, mock := util.InitSqlMockWithPrettyPrint(t)
					lm.lm.chDb = db
					defer db.Close()

					// info: result values aren't important, this '.WillReturnResult[...]' just needs to be there
					if !lm.tableAlreadyCreated {
						// we check here if we try to create table from predefined schema, not from insert's JSON
						if lm.lm.schemaLoader.TableDefinitions().Size() > 0 {
							mock.ExpectExec(`CREATE TABLE IF NOT EXISTS "` + tableName + `.*non-insert-field`).WillReturnResult(sqlmock.NewResult(0, 0))
						} else {
							mock.ExpectExec(`CREATE TABLE IF NOT EXISTS "` + tableName).WillReturnResult(sqlmock.NewResult(0, 0))
						}
					}
					if len(config.attributes) == 0 || (lm.lm.schemaLoader.TableDefinitions().Size() == 0) {
						mock.ExpectExec(expectedInserts[2*index1]).WillReturnResult(sqlmock.NewResult(545, 54))
					} else {
						mock.ExpectExec(expectedInserts[2*index1+1]).WillReturnResult(sqlmock.NewResult(1, 1))
					}

					err := lm.lm.ProcessInsertQuery(ctx, tableName, []types.JSON{types.MustJSON(tt.insertJson)})
					assert.NoError(t, err)
					if err := mock.ExpectationsWereMet(); err != nil {
						t.Fatal("there were unfulfilled expections:", err)
					}
				})
			}
		}
	}
}

// Tests a big integer both as a schema field and as an attribute
func TestInsertVeryBigIntegers(t *testing.T) {
	t.Skip("TODO not implemented yet. Need a custom unmarshaller, and maybe also a marshaller.")
	bigInts := []string{"18444073709551615", "9223372036854775807"} // ~2^54, 2^63-1
	expectedInsertJsons := []string{
		fmt.Sprintf(`INSERT INTO "%s" FORMAT JSONEachRow {"int": %s, "severity": "sev"}`, tableName, bigInts[0]),
		fmt.Sprintf(`INSERT INTO "%s" FORMAT JSONEachRow {"int": %s, "severity": "sev"}`, tableName, bigInts[1]),
	}

	// big integer as a schema field
	for i, bigInt := range bigInts {
		t.Run("big integer schema field: "+bigInt, func(t *testing.T) {
			db, mock := util.InitSqlMockWithPrettyPrint(t)
			lm := NewLogManagerEmpty()
			lm.chDb = db
			defer db.Close()

			mock.ExpectExec(`CREATE TABLE IF NOT EXISTS "` + tableName).WillReturnResult(sqlmock.NewResult(0, 0))
			mock.ExpectExec(expectedInsertJsons[i]).WillReturnResult(sqlmock.NewResult(0, 0))

			err := lm.ProcessInsertQuery(context.Background(), tableName, []types.JSON{types.MustJSON(fmt.Sprintf(`{"severity":"sev","int": %s}`, bigInt))})
			assert.NoError(t, err)
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal("there were unfulfilled expections:", err)
			}
		})
	}

	// big integer as an attribute field
	tableMapNoSchemaFields := concurrent.NewMapWith(tableName, &Table{
		Name:    tableName,
		Config:  NewChTableConfigFourAttrs(),
		Cols:    map[string]*Column{},
		Created: true,
	})

	for i, bigInt := range bigInts {
		t.Run("big integer attribute field: "+bigInt, func(t *testing.T) {
			db, mock := util.InitSqlMockWithPrettyPrint(t)
			lm := NewLogManagerEmpty()
			lm.chDb = db
			var ptr = atomic.Pointer[TableMap]{}
			ptr.Store(tableMapNoSchemaFields)
			lm.schemaLoader.tableDefinitions = &ptr
			defer db.Close()

			mock.ExpectExec(`CREATE TABLE IF NOT EXISTS "` + tableName).WillReturnResult(sqlmock.NewResult(0, 0))
			mock.ExpectExec(expectedInsertJsons[i]).WillReturnResult(sqlmock.NewResult(0, 0))

			bigIntAsInt, _ := strconv.ParseInt(bigInt, 10, 64)
			fmt.Printf(`{"severity":"sev","int": %d}\n`, bigIntAsInt)
			err := lm.ProcessInsertQuery(context.Background(), tableName, []types.JSON{types.MustJSON(fmt.Sprintf(`{"severity":"sev","int": %d}`, bigIntAsInt))})
			assert.NoError(t, err)
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal("there were unfulfilled expections:", err)
			}
		})
	}
}

func genericString(name string) *Column {
	return &Column{
		Name: name,
		Type: BaseType{
			Name:   "String",
			goType: NewBaseType("String").goType,
		},
		Modifiers: "CODEC(ZSTD(1))",
	}
}

func lowCardinalityString(name string) *Column {
	return &Column{
		Name: name,
		Type: BaseType{
			Name:   "LowCardinality(String)",
			goType: NewBaseType("LowCardinality(String)").goType,
		},
	}
}

func dateTime(name string) *Column {
	return &Column{
		Name: name,
		Type: BaseType{
			Name:   "DateTime64",
			goType: NewBaseType("DateTime64").goType,
		},
		Modifiers: "CODEC(DoubleDelta, LZ4)",
	}
}
