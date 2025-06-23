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
	"github.com/QuesmaOrg/quesma/platform/persistence"
	"github.com/QuesmaOrg/quesma/platform/schema"
	"github.com/QuesmaOrg/quesma/platform/table_resolver"
	"github.com/QuesmaOrg/quesma/platform/types"
	"github.com/QuesmaOrg/quesma/platform/util"
	mux "github.com/QuesmaOrg/quesma/platform/v2/core"
	"github.com/stretchr/testify/assert"
	"slices"
	"strconv"
	"strings"
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
			`	"host::name" String`,
			`	"message" String`,
			`	"service::name" String`,
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
			`"attributes_values" Map(String,String),`,
			`"attributes_metadata" Map(String,String),`,
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
			`	"host::name" String`,
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
			`"attributes_values" Map(String,String),`,
			`"attributes_metadata" Map(String,String),`,
			``,
		},
	},
}

var configs = []*clickhouse.ChTableConfig{
	NewChTableConfigNoAttrs(),
	NewDefaultCHConfig(),
}

var expectedInserts = [][]string{
	[]string{EscapeBrackets(`INSERT INTO "` + tableName + `" FORMAT JSONEachRow {"@timestamp":"2024-01-27T16:11:19.94Z","host_name":"hermes","message":"User password reset failed","service_name":"frontend","severity":"debug","source":"rhel"}`)},
	[]string{
		EscapeBrackets(`ALTER TABLE "` + tableName + `" ADD COLUMN IF NOT EXISTS "service_name" Nullable(String)`),
		EscapeBrackets(`ALTER TABLE "` + tableName + `" COMMENT COLUMN "service_name" 'quesmaMetadataV1:fieldName=service.name`),

		EscapeBrackets(`ALTER TABLE "` + tableName + `" ADD COLUMN IF NOT EXISTS "severity" Nullable(String)`),
		EscapeBrackets(`ALTER TABLE "` + tableName + `" COMMENT COLUMN "severity" 'quesmaMetadataV1:fieldName=severity'`),

		EscapeBrackets(`ALTER TABLE "` + tableName + `" ADD COLUMN IF NOT EXISTS "source" Nullable(String)`),
		EscapeBrackets(`ALTER TABLE "` + tableName + `" COMMENT COLUMN "source" 'quesmaMetadataV1:fieldName=source'`),

		EscapeBrackets(`INSERT INTO "` + tableName + `" FORMAT JSONEachRow {"@timestamp":"2024-01-27T16:11:19.94Z","host_name":"hermes","message":"User password reset failed","service_name":"frontend","severity":"debug","source":"rhel"}`),
	},
	[]string{
		EscapeBrackets(`INSERT INTO "` + tableName + `" FORMAT JSONEachRow {"@timestamp":"2024-01-27T16:11:19.94Z","host_name":"hermes","message":"User password reset failed","random1":["debug"],"random2":"random-string","severity":"frontend"}`),
	},
	[]string{
		EscapeBrackets(`ALTER TABLE "` + tableName + `" ADD COLUMN IF NOT EXISTS "random1" Array(String)`),
		EscapeBrackets(`ALTER TABLE "` + tableName + `" COMMENT COLUMN "random1" 'quesmaMetadataV1:fieldName=random1'`),
		EscapeBrackets(`ALTER TABLE "` + tableName + `" ADD COLUMN IF NOT EXISTS "random2" Nullable(String)`),
		EscapeBrackets(`ALTER TABLE "` + tableName + `" COMMENT COLUMN "random2" 'quesmaMetadataV1:fieldName=random2'`),
		EscapeBrackets(`ALTER TABLE "` + tableName + `" ADD COLUMN IF NOT EXISTS "severity" Nullable(String)`),
		EscapeBrackets(`ALTER TABLE "` + tableName + `" COMMENT COLUMN "severity" 'quesmaMetadataV1:fieldName=severity'`),
		EscapeBrackets(`INSERT INTO "` + tableName + `" FORMAT JSONEachRow {"@timestamp":"2024-01-27T16:11:19.94Z","host_name":"hermes","message":"User password reset failed","random1":["debug"],"random2":"random-string","severity":"frontend"}`),
	},
}

type ingestProcessorHelper struct {
	ip                  *IngestProcessor
	tableAlreadyCreated bool
}

type IngestTransformerTest struct {
}

func (*IngestTransformerTest) Transform(document types.JSON) (types.JSON, error) {
	return document, nil
}

func ingestProcessorsNonEmpty(cfg *clickhouse.ChTableConfig) []ingestProcessorHelper {
	lms := make([]ingestProcessorHelper, 0, 4)
	for _, created := range []bool{true, false} {
		full := util.NewSyncMapWith(tableName, &clickhouse.Table{
			Name:   tableName,
			Config: cfg,
			Cols: map[string]*clickhouse.Column{
				"@timestamp":       dateTime("@timestamp"),
				"host_name":        genericString("host_name"),
				"message":          lowCardinalityString("message"),
				"non-insert-field": genericString("non-insert-field"),
			},
		})
		lms = append(lms, ingestProcessorHelper{newIngestProcessorWithEmptyTableMap(full, &config.QuesmaConfiguration{}), created})
	}
	return lms
}

func ingestProcessors(config *clickhouse.ChTableConfig) []ingestProcessorHelper {
	ingestProcessor := newIngestProcessorEmpty()
	ingestProcessor.schemaRegistry = &schema.StaticRegistry{}
	return append([]ingestProcessorHelper{{ingestProcessor, false}}, ingestProcessorsNonEmpty(config)...)
}

func TestAutomaticTableCreationAtInsert(t *testing.T) {
	for index1, tt := range insertTests {
		for index2, tableConfig := range configs {
			for index3, ip := range ingestProcessors(tableConfig) {
				t.Run("case insertTest["+strconv.Itoa(index1)+"], config["+strconv.Itoa(index2)+"], ingestProcessor["+strconv.Itoa(index3)+"]", func(t *testing.T) {
					ip.ip.schemaRegistry = &schema.StaticRegistry{}
					encodings := populateFieldEncodings([]types.JSON{types.MustJSON(tt.insertJson)}, tableName)
					columnsFromJson := JsonToColumns(types.MustJSON(tt.insertJson), tableConfig)
					columnsFromSchema := SchemaToColumns(findSchemaPointer(ip.ip.schemaRegistry, tableName), &columNameFormatter{separator: "::"}, tableName, encodings)
					columns := columnsWithIndexes(columnsToString(columnsFromJson, columnsFromSchema, encodings, tableName), Indexes(types.MustJSON(tt.insertJson)))
					query := createTableQuery(tableName, columns, tableConfig)

					table := ip.ip.createTableObject(tableName, columnsFromJson, columnsFromSchema, tableConfig)

					query = addOurFieldsToCreateTableQuery(query, tableConfig, table)

					// check if CREATE TABLE string is OK
					queryByLine := strings.Split(query, "\n")
					if len(tableConfig.Attributes) > 0 {
						assert.Equal(t, len(tt.createTableLines)+len(tableConfig.Attributes)-1, len(queryByLine))
						for _, line := range tt.createTableLines {
							assert.True(t, slices.Contains(tt.createTableLines, line) || slices.Contains(tt.createTableLinesAttrs, line))
						}
					} else {
						assert.Equal(t, len(tt.createTableLines), len(queryByLine))
						for _, line := range tt.createTableLines {
							assert.Contains(t, tt.createTableLines, line)
						}
					}
					ingestProcessorEmpty := ip.ip.tableDiscovery.TableDefinitions().Size() == 0

					// check if we properly create table in our tables table :) (:) suggested by Copilot) if needed
					tableInMemory := ip.ip.FindTable(tableName)
					needCreate := true
					if tableInMemory != nil {
						needCreate = false
					}
					noSuchTable := ip.ip.AddTableIfDoesntExist(table)
					assert.Equal(t, needCreate, noSuchTable)

					// and Created is set to true
					tableInMemory = ip.ip.FindTable(tableName)
					assert.NotNil(t, tableInMemory)

					// and we have a schema in memory in every case
					assert.Equal(t, 1, ip.ip.tableDiscovery.TableDefinitions().Size())

					// and that schema in memory is what it should be (predefined, if it was predefined, new if it was new)
					resolvedTable, _ := ip.ip.tableDiscovery.TableDefinitions().Load(tableName)
					if ingestProcessorEmpty {
						if len(tableConfig.Attributes) > 0 {
							assert.Equal(t, len(tableConfig.Attributes)+4, len(resolvedTable.Cols))
						} else {
							assert.Equal(t, 6+2*len(tableConfig.Attributes), len(resolvedTable.Cols))
						}
					} else if ip.ip.tableDiscovery.TableDefinitions().Size() > 0 {
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
			for index3, ip := range ingestProcessors(config) {
				t.Run("case insertTest["+strconv.Itoa(index1)+"], config["+strconv.Itoa(index2)+"], ingestProcessor["+strconv.Itoa(index3)+"]", func(t *testing.T) {
					conn, mock := util.InitSqlMockWithPrettyPrint(t, true)
					db := backend_connectors.NewClickHouseBackendConnectorWithConnection("", conn)
					ip.ip.chDb = db
					resolver := table_resolver.NewEmptyTableResolver()
					decision := &mux.Decision{
						UseConnectors: []mux.ConnectorDecision{&mux.ConnectorDecisionClickhouse{
							ClickhouseTableName: "test_table",
						}}}
					resolver.Decisions["test_table"] = decision

					ip.ip.tableResolver = resolver
					defer db.Close()

					// info: result values aren't important, this '.WillReturnResult[...]' just needs to be there
					if !ip.tableAlreadyCreated {
						// we check here if we try to create table from predefined schema, not from insert's JSON
						if ip.ip.tableDiscovery.TableDefinitions().Size() == 0 {
							mock.ExpectExec(`CREATE TABLE IF NOT EXISTS "` + tableName).WillReturnResult(sqlmock.NewResult(0, 0))
						}
					}
					if len(config.Attributes) == 0 || (ip.ip.tableDiscovery.TableDefinitions().Size() == 0) {
						for i := range expectedInserts[2*index1] {
							mock.ExpectExec(expectedInserts[2*index1][i]).WillReturnResult(sqlmock.NewResult(545, 54))
						}
					} else {
						for i := range expectedInserts[2*index1+1] {
							mock.ExpectExec(expectedInserts[2*index1+1][i]).WillReturnResult(sqlmock.NewResult(545, 54))
						}
					}

					err := ip.ip.ProcessInsertQuery(ctx, tableName, []types.JSON{types.MustJSON(tt.insertJson)}, &IngestTransformerTest{}, &columNameFormatter{separator: "::"})
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
			conn, mock := util.InitSqlMockWithPrettyPrint(t, true)
			db := backend_connectors.NewClickHouseBackendConnectorWithConnection("", conn)
			lm := newIngestProcessorEmpty()
			lm.chDb = db
			defer db.Close()

			mock.ExpectExec(`CREATE TABLE IF NOT EXISTS "` + tableName).WillReturnResult(sqlmock.NewResult(0, 0))
			mock.ExpectExec(expectedInsertJsons[i]).WillReturnResult(sqlmock.NewResult(0, 0))

			err := lm.ProcessInsertQuery(context.Background(), tableName, []types.JSON{types.MustJSON(fmt.Sprintf(`{"severity":"sev","int": %s}`, bigInt))}, &IngestTransformerTest{}, &columNameFormatter{separator: "::"})
			assert.NoError(t, err)
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal("there were unfulfilled expections:", err)
			}
		})
	}

	// big integer as an attribute field
	tableMapNoSchemaFields := util.NewSyncMapWith(tableName, &clickhouse.Table{
		Name:   tableName,
		Config: NewChTableConfigFourAttrs(),
		Cols:   map[string]*clickhouse.Column{},
	})

	for i, bigInt := range bigInts {
		t.Run("big integer attribute field: "+bigInt, func(t *testing.T) {
			conn, mock := util.InitSqlMockWithPrettyPrint(t, true)
			db := backend_connectors.NewClickHouseBackendConnectorWithConnection("", conn)
			lm := newIngestProcessorEmpty()
			lm.chDb = db
			lm.tableDiscovery = clickhouse.NewTableDiscoveryWith(&config.QuesmaConfiguration{}, nil, *tableMapNoSchemaFields)
			defer db.Close()

			mock.ExpectExec(`CREATE TABLE IF NOT EXISTS "` + tableName).WillReturnResult(sqlmock.NewResult(0, 0))
			mock.ExpectExec(expectedInsertJsons[i]).WillReturnResult(sqlmock.NewResult(0, 0))

			bigIntAsInt, _ := strconv.ParseInt(bigInt, 10, 64)
			fmt.Printf(`{"severity":"sev","int": %d}\n`, bigIntAsInt)
			err := lm.ProcessInsertQuery(context.Background(), tableName, []types.JSON{types.MustJSON(fmt.Sprintf(`{"severity":"sev","int": %d}`, bigIntAsInt))}, nil, &columNameFormatter{separator: "::"})
			assert.NoError(t, err)
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal("there were unfulfilled expections:", err)
			}
		})
	}
}

func genericString(name string) *clickhouse.Column {
	return &clickhouse.Column{
		Name: name,
		Type: clickhouse.BaseType{
			Name:   "String",
			GoType: clickhouse.NewBaseType("String").GoType,
		},
		Modifiers: "CODEC(ZSTD(1))",
	}
}

func lowCardinalityString(name string) *clickhouse.Column {
	return &clickhouse.Column{
		Name: name,
		Type: clickhouse.BaseType{
			Name:   "LowCardinality(String)",
			GoType: clickhouse.NewBaseType("LowCardinality(String)").GoType,
		},
	}
}

func dateTime(name string) *clickhouse.Column {
	return &clickhouse.Column{
		Name: name,
		Type: clickhouse.BaseType{
			Name:   "DateTime64",
			GoType: clickhouse.NewBaseType("DateTime64").GoType,
		},
		Modifiers: "CODEC(DoubleDelta, LZ4)",
	}
}

// TestCreateTableIfSomeFieldsExistsInSchemaAlready verifies the result `CREATE TABLE` statement when the ingested JSON doesn't contain *all* the columns from the table. In this case we should take the column data from schema and make sure the final statement includes all the columns.
func TestCreateTableIfSomeFieldsExistsInSchemaAlready(t *testing.T) {

	tests := []struct {
		name               string
		documents          []types.JSON
		expectedStatements []string
	}{
		{
			name: "simple single insert",
			documents: []types.JSON{
				{"new_field": "bar"},
			},
			expectedStatements: []string{
				`CREATE TABLE IF NOT EXISTS "test_index" ( "@timestamp" DateTime64(3) DEFAULT now64(), "attributes_values" Map(String,String), "attributes_metadata" Map(String,String), "new_field" Nullable(String) COMMENT 'quesmaMetadataV1:fieldName=new_field', "nested_field" Nullable(String) COMMENT 'quesmaMetadataV1:fieldName=nested.field', ) ENGINE = MergeTree ORDER BY ("@timestamp") COMMENT 'created by Quesma'`,
				`INSERT INTO "test_index" FORMAT JSONEachRow {"new_field":"bar"}`,
			},
		},
	}

	for i, tt := range tests {
		t.Run(util.PrettyTestName(tt.name, i), func(t *testing.T) {

			indexName := "test_index"

			quesmaConfig := &config.QuesmaConfiguration{
				IndexConfig: map[string]config.IndexConfiguration{
					indexName: {},
				},
			}

			indexSchema := schema.Schema{
				ExistsInDataSource: false,
				Fields: map[schema.FieldName]schema.Field{
					"nested.field": {
						PropertyName:         "nested.field",
						InternalPropertyName: "nested_field",
						InternalPropertyType: "String",
						Type:                 schema.QuesmaTypeKeyword},
				},
			}

			tables := NewTableMap()

			conn, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
			db := backend_connectors.NewClickHouseBackendConnectorWithConnection("", conn)
			if err != nil {
				t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
			}

			virtualTableStorage := persistence.NewStaticJSONDatabase()
			schemaRegistry := &schema.StaticRegistry{
				Tables: make(map[schema.IndexName]schema.Schema),
			}
			schemaRegistry.Tables[schema.IndexName(indexName)] = indexSchema

			resolver := table_resolver.NewEmptyTableResolver()
			decision := &mux.Decision{
				UseConnectors: []mux.ConnectorDecision{&mux.ConnectorDecisionClickhouse{
					ClickhouseTableName: "test_index",
				}}}
			resolver.Decisions["test_index"] = decision

			schemaRegistry.FieldEncodings = make(map[schema.FieldEncodingKey]schema.EncodedFieldName)
			schemaRegistry.FieldEncodings[schema.FieldEncodingKey{TableName: indexName, FieldName: "nested.field"}] = "nested_field"

			ingest := newIngestProcessorWithEmptyTableMap(tables, quesmaConfig)
			ingest.chDb = db
			ingest.lowerer.virtualTableStorage = virtualTableStorage
			ingest.schemaRegistry = schemaRegistry
			ingest.tableResolver = resolver

			ctx := context.Background()
			formatter := DefaultColumnNameFormatter()

			transformer := IngestTransformerFor(indexName, quesmaConfig)

			for _, stm := range tt.expectedStatements {
				mock.ExpectExec(stm).WillReturnResult(sqlmock.NewResult(1, 1))
			}

			err = ingest.ProcessInsertQuery(ctx, indexName, tt.documents, transformer, formatter)

			if err != nil {
				t.Fatalf("error processing insert query: %v", err)
			}

		})
	}
}

func TestHydrolixIngest(t *testing.T) {
	indexName := "test_index"

	quesmaConfig := &config.QuesmaConfiguration{
		IndexConfig: map[string]config.IndexConfiguration{
			indexName: {},
		},
	}
	projectName := quesmaConfig.Hydrolix.Database
	tests := []struct {
		name               string
		documents          []types.JSON
		expectedStatements []string
	}{
		{
			name: "simple single insert",
			documents: []types.JSON{
				{"new_field": "bar"},
			},

			expectedStatements: []string{
				fmt.Sprintf(`{
  "schema": {
    "project": "%s",
    "name": "test_index",
    "time_column": "ingest_time",
    "columns": [
      { "name": "new_field", "type": "string" },
      { "name": "ingest_time", "type": "datetime", "default": "NOW" }
    ],
    "partitioning": {
      "strategy": "time",
      "field": "ingest_time",
      "granularity": "day"
    }
  },
  "events": [
    {
      "new_field": "bar"
    }
  ]
}`, projectName),
			},
		},
	}

	for i, tt := range tests {
		t.Run(util.PrettyTestName(tt.name, i), func(t *testing.T) {

			indexSchema := schema.Schema{}

			tables := NewTableMap()

			conn, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
			db := backend_connectors.NewHydrolixBackendConnectorWithConnection("", conn)
			if err != nil {
				t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
			}

			schemaRegistry := &schema.StaticRegistry{
				Tables: make(map[schema.IndexName]schema.Schema),
			}
			schemaRegistry.Tables[schema.IndexName(indexName)] = indexSchema

			resolver := table_resolver.NewEmptyTableResolver()
			decision := &mux.Decision{
				UseConnectors: []mux.ConnectorDecision{&mux.ConnectorDecisionClickhouse{
					ClickhouseTableName: "test_index",
				}}}
			resolver.Decisions["test_index"] = decision

			ingest := newIngestProcessorWithHydrolixLowerer(tables, quesmaConfig)
			ingest.chDb = db

			ingest.schemaRegistry = schemaRegistry
			ingest.tableResolver = resolver

			ctx := context.Background()
			formatter := DefaultColumnNameFormatter()

			transformer := IngestTransformerFor(indexName, quesmaConfig)

			for _, stm := range tt.expectedStatements {
				mock.ExpectExec(stm).WillReturnResult(sqlmock.NewResult(1, 1))
			}

			err = ingest.ProcessInsertQuery(ctx, indexName, tt.documents, transformer, formatter)

			// For now we expect an error because the Hydrolix backend connector does not implement the Exec method
			if err != nil {
				t.Fatalf("error processing insert query: %v", err)
			}

		})
	}
}
