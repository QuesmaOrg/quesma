// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package ingest

import (
	"context"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/QuesmaOrg/quesma/quesma/backend_connectors"
	"github.com/QuesmaOrg/quesma/quesma/clickhouse"
	"github.com/QuesmaOrg/quesma/quesma/common_table"
	"github.com/QuesmaOrg/quesma/quesma/jsonprocessor"
	"github.com/QuesmaOrg/quesma/quesma/persistence"
	"github.com/QuesmaOrg/quesma/quesma/quesma/config"
	"github.com/QuesmaOrg/quesma/quesma/quesma/types"
	"github.com/QuesmaOrg/quesma/quesma/schema"
	"github.com/QuesmaOrg/quesma/quesma/table_resolver"
	mux "github.com/QuesmaOrg/quesma/quesma/v2/core"
	"github.com/goccy/go-json"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestIngestToCommonTable(t *testing.T) {

	tests := []struct {
		name                   string
		alreadyExistingColumns []*clickhouse.Column // list of columns that exists in the common table and virtual table
		documents              []types.JSON
		expectedStatements     []string
		virtualTableColumns    []string
	}{
		{
			name: "simple single insert",
			documents: []types.JSON{
				{"foo": "bar"},
			},
			expectedStatements: []string{
				`ALTER TABLE "quesma_common_table" ADD COLUMN IF NOT EXISTS "foo" Nullable(String)`,
				`ALTER TABLE "quesma_common_table" COMMENT COLUMN "foo" 'quesmaMetadataV1:fieldName=foo'`,
				`INSERT INTO "quesma_common_table" FORMAT JSONEachRow {"__quesma_index_name":"test_index","foo":"bar"}`,
			},
			virtualTableColumns: []string{"@timestamp", "foo"},
		},
		{
			name: "simple inserts",
			documents: []types.JSON{
				{"foo": "bar"},
				{"foo": "baz"},
			},
			expectedStatements: []string{
				`ALTER TABLE "quesma_common_table" ADD COLUMN IF NOT EXISTS "foo" Nullable(String)`,
				`ALTER TABLE "quesma_common_table" COMMENT COLUMN "foo" 'quesmaMetadataV1:fieldName=foo'`,
				`INSERT INTO "quesma_common_table" FORMAT JSONEachRow {"__quesma_index_name":"test_index","foo":"bar"}, {"__quesma_index_name":"test_index","foo":"baz"}`,
			},
			virtualTableColumns: []string{"@timestamp", "foo"},
		},
		{
			name: "simple inserts and new column",
			documents: []types.JSON{
				{"foo": "bar"},
				{"foo": "baz"},
				{"foo": "1", "baz": "qux"},
			},
			expectedStatements: []string{
				`ALTER TABLE "quesma_common_table" ADD COLUMN IF NOT EXISTS "foo" Nullable(String)`,
				`ALTER TABLE "quesma_common_table" COMMENT COLUMN "foo" 'quesmaMetadataV1:fieldName=foo'`,
				`ALTER TABLE "quesma_common_table" ADD COLUMN IF NOT EXISTS "baz" Nullable(String)`,
				`ALTER TABLE "quesma_common_table" COMMENT COLUMN "baz" 'quesmaMetadataV1:fieldName=baz'`,

				`INSERT INTO "quesma_common_table" FORMAT JSONEachRow {"__quesma_index_name":"test_index","foo":"bar"}, {"__quesma_index_name":"test_index","foo":"baz"}, {"__quesma_index_name":"test_index","baz":"qux","foo":"1"} `,
			},
			virtualTableColumns: []string{"@timestamp", "baz", "foo"},
		},
		{
			name: "simple inserts, column exists, but not ingested",
			alreadyExistingColumns: []*clickhouse.Column{
				{Name: "a", Type: clickhouse.BaseType{Name: "String"}},
			},
			documents: []types.JSON{
				{"foo": "bar"},
				{"foo": "baz"},
				{"foo": "1", "baz": "qux"},
			},
			expectedStatements: []string{
				`ALTER TABLE "quesma_common_table" ADD COLUMN IF NOT EXISTS "foo" Nullable(String)`,
				`ALTER TABLE "quesma_common_table" COMMENT COLUMN "foo" 'quesmaMetadataV1:fieldName=foo'`,
				`ALTER TABLE "quesma_common_table" ADD COLUMN IF NOT EXISTS "baz" Nullable(String)`,
				`ALTER TABLE "quesma_common_table" COMMENT COLUMN "baz" 'quesmaMetadataV1:fieldName=baz'`,

				`INSERT INTO "quesma_common_table" FORMAT JSONEachRow {"__quesma_index_name":"test_index","foo":"bar"}, {"__quesma_index_name":"test_index","foo":"baz"}, {"__quesma_index_name":"test_index","baz":"qux","foo":"1"} `,
			},
			virtualTableColumns: []string{"@timestamp", "a", "baz", "foo"},
		},
		{
			name: "ingest to existing column",
			alreadyExistingColumns: []*clickhouse.Column{
				{Name: "a", Type: clickhouse.BaseType{Name: "String"}},
			},
			documents: []types.JSON{
				{"a": "bar"},
			},
			expectedStatements: []string{
				`INSERT INTO "quesma_common_table" FORMAT JSONEachRow {"__quesma_index_name":"test_index","a":"bar"}`,
			},
			virtualTableColumns: []string{"@timestamp", "a"},
		},
		{
			name: "ingest to existing column and new column",
			alreadyExistingColumns: []*clickhouse.Column{
				{Name: "a", Type: clickhouse.BaseType{Name: "String"}},
			},
			documents: []types.JSON{
				{"a": "bar", "b": "baz"},
			},
			expectedStatements: []string{
				`ALTER TABLE "quesma_common_table" ADD COLUMN IF NOT EXISTS "b" Nullable(String)`,
				`ALTER TABLE "quesma_common_table" COMMENT COLUMN "b" 'quesmaMetadataV1:fieldName=b'`,

				`INSERT INTO "quesma_common_table" FORMAT JSONEachRow {"__quesma_index_name":"test_index","a":"bar","b":"baz"}`,
			},
			virtualTableColumns: []string{"@timestamp", "a", "b"},
		},
		{
			name:                   "ingest to name with a dot",
			alreadyExistingColumns: []*clickhouse.Column{},
			documents: []types.JSON{
				{"a.b": "c"},
			},
			expectedStatements: []string{
				`ALTER TABLE "quesma_common_table" ADD COLUMN IF NOT EXISTS "a_b" Nullable(String)`,
				`ALTER TABLE "quesma_common_table" COMMENT COLUMN "a_b" 'quesmaMetadataV1:fieldName=a.b'`,

				`INSERT INTO "quesma_common_table" FORMAT JSONEachRow {"__quesma_index_name":"test_index","a_b":"c"}`,
			},
			virtualTableColumns: []string{"@timestamp", "a_b"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			indexName := "test_index"

			quesmaConfig := &config.QuesmaConfiguration{
				IndexConfig: map[string]config.IndexConfiguration{
					indexName: {
						UseCommonTable: true,
					},
				},
			}

			tables := NewTableMap()

			quesmaCommonTable := &clickhouse.Table{
				Name: common_table.TableName,
				Cols: map[string]*clickhouse.Column{
					"@timestmap": {
						Name: "@timestamp",
						Type: clickhouse.BaseType{Name: "DateTime64"},
					},
					common_table.IndexNameColumn: {
						Name: common_table.IndexNameColumn,
						Type: clickhouse.BaseType{Name: "String"},
					},
					clickhouse.AttributesValuesColumn: {
						Name: clickhouse.AttributesValuesColumn,
						Type: clickhouse.BaseType{Name: "Map(String, String)"},
					},
					clickhouse.AttributesMetadataColumn: {
						Name: clickhouse.AttributesMetadataColumn,
						Type: clickhouse.BaseType{Name: "Map(String, String)"},
					},
				},
				Config:  NewDefaultCHConfig(),
				Created: true,
			}

			for _, col := range tt.alreadyExistingColumns {
				quesmaCommonTable.Cols[col.Name] = col
			}

			tables.Store(common_table.TableName, quesmaCommonTable)

			conn, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
			db := backend_connectors.NewClickHouseBackendConnectorWithConnection("", conn)
			if err != nil {
				t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
			}

			virtualTableStorage := persistence.NewStaticJSONDatabase()

			tableDisco := clickhouse.NewTableDiscovery(quesmaConfig, db, virtualTableStorage)
			schemaRegistry := schema.NewSchemaRegistry(clickhouse.TableDiscoveryTableProviderAdapter{TableDiscovery: tableDisco}, quesmaConfig, clickhouse.SchemaTypeAdapter{})
			schemaRegistry.Start()
			defer schemaRegistry.Stop()

			resolver := table_resolver.NewEmptyTableResolver()

			decision := &mux.Decision{
				UseConnectors: []mux.ConnectorDecision{
					&mux.ConnectorDecisionClickhouse{
						ClickhouseTableName: common_table.TableName,
						ClickhouseIndexes:   []string{indexName},
						IsCommonTable:       true,
					},
				},
			}

			resolver.Decisions[indexName] = decision

			ingest := newIngestProcessorWithEmptyTableMap(tables, quesmaConfig)
			ingest.chDb = db
			ingest.virtualTableStorage = virtualTableStorage
			ingest.schemaRegistry = schemaRegistry
			ingest.tableResolver = resolver

			if len(tt.alreadyExistingColumns) > 0 {

				testTable := &clickhouse.Table{
					Name:         indexName,
					Cols:         map[string]*clickhouse.Column{},
					Config:       NewDefaultCHConfig(),
					Created:      true,
					VirtualTable: true,
				}

				for _, col := range tt.alreadyExistingColumns {
					testTable.Cols[col.Name] = col
				}

				tables.Store(indexName, testTable)
				err = ingest.storeVirtualTable(testTable)
				if err != nil {
					t.Fatalf("error storing virtual table: %v", err)
				}
			}

			ctx := context.Background()
			formatter := DefaultColumnNameFormatter()

			transformer := jsonprocessor.IngestTransformerFor(indexName, quesmaConfig)

			for _, stm := range tt.expectedStatements {
				mock.ExpectExec(stm).WillReturnResult(sqlmock.NewResult(1, 1))
			}

			err = ingest.ProcessInsertQuery(ctx, indexName, tt.documents, transformer, formatter)

			if err != nil {
				t.Fatalf("error processing insert query: %v", err)
			}

			vTableAsJson, ok, err := virtualTableStorage.Get(indexName)
			if err != nil {
				t.Fatalf("error getting virtual table: %v", err)
			}
			if !ok {
				t.Fatalf("virtual table not found")
			}

			var vTable common_table.VirtualTable

			err = json.Unmarshal([]byte(vTableAsJson), &vTable)
			if err != nil {
				t.Fatalf("error unmarshalling virtual table: %v", err)
			}

			var virtualTableColumn []string
			for _, col := range vTable.Columns {
				virtualTableColumn = append(virtualTableColumn, col.Name)
			}

			assert.Equal(t, tt.virtualTableColumns, virtualTableColumn)
		})
	}
}
