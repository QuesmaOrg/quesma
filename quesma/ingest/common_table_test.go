// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package ingest

import (
	"context"
	"encoding/json"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"quesma/clickhouse"
	"quesma/common_table"
	"quesma/jsonprocessor"
	"quesma/persistence"
	"quesma/quesma/config"
	"quesma/quesma/types"
	"quesma/schema"
	"testing"
)

func TestIngestToCommonTable(t *testing.T) {

	tests := []struct {
		name                string
		documents           []types.JSON
		expectedStatements  []string
		virtualTableColumns []string
	}{
		{
			name: "simple single insert",
			documents: []types.JSON{
				{"foo": "bar"},
			},
			expectedStatements: []string{
				`ALTER TABLE "quesma_common_table" ADD COLUMN IF NOT EXISTS "foo" Nullable(String)`,
				`ALTER TABLE "quesma_common_table" COMMENT COLUMN "foo" 'foo'`,
				`INSERT INTO "quesma_common_table" FORMAT JSONEachRow {"__quesma_index_name":"test_index","foo":"bar"}`,
			},
			virtualTableColumns: []string{"foo"},
		},
		{
			name: "simple inserts",
			documents: []types.JSON{
				{"foo": "bar"},
				{"foo": "baz"},
			},
			expectedStatements: []string{
				`ALTER TABLE "quesma_common_table" ADD COLUMN IF NOT EXISTS "foo" Nullable(String)`,
				`ALTER TABLE "quesma_common_table" COMMENT COLUMN "foo" 'foo'`,
				`INSERT INTO "quesma_common_table" FORMAT JSONEachRow {"__quesma_index_name":"test_index","foo":"bar"}, {"__quesma_index_name":"test_index","foo":"baz"}`,
			},
			virtualTableColumns: []string{"foo"},
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
				`ALTER TABLE "quesma_common_table" COMMENT COLUMN "foo" 'foo'`,
				`ALTER TABLE "quesma_common_table" ADD COLUMN IF NOT EXISTS "baz" Nullable(String)`,
				`ALTER TABLE "quesma_common_table" COMMENT COLUMN "baz" 'baz'`,

				`INSERT INTO "quesma_common_table" FORMAT JSONEachRow {"__quesma_index_name":"test_index","foo":"bar"}, {"__quesma_index_name":"test_index","foo":"baz"}, {"__quesma_index_name":"test_index","baz":"qux","foo":"1"} `,
			},
			virtualTableColumns: []string{"foo", "baz"},
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

			tables.Store(common_table.TableName, quesmaCommonTable)

			db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
			if err != nil {
				t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
			}

			virtualTableStorage := persistence.NewStaticJSONDatabase()

			tableDisco := clickhouse.NewTableDiscovery(quesmaConfig, db, virtualTableStorage)
			schemaRegistry := schema.NewSchemaRegistry(clickhouse.TableDiscoveryTableProviderAdapter{TableDiscovery: tableDisco}, quesmaConfig, clickhouse.SchemaTypeAdapter{})

			ingest := NewIngestProcessor(tables, quesmaConfig)
			ingest.chDb = db
			ingest.virtualTableStorage = virtualTableStorage
			ingest.schemaRegistry = schemaRegistry

			ctx := context.Background()
			formatter := clickhouse.DefaultColumnNameFormatter()

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
