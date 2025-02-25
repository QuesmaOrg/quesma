// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package quesma

import (
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/QuesmaOrg/quesma/quesma/ab_testing"
	"github.com/QuesmaOrg/quesma/quesma/backend_connectors"
	"github.com/QuesmaOrg/quesma/quesma/clickhouse"
	"github.com/QuesmaOrg/quesma/quesma/logger"
	"github.com/QuesmaOrg/quesma/quesma/quesma/ui"
	"github.com/QuesmaOrg/quesma/quesma/schema"
	"github.com/QuesmaOrg/quesma/quesma/table_resolver"
	"github.com/QuesmaOrg/quesma/quesma/util"
	"github.com/QuesmaOrg/quesma/quesma/v2/core/diag"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestCountEndpoint(t *testing.T) {
	staticRegistry := &schema.StaticRegistry{
		Tables: map[schema.IndexName]schema.Schema{
			"no_db_name":      {Fields: map[schema.FieldName]schema.Field{}},
			"with_db_name":    {Fields: map[schema.FieldName]schema.Field{}, DatabaseName: "db_name"},
			"common_prefix_1": {Fields: map[schema.FieldName]schema.Field{}, DatabaseName: "db_name"},
			"common_prefix_2": {Fields: map[schema.FieldName]schema.Field{}},
		},
	}

	tables := clickhouse.NewTableMap()
	tables.Store("no_db_name", &clickhouse.Table{
		Name: "no_db_name", Config: clickhouse.NewChTableConfigTimestampStringAttr(), Created: true, Cols: map[string]*clickhouse.Column{},
	})
	tables.Store("with_db_name", &clickhouse.Table{
		Name: "with_db_name", Config: clickhouse.NewChTableConfigTimestampStringAttr(), Created: true, Cols: map[string]*clickhouse.Column{}, DatabaseName: "db_name",
	})
	tables.Store("common_prefix_1", &clickhouse.Table{
		Name: "common_prefix_1", Config: clickhouse.NewChTableConfigTimestampStringAttr(), Created: true, Cols: map[string]*clickhouse.Column{}, DatabaseName: "db_name",
	})
	tables.Store("common_prefix_2", &clickhouse.Table{
		Name: "common_prefix_2", Config: clickhouse.NewChTableConfigTimestampStringAttr(), Created: true, Cols: map[string]*clickhouse.Column{},
	})

	conn, mock := util.InitSqlMockWithPrettySqlAndPrint(t, false)
	defer conn.Close()
	db := backend_connectors.NewClickHouseBackendConnectorWithConnection("", conn)

	lm := clickhouse.NewLogManagerWithConnection(db, tables)
	logChan := logger.InitOnlyChannelLoggerForTests()
	resolver := table_resolver.NewEmptyTableResolver()

	tableDiscovery := clickhouse.NewEmptyTableDiscovery()
	tableDiscovery.TableMap = tables

	managementConsole := ui.NewQuesmaManagementConsole(&DefaultConfig, nil, logChan, diag.EmptyPhoneHomeRecentStatsProvider(), nil, resolver)
	go managementConsole.RunOnlyChannelProcessor()

	queryRunner := NewQueryRunner(lm, &DefaultConfig, managementConsole, staticRegistry, ab_testing.NewEmptySender(), resolver, tableDiscovery)

	testcases := []struct {
		index       string
		expectedSQL string
	}{
		{"no_db_name", `SELECT count(*) FROM "no_db_name"`},
		{"with_db_name", `SELECT count(*) FROM "db_name"."with_db_name"`},
		{"common_prefix*", `SELECT sum(*) as count FROM ((SELECT count(*) FROM "db_name"."common_prefix_1") UNION ALL (SELECT count(*) FROM "common_prefix_2"))`},
		{"common_prefix_1,common_prefix_2", `SELECT sum(*) as count FROM ((SELECT count(*) FROM "db_name"."common_prefix_1") UNION ALL (SELECT count(*) FROM "common_prefix_2"))`},
	}

	for _, tc := range testcases {
		returnedRows := sqlmock.NewRows([]string{"count"})
		returnedRows.AddRow(10)
		mock.ExpectQuery(tc.expectedSQL).WillReturnRows(returnedRows)

		cnt, err := queryRunner.HandleCount(ctx, tc.index)
		assert.NoError(t, err)
		assert.Equal(t, int64(10), cnt)

		if err = mock.ExpectationsWereMet(); err != nil {
			t.Fatal("there were unfulfilled expections:", err)
		}
	}
}
