// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package frontend_connectors

import (
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/QuesmaOrg/quesma/platform/ab_testing"
	"github.com/QuesmaOrg/quesma/platform/backend_connectors"
	"github.com/QuesmaOrg/quesma/platform/database_common"
	"github.com/QuesmaOrg/quesma/platform/logger"
	"github.com/QuesmaOrg/quesma/platform/schema"
	"github.com/QuesmaOrg/quesma/platform/table_resolver"
	"github.com/QuesmaOrg/quesma/platform/ui"
	"github.com/QuesmaOrg/quesma/platform/util"
	"github.com/QuesmaOrg/quesma/platform/v2/core/diag"
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

	tables := database_common.NewTableMap()
	tables.Store("no_db_name", &database_common.Table{
		Name: "no_db_name", Config: database_common.NewChTableConfigTimestampStringAttr(), Cols: map[string]*database_common.Column{},
	})
	tables.Store("with_db_name", &database_common.Table{
		Name: "with_db_name", Config: database_common.NewChTableConfigTimestampStringAttr(), Cols: map[string]*database_common.Column{}, DatabaseName: "db_name",
	})
	tables.Store("common_prefix_1", &database_common.Table{
		Name: "common_prefix_1", Config: database_common.NewChTableConfigTimestampStringAttr(), Cols: map[string]*database_common.Column{}, DatabaseName: "db_name",
	})
	tables.Store("common_prefix_2", &database_common.Table{
		Name: "common_prefix_2", Config: database_common.NewChTableConfigTimestampStringAttr(), Cols: map[string]*database_common.Column{},
	})

	conn, mock := util.InitSqlMockWithPrettySqlAndPrint(t, false)
	defer conn.Close()
	db := backend_connectors.NewClickHouseBackendConnectorWithConnection("", conn)

	lm := database_common.NewLogManagerWithConnection(db, tables)
	logChan := logger.InitOnlyChannelLoggerForTests()
	resolver := table_resolver.NewEmptyTableResolver()

	tableDiscovery := database_common.NewEmptyTableDiscovery()
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
