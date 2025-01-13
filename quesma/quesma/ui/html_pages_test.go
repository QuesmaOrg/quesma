// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package ui

import (
	"fmt"
	"github.com/QuesmaOrg/quesma/quesma/clickhouse"
	"github.com/QuesmaOrg/quesma/quesma/logger"
	"github.com/QuesmaOrg/quesma/quesma/quesma/config"
	"github.com/QuesmaOrg/quesma/quesma/quesma/types"
	"github.com/QuesmaOrg/quesma/quesma/stats"
	"github.com/QuesmaOrg/quesma/quesma/table_resolver"
	"github.com/QuesmaOrg/quesma/quesma/util"
	"github.com/QuesmaOrg/quesma/v2/core/diag"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestHtmlPages(t *testing.T) {
	xss := "<script>alert('xss')</script>"
	xssBytes := []byte(xss)
	id := "b1c4a89e-4905-5e3c-b57f-dc92627d011e"
	logChan := make(chan logger.LogWithLevel, 5)
	resolver := table_resolver.NewEmptyTableResolver()
	qmc := NewQuesmaManagementConsole(&config.QuesmaConfiguration{}, nil, nil, logChan, diag.EmptyPhoneHomeRecentStatsProvider(), nil, resolver)
	qmc.PushPrimaryInfo(&diag.QueryDebugPrimarySource{Id: id, QueryResp: xssBytes})
	qmc.PushSecondaryInfo(&diag.QueryDebugSecondarySource{Id: id,
		Path:                   xss,
		IncomingQueryBody:      xssBytes,
		QueryBodyTranslated:    []diag.TranslatedSQLQuery{{Query: xssBytes}},
		QueryTranslatedResults: xssBytes,
	})
	log := fmt.Sprintf(`{"request_id": "%s", "message": "%s"}`, id, xss)
	logChan <- logger.LogWithLevel{Level: zerolog.ErrorLevel, Msg: log}
	// Manually process channel
	for i := 0; i < 3; i++ {
		qmc.processChannelMessage()
	}

	t.Run("queries got our id", func(t *testing.T) {
		response := string(qmc.generateQueries())
		assert.Contains(t, response, id)
	})

	t.Run("queries got no XSS", func(t *testing.T) {
		response := string(qmc.generateQueries())
		assert.NotContains(t, response, xss)
	})

	t.Run("reason got no XSS", func(t *testing.T) {
		response := string(qmc.generateErrorForReason(xss))
		assert.NotContains(t, response, xss)
	})

	t.Run("logs got no XSS", func(t *testing.T) {
		response := string(qmc.generateReportForRequestId(id))
		assert.NotContains(t, response, xss)
	})

	t.Run("statistics got no XSS", func(t *testing.T) {
		stats.GlobalStatistics.Process(false, xss, types.MustJSON("{}"), clickhouse.NestedSeparator)
		response := string(qmc.generateStatistics())
		assert.NotContains(t, response, xss)
	})

	// generateTables relies on the LogManager instance, which is not initialized in this test
	t.Run("schema got no XSS and no panic", func(t *testing.T) {
		response := string(qmc.generateTables())
		assert.NotContains(t, response, xss)
	})
}

func TestHtmlSchemaPage(t *testing.T) {
	xss := "<script>alert('xss')</script>"

	logChan := make(chan logger.LogWithLevel, 5)

	var columnsMap = make(map[string]*clickhouse.Column)

	column := &clickhouse.Column{
		Name:      xss,
		Modifiers: xss,
		Type:      clickhouse.NewBaseType(xss),
	}

	columnsMap[xss] = column

	table := &clickhouse.Table{
		Created:      true,
		Name:         xss,
		DatabaseName: xss,
		Cols:         columnsMap,
		Config:       &clickhouse.ChTableConfig{},
	}

	cfg := config.QuesmaConfiguration{}

	cfg.IndexConfig = map[string]config.IndexConfiguration{xss: {}}

	tables := util.NewSyncMap[string, *clickhouse.Table]()
	tables.Store(table.Name, table)

	logManager := clickhouse.NewLogManager(tables, &cfg)

	resolver := table_resolver.NewEmptyTableResolver()
	qmc := NewQuesmaManagementConsole(&cfg, logManager, nil, logChan, diag.EmptyPhoneHomeRecentStatsProvider(), nil, resolver)

	t.Run("schema got no XSS and no panic", func(t *testing.T) {
		response := string(qmc.generateTables())
		assert.NotContains(t, response, xss)
	})
}
