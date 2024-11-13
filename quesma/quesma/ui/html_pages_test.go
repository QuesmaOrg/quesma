// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package ui

import (
	"fmt"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"quesma/clickhouse"
	"quesma/concurrent"
	"quesma/logger"
	"quesma/quesma/config"
	"quesma/quesma/types"
	"quesma/stats"
	"quesma/table_resolver"
	"quesma/telemetry"
	"testing"
)

func TestHtmlPages(t *testing.T) {
	xss := "<script>alert('xss')</script>"
	xssBytes := []byte(xss)
	id := "b1c4a89e-4905-5e3c-b57f-dc92627d011e"
	logChan := make(chan logger.LogWithLevel, 5)
	resolver := table_resolver.NewEmptyTableResolver()
	qmc := NewQuesmaManagementConsole(&config.QuesmaConfiguration{}, nil, nil, logChan, telemetry.NewPhoneHomeEmptyAgent(), nil, resolver)
	qmc.PushPrimaryInfo(&QueryDebugPrimarySource{Id: id, QueryResp: xssBytes})
	qmc.PushSecondaryInfo(&QueryDebugSecondarySource{Id: id,
		Path:                   xss,
		IncomingQueryBody:      xssBytes,
		QueryBodyTranslated:    []types.TranslatedSQLQuery{{Query: xssBytes}},
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
		stats.GlobalStatistics.Process(&config.QuesmaConfiguration{}, xss, types.MustJSON("{}"), clickhouse.NestedSeparator)
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

	cfg.IndexConfig = map[string]config.IndexConfiguration{xss: {Name: xss}}

	tables := concurrent.NewMap[string, *clickhouse.Table]()
	tables.Store(table.Name, table)

	logManager := clickhouse.NewLogManager(tables, &cfg)

	resolver := table_resolver.NewEmptyTableResolver()
	qmc := NewQuesmaManagementConsole(&cfg, logManager, nil, logChan, telemetry.NewPhoneHomeEmptyAgent(), nil, resolver)

	t.Run("schema got no XSS and no panic", func(t *testing.T) {
		response := string(qmc.generateTables())
		assert.NotContains(t, response, xss)
	})
}
