package ui

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"mitmproxy/quesma/clickhouse"
	"mitmproxy/quesma/quesma/config"
	"mitmproxy/quesma/stats"
	"testing"
)

func TestHtmlPages(t *testing.T) {
	xss := "<script>alert('xss')</script>"
	xssBytes := []byte(xss)
	id := "MagicId_123"
	logChan := make(chan string, 5)
	qmc := NewQuesmaManagementConsole(config.Load(), nil, logChan)
	qmc.PushPrimaryInfo(&QueryDebugPrimarySource{Id: id, QueryResp: xssBytes})
	qmc.PushSecondaryInfo(&QueryDebugSecondarySource{Id: id,
		IncomingQueryBody:      xssBytes,
		QueryBodyTranslated:    xssBytes,
		QueryRawResults:        xssBytes,
		QueryTranslatedResults: xssBytes,
	})
	log := fmt.Sprintf(`{"request_id": "%s", "message": "%s"}`, id, xss)
	logChan <- log
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
		response := string(qmc.generateLogForRequestId(id))
		assert.NotContains(t, response, xss)
	})

	t.Run("statistics got no XSS", func(t *testing.T) {
		stats.GlobalStatistics.Process(xss, "{}", clickhouse.NestedSeparator)
		response := string(qmc.generateStatistics())
		assert.NotContains(t, response, xss)
	})

	// generateSchema relies on the LogManager instance, which is not initialized in this test
	t.Run("schema got no XSS and no panic", func(t *testing.T) {
		response := string(qmc.generateSchema())
		assert.NotContains(t, response, xss)
	})
}
