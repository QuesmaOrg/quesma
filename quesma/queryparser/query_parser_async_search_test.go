package queryparser

import (
	"github.com/stretchr/testify/assert"
	"mitmproxy/quesma/clickhouse"
	"mitmproxy/quesma/testdata"
	"testing"
)

func TestQueryParserAsyncSearch(t *testing.T) {
	lm := clickhouse.NewLogManager(make(clickhouse.TableMap), make(clickhouse.TableMap))
	cw := ClickhouseQueryTranslator{lm}
	for _, tt := range testdata.TestsAsyncSearch {
		t.Run(tt.Name, func(t *testing.T) {
			query, queryInfo := cw.parseQueryAsyncSearch(tt.QueryJson)
			assert.True(t, query.CanParse)
			assert.Equal(t, tt.WantedParseResult, queryInfo)
		})
	}
}
