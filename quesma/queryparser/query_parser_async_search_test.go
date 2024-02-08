package queryparser

import (
	"github.com/stretchr/testify/assert"
	"mitmproxy/quesma/clickhouse"
	"mitmproxy/quesma/testdata"
	"testing"
)

func TestQueryParserAsyncSearch(t *testing.T) {
	lm := clickhouse.NewLogManager(make(clickhouse.TableMap), make(clickhouse.TableMap))
	cw := ClickhouseQueryTranslator{ClickhouseLM: lm, TableName: `"logs-generic-default"`}
	for _, tt := range testdata.TestsAsyncSearch {
		t.Run(tt.Name, func(t *testing.T) {
			query, queryInfo := cw.ParseQueryAsyncSearch(tt.QueryJson)
			assert.True(t, query.CanParse)
			assert.Equal(t, tt.WantedParseResult, queryInfo)
		})
	}
}
