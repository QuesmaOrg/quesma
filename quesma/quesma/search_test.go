package quesma

import (
	"github.com/stretchr/testify/assert"
	"mitmproxy/quesma/clickhouse"
	"mitmproxy/quesma/queryparser"
	"testing"
)

func Test(t *testing.T) {
	t.Log("Test")
	requestBody := ([]byte)(`{
		"query": {
			"match_all": {}
		}
	}`)
	lm := clickhouse.NewLogManagerEmpty()
	queryTranslator := &queryparser.ClickhouseQueryTranslator{ClickhouseLM: lm}
	query, queryInfo := queryTranslator.WriteAsyncSearch(requestBody)
	assert.True(t, query.CanParse)
	assert.Equal(t, `SELECT * FROM "logs-generic-default"`, query.Sql)
	assert.Equal(t, queryparser.NewQueryInfoNone(), queryInfo)
}
