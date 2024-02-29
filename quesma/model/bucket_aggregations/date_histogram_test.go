package bucket_aggregations

import (
	"github.com/stretchr/testify/assert"
	"mitmproxy/quesma/model"
	"testing"
)

func TestTranslateSqlResponseToJson(t *testing.T) {
	resultRows := []model.QueryResultRow{
		{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", int64(56962398)), model.NewQueryResultCol("doc_count", 8)}},
		{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", int64(56962370)), model.NewQueryResultCol("doc_count", 14)}},
	}
	interval := "30s"
	expectedResponse := []model.JsonMap{
		{"key": int64(56962398) * 30_000, "doc_count": 8, "key_as_string": "2024-02-25T14:39:00.000"},
		{"key": int64(56962370) * 30_000, "doc_count": 14, "key_as_string": "2024-02-25T14:25:00.000"},
	}
	response := QueryTypeDateHistogram{Interval: interval}.TranslateSqlResponseToJson(resultRows, 1)
	assert.Equal(t, expectedResponse, response)
}
