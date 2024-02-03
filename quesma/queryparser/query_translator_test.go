package queryparser

import (
	"encoding/json"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"mitmproxy/quesma/clickhouse"
	"mitmproxy/quesma/model"
	"testing"
)

type Row struct {
}

const searchResponseExpectedString = `
{
		"took": 0,
		"timed_out": false,
		"_shards": {
			"total": 0,
			"successful": 0,
			"failed": 0,
			"failures": null,
			"skipped": 0
		},
		"hits": {
			"total": {
				"value": 1,
				"relation": ""
		},
		"max_score": 0,
		"hits": [{
				"_index": "",
				"_id": "",
				"_score": 0,
				"_source": {` + "\n" + `"@timestamp": "2024-01-01"` + "\n" + `},
				"_type": "",
				"sort": null
		}]
	  },
	  "errors": false,
	  "aggregations": null
}
`

const asyncSearchResponseExpectedString = `
{
	"completion_time_in_millis": 0,
	"expiration_time_in_millis": 0,
	"id": "",
	"is_partial": false,
	"is_running": false,
	"response":{
		"took": 0,
		"timed_out": false,
		"_shards": {
			"total": 0,
			"successful": 0,
			"failed": 0,
			"failures": null,
			"skipped": 0
		},
		"hits": {
			"total": {
				"value": 1,
				"relation": ""
		},
		"max_score": 0,
		"hits": [{
				"_index": "",
				"_id": "",
				"_score": 0,
				"_source": {` + "\n" + `"@timestamp": "2024-01-01"` + "\n" + `},
				"_type": "",
				"sort": null
		}]
	  },
	  "errors": false,
	  "aggregations": null
	}
}
`

func (row Row) String() string {
	return `{"@timestamp":  "2024-01-01"}`
}

func TestSearchResponse(t *testing.T) {
	{
		row := []Row{{}}

		searchRespBuf, err := MakeResponse(row, false)
		require.NoError(t, err)
		var searchResponseResult model.SearchResp
		err = json.Unmarshal([]byte(searchRespBuf), &searchResponseResult)
		require.NoError(t, err)
		var searchResponseExpected model.SearchResp
		err = json.Unmarshal([]byte(searchResponseExpectedString), &searchResponseExpected)
		require.NoError(t, err)

		assert.Equal(t, searchResponseExpected, searchResponseResult)
		require.NoError(t, err)
	}

	{
		row := []Row{{}}

		searchRespBuf, err := MakeResponse(row, true)
		require.NoError(t, err)
		var searchResponseResult model.Response
		err = json.Unmarshal([]byte(searchRespBuf), &searchResponseResult)
		require.NoError(t, err)
		var searchResponseExpected model.Response
		err = json.Unmarshal([]byte(asyncSearchResponseExpectedString), &searchResponseExpected)
		require.NoError(t, err)

		assert.Equal(t, searchResponseExpected, searchResponseResult)
		require.NoError(t, err)
	}
}

// tests MakeResponse, in particular if JSON we return is a proper JSON.
// used to fail before we fixed field quoting.
func TestMakeResponse(t *testing.T) {
	queryTranslator := ClickhouseQueryTranslator{}
	builtQueries := []*model.Query{
		queryTranslator.BuildSimpleSelectQuery("@", ""),
		queryTranslator.BuildSimpleCountQuery("@", ""),
		queryTranslator.BuildNMostRecentRowsQuery("a", "@", "", "", 0),
		queryTranslator.BuildHistogramQuery("a@", "@", ""),
		queryTranslator.BuildAutocompleteSuggestionsQuery("@", "@", "", 0),
		queryTranslator.BuildFacetsQuery("@", "@", "", 0),
		queryTranslator.BuildTimestampQuery("@", "@", "", true),
	}
	for _, query := range builtQueries {
		resultRow := clickhouse.QueryResultRow{Cols: make([]clickhouse.QueryResultCol, 0)}
		for _, field := range query.NonSchemaFields {
			resultRow.Cols = append(resultRow.Cols, clickhouse.QueryResultCol{ColName: field, Value: "not-important"})
		}
		_, err := MakeResponse([]clickhouse.QueryResultRow{resultRow}, false)
		assert.NoError(t, err)
		_, err = MakeResponse([]clickhouse.QueryResultRow{resultRow}, true)
		assert.NoError(t, err)
	}
}
