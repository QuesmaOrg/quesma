package queryparser

import (
	"encoding/json"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
