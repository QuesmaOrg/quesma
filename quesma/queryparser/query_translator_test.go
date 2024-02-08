package queryparser

import (
	"encoding/json"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"mitmproxy/quesma/clickhouse"
	"mitmproxy/quesma/model"
	"mitmproxy/quesma/util"
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
				"skipped": 0
			},
			"hits": {
				"total": {
					"value": 1,
					"relation": "eq"
			},
			"max_score": 0,
			"hits": [{
					"_index": "",
					"_id": "",
					"_score": 0,
					"_source": {` + "\n" + `          "@timestamp": "2024-01-01"` + "\n" + `        }
			}]
		  }
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
					"_type": ""
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

		searchRespBuf, err := MakeResponseSearchQuery(row, model.Normal)
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
		row := []clickhouse.QueryResultRow{{}}

		searchRespBuf, err := MakeResponseAsyncSearchQuery(row, model.ListAllFields)
		require.NoError(t, err)
		var searchResponseResult model.AsyncSearchResp
		err = json.Unmarshal([]byte(searchRespBuf), &searchResponseResult)
		require.NoError(t, err)
		var searchResponseExpected model.AsyncSearchResp
		err = json.Unmarshal([]byte(asyncSearchResponseExpectedString), &searchResponseExpected)
		require.NoError(t, err)
		assert.Equal(t, searchResponseExpected, searchResponseResult)
		require.NoError(t, err)
	}
}

func TestMakeResponseSearchQuery(t *testing.T) {
	var args = []struct {
		elasticResponseJson string
		ourQueryResult      clickhouse.QueryResultRow
		queryType           model.SearchQueryType
	}{
		{
			`
	{
		"_shards": {
			"failed": 0,
			"skipped": 0,
			"successful": 2,
			"total": 2
		},
		"hits": {
			"hits": [
				{
					"_id": "vZ_XWo0B384RLK7zriXv",
					"_index": ".ds-logs-generic-default-2024.01.30-000001",
					"_score": 1,
					"_source": {
						"@timestamp": "2024-01-30T14:48:20.962Z",
						"source": "ubuntu"
					}
				},
				{
					"_id": "sp_XWo0B384RLK7zqiVB",
					"_index": ".ds-logs-generic-default-2024.01.30-000001",
					"_score": 1,
					"_source": {
						"@timestamp": "2024-01-30T14:48:19.761Z",
						"source": "suse"
					}
				}
			],
			"max_score": 1,
			"total": {
				"relation": "eq",
				"value": 1327
			}
		},
		"timed_out": false,
		"took": 1
	}`,
			clickhouse.QueryResultRow{
				Cols: []clickhouse.QueryResultCol{
					clickhouse.NewQueryResultCol("source", "ubuntu"),
					clickhouse.NewQueryResultCol("@timestamp", "2024-01-30T14:48:20.962Z"),
				},
			},
			model.Normal,
		},
		{
			`
	{
		"_shards": {
			"failed": 0,
			"skipped": 0,
			"successful": 2,
			"total": 2
		},
		"aggregations": {
			"suggestions": {
				"buckets": [],
				"doc_count_error_upper_bound": 0,
				"sum_other_doc_count": 0
			},
			"unique_terms": {
				"value": 0
			}
		},
		"hits": {
			"hits": [],
			"max_score": null,
			"total": {
				"relation": "eq",
				"value": 1376
			}
		},
		"terminated_early": false,
		"timed_out": false,
		"took": 1
	}`,
			clickhouse.QueryResultRow{
				Cols: []clickhouse.QueryResultCol{
					clickhouse.NewQueryResultCol("count()", 1376),
				},
			},
			model.Count,
		},
	}

	for i, tt := range args {
		t.Run(tt.queryType.String(), func(t *testing.T) {
			ourResponse, err := MakeResponseSearchQuery([]clickhouse.QueryResultRow{args[i].ourQueryResult}, args[i].queryType)
			assert.NoError(t, err)

			difference1, difference2, err := util.JsonDifference(args[i].elasticResponseJson, string(ourResponse))
			if err != nil {
				t.Error(err)
			}
			assert.Empty(t, difference1)
			assert.Empty(t, difference2)
		})
	}
}

func TestMakeResponseAsyncSearchQuery(t *testing.T) {
	var args = []struct {
		elasticResponseJson string
		ourQueryResult      []clickhouse.QueryResultRow
		queryType           model.AsyncSearchQueryType
	}{
		{
			`
	{
		"completion_time_in_millis": 1706639337527,
		"expiration_time_in_millis": 1706639397521,
		"id": "FnhMY09KX3ZLUmFDeGtjLU1YM1RMMGccTTF2dnY2R0dSNEtZYVQ3cjR5ZnBuQTo3NjM0MQ==",
		"is_partial": false,
		"is_running": false,
		"response": {
			"_shards": {
				"failed": 0,
				"skipped": 0,
				"successful": 1,
				"total": 1
			},
			"aggregations": {
				"0": {
					"buckets": [
						{
							"doc_count": 1,
							"key": 1706638410000,
							"key_as_string": "2024-01-30T19:13:30.000+01:00"
						},
						{
							"doc_count": 14,
							"key": 1706638440000,
							"key_as_string": "2024-01-30T19:14:00.000+01:00"
						}
					]
				}
			},
			"hits": {
				"hits": [],
				"max_score": null,
				"total": {
					"relation": "eq",
					"value": 87
				}
			},
			"timed_out": false,
			"took": 6
		},
		"start_time_in_millis": 1706639337521
	}`,
			[]clickhouse.QueryResultRow{
				{
					Cols: []clickhouse.QueryResultCol{
						clickhouse.NewQueryResultCol("key", 1706638410000),
						clickhouse.NewQueryResultCol("doc_count", uint64(1)),
						clickhouse.NewQueryResultCol("key_as_string", "2024-01-30T19:13:30.000+01:00"),
					},
				},
				{
					Cols: []clickhouse.QueryResultCol{
						clickhouse.NewQueryResultCol("key", 1706638440000),
						clickhouse.NewQueryResultCol("doc_count", uint64(14)),
						clickhouse.NewQueryResultCol("key_as_string", "2024-01-30T19:14:00.000+01:00"),
					},
				},
			},
			model.Histogram,
		},
		{
			`
	{
		"completion_time_in_millis": 1706642705532,
		"expiration_time_in_millis": 1706642765524,
		"is_partial": false,
  		"is_running": false,
  		"response": {
			"_shards": {
				"failed": 0,
				"skipped": 0,
				"successful": 1,
				"total": 1
			},
			"aggregations": {
				"sample": {
					"doc_count": 27,
					"sample_count": {
						"value": 27
					},
					"top_values": {
						"buckets": [
							{
								"doc_count": 3,
								"key": "hercules"
							},
							{
								"doc_count": 2,
								"key": "athena"
							}
						],
						"doc_count_error_upper_bound": 0,
						"sum_other_doc_count": 9
					}
				}
			},
			"hits": {
				"hits": [],
				"max_score": null,
				"total": {
					"relation": "eq",
					"value": 27
				}
			},
			"timed_out": false,
			"took": 8
		},
		"start_time_in_millis": 1706642705524
	}`,
			[]clickhouse.QueryResultRow{
				{
					Cols: []clickhouse.QueryResultCol{
						clickhouse.NewQueryResultCol("key", "hercules"),
						clickhouse.NewQueryResultCol("doc_count", uint64(3)),
					},
				},
				{
					Cols: []clickhouse.QueryResultCol{
						clickhouse.NewQueryResultCol("key", "athena"),
						clickhouse.NewQueryResultCol("doc_count", uint64(2)),
					},
				},
			},
			model.AggsByField,
		},
		{
			`
	{
		"is_partial": false,
		"is_running": false,
		"start_time_in_millis": 1706643496415,
		"expiration_time_in_millis": 1706643556415,
		"completion_time_in_millis": 1706643496422,
		"response": {
			"took": 7,
			"timed_out": false,
			"_shards": {
				"total": 1,
				"successful": 1,
				"skipped": 0,
				"failed": 0
			},
			"hits": {
				"total": {
					"value": 423,
					"relation": "eq"
				},
				"max_score": 0,
				"hits": [
					{
						"_index": ".ds-logs-generic-default-2024.01.30-000001",
						"_id": "YufhW40BF3dSPdkaDfTu",
						"_score": 0,
						"fields": {
							"message": [
							  "User deleted"
							]
						}
					},
					{
						"_index": ".ds-logs-generic-default-2024.01.30-000001",
						"_id": "V-fhW40BF3dSPdkaAvT3",
						"_score": 0,
						"fields": {
							"message": [
							  	"User updated"
							]
						}
					},
					{
						"_index": ".ds-logs-generic-default-2024.01.30-000001",
						"_id": "v-fWW40BF3dSPdkafuWP",
						"_score": 0,
						"fields": {
							"message": [
								"User created"
							]
						}
					}
      			]
			}
		}
	}`,
			[]clickhouse.QueryResultRow{
				{
					Cols: []clickhouse.QueryResultCol{
						clickhouse.NewQueryResultCol("message", "User deleted"),
					},
				},
				{
					Cols: []clickhouse.QueryResultCol{
						clickhouse.NewQueryResultCol("message", "User updated"),
					},
				},
				{
					Cols: []clickhouse.QueryResultCol{
						clickhouse.NewQueryResultCol("message", "User created"),
					},
				},
			},

			model.ListByField,
		},
		{
			`
	{
		"completion_time_in_millis": 1706643613508,
		"expiration_time_in_millis": 1706643673499,
		"id": "FlpqVDhsdkZJVFBTVDFJV2Q5T2l6Q0EdTTF2dnY2R0dSNEtZYVQ3cjR5ZnBuQToxMzM1NDA=",
		"is_partial": false,
		"is_running": false,
		"response": {
			"_shards": {
				"failed": 0,
				"skipped": 0,
				"successful": 1,
				"total": 1
			},
			"hits": {
				"hits": [
					{
						"_id": "BufiW40BF3dSPdkaU_bj",
						"_index": ".ds-logs-generic-default-2024.01.30-000001",
						"_score": null,
						"_version": 1,
						"fields": {
							"@timestamp": [
								"2024-01-30T19:39:35.767Z"
							],
							"data_stream.type": [
								"logs"
							],
							"host.name": [
								"apollo"
							],
							"host.name.text": [
								"apollo"
							],
							"message": [
								"User password changed"
							],
							"service.name": [
								"frontend"
							],
            				"service.name.text": [
								"frontend"
							],
							"severity": [
								"info"
							],
							"source": [
								"alpine"
							]
						},
						"sort": [
							"2024-01-30T19:39:35.767Z",
							16
						]
					},
					{
						"_id": "R-fhW40BF3dSPdkas_UW",
						"_index": ".ds-logs-generic-default-2024.01.30-000001",
						"_score": null,
						"_version": 1,
						"fields": {
							"@timestamp": [
								"2024-01-30T19:38:54.607Z"
							],
							"data_stream.type": [
								"logs"
							],
							"host.name": [
								"apollo"
							],
							"host.name.text": [
								"apollo"
							],
							"message": [
								"User logged out"
							],
							"service.name": [
								"proxy"
							],
							"service.name.text": [
								"proxy"
							],
							"severity": [
								"warning"
							],
							"source": [
								"hyperv"
							]
						},
						"sort": [
							"2024-01-30T19:38:54.607Z",
							2944
						]
					}
				],
				"max_score": null
			},
			"timed_out": false,
			"took": 9
  		},
		"start_time_in_millis": 1706643613499
	}`,
			[]clickhouse.QueryResultRow{
				{
					Cols: []clickhouse.QueryResultCol{
						clickhouse.NewQueryResultCol("message", "User logged out"),
						clickhouse.NewQueryResultCol("host.name", "apollo"),
						clickhouse.NewQueryResultCol("host.name.text", "apollo"),
						clickhouse.NewQueryResultCol("@timestamp", "2024-01-30T19:38:54.607Z"),
						clickhouse.NewQueryResultCol("service.name", "frontend"),
						clickhouse.NewQueryResultCol("service.name.text", "frontend"),
						clickhouse.NewQueryResultCol("severity", "warning"),
						clickhouse.NewQueryResultCol("source", "hyperv"),
						clickhouse.NewQueryResultCol("data_stream.type", "logs"),
					},
				},
			},
			model.ListAllFields,
		},
	}

	for i, tt := range args {
		t.Run(tt.queryType.String(), func(t *testing.T) {
			ourResponse, err := MakeResponseAsyncSearchQuery(args[i].ourQueryResult, args[i].queryType)
			assert.NoError(t, err)

			difference1, difference2, err := util.JsonDifference(args[i].elasticResponseJson, string(ourResponse))
			assert.NoError(t, err)
			assert.Empty(t, difference1)
			assert.Empty(t, difference2)
		})
	}
}

// tests MakeResponseSearchQuery, in particular if JSON we return is a proper JSON.
// used to fail before we fixed field quoting.
func TestMakeResponseSearchQueryIsProperJson(t *testing.T) {
	queryTranslator := ClickhouseQueryTranslator{}
	queries := []*model.Query{
		queryTranslator.BuildSimpleSelectQuery("@", ""),
		queryTranslator.BuildSimpleCountQuery("@", ""),
		queryTranslator.BuildNMostRecentRowsQuery("a", "@", "", "", 0),
	}
	types := []model.SearchQueryType{model.Normal, model.Count, model.Normal}
	for i, query := range queries {
		resultRow := clickhouse.QueryResultRow{Cols: make([]clickhouse.QueryResultCol, 0)}
		for _, field := range query.NonSchemaFields {
			resultRow.Cols = append(resultRow.Cols, clickhouse.QueryResultCol{ColName: field, Value: "not-important"})
		}
		_, err := MakeResponseSearchQuery([]clickhouse.QueryResultRow{resultRow}, types[i])
		assert.NoError(t, err)
	}
}

// tests MakeResponseAsyncSearchQuery, in particular if JSON we return is a proper JSON.
// used to fail before we fixed field quoting.
func TestMakeResponseAsyncSearchQueryIsProperJson(t *testing.T) {
	queryTranslator := ClickhouseQueryTranslator{}
	queries := []*model.Query{
		queryTranslator.BuildHistogramQuery("a@", "@", ""),
		queryTranslator.BuildAutocompleteSuggestionsQuery("@", "@", "", 0),
		queryTranslator.BuildFacetsQuery("@", "@", "", 0),
		// queryTranslator.BuildTimestampQuery("@", "@", "", true), TODO uncomment when add unification for this query type
	}
	types := []model.AsyncSearchQueryType{model.Histogram, model.ListAllFields, model.ListByField} //, model.EarliestLatestTimestamp}
	for i, query := range queries {
		resultRow := clickhouse.QueryResultRow{Cols: make([]clickhouse.QueryResultCol, 0)}
		for j, field := range query.NonSchemaFields {
			var value interface{} = "not-important"
			if j == clickhouse.DocCount {
				value = uint64(5)
			}
			resultRow.Cols = append(resultRow.Cols, clickhouse.QueryResultCol{ColName: field, Value: value})
		}
		_, err := MakeResponseAsyncSearchQuery([]clickhouse.QueryResultRow{resultRow}, types[i])
		assert.NoError(t, err)
	}
}
