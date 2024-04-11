package queryparser

import (
	"context"
	"encoding/json"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"mitmproxy/quesma/clickhouse"
	"mitmproxy/quesma/concurrent"
	"mitmproxy/quesma/model"
	"mitmproxy/quesma/quesma/config"
	"mitmproxy/quesma/util"
	"testing"
	"time"
)

const (
	asyncRequestIdStr                 = "0"
	asyncSearchResponseExpectedString = `
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
)

func TestSearchResponse(t *testing.T) {
	row := []model.QueryResultRow{{}}
	cw := ClickhouseQueryTranslator{Table: &clickhouse.Table{Name: "test"}, Ctx: context.Background()}
	searchRespBuf, err := cw.MakeAsyncSearchResponseMarshalled(row, model.ListAllFields, NewEmptyHighlighter(), asyncRequestIdStr, false)
	require.NoError(t, err)
	var searchResponseResult model.SearchResp
	err = json.Unmarshal(searchRespBuf, &searchResponseResult)
	require.NoError(t, err)
	var searchResponseExpected model.SearchResp
	err = json.Unmarshal([]byte(asyncSearchResponseExpectedString), &searchResponseExpected)
	require.NoError(t, err)
	assert.Equal(t, searchResponseExpected, searchResponseResult)
	require.NoError(t, err)
}

func TestMakeResponseSearchQuery(t *testing.T) {
	var args = []struct {
		elasticResponseJson string
		ourQueryResult      model.QueryResultRow
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
			model.QueryResultRow{
				Cols: []model.QueryResultCol{
					model.NewQueryResultCol("source", "ubuntu"),
					model.NewQueryResultCol("@timestamp", "2024-01-30T14:48:20.962Z"),
				},
			},
			model.Normal,
		},
	}

	cw := ClickhouseQueryTranslator{Table: &clickhouse.Table{Name: "test"}, Ctx: context.Background()}
	for i, tt := range args {
		t.Run(tt.queryType.String(), func(t *testing.T) {
			ourResponse, err := cw.MakeSearchResponseMarshalled([]model.QueryResultRow{args[i].ourQueryResult}, args[i].queryType, NewEmptyHighlighter())
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
		ourQueryResult      []model.QueryResultRow
		queryType           model.SearchQueryType
	}{
		{
			`
	{
		"completion_status": 200,
		"completion_time_in_millis": 1706642705532,
		"expiration_time_in_millis": 1706642765524,
		"is_partial": false,
  		"is_running": false,
		"id": 0,
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
			[]model.QueryResultRow{
				{
					Cols: []model.QueryResultCol{
						model.NewQueryResultCol("key", "hercules"),
						model.NewQueryResultCol("doc_count", uint64(3)),
					},
				},
				{
					Cols: []model.QueryResultCol{
						model.NewQueryResultCol("key", "athena"),
						model.NewQueryResultCol("doc_count", uint64(2)),
					},
				},
			},
			model.Facets,
		},
		{
			`
	{
		"is_partial": false,
		"is_running": false,
		"completion_status": 200,
		"start_time_in_millis": 1706643496415,
		"id": 0,
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
			[]model.QueryResultRow{
				{
					Cols: []model.QueryResultCol{
						model.NewQueryResultCol("message", "User deleted"),
					},
				},
				{
					Cols: []model.QueryResultCol{
						model.NewQueryResultCol("message", "User updated"),
					},
				},
				{
					Cols: []model.QueryResultCol{
						model.NewQueryResultCol("message", "User created"),
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
		"completion_status": 200,
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
				"max_score": null,
					  "total": {
						"relation": "eq",
						"value": 1
				  }
			},
			"timed_out": false,
			"took": 9
  		},
		"start_time_in_millis": 1706643613499
	}`,
			[]model.QueryResultRow{
				{
					Cols: []model.QueryResultCol{
						model.NewQueryResultCol("message", "User logged out"),
						model.NewQueryResultCol("host.name", "apollo"),
						model.NewQueryResultCol("host.name.text", "apollo"),
						model.NewQueryResultCol("@timestamp", "2024-01-30T19:38:54.607Z"),
						model.NewQueryResultCol("service.name", "frontend"),
						model.NewQueryResultCol("service.name.text", "frontend"),
						model.NewQueryResultCol("severity", "warning"),
						model.NewQueryResultCol("source", "hyperv"),
						model.NewQueryResultCol("data_stream.type", "logs"),
					},
				},
			},
			model.ListAllFields,
		},
	}
	cw := ClickhouseQueryTranslator{Table: &clickhouse.Table{Name: "test"}, Ctx: context.Background()}
	for i, tt := range args {
		t.Run(tt.queryType.String(), func(t *testing.T) {
			ourResponse, err := cw.MakeAsyncSearchResponseMarshalled(args[i].ourQueryResult, args[i].queryType, NewEmptyHighlighter(), asyncRequestIdStr, false)
			assert.NoError(t, err)

			difference1, difference2, err := util.JsonDifference(args[i].elasticResponseJson, string(ourResponse))
			assert.NoError(t, err)
			assert.Empty(t, difference1)
			assert.Empty(t, difference2)
		})
	}
}

// tests MakeSearchResponse, in particular if JSON we return is a proper JSON.
// used to fail before we fixed field quoting.
func TestMakeResponseSearchQueryIsProperJson(t *testing.T) {
	cw := ClickhouseQueryTranslator{ClickhouseLM: nil, Table: clickhouse.NewEmptyTable("@"), Ctx: context.Background()}
	queries := []*model.Query{
		cw.BuildSimpleSelectQuery(""),
		cw.BuildNRowsQuery("@", SimpleQuery{}, 0),
	}
	for _, query := range queries {
		resultRow := model.QueryResultRow{Cols: make([]model.QueryResultCol, 0)}
		for _, field := range query.NonSchemaFields {
			resultRow.Cols = append(resultRow.Cols, model.QueryResultCol{ColName: field, Value: "not-important"})
		}
		_, err := cw.MakeSearchResponse([]model.QueryResultRow{resultRow}, model.Normal, NewEmptyHighlighter())
		assert.NoError(t, err)
	}
}

// tests MakeAsyncSearchResponse, in particular if JSON we return is a proper JSON.
// used to fail before we fixed field quoting.
func TestMakeResponseAsyncSearchQueryIsProperJson(t *testing.T) {
	table, _ := clickhouse.NewTable(`CREATE TABLE `+tableName+`
		( "message" String, "timestamp" DateTime )
		ENGINE = Memory`,
		clickhouse.NewNoTimestampOnlyStringAttrCHConfig(),
	)
	lm := clickhouse.NewLogManager(concurrent.NewMapWith(tableName, table), config.QuesmaConfiguration{})
	cw := ClickhouseQueryTranslator{ClickhouseLM: lm, Table: table, Ctx: context.Background()}
	queries := []*model.Query{
		cw.BuildAutocompleteSuggestionsQuery("@", "", 0),
		cw.BuildFacetsQuery("@", newSimpleQuery(NewSimpleStatement(""), true), 0),
		// queryTranslator.BuildTimestampQuery("@", "@", "", true), TODO uncomment when add unification for this query type
	}
	types := []model.SearchQueryType{model.ListAllFields, model.ListByField}
	for i, query := range queries {
		resultRow := model.QueryResultRow{Cols: make([]model.QueryResultCol, 0)}
		for j, field := range query.NonSchemaFields {
			var value interface{} = "not-important"
			if j == model.ResultColDocCountIndex {
				value = uint64(5)
			}
			resultRow.Cols = append(resultRow.Cols, model.QueryResultCol{ColName: field, Value: value})
		}
		_, err := cw.MakeAsyncSearchResponse([]model.QueryResultRow{resultRow}, types[i], NewEmptyHighlighter(), asyncRequestIdStr, false)
		assert.NoError(t, err)
	}
}

func Test_extractAndCalculateDuration(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  time.Duration
	}{
		{
			name:  "1 minute",
			input: `"@timestamp">=parseDateTime64BestEffort('2024-02-13T10:04:40.703Z') AND "@timestamp"<=parseDateTime64BestEffort('2024-02-13T10:05:40.703Z')"`,
			want:  1 * time.Minute,
		},
		{
			name:  "5 minutes",
			input: `"@timestamp">=parseDateTime64BestEffort('2024-02-13T10:04:40.703Z') AND "@timestamp"<=parseDateTime64BestEffort('2024-02-13T10:09:40.703Z')"`,
			want:  5 * time.Minute,
		},
		{
			name:  "15 minutes",
			input: `"@timestamp">=parseDateTime64BestEffort('2024-02-13T10:04:40.703Z') AND "@timestamp"<=parseDateTime64BestEffort('2024-02-13T10:19:40.703Z')"`,
			want:  15 * time.Minute,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, _ := durationFromWhere(tt.input)
			assert.Equalf(t, tt.want, got, "extractAndCalculateDuration(%v)", tt.input)
		})
	}
}
