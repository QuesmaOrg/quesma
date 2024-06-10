package queryparser

import (
	"context"
	"encoding/json"
	"github.com/k0kubun/pp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"mitmproxy/quesma/clickhouse"
	"mitmproxy/quesma/concurrent"
	"mitmproxy/quesma/model"
	"mitmproxy/quesma/queryparser/query_util"
	"mitmproxy/quesma/quesma/config"
	"mitmproxy/quesma/util"
	"reflect"
	"strconv"
	"testing"
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
	searchResp, err := cw.MakeAsyncSearchResponse(row, &model.Query{QueryInfoType: model.ListAllFields, Highlighter: NewEmptyHighlighter()}, asyncRequestIdStr, false)
	require.NoError(t, err)
	searchRespBuf, err2 := searchResp.Marshal()
	require.NoError(t, err2)
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
		ourQueryResult      []model.QueryResultRow
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
					},
					"fields": {
						"source": ["ubuntu"],
						"@timestamp": ["2024-01-30T14:48:20.962Z"]
					}
				},
				{
					"_id": "sp_XWo0B384RLK7zqiVB",
					"_index": ".ds-logs-generic-default-2024.01.30-000001",
					"_score": 1,
					"_source": {
						"@timestamp": "2024-01-30T14:48:19.761Z",
						"source": "suse"
					},
					"fields": {
						"source": ["suse"],
						"@timestamp": ["2024-01-30T14:48:19.761Z"]
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
			[]model.QueryResultRow{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("source", "ubuntu"),
					model.NewQueryResultCol("@timestamp", "2024-01-30T14:48:20.962Z"),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("source", "suse"),
					model.NewQueryResultCol("@timestamp", "2024-01-30T14:48:19.761Z"),
				}},
			},
			model.Normal,
		},
	}

	cw := ClickhouseQueryTranslator{Table: &clickhouse.Table{Name: "test"}, Ctx: context.Background()}
	for i, tt := range args {
		t.Run(tt.queryType.String(), func(t *testing.T) {
			ourResponseRaw := cw.MakeSearchResponse(
				[]*model.Query{
					query_util.BuildHitsQuery(
						context.Background(), "test", "*",
						&model.SimpleQuery{FieldName: "*"}, model.WeNeedUnlimitedCount,
					),
				},
				[][]model.QueryResultRow{args[i].ourQueryResult},
			)
			ourResponse, err := ourResponseRaw.Marshal()
			assert.NoError(t, err)
			actualMinusExpected, expectedMinusActual, err := util.JsonDifference(string(ourResponse), args[i].elasticResponseJson)
			if err != nil {
				t.Error(err)
			}
			assert.Empty(t, actualMinusExpected)
			assert.Empty(t, expectedMinusActual)
		})
	}
}

func TestMakeResponseAsyncSearchQuery(t *testing.T) {
	cw := ClickhouseQueryTranslator{Table: &clickhouse.Table{Name: "test"}, Ctx: context.Background()}
	var args = []struct {
		elasticResponseJson string
		ourQueryResult      []model.QueryResultRow
		query               *model.Query
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
			cw.BuildFacetsQuery("not-important", &model.SimpleQuery{}, false),
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
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("message", "User deleted")}},
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("message", "User updated")}},
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("message", "User created")}},
			},
			query_util.BuildHitsQuery(context.Background(), "test", "message", &model.SimpleQuery{}, model.WeNeedUnlimitedCount),
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
											"apollo"
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
											"apollo"
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
				{
					Cols: []model.QueryResultCol{
						model.NewQueryResultCol("message", "User password changed"),
						model.NewQueryResultCol("host.name", "apollo"),
						model.NewQueryResultCol("host.name.text", "apollo"),
						model.NewQueryResultCol("@timestamp", "2024-01-30T19:39:35.767Z"),
						model.NewQueryResultCol("service.name", "proxy"),
						model.NewQueryResultCol("service.name.text", "proxy"),
						model.NewQueryResultCol("severity", "info"),
						model.NewQueryResultCol("source", "alpine"),
						model.NewQueryResultCol("data_stream.type", "logs"),
					},
				},
			},
			query_util.BuildHitsQuery(context.Background(), "test", "*", &model.SimpleQuery{}, model.WeNeedUnlimitedCount)},
	}
	for i, tt := range args {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			if i != 0 {
				t.Skip()
			}
			ourResponse, err := cw.MakeAsyncSearchResponse(args[i].ourQueryResult, tt.query, asyncRequestIdStr, false)
			assert.NoError(t, err)
			ourResponseBuf, err2 := ourResponse.Marshal()
			assert.NoError(t, err2)

			actualMinusExpected, expectedMinusActual, err := util.JsonDifference(string(ourResponseBuf), args[i].elasticResponseJson)
			pp.Println(actualMinusExpected, expectedMinusActual)
			assert.NoError(t, err)

			acceptableDifference := []string{"sort", "_score", "_version"}
			assert.True(t, util.AlmostEmpty(actualMinusExpected, acceptableDifference), "actualMinusExpected: %s", actualMinusExpected)
			assert.True(t, util.AlmostEmpty(expectedMinusActual, acceptableDifference), "expectedMinusActual: %s", expectedMinusActual)
		})
	}
}

// tests MakeSearchResponse, in particular if JSON we return is a proper JSON.
// used to fail before we fixed field quoting.
func TestMakeResponseSearchQueryIsProperJson(t *testing.T) {
	cw := ClickhouseQueryTranslator{ClickhouseLM: nil, Table: clickhouse.NewEmptyTable("@"), Ctx: context.Background()}
	const limit = 1000
	queries := []*model.Query{
		cw.BuildNRowsQuery("*", &model.SimpleQuery{}, limit),
		cw.BuildNRowsQuery("@", &model.SimpleQuery{}, 0),
	}
	for _, query := range queries {
		resultRow := model.QueryResultRow{Cols: make([]model.QueryResultCol, 0)}
		for _, field := range query.Columns {
			resultRow.Cols = append(resultRow.Cols, model.QueryResultCol{ColName: field.Alias, Value: "not-important"})
		}
		_ = cw.MakeSearchResponse([]*model.Query{{QueryInfoType: model.Normal, Highlighter: NewEmptyHighlighter()}}, [][]model.QueryResultRow{{resultRow}})
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
		cw.BuildFacetsQuery("@", &model.SimpleQuery{}, true),
		cw.BuildFacetsQuery("@", &model.SimpleQuery{}, false),
		// queryTranslator.BuildTimestampQuery("@", "@", "", true), TODO uncomment when add unification for this query type
	}
	types := []model.SearchQueryType{model.ListAllFields, model.FacetsNumeric, model.Facets}
	for i, query := range queries {
		resultRow := model.QueryResultRow{Cols: make([]model.QueryResultCol, 0)}
		for j, field := range query.Columns {
			var value interface{} = "not-important"
			if j == model.ResultColDocCountIndex {
				value = uint64(5)
			}
			resultRow.Cols = append(resultRow.Cols, model.QueryResultCol{ColName: field.Alias, Value: value})
		}
		_, err := cw.MakeAsyncSearchResponse([]model.QueryResultRow{resultRow}, &model.Query{QueryInfoType: types[i], Highlighter: NewEmptyHighlighter()}, asyncRequestIdStr, false)
		assert.NoError(t, err)
	}
}

func Test_makeSearchResponseFacetsNumericInts(t *testing.T) {
	oneUint8 := uint8(1)
	cw := ClickhouseQueryTranslator{Table: &clickhouse.Table{Name: "test"}, Ctx: context.Background()}
	var testcases = []struct {
		name                 string
		rows                 []model.QueryResultRow
		wantedAggregationMap JsonMap
	}{
		{
			name: "2 buckets, all present",
			rows: []model.QueryResultRow{
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", int64(1)), model.NewQueryResultCol("doc_count", uint64(2))}},
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", int8(3)), model.NewQueryResultCol("doc_count", uint64(4))}}, // maybe in future we'd like to use that all rows have same types (here we have mixed int8 and int64), but now let's use different to test more cases
			},
			wantedAggregationMap: JsonMap{
				"sample": JsonMap{
					"min_value":    JsonMap{"value": int64(1)},
					"max_value":    JsonMap{"value": int64(3)},
					"doc_count":    6,
					"sample_count": JsonMap{"value": 6},
					"top_values": JsonMap{
						"buckets": []JsonMap{
							{"key": int64(1), "doc_count": uint64(2)},
							{"key": int8(3), "doc_count": uint64(4)},
						},
						"sum_other_doc_count":         0,
						"doc_count_error_upper_bound": 0,
					},
				},
			},
		},
		{
			name: "1 bucket, all nulls",
			rows: []model.QueryResultRow{
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", nil), model.NewQueryResultCol("doc_count", uint64(2))}},
			},
			wantedAggregationMap: JsonMap{
				"sample": JsonMap{
					"min_value":    JsonMap{"value": nil},
					"max_value":    JsonMap{"value": nil},
					"doc_count":    2,
					"sample_count": JsonMap{"value": 2},
					"top_values": JsonMap{
						"buckets": []JsonMap{
							{"key": nil, "doc_count": uint64(2)},
						},
						"sum_other_doc_count":         0,
						"doc_count_error_upper_bound": 0,
					},
				},
			},
		},
		{
			name: "2 buckets, first &value, second null",
			rows: []model.QueryResultRow{
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", &oneUint8), model.NewQueryResultCol("doc_count", uint64(2))}},
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", nil), model.NewQueryResultCol("doc_count", uint64(2))}},
			},
			wantedAggregationMap: JsonMap{
				"sample": JsonMap{
					"min_value":    JsonMap{"value": int64(1)},
					"max_value":    JsonMap{"value": int64(1)},
					"doc_count":    4,
					"sample_count": JsonMap{"value": 4},
					"top_values": JsonMap{
						"buckets": []JsonMap{
							{"key": &oneUint8, "doc_count": uint64(2)},
							{"key": nil, "doc_count": uint64(2)},
						},
						"sum_other_doc_count":         0,
						"doc_count_error_upper_bound": 0,
					},
				},
			},
		},
		{
			name: "2 buckets, first null second int32",
			rows: []model.QueryResultRow{
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", nil), model.NewQueryResultCol("doc_count", uint64(5))}},
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", int32(5)), model.NewQueryResultCol("doc_count", uint64(2))}},
			},
			wantedAggregationMap: JsonMap{
				"sample": JsonMap{
					"min_value":    JsonMap{"value": int64(5)},
					"max_value":    JsonMap{"value": int64(5)},
					"doc_count":    7,
					"sample_count": JsonMap{"value": 7},
					"top_values": JsonMap{
						"buckets": []JsonMap{
							{"key": nil, "doc_count": uint64(5)},
							{"key": int32(5), "doc_count": uint64(2)},
						},
						"sum_other_doc_count":         0,
						"doc_count_error_upper_bound": 0,
					},
				},
			},
		},
	}
	for i, tt := range testcases {
		t.Run(strconv.Itoa(i)+tt.name, func(t *testing.T) {
			query := cw.BuildFacetsQuery("not-important", &model.SimpleQuery{}, true)
			searchResp := cw.MakeSearchResponse([]*model.Query{query}, [][]model.QueryResultRow{tt.rows})
			assert.True(t, reflect.DeepEqual(searchResp.Aggregations, tt.wantedAggregationMap))
		})
	}
}

func Test_makeSearchResponseFacetsNumericFloats(t *testing.T) {
	oneFloat32 := float32(1)
	cw := ClickhouseQueryTranslator{Table: &clickhouse.Table{Name: "test"}, Ctx: context.Background()}
	var testcases = []struct {
		name                 string
		rows                 []model.QueryResultRow
		wantedAggregationMap JsonMap
	}{
		{
			name: "2 buckets, all present",
			rows: []model.QueryResultRow{
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", float64(1.2)), model.NewQueryResultCol("doc_count", uint64(2))}},
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", float64(3.2)), model.NewQueryResultCol("doc_count", uint64(4))}},
			},
			wantedAggregationMap: JsonMap{
				"sample": JsonMap{
					"min_value":    JsonMap{"value": float64(1.2)},
					"max_value":    JsonMap{"value": float64(3.2)},
					"doc_count":    6,
					"sample_count": JsonMap{"value": 6},
					"top_values": JsonMap{
						"buckets": []JsonMap{
							{"key": float64(1.2), "doc_count": uint64(2)},
							{"key": float64(3.2), "doc_count": uint64(4)},
						},
						"sum_other_doc_count":         0,
						"doc_count_error_upper_bound": 0,
					},
				},
			},
		},
		{
			name: "1 bucket, all nulls",
			rows: []model.QueryResultRow{
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", nil), model.NewQueryResultCol("doc_count", uint64(2))}},
			},
			wantedAggregationMap: JsonMap{
				"sample": JsonMap{
					"min_value":    JsonMap{"value": nil},
					"max_value":    JsonMap{"value": nil},
					"doc_count":    2,
					"sample_count": JsonMap{"value": 2},
					"top_values": JsonMap{
						"buckets": []JsonMap{
							{"key": nil, "doc_count": uint64(2)},
						},
						"sum_other_doc_count":         0,
						"doc_count_error_upper_bound": 0,
					},
				},
			},
		},
		{
			name: "2 buckets, first &value, second null",
			rows: []model.QueryResultRow{
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", &oneFloat32), model.NewQueryResultCol("doc_count", uint64(2))}},
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", nil), model.NewQueryResultCol("doc_count", uint64(2))}},
			},
			wantedAggregationMap: JsonMap{
				"sample": JsonMap{
					"min_value":    JsonMap{"value": float64(1)},
					"max_value":    JsonMap{"value": float64(1)},
					"doc_count":    4,
					"sample_count": JsonMap{"value": 4},
					"top_values": JsonMap{
						"buckets": []JsonMap{
							{"key": &oneFloat32, "doc_count": uint64(2)},
							{"key": nil, "doc_count": uint64(2)},
						},
						"sum_other_doc_count":         0,
						"doc_count_error_upper_bound": 0,
					},
				},
			},
		},
		{
			name: "2 buckets, first null second float32",
			rows: []model.QueryResultRow{
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", nil), model.NewQueryResultCol("doc_count", uint64(5))}},
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", float32(5.5)), model.NewQueryResultCol("doc_count", uint64(2))}},
			},
			wantedAggregationMap: JsonMap{
				"sample": JsonMap{
					"min_value":    JsonMap{"value": 5.5},
					"max_value":    JsonMap{"value": 5.5},
					"doc_count":    7,
					"sample_count": JsonMap{"value": 7},
					"top_values": JsonMap{
						"buckets": []JsonMap{
							{"key": nil, "doc_count": uint64(5)},
							{"key": float32(5.5), "doc_count": uint64(2)},
						},
						"sum_other_doc_count":         0,
						"doc_count_error_upper_bound": 0,
					},
				},
			},
		},
	}
	for i, tt := range testcases {
		t.Run(strconv.Itoa(i)+tt.name, func(t *testing.T) {
			query := cw.BuildFacetsQuery("not-important", &model.SimpleQuery{}, true)
			searchResp := cw.MakeSearchResponse([]*model.Query{query}, [][]model.QueryResultRow{tt.rows})
			assert.True(t, reflect.DeepEqual(searchResp.Aggregations, tt.wantedAggregationMap))
		})
	}
}

func Test_sortInTopologicalOrder(t *testing.T) {
	var testcases = []struct {
		queries                []*model.Query
		wantedTopologicalOrder []int
	}{
		{
			queries: []*model.Query{
				{Parent: "b", NoDBQuery: true, Aggregators: []model.Aggregator{{Name: "c"}}},
				{Parent: "", Aggregators: []model.Aggregator{{Name: "b"}}},
				{Parent: "c", NoDBQuery: true, Aggregators: []model.Aggregator{{Name: "d"}}},
			},
			wantedTopologicalOrder: []int{1, 0, 2},
		},
		{
			queries: []*model.Query{
				{Parent: "", Aggregators: []model.Aggregator{{Name: "c"}}},
				{Parent: "", Aggregators: []model.Aggregator{{Name: "b"}}},
				{Parent: "", Aggregators: []model.Aggregator{{Name: "d"}}},
				{Parent: "", Aggregators: []model.Aggregator{{Name: "e"}}},
			},
			wantedTopologicalOrder: []int{0, 1, 2, 3},
		},
		{
			queries: []*model.Query{
				{Parent: "a", NoDBQuery: true, Aggregators: []model.Aggregator{{Name: "b1"}}},
				{Parent: "a", NoDBQuery: true, Aggregators: []model.Aggregator{{Name: "b2"}}},
				{Parent: "", Aggregators: []model.Aggregator{{Name: "a"}}},
				{Parent: "b2", NoDBQuery: true, Aggregators: []model.Aggregator{{Name: "c"}}},
			},
			wantedTopologicalOrder: []int{2, 0, 1, 3},
		},
	}
	for i, tt := range testcases {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			cw := ClickhouseQueryTranslator{}
			actual := cw.sortInTopologicalOrder(tt.queries)
			assert.Equal(t, tt.wantedTopologicalOrder, actual)
		})
	}
}
