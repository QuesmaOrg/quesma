// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package queryparser

import (
	"context"
	"github.com/QuesmaOrg/quesma/quesma/clickhouse"
	"github.com/QuesmaOrg/quesma/quesma/model"
	"github.com/QuesmaOrg/quesma/quesma/model/typical_queries"
	"github.com/QuesmaOrg/quesma/quesma/queryparser/query_util"
	"github.com/QuesmaOrg/quesma/quesma/schema"
	"github.com/QuesmaOrg/quesma/quesma/util"
	"github.com/goccy/go-json"
	"github.com/k0kubun/pp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	s := schema.StaticRegistry{
		Tables: map[schema.IndexName]schema.Schema{
			"test": {
				Fields: map[schema.FieldName]schema.Field{
					"host.name":         {PropertyName: "host.name", InternalPropertyName: "host.name", Type: schema.QuesmaTypeObject},
					"type":              {PropertyName: "type", InternalPropertyName: "type", Type: schema.QuesmaTypeText},
					"name":              {PropertyName: "name", InternalPropertyName: "name", Type: schema.QuesmaTypeText},
					"content":           {PropertyName: "content", InternalPropertyName: "content", Type: schema.QuesmaTypeText},
					"message":           {PropertyName: "message", InternalPropertyName: "message", Type: schema.QuesmaTypeText},
					"host_name.keyword": {PropertyName: "host_name.keyword", InternalPropertyName: "host_name.keyword", Type: schema.QuesmaTypeKeyword},
					"FlightDelay":       {PropertyName: "FlightDelay", InternalPropertyName: "FlightDelay", Type: schema.QuesmaTypeText},
					"Cancelled":         {PropertyName: "Cancelled", InternalPropertyName: "Cancelled", Type: schema.QuesmaTypeText},
					"FlightDelayMin":    {PropertyName: "FlightDelayMin", InternalPropertyName: "FlightDelayMin", Type: schema.QuesmaTypeText},
					"_id":               {PropertyName: "_id", InternalPropertyName: "_id", Type: schema.QuesmaTypeText},
				},
			},
		},
	}
	cw := ClickhouseQueryTranslator{Table: &clickhouse.Table{Name: "test"}, Ctx: context.Background(), Schema: s.Tables["test"]}
	searchResp, err := cw.MakeAsyncSearchResponse(row, &model.Query{Highlighter: NewEmptyHighlighter()}, asyncRequestIdStr, false)
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
		queryType           model.HitsInfo
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
			model.ListByField,
		},
	}
	s := schema.StaticRegistry{
		Tables: map[schema.IndexName]schema.Schema{
			"test": {
				Fields: map[schema.FieldName]schema.Field{
					"host.name":         {PropertyName: "host.name", InternalPropertyName: "host.name", Type: schema.QuesmaTypeObject},
					"type":              {PropertyName: "type", InternalPropertyName: "type", Type: schema.QuesmaTypeText},
					"name":              {PropertyName: "name", InternalPropertyName: "name", Type: schema.QuesmaTypeText},
					"content":           {PropertyName: "content", InternalPropertyName: "content", Type: schema.QuesmaTypeText},
					"message":           {PropertyName: "message", InternalPropertyName: "message", Type: schema.QuesmaTypeText},
					"host_name.keyword": {PropertyName: "host_name.keyword", InternalPropertyName: "host_name.keyword", Type: schema.QuesmaTypeKeyword},
					"FlightDelay":       {PropertyName: "FlightDelay", InternalPropertyName: "FlightDelay", Type: schema.QuesmaTypeText},
					"Cancelled":         {PropertyName: "Cancelled", InternalPropertyName: "Cancelled", Type: schema.QuesmaTypeText},
					"FlightDelayMin":    {PropertyName: "FlightDelayMin", InternalPropertyName: "FlightDelayMin", Type: schema.QuesmaTypeText},
					"_id":               {PropertyName: "_id", InternalPropertyName: "_id", Type: schema.QuesmaTypeText},
				},
			},
		},
	}
	cw := ClickhouseQueryTranslator{Table: &clickhouse.Table{Name: "test"}, Ctx: context.Background(), Schema: s.Tables["test"], SearchAfterStrategy: SearchAfterStrategyFactory(model.DefaultSearchAfterStrategy)}
	for i, tt := range args {
		t.Run(tt.queryType.String(), func(t *testing.T) {
			hitQuery := query_util.BuildHitsQuery(
				context.Background(), "test", []string{"*"},
				&model.SimpleQuery{}, model.WeNeedUnlimitedCount, model.SearchAfterEmpty, cw.SearchAfterStrategy,
			)
			highlighter := NewEmptyHighlighter()
			queryType := typical_queries.NewHits(cw.Ctx, cw.Table, &highlighter, cw.SearchAfterStrategy, hitQuery.SelectCommand.OrderByFieldNames(), true, false, false, []string{cw.Table.Name})
			hitQuery.Type = &queryType
			ourResponseRaw := cw.MakeSearchResponse(
				[]*model.Query{hitQuery},
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
			query_util.BuildHitsQuery(context.Background(), "test", []string{"message"}, &model.SimpleQuery{}, model.WeNeedUnlimitedCount, model.SearchAfterEmpty, cw.SearchAfterStrategy),
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
			query_util.BuildHitsQuery(context.Background(), "test", []string{"*"}, &model.SimpleQuery{}, model.WeNeedUnlimitedCount, model.SearchAfterEmpty, cw.SearchAfterStrategy)},
	}
	for i, tt := range args {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			t.Skip()
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
	cw := ClickhouseQueryTranslator{Table: clickhouse.NewEmptyTable("@"), Ctx: context.Background()}
	const limit = 1000
	queries := []*model.Query{
		cw.BuildNRowsQuery([]string{"*"}, &model.SimpleQuery{}, model.HitsCountInfo{Size: limit}),
		cw.BuildNRowsQuery([]string{"@"}, &model.SimpleQuery{}, model.HitsCountInfo{Size: 0}),
	}
	for _, query := range queries {
		resultRow := model.QueryResultRow{Cols: make([]model.QueryResultCol, 0)}
		for _, field := range query.SelectCommand.Columns {
			//TODO - this used to take alias into account, but now it doesn't (model.QueryResultCol{ColName: field.Alias, Value: "not-important"}))
			resultRow.Cols = append(resultRow.Cols, model.QueryResultCol{ColName: model.AsString(field), Value: "not-important"})
		}
		_ = cw.MakeSearchResponse([]*model.Query{{Highlighter: NewEmptyHighlighter()}}, [][]model.QueryResultRow{{resultRow}})
	}
}
