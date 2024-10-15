// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package clients

import (
	"quesma/model"
	"quesma/testdata"
)

var CloverTests = []testdata.AggregationTestCase{
	{ // [0]
		TestName: "simplest auto_date_histogram",
		QueryRequestJson: `
		{
			"aggs": {
				"timeseries": {
					"aggs": {
						"469ef7fe-5927-42d1-918b-37c738c600f0": {
							"bucket_script": {
								"buckets_path": {
									"count": "_count"
								},
								"gap_policy": "skip",
								"script": {
									"lang": "expression",
									"source": "count * 1"
								}
							}
						}
					},
					"auto_date_histogram": {
						"buckets": 1,
						"field": "timestamp"
					},
					"meta": {
						"dataViewId": "d3d7af60-4c81-11e8-b3d7-01146121b73d",
						"indexPatternString": "kibana_sample_data_flights",
						"intervalString": "54000000ms",
						"normalized": true,
						"panelId": "1a1d745d-0c21-4103-a2ae-df41d4fbd366",
						"seriesId": "866fb08f-b9a4-43eb-a400-38ebb6c13aed",
						"timeField": "timestamp"
					}
				}
			},
			"query": {
				"bool": {
					"filter": [],
					"must": [
						{
							"range": {
								"timestamp": {
									"format": "strict_date_optional_time",
									"gte": "2024-10-10T17:33:47.125Z",
									"lte": "2024-10-11T08:33:47.125Z"
								}
							}
						}
					],
					"must_not": [],
					"should": []
				}
			},
			"runtime_mappings": {
				"hour_of_day": {
					"script": {
						"source": "emit(doc['timestamp'].value.getHour());"
					},
					"type": "long"
				}
			},
			"size": 0,
			"timeout": "30000ms",
			"track_total_hits": true
		}`,
		ExpectedResponse: `
		{
			"completion_time_in_millis": 1728635627258,
			"expiration_time_in_millis": 1728635687254,
			"id": "FlhaTzBhMkpQU3lLMmlzNHhBeU9FMHcbaUp3ZGNYdDNSaGF3STVFZ2xWY3RuQTo2MzU4",
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
					"timeseries": {
						"buckets": [
							{
								"469ef7fe-5927-42d1-918b-37c738c600f0": {
									"value": 202.0
								},
								"doc_count": 202,
								"key": 1728518400000,
								"key_as_string": "2024-10-10T00:00:00.000Z"
							}
						],
						"interval": "7d",
						"meta": {
							"dataViewId": "d3d7af60-4c81-11e8-b3d7-01146121b73d",
							"indexPatternString": "kibana_sample_data_flights",
							"intervalString": "54000000ms",
							"normalized": true,
							"panelId": "1a1d745d-0c21-4103-a2ae-df41d4fbd366",
							"seriesId": "866fb08f-b9a4-43eb-a400-38ebb6c13aed",
							"timeField": "timestamp"
						}
					}
				},
				"hits": {
					"hits": [],
					"max_score": null,
					"total": {
						"relation": "eq",
						"value": 202
					}
				},
				"timed_out": false,
				"took": 4
			},
			"start_time_in_millis": 1728635627254
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__timeseries__count", int64(202)),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT count(*) AS "aggr__timeseries__count"
			FROM __quesma_table_name
			WHERE ("timestamp">=parseDateTime64BestEffort('2024-10-10T17:33:47.125Z') AND
			  "timestamp"<=parseDateTime64BestEffort('2024-10-11T08:33:47.125Z'))`,
	},
}
