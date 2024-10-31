// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package clients

import (
	"quesma/model"
	"quesma/testdata"
)

var CloverTests = []testdata.AggregationTestCase{
	{ // [0] TODO: add empty bucket for 1 of date_histogram buckets, because of min_doc_count=0 and extended_bounds. After extended_bounds PR.
		TestName: "todo",
		QueryRequestJson: `
		{
			"aggs": {
				"1": {
					"aggs": {
						"timeseries": {
							"aggs": {
								"2": {
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
							"date_histogram": {
								"extended_bounds": {
									"max": 1726264900000,
									"min": 1726264900000
								},
								"field": "@timestamp",
								"fixed_interval": "2592000s",
								"min_doc_count": 0,
								"time_zone": "Europe/Warsaw"
							}
						}
					},
					"meta": {
						"indexPatternString": "ab*",
						"intervalString": "2592000s",
						"panelId": "0",
						"seriesId": "1",
						"timeField": "timestamp"
					},
					"terms": {
						"field": "nobel_laureate",
						"order": {
							"_count": "desc"
						}
					}
				}
			},
			"size": 0
		}`,
		ExpectedResponse: `
		{
			"took": 0,
			"timed_out": false,
			"_shards": {
				"total": 1,
				"successful": 1,
				"failed": 0,
				"skipped": 0
			},
			"hits": {
				"total": {
					"value": 14074,
					"relation": "eq"
				},
				"max_score": null,
				"hits": []
			},
			"aggregations": {
				"1": {
					"buckets": [
						{
							"doc_count": 672,
							"key": "/apm",
							"timeseries": {
								"buckets": [
									{
										"2": {
											"value": 319
										},
										"doc_count": 319,
										"key": 1728856800000,
										"key_as_string": "2024-10-13T22:00:00.000"
									}
								]
							}
						},
						{
							"doc_count": 655,
							"key": "/",
							"timeseries": {
								"buckets": [
									{
										"2": {
											"value": 12
										},
										"doc_count": 12,
										"key": 1726264800000,
										"key_as_string": "2024-09-13T22:00:00.000"
									},
									{
										"2": {
											"value": 301
										},
										"doc_count": 301,
										"key": 1728856800000,
										"key_as_string": "2024-10-13T22:00:00.000"
									}
								]
							}
						}
					],
					"doc_count_error_upper_bound": 0,
					"meta": {
						"indexPatternString": "ab*",
						"intervalString": "2592000s",
						"panelId": "0",
						"seriesId": "1",
						"timeField": "timestamp"
					},
					"sum_other_doc_count": 49100
				}
			}
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__1__parent_count", int64(50427)),
				model.NewQueryResultCol("aggr__1__key_0", "/apm"),
				model.NewQueryResultCol("aggr__1__count", int64(672)),
				model.NewQueryResultCol("aggr__1__timeseries__key_0", int64(1728864000000/2592000000)),
				model.NewQueryResultCol("aggr__1__timeseries__count", int64(319)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__1__parent_count", int64(50427)),
				model.NewQueryResultCol("aggr__1__key_0", "/"),
				model.NewQueryResultCol("aggr__1__count", int64(655)),
				model.NewQueryResultCol("aggr__1__timeseries__key_0", int64(1726272000000/2592000000)),
				model.NewQueryResultCol("aggr__1__timeseries__count", int64(12)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__1__parent_count", int64(50427)),
				model.NewQueryResultCol("aggr__1__key_0", "/"),
				model.NewQueryResultCol("aggr__1__count", int64(655)),
				model.NewQueryResultCol("aggr__1__timeseries__key_0", int64(1728864000000/2592000000)),
				model.NewQueryResultCol("aggr__1__timeseries__count", int64(301)),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT "aggr__1__parent_count", "aggr__1__key_0", "aggr__1__count",
			  "aggr__1__timeseries__key_0", "aggr__1__timeseries__count"
			FROM (
			  SELECT "aggr__1__parent_count", "aggr__1__key_0", "aggr__1__count",
				"aggr__1__timeseries__key_0", "aggr__1__timeseries__count",
				dense_rank() OVER (ORDER BY "aggr__1__count" DESC, "aggr__1__key_0" ASC) AS
				"aggr__1__order_1_rank",
				dense_rank() OVER (PARTITION BY "aggr__1__key_0" ORDER BY
				"aggr__1__timeseries__key_0" ASC) AS "aggr__1__timeseries__order_1_rank"
			  FROM (
				SELECT sum(count(*)) OVER () AS "aggr__1__parent_count",
				  "nobel_laureate" AS "aggr__1__key_0",
				  sum(count(*)) OVER (PARTITION BY "aggr__1__key_0") AS "aggr__1__count",
				  toInt64((toUnixTimestamp64Milli("@timestamp")+timeZoneOffset(toTimezone(
				  "@timestamp", 'Europe/Warsaw'))*1000) / 2592000000) AS
				  "aggr__1__timeseries__key_0", count(*) AS "aggr__1__timeseries__count"
				FROM __quesma_table_name
				GROUP BY "nobel_laureate" AS "aggr__1__key_0",
				  toInt64((toUnixTimestamp64Milli("@timestamp")+timeZoneOffset(toTimezone(
				  "@timestamp", 'Europe/Warsaw'))*1000) / 2592000000) AS
				  "aggr__1__timeseries__key_0"))
			WHERE "aggr__1__order_1_rank"<=11
			ORDER BY "aggr__1__order_1_rank" ASC, "aggr__1__timeseries__order_1_rank" ASC
			`,
	},
	{ // [1]
		TestName: "multiple buckets_path",
		QueryRequestJson: `
		{
			"aggs": {
				"timeseries": {
					"aggs": {
						"a2": {
							"bucket_script": {
								"buckets_path": {
									"denominator": "a2-denominator>_count",
									"numerator": "a2-numerator>_count"
								},
								"script": "params.numerator != null && params.denominator != null && params.denominator != 0 ? params.numerator / params.denominator : 0"
							}
						},
						"a2-denominator": {
							"filter": {
								"bool": {
									"filter": [],
									"must": [],
									"must_not": [],
									"should": []
								}
							}
						},
						"a2-numerator": {
							"filter": {
								"bool": {
									"filter": [],
									"must": [
										{
											"query_string": {
												"analyze_wildcard": true,
												"query": "NOT table.flower : clover"
											}
										}
									],
									"must_not": [],
									"should": []
								}
							}
						}
					},
					"auto_date_histogram": {
						"buckets": 1,
						"field": "@timestamp"
					},
					"meta": {
						"indexPatternString": "ab*",
						"intervalString": "900000ms",
						"normalized": true,
						"panelId": "0",
						"seriesId": "1",
						"timeField": "@timestamp"
					}
				}
			},
			"query": {
				"bool": {
					"filter": [],
					"must": [
						{
							"range": {
								"@timestamp": {
									"format": "strict_date_optional_time",
									"gte": "2024-10-11T09:58:03.723Z",
									"lte": "2024-10-11T10:13:03.723Z"
								}
							}
						},
						{
							"bool": {
								"filter": [],
								"must": [],
								"must_not": [],
								"should": []
							}
						},
						{
							"bool": {
								"filter": [],
								"must": [],
								"must_not": [],
								"should": []
							}
						}
					],
					"must_not": [],
					"should": []
				}
			},
			"runtime_mappings": {},
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
								"a2": {
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
			SELECT count(*) AS "aggr__timeseries__count",
			  countIf(True) AS
			  "name",
			  countIf(NOT ("field" = 'sth')) AS
			  "name"
			FROM __quesma_table_name
			WHERE ("@timestamp">=parseDateTime64BestEffort('2024-10-11T09:58:03.723Z') AND
			  "@timestamp"<=parseDateTime64BestEffort('2024-10-11T10:13:03.723Z'))`,
	},
}
