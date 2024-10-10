// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package clients

import (
	"quesma/model"
	"quesma/testdata"
)

var CloverTests = []testdata.AggregationTestCase{
	{ // [0]
		TestName: "Ophelia Test 1: triple terms + default order",
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
									"max": 1728475016476,
									"min": 687013016476
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
						"timeField": "@timestamp"
					},
					"terms": {
						"field": "shoe_size",
						"order": {
							"_count": "desc"
						},
						"size": "20"
					}
				}
			},
			"runtime_mappings": {},
			"size": 0,
			"timeout": "30000ms"
		}`,
		ExpectedResponse: `
		{
			"completion_time_in_millis": 1720352002293,
			"expiration_time_in_millis": 1720352062445,
			"id": "FnpTUXdfTTZLUlBtQVo1YzBTVFBseEEcM19IaHdFWG5RN1d1eV9VaUcxenYwdzo0MTc0MA==",
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
					"2": {
						"buckets": [
							{
								"8": {
									"buckets": [
										{
											"4": {
												"buckets": [
													{
														"doc_count": 21,
														"key": "c11"
													}
												],
												"doc_count_error_upper_bound": 0,
												"sum_other_doc_count": 0
											},
											"doc_count": 21,
											"key": "b11"
										},
										{
											"4": {
												"buckets": [
													{
														"doc_count": 24,
														"key": "c12"
													}
												],
												"doc_count_error_upper_bound": 0,
												"sum_other_doc_count": 0
											},
											"doc_count": 24,
											"key": "b12"
										}
									],
									"doc_count_error_upper_bound": -1,
									"sum_other_doc_count": 991
								},
								"doc_count": 1036,
								"key": "a1"
							},
							{
								"8": {
									"buckets": [
										{
											"4": {
												"buckets": [
													{
														"doc_count": 17,
														"key": "c21"
													}
												],
												"doc_count_error_upper_bound": 0,
												"sum_other_doc_count": 0
											},
											"doc_count": 17,
											"key": "b21"
										},
										{
											"4": {
												"buckets": [
													{
														"doc_count": 17,
														"key": "c22"
													}
												],
												"doc_count_error_upper_bound": 0,
												"sum_other_doc_count": 0
											},
											"doc_count": 17,
											"key": "b22"
										}
									],
									"doc_count_error_upper_bound": 0,
									"sum_other_doc_count": 0
								},
								"doc_count": 34,
								"key": "a2"
							}
						],
						"doc_count_error_upper_bound": -1,
						"sum_other_doc_count": 33220
					}
				},
				"hits": {
					"hits": [],
					"max_score": null,
					"total": {
						"relation": "eq",
						"value": 50427
					}
				},
				"timed_out": false,
				"took": 554
			},
			"start_time_in_millis": 1720352001739
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{ // mixing different int types (int, int64, uint64) on purpose, at least both (u)int64 can be returned from ClickHouse
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__1__parent_count", int64(50427)),
				model.NewQueryResultCol("aggr__1__key_0", "a1"),
				model.NewQueryResultCol("aggr__1__count", int64(1036)),
				model.NewQueryResultCol("aggr__1__timeseries__key_0", int64(50)),
				model.NewQueryResultCol("aggr__1__timeseries__count", int64(21)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__1__parent_count", int64(50427)),
				model.NewQueryResultCol("aggr__1__key_0", "a1"),
				model.NewQueryResultCol("aggr__1__count", int64(1036)),
				model.NewQueryResultCol("aggr__1__timeseries__key_0", int64(52)),
				model.NewQueryResultCol("aggr__1__timeseries__count", int64(24)),
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
				  "shoe_size" AS "aggr__1__key_0",
				  sum(count(*)) OVER (PARTITION BY "aggr__1__key_0") AS "aggr__1__count",
				  toInt64((toUnixTimestamp64Milli("@timestamp")+timeZoneOffset(toTimezone(
				  "@timestamp", 'Europe/Warsaw'))*1000) / 2592000000) AS
				  "aggr__1__timeseries__key_0", count(*) AS "aggr__1__timeseries__count"
				FROM __quesma_table_name
				GROUP BY "shoe_size" AS "aggr__1__key_0",
				  toInt64((toUnixTimestamp64Milli("@timestamp")+timeZoneOffset(toTimezone(
				  "@timestamp", 'Europe/Warsaw'))*1000) / 2592000000) AS
				  "aggr__1__timeseries__key_0"))
			WHERE "aggr__1__order_1_rank"<=21
			ORDER BY "aggr__1__order_1_rank" ASC, "aggr__1__timeseries__order_1_rank" ASC`,
	},
	{ // [1]
		TestName: "Clover Panel 2",
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
									"max": 1728481915311,
									"min": 687019915311
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
			"completion_time_in_millis": 1720352002293,
			"expiration_time_in_millis": 1720352062445,
			"id": "FnpTUXdfTTZLUlBtQVo1YzBTVFBseEEcM19IaHdFWG5RN1d1eV9VaUcxenYwdzo0MTc0MA==",
			"is_partial": false,
			"is_running": false,
			"response": {
				"_shards": {
					"failed": 0,
					"skipped": 0,
					"successful": 1,
					"total": 1
				},
				"aggregations": {},
				"hits": {
					"hits": [],
					"max_score": null,
					"total": {
						"relation": "eq",
						"value": 50427
					}
				},
				"timed_out": false,
				"took": 554
			},
			"start_time_in_millis": 1720352001739
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{ // mixing different int types (int, int64, uint64) on purpose, at least both (u)int64 can be returned from ClickHouse
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__1__parent_count", int64(50427)),
				model.NewQueryResultCol("aggr__1__key_0", "a1"),
				model.NewQueryResultCol("aggr__1__count", int64(1036)),
				model.NewQueryResultCol("aggr__1__timeseries__key_0", int64(50)),
				model.NewQueryResultCol("aggr__1__timeseries__count", int64(21)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__1__parent_count", int64(50427)),
				model.NewQueryResultCol("aggr__1__key_0", "a2"),
				model.NewQueryResultCol("aggr__1__count", int64(1036)),
				model.NewQueryResultCol("aggr__1__timeseries__key_0", int64(50)),
				model.NewQueryResultCol("aggr__1__timeseries__count", int64(24)),
			}},
		},
		ExpectedPancakeSQL: ``,
	},
}
