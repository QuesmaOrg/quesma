// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package clients

import (
	"github.com/QuesmaOrg/quesma/platform/model"
	"github.com/QuesmaOrg/quesma/platform/testdata"
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
									"value": 1
								},
								"a2-denominator": {
									"doc_count": 202
								},
								"a2-numerator": {
									"doc_count": 202
								},
								"doc_count": 202,
								"key": 1728640683723,
								"key_as_string": "2024-10-11T09:58:03.723"
							}
						],
						"interval": "100y",
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
				model.NewQueryResultCol("metric__timeseries__a2-denominator_col_0", int64(202)),
				model.NewQueryResultCol("metric__timeseries__a2-numerator_col_0", int64(202)),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT count(*) AS "aggr__timeseries__count",
			  countIf(true) AS "metric__timeseries__a2-denominator_col_0",
			  countIf(NOT ("table.flower" __quesma_match 'clover')) AS
			  "metric__timeseries__a2-numerator_col_0"
			FROM __quesma_table_name
			WHERE ("@timestamp">=fromUnixTimestamp64Milli(1728640683723) AND "@timestamp"<=
			  fromUnixTimestamp64Milli(1728641583723))`,
	},
	{ // [2]
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
								"key": 1728581627125,
								"key_as_string": "2024-10-10T19:33:47.125+02:00"
							}
						],
						"interval": "100y",
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
			WHERE ("timestamp">=fromUnixTimestamp64Milli(1728581627125) AND "timestamp"<=
			  fromUnixTimestamp64Milli(1728635627125))`,
		AdditionalAcceptableDifference: []string{"key_as_string"}, // timezone differences between local and github runs... There's always 2h difference between those, need to investigate. Maybe come back to .UTC() so there's no "+timezone" (e.g. +02:00)?
	},
	{ // [3]
		TestName: "bucket_script with multiple buckets_path",
		QueryRequestJson: `
		{
			"aggs": {
				"timeseries": {
					"aggs": {
						"f2": {
							"bucket_script": {
								"buckets_path": {
									"denominator": "f2-denominator>_count",
									"numerator": "f2-numerator>_count"
								},
								"script": "params.numerator != null && params.denominator != null && params.denominator != 0 ? params.numerator / params.denominator : 0"
							}
						},
						"f2-denominator": {
							"filter": {
								"bool": {
									"filter": [],
									"must": [],
									"must_not": [],
									"should": []
								}
							}
						},
						"f2-numerator": {
							"filter": {
								"bool": {
									"filter": [],
									"must": [
										{
											"query_string": {
												"analyze_wildcard": true,
												"query": "!_exists_:a.b_str"
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
						"intervalString": "9075600000ms",
						"normalized": true,
						"panelId": "f0",
						"seriesId": "f1",
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
									"gte": "2024-07-19T14:38:24.783Z",
									"lte": "2024-11-01T15:38:24.783Z"
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
			"completion_status": 200,
			"completion_time_in_millis": 1730475504882,
			"expiration_time_in_millis": 1730898929116,
			"id": "quesma_async_0192e860-b4cb-7be4-a193-43d38686c80d",
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
								"f2": {
									"value": 0.178
								},
								"f2-denominator": {
									"doc_count": 1000
								},
								"f2-numerator": {
									"doc_count": 178
								},
								"doc_count": 1000,
								"key": 1721399904783,
								"key_as_string": "2024-07-19T14:38:24.783"
							}
						],
						"interval": "100y",
						"meta": {
							"indexPatternString": "ab*",
							"intervalString": "9075600000ms",
							"normalized": true,
							"panelId": "f0",
							"seriesId": "f1",
							"timeField": "@timestamp"
						}
					}
				},
				"hits": {
					"hits": [],
					"max_score": null,
					"total": {
						"relation": "eq",
						"value": 1000
					}
				},
				"timed_out": false,
				"took": 0
			},
			"start_time_in_millis": 0
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__timeseries__count", int64(1000)),
				model.NewQueryResultCol("metric__timeseries__f2-denominator_col_0", int64(1000)),
				model.NewQueryResultCol("metric__timeseries__f2-numerator_col_0", int64(178)),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT count(*) AS "aggr__timeseries__count",
			  countIf(true) AS "metric__timeseries__f2-denominator_col_0",
			  countIf(NOT ("a.b_str" IS NOT NULL)) AS
			  "metric__timeseries__f2-numerator_col_0"
			FROM __quesma_table_name
			WHERE ("@timestamp">=fromUnixTimestamp64Milli(1721399904783) AND "@timestamp"<=
			  fromUnixTimestamp64Milli(1730475504783))`,
	},
	{ // [4]
		TestName: "todo",
		QueryRequestJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"other-filter": {
					"aggs": {
						"3": {
							"terms": {
								"field": "field",
								"order": {
									"_count": "desc"
								},
								"size": 15
							}
						}
					},
					"filters": {
						"filters": {
							"": {
								"bool": {
									"filter": [],
									"must": [
										{
											"match_phrase": {
												"a": "b"
											}
										},
										{
											"match_phrase": {
												"c": "d"
											}
										}
									],
									"must_not": [],
									"should": []
								}
							}
						}
					}
				}
			},
		}`,
		ExpectedResponse: `
		{
			"completion_time_in_millis": 1707486436398,
			"expiration_time_in_millis": 1707486496397,
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
					"other-filter": {
						"buckets": {
							"": {
								"doc_count": 4675,
								"3": {
									"sum_other_doc_count":         0,
									"doc_count_error_upper_bound": 0,
									"buckets": [
										{
											"doc_count": 4674,
											"key": "field"
										},
										{
											"doc_count": 1,
											"key": "bad-field"
										}
									]
								}
							}
						}
					}
				},
				"hits": {
					"hits": [],
					"max_score": null,
					"total": {
						"relation": "eq",
						"value": 2200
					}
				},
				"timed_out": false,
				"took": 1
			},
			"start_time_in_millis": 1707486436397
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__other-filter__count", int64(4675)),
				model.NewQueryResultCol("aggr__other-filter__3__parent_count", int64(4675)),
				model.NewQueryResultCol("aggr__other-filter__3__key_0", "field"),
				model.NewQueryResultCol("aggr__other-filter__3__count", int64(4674)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__other-filter__count", int64(4675)),
				model.NewQueryResultCol("aggr__other-filter__3__parent_count", int64(4675)),
				model.NewQueryResultCol("aggr__other-filter__3__key_0", "bad-field"),
				model.NewQueryResultCol("aggr__other-filter__3__count", int64(1)),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT sum(count(*)) OVER () AS "aggr__other-filter__count",
			  sum(count(*)) OVER () AS "aggr__other-filter__3__parent_count",
			  "field" AS "aggr__other-filter__3__key_0",
			  count(*) AS "aggr__other-filter__3__count"
			FROM __quesma_table_name
			WHERE ("a" __quesma_match '%b%' AND "c" __quesma_match '%d%')
			GROUP BY "field" AS "aggr__other-filter__3__key_0"
			ORDER BY "aggr__other-filter__3__count" DESC,
			  "aggr__other-filter__3__key_0" ASC
			LIMIT 16`,
	},
	{ // [5]
		TestName: "todo",
		QueryRequestJson: `
		{
			"aggs": {
				"q": {
					"aggs": {
						"time_buckets": {
							"aggs": {
								"count": {
									"bucket_script": {
										"buckets_path": "_count",
										"script": {
											"lang": "expression",
											"source": "_value"
										}
									}
								}
							},
							"date_histogram": {
								"extended_bounds": {
									"max": 1728507732621,
									"min": 1728507729621
								},
								"field": "@timestamp",
								"fixed_interval": "7d",
								"min_doc_count": 0
							},
							"meta": {
								"type": "time_buckets"
							}
						}
					},
					"filters": {
						"filters": {
							"*": {
								"query_string": {
									"query": "*"
								}
							}
						}
					},
					"meta": {
						"type": "split"
					}
				}
			},
			"query": {
				"bool": {
					"filter": {
						"bool": {
							"filter": [],
							"must": [],
							"must_not": [],
							"should": []
						}
					},
					"must": [
						{
							"range": {
								"@timestamp": {
									"format": "strict_date_optional_time",
									"gte": "2024-10-09T21:02:09.621Z",
									"lte": "2024-10-09T21:02:12.621Z"
								}
							}
						}
					]
				}
			},
			"runtime_mappings": {},
			"size": 0,
			"timeout": "30000ms",
			"track_total_hits": true
		}`,
		ExpectedResponse: `
		{
			"completion_time_in_millis": 1707486436398,
			"expiration_time_in_millis": 1707486496397,
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
					"q": {
						"meta": {
							"type": "split"
						},
						"buckets": {
							"*": {
								"doc_count": 4675,
								"time_buckets": {
									"meta": {
										"type": "time_buckets"
									},
									"buckets": [
										{
											"doc_count": 442,
											"key": 1726704000000,
											"key_as_string": "2024-09-19T00:00:00.000",
											"count": {
												"value": 442
											}
										},
										{
											"doc_count": 0,
											"key": 1727308800000,
											"key_as_string": "2024-09-26T00:00:00.000",
											"count": {
												"value": 0
											}
										},
										{
											"doc_count": 0,
											"key": 1727913600000,
											"key_as_string": "2024-10-03T00:00:00.000",
											"count": {
												"value": 0
											}
										},
										{
											"doc_count": 0,
											"key": 1728518400000,
											"key_as_string": "2024-10-10T00:00:00.000",
											"count": {
												"value": 0
											}
										},
										{
											"doc_count": 1,
											"key": 1729123200000,
											"key_as_string": "2024-10-17T00:00:00.000",
											"count": {
												"value": 1.0
											}
										}
									]
								}
							}
						}
					}
				},
				"hits": {
					"hits": [],
					"max_score": null,
					"total": {
						"relation": "eq",
						"value": 2200
					}
				},
				"timed_out": false,
				"took": 1
			},
			"start_time_in_millis": 1707486436397
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__q__count", int64(4675)),
				model.NewQueryResultCol("aggr__q__time_buckets__key_0", int64(2855)),
				model.NewQueryResultCol("aggr__q__time_buckets__count", int64(442)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__q__count", int64(4675)),
				model.NewQueryResultCol("aggr__q__time_buckets__key_0", int64(2859)),
				model.NewQueryResultCol("aggr__q__time_buckets__count", int64(1)),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT sum(count(*)) OVER () AS "aggr__q__count",
			  toInt64(toUnixTimestamp64Milli("@timestamp") / 604800000) AS
			  "aggr__q__time_buckets__key_0", count(*) AS "aggr__q__time_buckets__count"
			FROM __quesma_table_name
			WHERE (("@timestamp">=fromUnixTimestamp64Milli(1728507729621) AND "@timestamp"<=
			  fromUnixTimestamp64Milli(1728507732621)) AND "__quesma_fulltext_field_name"
			  __quesma_match '%')
			GROUP BY toInt64(toUnixTimestamp64Milli("@timestamp") / 604800000) AS
			  "aggr__q__time_buckets__key_0"
			ORDER BY "aggr__q__time_buckets__key_0" ASC`,
	},
	{ // [6]
		TestName: "Clover",
		QueryRequestJson: `
		{
			"aggs": {
				"q": {
					"aggs": {
						"time_buckets": {
							"aggs": {
								"sum(count)": {
									"sum": {
										"field": "count"
									}
								}
							},
							"date_histogram": {
								"field": "@timestamp",
								"fixed_interval": "30m",
								"min_doc_count": 0,
								"time_zone": "Europe/Warsaw"
							},
							"meta": {
								"type": "time_buckets"
							}
						}
					},
					"filters": {
						"filters": {
							"!str_field:CRASH": {
								"query_string": {
									"query": "!str_field:CRASH"
								}
							}
						}
					},
					"meta": {
						"type": "split"
					}
				}
			},
			"runtime_mappings": {},
			"size": 0,
			"timeout": "30000ms",
			"track_total_hits": true
		}`,
		ExpectedResponse: `
		{
			"completion_time_in_millis": 1707486436398,
			"expiration_time_in_millis": 1707486496397,
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
					"q": {
						"meta": {
							"type": "split"
						},
						"buckets": {
							"!str_field:CRASH": {
								"doc_count": 4675,
								"time_buckets": {
									"meta": {
										"type": "time_buckets"
									},
									"buckets": [
										{
											"doc_count": 442,
											"key": 1726351200000,
											"key_as_string": "2024-09-14T22:00:00.000",
											"sum(count)": {
												"value": 442
											}
										},
										{
											"doc_count": 0,
											"key": 1726353000000,
											"key_as_string": "2024-09-14T22:30:00.000",
											"sum(count)": {
												"value": null
											}
										},
										{
											"doc_count": 0,
											"key": 1726354800000,
											"key_as_string": "2024-09-14T23:00:00.000",
											"sum(count)": {
												"value": null
											}
										},
										{
											"doc_count": 0,
											"key": 1726356600000,
											"key_as_string": "2024-09-14T23:30:00.000",
											"sum(count)": {
												"value": null
											}
										},
										{
											"doc_count": 0,
											"key": 1726358400000,
											"key_as_string": "2024-09-15T00:00:00.000",
											"sum(count)": {
												"value": null
											}
										},
										{
											"doc_count": 1,
											"key": 1726360200000,
											"key_as_string": "2024-09-15T00:30:00.000",
											"sum(count)": {
												"value": 1
											}
										}
									]
								}
							}
						}
					}
				},
				"hits": {
					"hits": [],
					"max_score": null,
					"total": {
						"relation": "eq",
						"value": 2200
					}
				},
				"timed_out": false,
				"took": 1
			},
			"start_time_in_millis": 1707486436397
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__q__count", int64(4675)),
				model.NewQueryResultCol("aggr__q__time_buckets__key_0", int64(1726358400000/1800000)),
				model.NewQueryResultCol("aggr__q__time_buckets__count", int64(442)),
				model.NewQueryResultCol("metric__q__time_buckets__sum(count)_col_0", int64(442)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__q__count", int64(4675)),
				model.NewQueryResultCol("aggr__q__time_buckets__key_0", int64(1726367400000/1800000)),
				model.NewQueryResultCol("aggr__q__time_buckets__count", int64(1)),
				model.NewQueryResultCol("metric__q__time_buckets__sum(count)_col_0", int64(1)),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT sum(count(*)) OVER () AS "aggr__q__count",
			  toInt64((toUnixTimestamp64Milli("@timestamp")+timeZoneOffset(toTimezone(
			  "@timestamp", 'Europe/Warsaw'))*1000) / 1800000) AS
			  "aggr__q__time_buckets__key_0", count(*) AS "aggr__q__time_buckets__count",
			  sumOrNull("count") AS "metric__q__time_buckets__sum(count)_col_0"
			FROM __quesma_table_name
			WHERE NOT ("str_field" __quesma_match 'CRASH')
			GROUP BY toInt64((toUnixTimestamp64Milli("@timestamp")+timeZoneOffset(toTimezone
			  ("@timestamp", 'Europe/Warsaw'))*1000) / 1800000) AS
			  "aggr__q__time_buckets__key_0"
			ORDER BY "aggr__q__time_buckets__key_0" ASC`,
	},
	{
		TestName: "Weird aggregation and filter names",
		QueryRequestJson: `
		{
			"aggs": {
				"q": {
					"aggs": {
						"time": {
							"aggs": {
								"cardinality(a.b.keyword)": {
									"cardinality": {
										"field": "a.b.keyword"
									}
								}
							},
							"date_histogram": {
								"field": "@timestamp",
								"fixed_interval": "12h",
								"min_doc_count": 0,
								"time_zone": "Europe/Warsaw"
							}
						}
					},
					"filters": {
						"filters": {
							"(a.b:*c* OR a.b:*d*)": {
								"query_string": {
									"query": "(a.b:*c* OR a.b:*d*)"
								}
							}
						}
					},
				}
			},
			"runtime_mappings": {},
			"size": 0,
			"timeout": "30000ms",
			"track_total_hits": true
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
				"q": {
					"buckets": {
						"(a.b:*c* OR a.b:*d*)": {
							"doc_count": 14074,
							"time": {
								"buckets": [
									{
										"key_as_string": "2024-10-13T10:00:00.000",
										"key": 1728813600000,
										"doc_count": 319,
										"cardinality(a.b.keyword)": {
											"value": 672
										}
									}
								]
							}
						}
					}
				}
			}
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__q__count", int64(14074)),
				model.NewQueryResultCol("aggr__q__time__key_0", int64(1728856800000/43200000)),
				model.NewQueryResultCol("aggr__q__time__count", int64(319)),
				model.NewQueryResultCol("metric__q__time__cardinality(a.b.keyword)_col_0", int64(672)),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT sum(count(*)) OVER () AS "aggr__q__count",
			  toInt64((toUnixTimestamp64Milli("@timestamp")+timeZoneOffset(toTimezone(
			  "@timestamp", 'Europe/Warsaw'))*1000) / 43200000) AS "aggr__q__time__key_0",
			  count(*) AS "aggr__q__time__count",
			  uniq("a.b") AS "metric__q__time__cardinality(a.b.keyword)_col_0"
			FROM __quesma_table_name
			WHERE (("a.b" __quesma_match '%c%') OR "a.b" __quesma_match '%d%')
			GROUP BY toInt64((toUnixTimestamp64Milli("@timestamp")+timeZoneOffset(toTimezone
			  ("@timestamp", 'Europe/Warsaw'))*1000) / 43200000) AS "aggr__q__time__key_0"
			ORDER BY "aggr__q__time__key_0" ASC`,
	},
}
