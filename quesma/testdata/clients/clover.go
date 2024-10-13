// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package clients

import (
	"quesma/model"
	"quesma/testdata"
)

var CloverTests = []testdata.AggregationTestCase{
	{ // [0]
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
	{ // [1]
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
					"sampler": {
						"doc_count": 4675,
						"eventRate": {
							"buckets": [
								{
									"doc_count": 442,
									"key": 1726358400000,
									"key_as_string": "2024-09-15T00:00:00.000"
								},
								{
									"doc_count": 0,
									"key": 1726963200000,
									"key_as_string": "2024-09-22T00:00:00.000"
								},
								{
									"doc_count": 0,
									"key": 1727568000000,
									"key_as_string": "2024-09-29T00:00:00.000"
								},
								{
									"doc_count": 0,
									"key": 1728172800000,
									"key_as_string": "2024-10-06T00:00:00.000"
								},
								{
									"doc_count": 1,
									"key": 1728777600000,
									"key_as_string": "2024-10-13T00:00:00.000"
								}
							]
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
				model.NewQueryResultCol("aggr__other-filter__3__count", int64(442)),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT sum(countIf(("a" iLIKE '%b%' AND "c" iLIKE '%d%'))) OVER () AS
			  "aggr__other-filter__count",
			  sum(countIf(("a" iLIKE '%b%' AND "c" iLIKE '%d%'))) OVER () AS
			  "aggr__other-filter__3__parent_count",
			  "field" AS "aggr__other-filter__3__key_0",
			  countIf(("a" iLIKE '%b%' AND "c" iLIKE '%d%')) AS
			  "aggr__other-filter__3__count"
			FROM __quesma_table_name
			GROUP BY "field" AS "aggr__other-filter__3__key_0"
			ORDER BY "aggr__other-filter__3__count" DESC,
			  "aggr__other-filter__3__key_0" ASC
			LIMIT 16`,
	},
	{ // [2]
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
								"fixed_interval": "100ms",
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
					"sampler": {
						"doc_count": 4675,
						"eventRate": {
							"buckets": [
								{
									"doc_count": 442,
									"key": 1726358400000,
									"key_as_string": "2024-09-15T00:00:00.000"
								},
								{
									"doc_count": 0,
									"key": 1726963200000,
									"key_as_string": "2024-09-22T00:00:00.000"
								},
								{
									"doc_count": 0,
									"key": 1727568000000,
									"key_as_string": "2024-09-29T00:00:00.000"
								},
								{
									"doc_count": 0,
									"key": 1728172800000,
									"key_as_string": "2024-10-06T00:00:00.000"
								},
								{
									"doc_count": 1,
									"key": 1728777600000,
									"key_as_string": "2024-10-13T00:00:00.000"
								}
							]
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
				model.NewQueryResultCol("aggr__other-filter__3__count", int64(442)),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT sum(countIf(("a" iLIKE '%b%' AND "c" iLIKE '%d%'))) OVER () AS
			  "aggr__other-filter__count",
			  sum(countIf(("a" iLIKE '%b%' AND "c" iLIKE '%d%'))) OVER () AS
			  "aggr__other-filter__3__parent_count",
			  "field" AS "aggr__other-filter__3__key_0",
			  countIf(("a" iLIKE '%b%' AND "c" iLIKE '%d%')) AS
			  "aggr__other-filter__3__count"
			FROM __quesma_table_name
			GROUP BY "field" AS "aggr__other-filter__3__key_0"
			ORDER BY "aggr__other-filter__3__count" DESC,
			  "aggr__other-filter__3__key_0" ASC
			LIMIT 16`,
	},
	{ // [1]
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
								"extended_bounds": {
									"max": 1728592321610,
									"min": 1728333121610
								},
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
					"sampler": {
						"doc_count": 4675,
						"eventRate": {
							"buckets": [
								{
									"doc_count": 442,
									"key": 1726358400000,
									"key_as_string": "2024-09-15T00:00:00.000"
								},
								{
									"doc_count": 0,
									"key": 1726963200000,
									"key_as_string": "2024-09-22T00:00:00.000"
								},
								{
									"doc_count": 0,
									"key": 1727568000000,
									"key_as_string": "2024-09-29T00:00:00.000"
								},
								{
									"doc_count": 0,
									"key": 1728172800000,
									"key_as_string": "2024-10-06T00:00:00.000"
								},
								{
									"doc_count": 1,
									"key": 1728777600000,
									"key_as_string": "2024-10-13T00:00:00.000"
								}
							]
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
				model.NewQueryResultCol("aggr__sampler__count", int64(4675)),
				model.NewQueryResultCol("aggr__sampler__eventRate__key_0", int64(1726358400000)),
				model.NewQueryResultCol("aggr__sampler__eventRate__count", int64(442)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__sampler__count", int64(4675)),
				model.NewQueryResultCol("aggr__sampler__eventRate__key_0", int64(1728777600000)),
				model.NewQueryResultCol("aggr__sampler__eventRate__count", int64(1)),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT sum(count(*)) OVER () AS "aggr__sampler__count",
			  toInt64(toUnixTimestamp(toStartOfWeek(toTimezone("order_date", 'UTC'))))*1000
			  AS "aggr__sampler__eventRate__key_0",
			  count(*) AS "aggr__sampler__eventRate__count"
			FROM (
			  SELECT "order_date"
			  FROM __quesma_table_name
			  LIMIT 20000)
			GROUP BY toInt64(toUnixTimestamp(toStartOfWeek(toTimezone("order_date", 'UTC')))
			  )*1000 AS "aggr__sampler__eventRate__key_0"
			ORDER BY "aggr__sampler__eventRate__key_0" ASC`,
	},
	{ // [1]
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
	{ // [1]
		TestName: "bucket_script + empty buckets",
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
									"max": 1728749017086,
									"min": 1727477017086
								},
								"field": "@timestamp",
								"fixed_interval": "12h",
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
									"gte": "2024-06-29T16:03:37.086Z",
									"lte": "2024-10-12T16:03:37.086Z"
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
				model.NewQueryResultCol("aggr__q__count", int64(202)),
				model.NewQueryResultCol("aggr__q__time_buckets__key_0", int64(1728518400000/43200000)),
				model.NewQueryResultCol("aggr__q__time_buckets__count", int64(202)),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT sum(countIf(True)) OVER () AS "aggr__q__count",
			  toInt64((toUnixTimestamp64Milli("@timestamp")+timeZoneOffset(toTimezone(
			  "@timestamp", 'Europe/Warsaw'))*1000) / 43200000) AS
			  "aggr__q__time_buckets__key_0",
			  countIf(True) AS "aggr__q__time_buckets__count"
			FROM __quesma_table_name
			WHERE ("@timestamp">=parseDateTime64BestEffort('2024-06-29T16:03:37.086Z') AND
			  "@timestamp"<=parseDateTime64BestEffort('2024-10-12T16:03:37.086Z'))
			GROUP BY toInt64((toUnixTimestamp64Milli("@timestamp")+timeZoneOffset(toTimezone
			  ("@timestamp", 'Europe/Warsaw'))*1000) / 43200000) AS
			  "aggr__q__time_buckets__key_0"
			ORDER BY "aggr__q__time_buckets__key_0" ASC`,
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
				model.NewQueryResultCol("metric__timeseries__61ca57f2-469d-11e7-af02-69e470af7417-denominator_col_0", int64(202)),
				model.NewQueryResultCol("metric__timeseries__61ca57f2-469d-11e7-af02-69e470af7417-numerator_col_0", int64(0)),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT count(*) AS "aggr__timeseries__count",
			  countIf(True) AS
			  "metric__timeseries__61ca57f2-469d-11e7-af02-69e470af7417-denominator_col_0",
			  countIf(NOT ("xdr.result_code_str" = 'DIAMETER_SUCCESS')) AS
			  "metric__timeseries__61ca57f2-469d-11e7-af02-69e470af7417-numerator_col_0"
			FROM __quesma_table_name
			WHERE ("@timestamp">=parseDateTime64BestEffort('2024-10-11T09:58:03.723Z') AND
			  "@timestamp"<=parseDateTime64BestEffort('2024-10-11T10:13:03.723Z'))`,
	},
	{ // [1]
		TestName: "date_histogram min_doc_count=0, no rows",
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
									"max": 1728768875239,
									"min": 1728768865239
								},
								"field": "@timestamp",
								"fixed_interval": "1s",
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
			"runtime_mappings": {},
			"size": 0,
			"timeout": "30000ms",
			"track_total_hits": true
		}`,
		ExpectedResponse: `
		{
			"completion_time_in_millis": 1728772246443,
			"expiration_time_in_millis": 1728772306341,
			"id": "FmRxVS1IZzBMU2JpUU56RUwwNWJiRXccMXQ1ZkV1ZjNRVnluVWZtbmZFYnJPUTozOTkzMg==",
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
						"buckets": {
							"*": {
								"doc_count": 0,
								"time_buckets": {
									"buckets": [
										{
											"count": {
												"value": 0.0
											},
											"doc_count": 0,
											"key": 1728772243000,
											"key_as_string": "2024/10/13 00:30:43"
										},
										{
											"count": {
												"value": 0.0
											},
											"doc_count": 0,
											"key": 1728772244000,
											"key_as_string": "2024/10/13 00:30:44"
										},
										{
											"count": {
												"value": 0.0
											},
											"doc_count": 0,
											"key": 1728772245000,
											"key_as_string": "2024/10/13 00:30:45"
										}
									],
									"meta": {
										"type": "time_buckets"
									}
								}
							}
						},
						"meta": {
							"type": "split"
						}
					}
				},
				"hits": {
					"hits": [],
					"max_score": null,
					"total": {
						"relation": "eq",
						"value": 0
					}
				},
				"timed_out": false,
				"took": 102
			},
			"start_time_in_millis": 1728772246341
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{},
		ExpectedPancakeSQL: `
			SELECT sum(countIf(True)) OVER () AS "aggr__q__count",
			  toInt64((toUnixTimestamp64Milli("@timestamp")+timeZoneOffset(toTimezone(
			  "@timestamp", 'Europe/Warsaw'))*1000) / 1000) AS
			  "aggr__q__time_buckets__key_0",
			  countIf(True) AS "aggr__q__time_buckets__count"
			FROM __quesma_table_name
			GROUP BY toInt64((toUnixTimestamp64Milli("@timestamp")+timeZoneOffset(toTimezone
			  ("@timestamp", 'Europe/Warsaw'))*1000) / 1000) AS
			  "aggr__q__time_buckets__key_0"
			ORDER BY "aggr__q__time_buckets__key_0" ASC`,
	},
}
