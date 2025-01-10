// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package testdata

import (
	"math"
	"quesma/clickhouse"
	"quesma/model"
	"time"
)

var timestampGroupByClause = model.AsString(clickhouse.TimestampGroupBy(
	model.NewColumnRef("@timestamp"), model.NewColumnRef("@timestamp"), 30*time.Second))

func groupBySQL(fieldName string, typ clickhouse.DateTimeType, groupByInterval time.Duration) string {
	return model.AsString(clickhouse.TimestampGroupBy(model.NewColumnRef(fieldName), model.NewColumnRef(fieldName), groupByInterval))
}

const fullTextFieldName = `"` + model.FullTextFieldNamePlaceHolder + `"`

// TODO change some tests to size > 0, and track_total_hits different values
var AggregationTests = []AggregationTestCase{
	{ // [0]
		TestName: "simple max/min aggregation as 2 siblings",
		QueryRequestJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"maxAgg": {
					"max": {
						"field": "AvgTicketPrice"
					}
				},
				"minAgg": {
					"min": {
						"field": "AvgTicketPrice"
					}
				}
			},
			"fields": [
				{
					"field": "timestamp",
					"format": "date_time"
				}
			],
			"query": {
				"bool": {
					"filter": [
						{
							"range": {
								"timestamp": {
									"format": "strict_date_optional_time",
									"gte": "2024-02-02T13:47:16.029Z",
									"lte": "2024-02-09T13:47:16.029Z"
								}
							}
						}
					],
					"must": [],
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
			"script_fields": {},
			"size": 0,
			"stored_fields": [
				"*"
			],
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
					"maxAgg": {
						"value": 1199.72900390625
					},
					"minAgg": {
						"value": 100.14596557617188
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
				model.NewQueryResultCol("metric__maxAgg_col_0", 1199.72900390625),
				model.NewQueryResultCol("metric__minAgg_col_0", 100.14596557617188),
			}},
		},
		ExpectedPancakeSQL: `SELECT maxOrNull("AvgTicketPrice") AS "metric__maxAgg_col_0", ` +
			`minOrNull("AvgTicketPrice") AS "metric__minAgg_col_0" ` +
			`FROM ` + TableName + ` ` +
			`WHERE ("timestamp">=fromUnixTimestamp64Milli(1706881636029) AND "timestamp"<=fromUnixTimestamp64Milli(1707486436029))`,
	},
	{ // [1]
		TestName: "2 sibling count aggregations",
		QueryRequestJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"0": {
					"aggs": {
						"1-bucket": {
							"filter": {
								"bool": {
									"filter": [{
										"bool": {
											"minimum_should_match": 1,
											"should": [{
												"match": {
													"FlightDelay": true
												}
											}]
										}
									}],
									"must": [],
									"must_not": [],
									"should": []
								}
							}
						},
						"3-bucket": {
							"filter": {
								"bool": {
									"filter": [{
										"bool": {
											"minimum_should_match": 1,
											"should": [
												{
													"match": {
														"Cancelled": true
													}
												}
											]
										}
									}],
									"must": [],
									"must_not": [],
									"should": []
								}
							}
						}
					},
					"terms": {
						"field": "OriginCityName",
						"order": {
							"_key": "asc"
						},
						"size": 1000
					}
				}
			},
			"fields": [{
				"field": "timestamp",
				"format": "date_time"
			}],
			"query": {
				"bool": {
					"filter": [{
						"range": {
							"timestamp": {
								"format": "strict_date_optional_time",
								"gte": "2024-02-02T13:47:16.029Z",
								"lte": "2024-02-09T13:47:16.029Z"
							}
						}
					}],
					"must": [],
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
			"script_fields": {},
			"size": 0,
			"stored_fields": [
				"*"
			],
			"track_total_hits": false
		}`,
		ExpectedResponse: `
		{
			"completion_time_in_millis": 1707486436408,
			"expiration_time_in_millis": 1707486496403,
			"id": "FllzVkVKUUxYUXJ1MXRWY3UyUEUtZnccc3VtTlI1T25TVGFSYlI0dFM1dkNHQTo0MzMwMA==",
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
								"1-bucket": {
									"doc_count": 7
								},
								"3-bucket": {
									"doc_count": 3
								},
								"doc_count": 23,
								"key": "Abu Dhabi"
							},
							{
								"1-bucket": {
									"doc_count": 3
								},
								"3-bucket": {
									"doc_count": 2
								},
								"doc_count": 20,
								"key": "Adelaide"
							},
							{
								"1-bucket": {
									"doc_count": 0
								},
								"3-bucket": {
									"doc_count": 2
								},
								"doc_count": 3,
								"key": "Albuquerque"
							}
						],
						"doc_count_error_upper_bound": 0,
						"sum_other_doc_count": 0
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
				"took": 5
			},
			"start_time_in_millis": 1707486436403
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__parent_count", 46),
				model.NewQueryResultCol("aggr__0__key_0", "Abu Dhabi"),
				model.NewQueryResultCol("aggr__0__count", uint64(23)),
				model.NewQueryResultCol("aggr__0__1-bucket__count", 7),
				model.NewQueryResultCol("metric__0__3-bucket_col_0", 3),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__parent_count", 46),
				model.NewQueryResultCol("aggr__0__key_0", "Adelaide"),
				model.NewQueryResultCol("aggr__0__count", uint64(20)),
				model.NewQueryResultCol("aggr__0__1-bucket__count", 3),
				model.NewQueryResultCol("metric__0__3-bucket_col_0", 2),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__parent_count", 46),
				model.NewQueryResultCol("aggr__0__key_0", "Albuquerque"),
				model.NewQueryResultCol("aggr__0__count", uint64(3)),
				model.NewQueryResultCol("aggr__0__1-bucket__count", 0),
				model.NewQueryResultCol("metric__0__3-bucket_col_0", 2),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT sum(count(*)) OVER () AS "aggr__0__parent_count",
			  "OriginCityName" AS "aggr__0__key_0", count(*) AS "aggr__0__count",
			  countIf("Cancelled"==true) AS "metric__0__3-bucket_col_0",
			  countIf("FlightDelay"==true) AS "aggr__0__1-bucket__count"
			FROM ` + TableName + `
			WHERE ("timestamp">=fromUnixTimestamp64Milli(1706881636029) AND "timestamp"<=fromUnixTimestamp64Milli(1707486436029))
			GROUP BY "OriginCityName" AS "aggr__0__key_0"
			ORDER BY "aggr__0__key_0" ASC
			LIMIT 1001`,
	},
	{ // [2]
		TestName: "date_histogram + size as string",
		QueryRequestJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"0": {
					"aggs": {
						"1": {
							"date_histogram": {
								"field": "timestamp",
								"fixed_interval": "3h",
								"time_zone": "Europe/Warsaw"
							}
						}
					},
					"terms": {
						"field": "FlightDelayType",
						"order": {
							"_count": "desc"
						},
						"shard_size": 25,
						"size": "10"
					}
				}
			},
			"fields": [
				{
					"field": "timestamp",
					"format": "date_time"
				}
			],
			"query": {
				"bool": {
					"filter": [
						{
							"range": {
								"timestamp": {
									"format": "strict_date_optional_time",
									"gte": "2024-02-02T13:47:16.029Z",
									"lte": "2024-02-09T13:47:16.029Z"
								}
							}
						}
					],
					"must": [],
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
			"script_fields": {},
			"size": 0,
			"stored_fields": [
				"*"
			],
			"track_total_hits": 12
		}`,
		ExpectedResponse: `
		{
			"completion_time_in_millis": 1707486436412,
			"expiration_time_in_millis": 1707486496405,
			"id": "FlhnZWJVYkNaUk1PdldIMU5od2RRSmccc3VtTlI1T25TVGFSYlI0dFM1dkNHQTo0MzMxMA==",
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
								"1": {
									"buckets": [
										{
											"doc_count": 2,
											"key": 1706871600000,
											"key_as_string": "2024-02-02T11:00:00.000"
										},
										{
											"doc_count": 27,
											"key": 1706882400000,
											"key_as_string": "2024-02-02T14:00:00.000"
										},
										{
											"doc_count": 34,
											"key": 1706893200000,
											"key_as_string": "2024-02-02T17:00:00.000"
										}
									]
								},
								"doc_count": 1647,
								"key": "No Delay"
							},
							{
								"1": {
									"buckets": [
										{
											"doc_count": 0,
											"key": 1706871600000,
											"key_as_string": "2024-02-02T11:00:00.000"
										},
										{
											"doc_count": 2,
											"key": 1706882400000,
											"key_as_string": "2024-02-02T14:00:00.000"
										}
									]
								},
								"doc_count": 45,
								"key": "Security Delay"
							}
						],
						"doc_count_error_upper_bound": 0,
						"sum_other_doc_count": 508
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
				"took": 7
			},
			"start_time_in_millis": 1707486436405
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__parent_count", uint64(2200)),
				model.NewQueryResultCol("aggr__0__key_0", "No Delay"),
				model.NewQueryResultCol("aggr__0__count", uint64(1647)),
				model.NewQueryResultCol("aggr__0__1__key_0", int64(1706875200000/1000/60/60/3)),
				model.NewQueryResultCol("aggr__0__1__count", uint64(2)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__parent_count", uint64(2200)),
				model.NewQueryResultCol("aggr__0__key_0", "No Delay"),
				model.NewQueryResultCol("aggr__0__count", uint64(1647)),
				model.NewQueryResultCol("aggr__0__1__key_0", int64(1706886000000/1000/60/60/3)),
				model.NewQueryResultCol("aggr__0__1__count", uint64(27)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__parent_count", uint64(2200)),
				model.NewQueryResultCol("aggr__0__key_0", "No Delay"),
				model.NewQueryResultCol("aggr__0__count", uint64(1647)),
				model.NewQueryResultCol("aggr__0__1__key_0", int64(1706896800000/1000/60/60/3)),
				model.NewQueryResultCol("aggr__0__1__count", uint64(34)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__parent_count", uint64(2200)),
				model.NewQueryResultCol("aggr__0__key_0", "Security Delay"),
				model.NewQueryResultCol("aggr__0__count", uint64(45)),
				model.NewQueryResultCol("aggr__0__1__key_0", int64(1706875200000/1000/60/60/3)),
				model.NewQueryResultCol("aggr__0__1__count", uint64(0)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__parent_count", uint64(2200)),
				model.NewQueryResultCol("aggr__0__key_0", "Security Delay"),
				model.NewQueryResultCol("aggr__0__count", uint64(45)),
				model.NewQueryResultCol("aggr__0__1__key_0", int64(1706886000000/1000/60/60/3)),
				model.NewQueryResultCol("aggr__0__1__count", uint64(2)),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT "aggr__0__parent_count", "aggr__0__key_0", "aggr__0__count",
			  "aggr__0__1__key_0", "aggr__0__1__count"
			FROM (
			  SELECT "aggr__0__parent_count", "aggr__0__key_0", "aggr__0__count",
				"aggr__0__1__key_0", "aggr__0__1__count",
				dense_rank() OVER (ORDER BY "aggr__0__count" DESC, "aggr__0__key_0" ASC)
				AS "aggr__0__order_1_rank",
				dense_rank() OVER (PARTITION BY "aggr__0__key_0" ORDER BY
				"aggr__0__1__key_0" ASC) AS "aggr__0__1__order_1_rank"
			  FROM (
				SELECT sum(count(*)) OVER () AS "aggr__0__parent_count",
				  "FlightDelayType" AS "aggr__0__key_0",
				  sum(count(*)) OVER (PARTITION BY "aggr__0__key_0") AS "aggr__0__count",
				  toInt64((toUnixTimestamp64Milli("timestamp")+timeZoneOffset(toTimezone(
				  "timestamp", 'Europe/Warsaw'))*1000) / 10800000) AS "aggr__0__1__key_0",
				  count(*) AS "aggr__0__1__count"
				FROM ` + TableName + `
				WHERE ("timestamp">=fromUnixTimestamp64Milli(1706881636029) AND "timestamp"<=fromUnixTimestamp64Milli(1707486436029))
				GROUP BY "FlightDelayType" AS "aggr__0__key_0",
				  toInt64((toUnixTimestamp64Milli("timestamp")+timeZoneOffset(toTimezone(
				  "timestamp", 'Europe/Warsaw'))*1000) / 10800000) AS "aggr__0__1__key_0"))
			WHERE "aggr__0__order_1_rank"<=11
			ORDER BY "aggr__0__order_1_rank" ASC, "aggr__0__1__order_1_rank" ASC`,
	},
	{ // [3]
		TestName: "Sum",
		QueryRequestJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"0": {
					"sum": {
						"field": "taxful_total_price"
					}
				}
			},
			"fields": [
				{
					"field": "customer_birth_date",
					"format": "date_time"
				},
				{
					"field": "order_date",
					"format": "date_time"
				},
				{
					"field": "products.created_on",
					"format": "date_time"
				}
			],
			"query": {
				"bool": {
					"filter": [
						{
							"range": {
								"order_date": {
									"format": "strict_date_optional_time",
									"gte": "2024-02-06T09:59:57.034Z",
									"lte": "2024-02-13T09:59:57.034Z"
								}
							}
						}
					],
					"must": [],
					"must_not": [],
					"should": []
				}
			},
			"runtime_mappings": {},
			"script_fields": {},
			"size": 0,
			"stored_fields": [
				"*"
			],
			"track_total_hits": true
		}`,
		ExpectedResponse: `
		{
			"completion_time_in_millis": 1707818398420,
			"expiration_time_in_millis": 1707818458417,
			"id": "FlN3NWxwZC1OVFN5ZHZvUU5CVkk1dGccQVQxSHZodzJSbW1penpRdThTa0lKUTo2NDg4Ng==",
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
						"value": 76631.67578125
					}
				},
				"hits": {
					"hits": [],
					"max_score": null,
					"total": {
						"relation": "eq",
						"value": 1043
					}
				},
				"timed_out": false,
				"took": 3
			},
			"start_time_in_millis": 1707818398417
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("metric__0_col_0", 76631.67578125),
			}},
		},
		ExpectedPancakeSQL: `SELECT sumOrNull("taxful_total_price") AS "metric__0_col_0" ` +
			`FROM ` + TableName + ` ` +
			`WHERE ("order_date">=fromUnixTimestamp64Milli(1707213597034) AND "order_date"<=fromUnixTimestamp64Milli(1707818397034))`,
	},
	{ // [4]
		TestName: "cardinality",
		QueryRequestJson: `
		{
			"aggs": {
				"suggestions": {
					"terms": {
						"field": "OriginCityName",
						"order": {
							"_count": "desc"
						},
						"shard_size": 10,
						"size": 10
					}
				},
				"unique_terms": {
					"cardinality": {
						"field": "OriginCityName"
					}
				}
			},
			"query": {
				"bool": {
					"filter": [
						{
							"bool": {
								"filter": [
									{
										"range": {
											"timestamp": {
												"format": "strict_date_optional_time",
												"gte": "2024-02-02T13:47:16.029Z",
												"lte": "2024-02-09T13:47:16.029Z"
											}
										}
									}
								],
								"must": [],
								"must_not": [],
								"should": []
							}
						}
					]
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
			"terminate_after": 100000,
			"timeout": "1000ms",
			"track_total_hits": true
		}`, // missing entire response below, just "response" field.
		ExpectedResponse: `
		{
			"response": {
				"_shards": {
					"failed": 0,
					"skipped": 0,
					"successful": 1,
					"total": 1
				},
				"aggregations": {
					"suggestions": {
						"buckets": [
							{
								"doc_count": 73,
								"key": "Rome"
							},
							{
								"doc_count": 44,
								"key": "Bogota"
							},
							{
								"doc_count": 32,
								"key": "Milan"
							}
						],
						"doc_count_error_upper_bound": 0,
						"sum_other_doc_count": 2051
					},
					"unique_terms": {
						"value": 143
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
				"terminated_early": false,
				"timed_out": false,
				"took": 6
			}
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("metric__unique_terms_col_0", 143),
				model.NewQueryResultCol("aggr__suggestions__parent_count", uint64(2200)),
				model.NewQueryResultCol("aggr__suggestions__key_0", "Rome"),
				model.NewQueryResultCol("aggr__suggestions__count", uint64(73)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("metric__unique_terms_col_0", 143),
				model.NewQueryResultCol("aggr__suggestions__parent_count", uint64(2200)),
				model.NewQueryResultCol("aggr__suggestions__key_0", "Bogota"),
				model.NewQueryResultCol("aggr__suggestions__count", uint64(44)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("metric__unique_terms_col_0", 143),
				model.NewQueryResultCol("aggr__suggestions__parent_count", uint64(2200)),
				model.NewQueryResultCol("aggr__suggestions__key_0", "Milan"),
				model.NewQueryResultCol("aggr__suggestions__count", uint64(32)),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT uniqMerge(uniqState("OriginCityName")) OVER () AS
			  "metric__unique_terms_col_0",
			  sum(count(*)) OVER () AS "aggr__suggestions__parent_count",
			  "OriginCityName" AS "aggr__suggestions__key_0",
			  count(*) AS "aggr__suggestions__count"
			FROM ` + TableName + `
			WHERE ("timestamp">=fromUnixTimestamp64Milli(1706881636029) AND "timestamp"<=fromUnixTimestamp64Milli(1707486436029))
			GROUP BY "OriginCityName" AS "aggr__suggestions__key_0"
			ORDER BY "aggr__suggestions__count" DESC, "aggr__suggestions__key_0" ASC
			LIMIT 11`,
	},
	{ // [5]
		TestName: "simple filter/count",
		QueryRequestJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"0-bucket": {
					"filter": {
						"bool": {
							"filter": [
								{
									"bool": {
										"minimum_should_match": 1,
										"should": [
											{
												"match": {
													"FlightDelay": true
												}
											}
										]
									}
								}
							],
							"must": [],
							"must_not": [],
							"should": []
						}
					}
				}
			},
			"fields": [
				{
					"field": "timestamp",
					"format": "date_time"
				}
			],
			"query": {
				"bool": {
					"filter": [
						{
							"range": {
								"timestamp": {
									"format": "strict_date_optional_time",
									"gte": "2024-02-02T13:47:16.029Z",
									"lte": "2024-02-09T13:47:16.029Z"
								}
							}
						}
					],
					"must": [],
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
			"script_fields": {},
			"size": 0,
			"stored_fields": [
				"*"
			],
			"track_total_hits": true
		}`,
		ExpectedResponse: `
		{
			"completion_time_in_millis": 1707486436416,
			"expiration_time_in_millis": 1707486496414,
			"id": "Fl9HbHEzajB2VERlRFNyYVh1RzlSRFEcc3VtTlI1T25TVGFSYlI0dFM1dkNHQTo0MzM1OA==",
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
					"0-bucket": {
						"doc_count": 553
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
				"took": 2
			},
			"start_time_in_millis": 1707486436414
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0-bucket__count", uint64(553)),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT countIf("FlightDelay"==true) AS "aggr__0-bucket__count"
			FROM ` + TableName + `
			 WHERE ("timestamp">=fromUnixTimestamp64Milli(1706881636029) AND "timestamp"<=fromUnixTimestamp64Milli(1707486436029))`,
	},
	{ // [6]
		TestName: "filters",
		QueryRequestJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"time_offset_split": {
					"aggs": {},
					"filters": {
						"filters": {
							"0": {
								"range": {
									"timestamp": {
										"format": "strict_date_optional_time",
										"gte": "2024-02-02T13:47:16.029Z",
										"lte": "2024-02-09T13:47:16.029Z"
									}
								}
							},
							"604800000": {
								"range": {
									"timestamp": {
										"format": "strict_date_optional_time",
										"gte": "2024-01-26T13:47:16.029Z",
										"lte": "2024-02-02T13:47:16.029Z"
									}
								}
							}
						}
					}
				}
			},
			"fields": [
				{
					"field": "timestamp",
					"format": "date_time"
				}
			],
			"query": {
				"bool": {
					"filter": [
						{
							"match_phrase": {
								"FlightDelay": true
							}
						},
						{
							"bool": {
								"minimum_should_match": 1,
								"should": [
									{
										"bool": {
											"filter": [
												{
													"range": {
														"timestamp": {
															"format": "strict_date_optional_time",
															"gte": "2024-02-02T13:47:16.029Z",
															"lte": "2024-02-09T13:47:16.029Z"
														}
													}
												}
											]
										}
									},
									{
										"bool": {
											"filter": [
												{
													"range": {
														"timestamp": {
															"format": "strict_date_optional_time",
															"gte": "2024-01-26T13:47:16.029Z",
															"lte": "2024-02-02T13:47:16.029Z"
														}
													}
												}
											]
										}
									}
								]
							}
						}
					],
					"must": [],
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
			"script_fields": {},
			"size": 0,
			"stored_fields": [
				"*"
			],
			"track_total_hits": true
		}`,
		ExpectedResponse: `
		{
			"completion_time_in_millis": 1707486436417,
			"expiration_time_in_millis": 1707486496414,
			"id": "FkVpLUZpYUpuUXpDdVY3RV9nbGVuX2ccc3VtTlI1T25TVGFSYlI0dFM1dkNHQTo0MzM2Nw==",
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
					"time_offset_split": {
						"buckets": {
							"0": {
								"doc_count": 553
							},
							"604800000": {
								"doc_count": 351
							}
						}
					}
				},
				"hits": {
					"hits": [],
					"max_score": null,
					"total": {
						"relation": "eq",
						"value": 904
					}
				},
				"timed_out": false,
				"took": 3
			},
			"start_time_in_millis": 1707486436414
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("filter_0__aggr__time_offset_split__count", uint64(553)),
				model.NewQueryResultCol("filter_1__aggr__time_offset_split__count", uint64(351)),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT countIf(("timestamp">=fromUnixTimestamp64Milli(1706881636029) AND
			  "timestamp"<=fromUnixTimestamp64Milli(1707486436029))) AS
			  "filter_0__aggr__time_offset_split__count",
			  countIf(("timestamp">=fromUnixTimestamp64Milli(1706276836029) AND "timestamp"
			  <=fromUnixTimestamp64Milli(1706881636029))) AS
			  "filter_1__aggr__time_offset_split__count"
			FROM __quesma_table_name
			WHERE ("FlightDelay"==true AND (("timestamp">=fromUnixTimestamp64Milli(
			  1706881636029) AND "timestamp"<=fromUnixTimestamp64Milli(1707486436029)) OR (
			  "timestamp">=fromUnixTimestamp64Milli(1706276836029) AND "timestamp"<=
			  fromUnixTimestamp64Milli(1706881636029))))`,
	},
	{ // [7]
		TestName: "top hits, quite complex",
		QueryRequestJson: `
		{
			"aggs": {
				"origins": {
					"aggs": {
						"distinations": {
							"aggs": {
								"destLocation": {
									"top_hits": {
										"_source": {
											"includes": [
												"DestLocation"
											]
										},
										"size": 1
									}
								}
							},
							"terms": {
								"field": "DestAirportID",
								"size": 10000
							}
						},
						"originLocation": {
							"top_hits": {
								"_source": {
									"includes": [
										"OriginLocation",
										"Origin"
									]
								},
								"size": 1
							}
						}
					},
					"terms": {
						"field": "OriginAirportID",
						"size": 10000
					}
				}
			},
			"query": {
				"bool": {
					"filter": [],
					"must": [],
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
			"track_total_hits": false
		}`,
		ExpectedResponse: `
		{
			"completion_time_in_millis": 1707486436538,
			"expiration_time_in_millis": 1707486496501,
			"id": "FmI0UThWNVhpUUxTRXhCUzZpdjAxT2ccc3VtTlI1T25TVGFSYlI0dFM1dkNHQTo0MzUxMw==",
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
					"origins": {
						"buckets": [
							{
								"distinations": {
									"buckets": [
										{
											"destLocation": {
												"hits": {
													"hits": [
														{
															"_id": "3rsTjo0Bz-04hDVlNJNA",
															"_index": "kibana_sample_data_flights",
															"_score": 1.0,
															"_source": {
																"DestLocation": {
																	"lat": "-34.8222",
																	"lon": "-58.5358"
																}
															}
														}
													],
													"max_score": 1.0,
													"total": {
														"relation": "eq",
														"value": 21
													}
												}
											},
											"doc_count": 21,
											"key": "EZE"
										},
										{
											"destLocation": {
												"hits": {
													"hits": [
														{
															"_id": "xrsTjo0Bz-04hDVlNJi_",
															"_index": "kibana_sample_data_flights",
															"_score": 1.0,
															"_source": {
																"DestLocation": {
																	"lat": "-0.129166667",
																	"lon": "-78.3575"
																}
															}
														}
													],
													"max_score": 1.0,
													"total": {
														"relation": "eq",
														"value": 12
													}
												}
											},
											"doc_count": 12,
											"key": "UIO"
										}
									],
									"doc_count_error_upper_bound": 0,
									"sum_other_doc_count": 250
								},
								"doc_count": 283,
								"key": "UIO",
								"originLocation": {
									"hits": {
										"hits": [
											{
												"_id": "3rsTjo0Bz-04hDVlNJNA",
												"_index": "kibana_sample_data_flights",
												"_score": 1.0,
												"_source": {
													"Origin": "Mariscal Sucre International Airport",
													"OriginLocation": {
														"lat": "-0.129166667",
														"lon": "-78.3575"
													}
												}
											}
										],
										"max_score": 1.0,
										"total": {
											"relation": "eq",
											"value": 283
										}
									}
								}
							},
							{
								"distinations": {
									"sum_other_doc_count": 4,
									"buckets": [
										{
											"destLocation": {
												"hits": {
													"hits": [
														{
															"_id": "hLsTjo0Bz-04hDVlNJm_",
															"_index": "kibana_sample_data_flights",
															"_score": 1.0,
															"_source": {
																"DestLocation": {
																	"lat": "45.47060013",
																	"lon": "-73.74079895"
																}
															}
														}
													],
													"max_score": 1.0,
													"total": {
														"relation": "eq",
														"value": 11
													}
												}
											},
											"doc_count": 11,
											"key": "YUL"
										},
										{
											"destLocation": {
												"hits": {
													"hits": [
														{
															"_id": "kLsTjo0Bz-04hDVlNJRA",
															"_index": "kibana_sample_data_flights",
															"_score": 1.0,
															"_source": {
																"DestLocation": {
																	"lat": "-34.8222",
																	"lon": "-58.5358"
																}
															}
														}
													],
													"max_score": 1.0,
													"total": {
														"relation": "eq",
														"value": 10
													}
												}
											},
											"doc_count": 10,
											"key": "EZE"
										}
									]
								},
								"doc_count": 25,
								"key": "DLH",
								"originLocation": {
									"hits": {
										"hits": [
											{
												"_id": "0bsTjo0Bz-04hDVlNJi_",
												"_index": "kibana_sample_data_flights",
												"_score": 1.0,
												"_source": {
													"Origin": "Duluth International Airport",
													"OriginLocation": {
														"lat": "46.84209824",
														"lon": "-92.19360352"
													}
												}
											}
										],
										"max_score": 1.0,
										"total": {
											"relation": "eq",
											"value": 15
										}
									}
								}
							}
						],
						"doc_count_error_upper_bound": 0,
						"sum_other_doc_count": 12706
					}
				},
				"hits": {
					"hits": [],
					"max_score": null,
					"total": {
						"relation": "eq",
						"value": 13014
					}
				},
				"timed_out": false,
				"took": 37
			},
			"start_time_in_millis": 1707486436501
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{
				Cols: []model.QueryResultCol{
					model.NewQueryResultCol("aggr__origins__parent_count", uint64(13014)),
					model.NewQueryResultCol("aggr__origins__key_0", "UIO"),
					model.NewQueryResultCol("aggr__origins__count", int64(283)),
					model.NewQueryResultCol("aggr__origins__distinations__parent_count", int64(283)),
					model.NewQueryResultCol("aggr__origins__distinations__key_0", "EZE"),
					model.NewQueryResultCol("aggr__origins__distinations__count", int64(21)),
					model.NewQueryResultCol("top_hits__origins__distinations__destLocation_col_0", map[string]interface{}{
						"lat": "-34.8222",
						"lon": "-58.5358",
					}),
					model.NewQueryResultCol("top_hits_rank", int64(1)),
				},
			},
			{
				Cols: []model.QueryResultCol{
					model.NewQueryResultCol("aggr__origins__parent_count", uint64(13014)),
					model.NewQueryResultCol("aggr__origins__key_0", "UIO"),
					model.NewQueryResultCol("aggr__origins__count", int64(283)),
					model.NewQueryResultCol("aggr__origins__distinations__parent_count", int64(283)),
					model.NewQueryResultCol("aggr__origins__distinations__key_0", "UIO"),
					model.NewQueryResultCol("aggr__origins__distinations__count", int64(12)),
					model.NewQueryResultCol("top_hits__origins__distinations__destLocation_col_0", map[string]interface{}{
						"lat": "-0.129166667",
						"lon": "-78.3575",
					}),
					model.NewQueryResultCol("top_hits_rank", int64(1)),
				},
			},
			{
				Cols: []model.QueryResultCol{
					model.NewQueryResultCol("aggr__origins__parent_count", uint64(13014)),
					model.NewQueryResultCol("aggr__origins__key_0", "DLH"),
					model.NewQueryResultCol("aggr__origins__count", int64(25)),
					model.NewQueryResultCol("aggr__origins__distinations__parent_count", int64(25)),
					model.NewQueryResultCol("aggr__origins__distinations__key_0", "YUL"),
					model.NewQueryResultCol("aggr__origins__distinations__count", int64(11)),
					model.NewQueryResultCol("top_hits__origins__distinations__destLocation_col_0", map[string]interface{}{
						"lat": "45.47060013",
						"lon": "-73.74079895",
					}),
					model.NewQueryResultCol("top_hits_rank", int64(1)),
				},
			},
			{
				Cols: []model.QueryResultCol{
					model.NewQueryResultCol("aggr__origins__parent_count", uint64(13014)),
					model.NewQueryResultCol("aggr__origins__key_0", "DLH"),
					model.NewQueryResultCol("aggr__origins__count", int64(25)),
					model.NewQueryResultCol("aggr__origins__distinations__parent_count", int64(25)),
					model.NewQueryResultCol("aggr__origins__distinations__key_0", "EZE"),
					model.NewQueryResultCol("aggr__origins__distinations__count", int64(10)),
					model.NewQueryResultCol("top_hits__origins__distinations__destLocation_col_0", map[string]interface{}{
						"lat": "-34.8222",
						"lon": "-58.5358",
					}),
					model.NewQueryResultCol("top_hits_rank", int64(1)),
				},
			},
		},
		AdditionalAcceptableDifference: []string{"_index", "_id", "value"},
		// TODO: Remove value as it is used for total hits
		ExpectedAdditionalPancakeResults: [][]model.QueryResultRow{
			{
				{
					Cols: []model.QueryResultCol{
						model.NewQueryResultCol("aggr__origins__parent_count", uint64(13014)),
						model.NewQueryResultCol("aggr__origins__key_0", "UIO"),
						model.NewQueryResultCol("aggr__origins__count", int64(283)),
						model.NewQueryResultCol("top_hits__origins__originLocation_col_0", map[string]interface{}{
							"lat": "-0.129166667",
							"lon": "-78.3575",
						}),
						model.NewQueryResultCol("top_hits__origins__originLocation_col_1", "Mariscal Sucre International Airport"),
						model.NewQueryResultCol("top_hits_rank", int64(1)),
					},
				},
				{
					Cols: []model.QueryResultCol{
						model.NewQueryResultCol("aggr__origins__parent_count", uint64(13014)),
						model.NewQueryResultCol("aggr__origins__key_0", "DLH"),
						model.NewQueryResultCol("aggr__origins__count", int64(15)),
						model.NewQueryResultCol("top_hits__origins__originLocation_col_0", map[string]interface{}{
							"lat": "46.84209824",
							"lon": "-92.19360352",
						}),
						model.NewQueryResultCol("top_hits__origins__originLocation_col_1", "Duluth International Airport"),
						model.NewQueryResultCol("top_hits_rank", int64(1)),
					},
				},
			},
		},
		ExpectedPancakeSQL: `
			WITH quesma_top_hits_group_table AS (
			  SELECT "aggr__origins__parent_count", "aggr__origins__key_0",
				"aggr__origins__count", "aggr__origins__distinations__parent_count",
				"aggr__origins__distinations__key_0", "aggr__origins__distinations__count",
				"aggr__origins__order_1_rank", "aggr__origins__distinations__order_1_rank"
			  FROM (
				SELECT "aggr__origins__parent_count", "aggr__origins__key_0",
				  "aggr__origins__count", "aggr__origins__distinations__parent_count",
				  "aggr__origins__distinations__key_0",
				  "aggr__origins__distinations__count",
				  dense_rank() OVER (ORDER BY "aggr__origins__count" DESC,
				  "aggr__origins__key_0" ASC) AS "aggr__origins__order_1_rank",
				  dense_rank() OVER (PARTITION BY "aggr__origins__key_0" ORDER BY
				  "aggr__origins__distinations__count" DESC,
				  "aggr__origins__distinations__key_0" ASC) AS
				  "aggr__origins__distinations__order_1_rank"
				FROM (
				  SELECT sum(count(*)) OVER () AS "aggr__origins__parent_count",
					"OriginAirportID" AS "aggr__origins__key_0",
					sum(count(*)) OVER (PARTITION BY "aggr__origins__key_0") AS
					"aggr__origins__count",
					sum(count(*)) OVER (PARTITION BY "aggr__origins__key_0") AS
					"aggr__origins__distinations__parent_count",
					"DestAirportID" AS "aggr__origins__distinations__key_0",
					count(*) AS "aggr__origins__distinations__count"
				  FROM __quesma_table_name
				  GROUP BY "OriginAirportID" AS "aggr__origins__key_0",
					"DestAirportID" AS "aggr__origins__distinations__key_0"))
			  WHERE ("aggr__origins__order_1_rank"<=10001 AND
				"aggr__origins__distinations__order_1_rank"<=10001)
			  ORDER BY "aggr__origins__order_1_rank" ASC,
				"aggr__origins__distinations__order_1_rank" ASC) ,
			quesma_top_hits_join AS (
			  SELECT "group_table"."aggr__origins__parent_count" AS
				"aggr__origins__parent_count",
				"group_table"."aggr__origins__key_0" AS "aggr__origins__key_0",
				"group_table"."aggr__origins__count" AS "aggr__origins__count",
				"group_table"."aggr__origins__distinations__parent_count" AS
				"aggr__origins__distinations__parent_count",
				"group_table"."aggr__origins__distinations__key_0" AS
				"aggr__origins__distinations__key_0",
				"group_table"."aggr__origins__distinations__count" AS
				"aggr__origins__distinations__count",
				"hit_table"."DestLocation" AS 
                "top_hits__origins__distinations__destLocation_col_0",
				ROW_NUMBER() OVER (PARTITION BY "group_table"."aggr__origins__key_0",
				"group_table"."aggr__origins__distinations__key_0") AS "top_hits_rank",
				"group_table"."aggr__origins__order_1_rank" AS "aggr__origins__order_1_rank"
				,
				"group_table"."aggr__origins__distinations__order_1_rank" AS
				"aggr__origins__distinations__order_1_rank"
			  FROM quesma_top_hits_group_table AS "group_table" LEFT OUTER JOIN
				__quesma_table_name AS "hit_table" ON (("group_table"."aggr__origins__key_0"
				="hit_table"."OriginAirportID" AND
				"group_table"."aggr__origins__distinations__key_0"=
				"hit_table"."DestAirportID")))
			SELECT "aggr__origins__parent_count", "aggr__origins__key_0",
			  "aggr__origins__count", "aggr__origins__distinations__parent_count",
			  "aggr__origins__distinations__key_0", "aggr__origins__distinations__count",
			  "top_hits__origins__distinations__destLocation_col_0", "top_hits_rank"
			FROM "quesma_top_hits_join"
			WHERE "top_hits_rank"<=1
			ORDER BY "aggr__origins__order_1_rank" ASC,
			  "aggr__origins__distinations__order_1_rank" ASC, "top_hits_rank" ASC`,
		ExpectedAdditionalPancakeSQLs: []string{`
			WITH quesma_top_hits_group_table AS (
			  SELECT sum(count(*)) OVER () AS "aggr__origins__parent_count",
				"OriginAirportID" AS "aggr__origins__key_0",
				count(*) AS "aggr__origins__count"
			  FROM __quesma_table_name
			  GROUP BY "OriginAirportID" AS "aggr__origins__key_0"
			  ORDER BY "aggr__origins__count" DESC, "aggr__origins__key_0" ASC
			  LIMIT 10001) ,
			quesma_top_hits_join AS (
			  SELECT "group_table"."aggr__origins__parent_count" AS
				"aggr__origins__parent_count",
				"group_table"."aggr__origins__key_0" AS "aggr__origins__key_0",
				"group_table"."aggr__origins__count" AS "aggr__origins__count",
				"hit_table"."OriginLocation" AS
				"top_hits__origins__originLocation_col_0",
				"hit_table"."Origin" AS "top_hits__origins__originLocation_col_1",
				ROW_NUMBER() OVER (PARTITION BY "group_table"."aggr__origins__key_0") AS
				"top_hits_rank"
			  FROM quesma_top_hits_group_table AS "group_table" LEFT OUTER JOIN
				__quesma_table_name AS "hit_table" ON ("group_table"."aggr__origins__key_0"=
				"hit_table"."OriginAirportID"))
			SELECT "aggr__origins__parent_count", "aggr__origins__key_0",
			  "aggr__origins__count", "top_hits__origins__originLocation_col_0",
			  "top_hits__origins__originLocation_col_1", "top_hits_rank"
			FROM "quesma_top_hits_join"
			WHERE "top_hits_rank"<=1
			ORDER BY "aggr__origins__count" DESC, "aggr__origins__key_0" ASC,
			  "top_hits_rank" ASC`},
	},
	{ // [8]
		TestName: "histogram, different field than timestamp",
		QueryRequestJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"0": {
					"histogram": {
						"field": "FlightDelayMin",
						"interval": 1,
						"min_doc_count": 1
					}
				}
			},
			"fields": [
				{
					"field": "timestamp",
					"format": "date_time"
				}
			],
			"query": {
				"bool": {
					"filter": [
						{
							"range": {
								"timestamp": {
									"format": "strict_date_optional_time",
									"gte": "2024-02-02T13:47:16.029Z",
									"lte": "2024-02-09T13:47:16.029Z"
								}
							}
						}
					],
					"must": [],
					"must_not": [
						{
							"match_phrase": {
								"FlightDelayMin": {
									"query": 0
								}
							}
						}
					],
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
			"script_fields": {},
			"size": 0,
			"stored_fields": [
				"*"
			],
			"track_total_hits": false
		}`,
		ExpectedResponse: `
		{
			"completion_time_in_millis": 1707486436932,
			"expiration_time_in_millis": 1707486496930,
			"id": "FlBmNVhsNlNkU3lXWEhQVzN1UmxEb2ccc3VtTlI1T25TVGFSYlI0dFM1dkNHQTo0MzU0OQ==",
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
								"doc_count": 21,
								"key": 15.0
							},
							{
								"doc_count": 22,
								"key": 30.0
							},
							{
								"doc_count": 13,
								"key": 345.0
							},
							{
								"doc_count": 22,
								"key": 360.0
							}
						]
					}
				},
				"hits": {
					"hits": [],
					"max_score": null,
					"total": {
						"relation": "eq",
						"value": 553
					}
				},
				"timed_out": false,
				"took": 2
			},
			"start_time_in_millis": 1707486436930
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__key_0", 15.0),
				model.NewQueryResultCol("aggr__0__count", 21),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__key_0", 30.0),
				model.NewQueryResultCol("aggr__0__count", 22),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__key_0", 345.0),
				model.NewQueryResultCol("aggr__0__count", 13),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__key_0", 360.0),
				model.NewQueryResultCol("aggr__0__count", 22),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT "FlightDelayMin" AS "aggr__0__key_0", count(*) AS "aggr__0__count"
			FROM ` + TableName + `
			WHERE (("timestamp">=fromUnixTimestamp64Milli(1706881636029) AND "timestamp"<=
              fromUnixTimestamp64Milli(1707486436029)) AND NOT ("FlightDelayMin"==0))
			GROUP BY "FlightDelayMin" AS "aggr__0__key_0"
			ORDER BY "aggr__0__key_0" ASC`,
	},
	{ // [9]
		TestName: "double aggregation with histogram + harder query",
		QueryRequestJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"0": {
					"aggs": {
						"1": {
							"date_histogram": {
								"field": "@timestamp",
								"fixed_interval": "3h",
								"min_doc_count": 1,
								"time_zone": "Europe/Warsaw"
							}
						}
					},
					"terms": {
						"field": "severity",
						"order": {
							"_count": "desc"
						},
						"shard_size": 25,
						"size": 3
					}
				}
			},
			"fields": [
				{
					"field": "@timestamp",
					"format": "date_time"
				}
			],
			"query": {
				"bool": {
					"filter": [
						{
							"match_phrase": {
								"host.name": "prometheus"
							}
						},
						{
							"range": {
								"@timestamp": {
									"format": "strict_date_optional_time",
									"gte": "2024-02-02T16:36:49.940Z",
									"lte": "2024-02-09T16:36:49.940Z"
								}
							}
						}
					],
					"must": [],
					"must_not": [],
					"should": []
				}
			},
			"runtime_mappings": {},
			"script_fields": {},
			"size": 0,
			"stored_fields": [
				"*"
			],
			"track_total_hits": true
		}`,
		ExpectedResponse: `
		{
			"completion_time_in_millis": 1707496610024,
			"expiration_time_in_millis": 1707496670022,
			"id": "FjdRTVZiQkVFU3FtQlpMVXdVeHhMdmcdc3VtTlI1T25TVGFSYlI0dFM1dkNHQToyMjM1MDk=",
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
								"1": {
									"buckets": [
										{
											"doc_count": 22,
											"key": 1707476400000,
											"key_as_string": "2024-02-09T11:00:00.000"
										},
										{
											"doc_count": 80,
											"key": 1707487200000,
											"key_as_string": "2024-02-09T14:00:00.000"
										}
									]
								},
								"doc_count": 102,
								"key": "info"
							},
							{
								"1": {
									"buckets": [
										{
											"doc_count": 17,
											"key": 1707476400000,
											"key_as_string": "2024-02-09T11:00:00.000"
										},
										{
											"doc_count": 32,
											"key": 1707487200000,
											"key_as_string": "2024-02-09T14:00:00.000"
										}
									]
								},
								"doc_count": 49,
								"key": "debug"
							},
							{
								"1": {
									"buckets": [
										{
											"doc_count": 5,
											"key": 1707476400000,
											"key_as_string": "2024-02-09T11:00:00.000"
										},
										{
											"doc_count": 11,
											"key": 1707487200000,
											"key_as_string": "2024-02-09T14:00:00.000"
										}
									]
								},
								"doc_count": 16,
								"key": "critical"
							}
						],
						"doc_count_error_upper_bound": 0,
						"sum_other_doc_count": 0
					}
				},
				"hits": {
					"hits": [],
					"max_score": null,
					"total": {
						"relation": "eq",
						"value": 198
					}
				},
				"timed_out": false,
				"took": 2
			},
			"start_time_in_millis": 1707496610022
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__parent_count", 167),
				model.NewQueryResultCol("aggr__0__key_0", "info"),
				model.NewQueryResultCol("aggr__0__count", int64(102)),
				model.NewQueryResultCol("aggr__0__1__key_0", int64(1707480000000/1000/60/60/3)),
				model.NewQueryResultCol("aggr__0__1__count", 22),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__parent_count", 167),
				model.NewQueryResultCol("aggr__0__key_0", "info"),
				model.NewQueryResultCol("aggr__0__count", int64(102)),
				model.NewQueryResultCol("aggr__0__1__key_0", int64(1707490800000/1000/60/60/3)),
				model.NewQueryResultCol("aggr__0__1__count", 80),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__parent_count", 167),
				model.NewQueryResultCol("aggr__0__key_0", "debug"),
				model.NewQueryResultCol("aggr__0__count", int64(49)),
				model.NewQueryResultCol("aggr__0__1__key_0", int64(1707480000000/1000/60/60/3)),
				model.NewQueryResultCol("aggr__0__1__count", 17),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__parent_count", 167),
				model.NewQueryResultCol("aggr__0__key_0", "debug"),
				model.NewQueryResultCol("aggr__0__count", int64(49)),
				model.NewQueryResultCol("aggr__0__1__key_0", int64(1707490800000/1000/60/60/3)),
				model.NewQueryResultCol("aggr__0__1__count", 32),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__parent_count", 167),
				model.NewQueryResultCol("aggr__0__key_0", "critical"),
				model.NewQueryResultCol("aggr__0__count", int64(16)),
				model.NewQueryResultCol("aggr__0__order_1", 16),
				model.NewQueryResultCol("aggr__0__1__key_0", int64(1707480000000/1000/60/60/3)),
				model.NewQueryResultCol("aggr__0__1__count", 5),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__parent_count", 167),
				model.NewQueryResultCol("aggr__0__key_0", "critical"),
				model.NewQueryResultCol("aggr__0__count", int64(16)),
				model.NewQueryResultCol("aggr__0__1__key_0", int64(1707490800000/1000/60/60/3)),
				model.NewQueryResultCol("aggr__0__1__count", 11),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT "aggr__0__parent_count", "aggr__0__key_0", "aggr__0__count",
			  "aggr__0__1__key_0", "aggr__0__1__count"
			FROM (
			  SELECT "aggr__0__parent_count", "aggr__0__key_0", "aggr__0__count",
				"aggr__0__1__key_0", "aggr__0__1__count",
				dense_rank() OVER (ORDER BY "aggr__0__count" DESC, "aggr__0__key_0" ASC) AS
				"aggr__0__order_1_rank",
				dense_rank() OVER (PARTITION BY "aggr__0__key_0" ORDER BY
				"aggr__0__1__key_0" ASC) AS "aggr__0__1__order_1_rank"
			  FROM (
				SELECT sum(count(*)) OVER () AS "aggr__0__parent_count",
				  "severity" AS "aggr__0__key_0",
				  sum(count(*)) OVER (PARTITION BY "aggr__0__key_0") AS "aggr__0__count",
				  toInt64((toUnixTimestamp64Milli("@timestamp")+timeZoneOffset(toTimezone(
				  "@timestamp", 'Europe/Warsaw'))*1000) / 10800000) AS "aggr__0__1__key_0",
				  count(*) AS "aggr__0__1__count"
				FROM __quesma_table_name
				WHERE (("host.name" __quesma_match 'prometheus') AND ("@timestamp">=
				  fromUnixTimestamp64Milli(1706891809940) AND "@timestamp"<=
				  fromUnixTimestamp64Milli(1707496609940)))
				GROUP BY "severity" AS "aggr__0__key_0",
				  toInt64((toUnixTimestamp64Milli("@timestamp")+timeZoneOffset(toTimezone(
				  "@timestamp", 'Europe/Warsaw'))*1000) / 10800000) AS "aggr__0__1__key_0"))
			WHERE "aggr__0__order_1_rank"<=4
			ORDER BY "aggr__0__order_1_rank" ASC, "aggr__0__1__order_1_rank" ASC`,
	},
	{ // [10]
		TestName: "very long: multiple top_metrics + histogram",
		QueryRequestJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"1": {
					"aggs": {
						"2": {
							"aggs": {
								"4": {
									"top_metrics": {
										"metrics": {
											"field": "order_date"
										},
										"size": 10,
										"sort": {
											"order_date": "asc"
										}
									}
								},
								"5": {
									"top_metrics": {
										"metrics": {
											"field": "taxful_total_price"
										},
										"size": 10,
										"sort": {
											"order_date": "asc"
										}
									}
								}
							},
							"date_histogram": {
								"field": "order_date",
								"fixed_interval": "12h",
								"min_doc_count": 1,
								"time_zone": "Europe/Warsaw"
							}
						}
					},
					"filters": {
						"filters": {
							"c8c30be0-b88f-11e8-a451-f37365e9f268": {
								"bool": {
									"filter": [],
									"must": [
										{
											"query_string": {
												"analyze_wildcard": true,
												"query": "taxful_total_price:>250",
												"time_zone": "Europe/Warsaw"
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
			"fields": [
				{
					"field": "customer_birth_date",
					"format": "date_time"
				},
				{
					"field": "order_date",
					"format": "date_time"
				},
				{
					"field": "products.created_on",
					"format": "date_time"
				}
			],
			"query": {
				"bool": {
					"filter": [
						{
							"range": {
								"order_date": {
									"format": "strict_date_optional_time",
									"gte": "2024-02-06T09:59:57.034Z",
									"lte": "2024-02-13T09:59:57.034Z"
								}
							}
						}
					],
					"must": [],
					"must_not": [],
					"should": []
				}
			},
			"runtime_mappings": {},
			"script_fields": {},
			"size": 0,
			"stored_fields": [
				"*"
			],
			"track_total_hits": false
		}`,
		ExpectedResponse: `
		{
			"completion_time_in_millis": 1707818398410,
			"expiration_time_in_millis": 1707818458408,
			"id": "FlFDakdBS29jUWNTRllCa1hGdS1wVmccQVQxSHZodzJSbW1penpRdThTa0lKUTo2NDg1Mg==",
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
					"1": {
						"buckets": {
							"c8c30be0-b88f-11e8-a451-f37365e9f268": {
								"2": {
									"buckets": [
										{
											"4": {
												"top": [
													{
														"metrics": {
															"order_date": "2024-02-09T17:16:48.000Z"
														},
														"sort": [
															"2024-02-09T17:16:48.000Z"
														]
													},
													{
														"metrics": {
															"order_date": "2024-02-09T21:34:34.000Z"
														},
														"sort": [
															"2024-02-09T21:34:34.000Z"
														]
													}
												]
											},
											"5": {
												"top": [
													{
														"metrics": {
															"taxful_total_price": 310.0
														},
														"sort": [
															"2024-02-09T17:16:48.000Z"
														]
													},
													{
														"metrics": {
															"taxful_total_price": 393.0
														},
														"sort": [
															"2024-02-09T21:34:34.000Z"
														]
													}
												]
											},
											"doc_count": 2,
											"key": 1707476400000,
											"key_as_string": "2024-02-09T11:00:00.000"
										},
										{
											"4": {
												"top": [
													{
														"metrics": {
															"order_date": "2024-02-12T11:38:24.000Z"
														},
														"sort": [
															"2024-02-12T11:38:24.000Z"
														]
													}
												]
											},
											"5": {
												"top": [
													{
														"metrics": {
															"taxful_total_price": 283.0
														},
														"sort": [
															"2024-02-12T11:38:24.000Z"
														]
													}
												]
											},
											"doc_count": 1,
											"key": 1707735600000,
											"key_as_string": "2024-02-12T11:00:00.000"
										},
										{
											"4": {
												"top": [
													{
														"metrics": {
															"order_date": "2024-02-13T03:50:24.000Z"
														},
														"sort": [
															"2024-02-13T03:50:24.000Z"
														]
													}
												]
											},
											"5": {
												"top": [
													{
														"metrics": {
															"taxful_total_price": 301.0
														},
														"sort": [
															"2024-02-13T03:50:24.000Z"
														]
													}
												]
											},
											"doc_count": 1,
											"key": 1707778800000,
											"key_as_string": "2024-02-12T23:00:00.000"
										}
									]
								},
								"doc_count": 4
							}
						}
					}
				},
				"hits": {
					"hits": [],
					"max_score": null,
					"total": {
						"relation": "eq",
						"value": 1043
					}
				},
				"timed_out": false,
				"took": 2
			},
			"start_time_in_millis": 1707818398408
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__1__count", uint64(4)),
				model.NewQueryResultCol("aggr__1__2__key_0", int64(1707480000000/1000/60/60/12)),
				model.NewQueryResultCol("aggr__1__2__count", 2),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__1__count", uint64(4)),
				model.NewQueryResultCol("aggr__1__2__key_0", int64(1707739200000/1000/60/60/12)),
				model.NewQueryResultCol("aggr__1__2__count", 1),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__1__count", uint64(4)),
				model.NewQueryResultCol("aggr__1__2__key_0", int64(1707782400000/1000/60/60/12)),
				model.NewQueryResultCol("aggr__1__2__count", 1),
			}},
		},
		ExpectedAdditionalPancakeResults: [][]model.QueryResultRow{
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("aggr__1__count", uint64(4)),
					model.NewQueryResultCol("aggr__1__2__key_0", int64(1707480000000/1000/60/60/12)),
					model.NewQueryResultCol("aggr__1__2__count", 2),
					model.NewQueryResultCol("top_metrics__1__2__4_col_0", "2024-02-09T17:16:48.000Z"),
					model.NewQueryResultCol("top_metrics__1__2__4_col_1", "2024-02-09T17:16:48.000Z"),
					model.NewQueryResultCol("top_hits_rank", 1),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("aggr__1__count", uint64(4)),
					model.NewQueryResultCol("aggr__1__2__key_0", int64(1707480000000/1000/60/60/12)),
					model.NewQueryResultCol("aggr__1__2__count", 2),
					model.NewQueryResultCol("top_metrics__1__2__4_col_0", "2024-02-09T21:34:34.000Z"),
					model.NewQueryResultCol("top_metrics__1__2__4_col_1", "2024-02-09T21:34:34.000Z"),
					model.NewQueryResultCol("top_hits_rank", 2),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("aggr__1__count", uint64(4)),
					model.NewQueryResultCol("aggr__1__2__key_0", int64(1707739200000/1000/60/60/12)),
					model.NewQueryResultCol("aggr__1__2__count", 1),
					model.NewQueryResultCol("top_metrics__1__2__4_col_0", "2024-02-12T11:38:24.000Z"),
					model.NewQueryResultCol("top_metrics__1__2__4_col_1", "2024-02-12T11:38:24.000Z"),
					model.NewQueryResultCol("top_hits_rank", 1),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("aggr__1__count", uint64(4)),
					model.NewQueryResultCol("aggr__1__2__key_0", int64(1707782400000/1000/60/60/12)),
					model.NewQueryResultCol("aggr__1__2__count", 1),
					model.NewQueryResultCol("top_metrics__1__2__4_col_0", "2024-02-13T03:50:24.000Z"),
					model.NewQueryResultCol("top_metrics__1__2__4_col_1", "2024-02-13T03:50:24.000Z"),
					model.NewQueryResultCol("top_hits_rank", 1),
				}},
			},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("aggr__1__count", uint64(4)),
					model.NewQueryResultCol("aggr__1__2__key_0", int64(1707480000000/1000/60/60/12)),
					model.NewQueryResultCol("aggr__1__2__count", 2),
					model.NewQueryResultCol("top_metrics__1__2__5_col_0", 310),
					model.NewQueryResultCol("top_metrics__1__2__5_col_1", "2024-02-09T17:16:48.000Z"),
					model.NewQueryResultCol("top_hits_rank", 1),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("aggr__1__count", uint64(4)),
					model.NewQueryResultCol("aggr__1__2__key_0", int64(1707480000000/1000/60/60/12)),
					model.NewQueryResultCol("aggr__1__2__count", 2),
					model.NewQueryResultCol("top_metrics__1__2__5_col_0", 393),
					model.NewQueryResultCol("top_metrics__1__2__5_col_1", "2024-02-09T21:34:34.000Z"),
					model.NewQueryResultCol("top_hits_rank", 2),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("aggr__1__count", uint64(4)),
					model.NewQueryResultCol("aggr__1__2__key_0", int64(1707739200000/1000/60/60/12)),
					model.NewQueryResultCol("aggr__1__2__count", 1),
					model.NewQueryResultCol("top_metrics__1__2__5_col_0", 283),
					model.NewQueryResultCol("top_metrics__1__2__5_col_1", "2024-02-12T11:38:24.000Z"),
					model.NewQueryResultCol("top_hits_rank", 1),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("aggr__1__count", uint64(4)),
					model.NewQueryResultCol("aggr__1__2__key_0", int64(1707782400000/1000/60/60/12)),
					model.NewQueryResultCol("aggr__1__2__count", 1),
					model.NewQueryResultCol("top_metrics__1__2__5_col_0", 301),
					model.NewQueryResultCol("top_metrics__1__2__5_col_1", "2024-02-13T03:50:24.000Z"),
					model.NewQueryResultCol("top_hits_rank", 1),
				}},
			},
		},
		ExpectedPancakeSQL: `
			SELECT sum(count(*)) OVER () AS "aggr__1__count",
			  toInt64((toUnixTimestamp64Milli("order_date")+timeZoneOffset(toTimezone(
			  "order_date", 'Europe/Warsaw'))*1000) / 43200000) AS "aggr__1__2__key_0",
			  count(*) AS "aggr__1__2__count"
			FROM __quesma_table_name
			WHERE (("order_date">=fromUnixTimestamp64Milli(1707213597034) AND "order_date"<=
			  fromUnixTimestamp64Milli(1707818397034)) AND "taxful_total_price" > '250')
			GROUP BY toInt64((toUnixTimestamp64Milli("order_date")+timeZoneOffset(toTimezone
			  ("order_date", 'Europe/Warsaw'))*1000) / 43200000) AS "aggr__1__2__key_0"
			ORDER BY "aggr__1__2__key_0" ASC`,
		ExpectedAdditionalPancakeSQLs: []string{`
			WITH quesma_top_hits_group_table AS (
			  SELECT sum(count(*)) OVER () AS "aggr__1__count",
				toInt64((toUnixTimestamp64Milli("order_date")+timeZoneOffset(toTimezone(
				"order_date", 'Europe/Warsaw'))*1000) / 43200000) AS "aggr__1__2__key_0",
				count(*) AS "aggr__1__2__count"
			  FROM __quesma_table_name
			  WHERE (("order_date">=fromUnixTimestamp64Milli(1707213597034) AND "order_date"
				<=fromUnixTimestamp64Milli(1707818397034)) AND "taxful_total_price" > '250')
			  GROUP BY toInt64((toUnixTimestamp64Milli("order_date")+timeZoneOffset(
				toTimezone("order_date", 'Europe/Warsaw'))*1000) / 43200000) AS
				"aggr__1__2__key_0"
			  ORDER BY "aggr__1__2__key_0" ASC) ,
			quesma_top_hits_join AS (
			  SELECT "group_table"."aggr__1__count" AS "aggr__1__count",
				"group_table"."aggr__1__2__key_0" AS "aggr__1__2__key_0",
				"group_table"."aggr__1__2__count" AS "aggr__1__2__count",
				"hit_table"."order_date" AS "top_metrics__1__2__4_col_0",
				"hit_table"."order_date" AS "top_metrics__1__2__4_col_1",
				ROW_NUMBER() OVER (PARTITION BY "group_table"."aggr__1__2__key_0" ORDER BY
				"order_date" ASC) AS "top_hits_rank"
			  FROM quesma_top_hits_group_table AS "group_table" LEFT OUTER JOIN
				__quesma_table_name AS "hit_table" ON ("group_table"."aggr__1__2__key_0"=
				toInt64((toUnixTimestamp64Milli("order_date")+timeZoneOffset(toTimezone(
				"order_date", 'Europe/Warsaw'))*1000) / 43200000))
			  WHERE (("order_date">=fromUnixTimestamp64Milli(1707213597034) AND "order_date"
				<=fromUnixTimestamp64Milli(1707818397034)) AND "taxful_total_price" > '250'))
			SELECT "aggr__1__count", "aggr__1__2__key_0", "aggr__1__2__count",
			  "top_metrics__1__2__4_col_0", "top_metrics__1__2__4_col_1", "top_hits_rank"
			FROM "quesma_top_hits_join"
			WHERE "top_hits_rank"<=10
			ORDER BY "aggr__1__2__key_0" ASC, "top_hits_rank" ASC`,
			`WITH quesma_top_hits_group_table AS (
			  SELECT sum(count(*)) OVER () AS "aggr__1__count",
				toInt64((toUnixTimestamp64Milli("order_date")+timeZoneOffset(toTimezone(
				"order_date", 'Europe/Warsaw'))*1000) / 43200000) AS "aggr__1__2__key_0",
				count(*) AS "aggr__1__2__count"
			  FROM __quesma_table_name
			  WHERE (("order_date">=fromUnixTimestamp64Milli(1707213597034) AND "order_date"
				<=fromUnixTimestamp64Milli(1707818397034)) AND "taxful_total_price" > '250')
			  GROUP BY toInt64((toUnixTimestamp64Milli("order_date")+timeZoneOffset(
				toTimezone("order_date", 'Europe/Warsaw'))*1000) / 43200000) AS
				"aggr__1__2__key_0"
			  ORDER BY "aggr__1__2__key_0" ASC) ,
			quesma_top_hits_join AS (
			  SELECT "group_table"."aggr__1__count" AS "aggr__1__count",
				"group_table"."aggr__1__2__key_0" AS "aggr__1__2__key_0",
				"group_table"."aggr__1__2__count" AS "aggr__1__2__count",
				"hit_table"."taxful_total_price" AS "top_metrics__1__2__5_col_0",
				"hit_table"."order_date" AS "top_metrics__1__2__5_col_1",
				ROW_NUMBER() OVER (PARTITION BY "group_table"."aggr__1__2__key_0" ORDER BY
				"order_date" ASC) AS "top_hits_rank"
			  FROM quesma_top_hits_group_table AS "group_table" LEFT OUTER JOIN
				__quesma_table_name AS "hit_table" ON ("group_table"."aggr__1__2__key_0"=
				toInt64((toUnixTimestamp64Milli("order_date")+timeZoneOffset(toTimezone(
				"order_date", 'Europe/Warsaw'))*1000) / 43200000))
			  WHERE (("order_date">=fromUnixTimestamp64Milli(1707213597034) AND "order_date"
				<=fromUnixTimestamp64Milli(1707818397034)) AND "taxful_total_price" > '250'))
			SELECT "aggr__1__count", "aggr__1__2__key_0", "aggr__1__2__count",
			  "top_metrics__1__2__5_col_0", "top_metrics__1__2__5_col_1", "top_hits_rank"
			FROM "quesma_top_hits_join"
			WHERE "top_hits_rank"<=10
			ORDER BY "aggr__1__2__key_0" ASC, "top_hits_rank" ASC`},
	},
	{ // [11], "old" test, also can be found in testdata/requests.go TestAsyncSearch[0]
		// Copied it also here to be more sure we do not create some regression
		TestName: "value_count + top_values: regression test",
		QueryRequestJson: `
		{
			"aggs": {
				"sample": {
					"aggs": {
						"sample_count": {
							"value_count": {
								"field": "host.name"
							}
						},
						"top_values": {
							"terms": {
								"field": "host.name",
								"shard_size": 25,
								"size": 10
							}
						}
					},
					"sampler": {
						"shard_size": 2000
					}
				}
			},
			"query": {
				"bool": {
					"filter": [
						{
							"range": {
								"@timestamp": {
									"format": "strict_date_optional_time",
									"gte": "2024-01-23T11:27:16.820Z",
									"lte": "2024-01-23T11:42:16.820Z"
								}
							}
						},
						{
							"bool": {
								"filter": [
									{
										"multi_match": {
											"lenient": true,
											"query": "user",
											"type": "best_fields"
										}
									}
								],
								"must": [],
								"must_not": [],
								"should": []
							}
						}
					]
				}
			},
			"runtime_mappings": {},
			"size": 2,
			"track_total_hits": 3
		}`,
		ExpectedResponse: `
		{
			"completion_time_in_millis": 1706010201967,
			"expiration_time_in_millis": 1706010261964,
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
						"doc_count": 262,
						"sample_count": {
							"value": 262
						},
						"top_values": {
							"buckets": [
								{"doc_count": 30, "key": "hephaestus"},
								{"doc_count": 29, "key": "poseidon"},
								{"doc_count": 28, "key": "jupiter"},
								{"doc_count": 26, "key": "selen"},
								{"doc_count": 24, "key": "demeter"},
								{"doc_count": 24, "key": "iris"},
								{"doc_count": 24, "key": "pan"},
								{"doc_count": 22, "key": "hades"},
								{"doc_count": 22, "key": "hermes"},
								{"doc_count": 21, "key": "persephone"}
							],
							"doc_count_error_upper_bound": 0,
							"sum_other_doc_count": 12
						}
					}
				},
				"hits": {
					"hits": [],
					"max_score": null,
					"total": {
						"relation": "eq",
						"value": 262
					}
				},
				"timed_out": false,
				"took": 3
			},
			"start_time_in_millis": 1706010201964
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__sample__count", 262),
				model.NewQueryResultCol("metric__sample__sample_count_col_0", 262),
				model.NewQueryResultCol("aggr__sample__top_values__parent_count", 262),
				model.NewQueryResultCol("aggr__sample__top_values__key_0", "hephaestus"),
				model.NewQueryResultCol("aggr__sample__top_values__count", int64(30)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__sample__count", 262),
				model.NewQueryResultCol("metric__sample__sample_count_col_0", 262),
				model.NewQueryResultCol("aggr__sample__top_values__parent_count", 262),
				model.NewQueryResultCol("aggr__sample__top_values__key_0", "poseidon"),
				model.NewQueryResultCol("aggr__sample__top_values__count", int64(29)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__sample__count", 262),
				model.NewQueryResultCol("metric__sample__sample_count_col_0", 262),
				model.NewQueryResultCol("aggr__sample__top_values__parent_count", 262),
				model.NewQueryResultCol("aggr__sample__top_values__key_0", "jupiter"),
				model.NewQueryResultCol("aggr__sample__top_values__count", int64(28)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__sample__count", 262),
				model.NewQueryResultCol("metric__sample__sample_count_col_0", 262),
				model.NewQueryResultCol("aggr__sample__top_values__parent_count", 262),
				model.NewQueryResultCol("aggr__sample__top_values__key_0", "selen"),
				model.NewQueryResultCol("aggr__sample__top_values__count", int64(26)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__sample__count", 262),
				model.NewQueryResultCol("metric__sample__sample_count_col_0", 262),
				model.NewQueryResultCol("aggr__sample__top_values__parent_count", 262),
				model.NewQueryResultCol("aggr__sample__top_values__key_0", "demeter"),
				model.NewQueryResultCol("aggr__sample__top_values__count", int64(24)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__sample__count", 262),
				model.NewQueryResultCol("metric__sample__sample_count_col_0", 262),
				model.NewQueryResultCol("aggr__sample__top_values__parent_count", 262),
				model.NewQueryResultCol("aggr__sample__top_values__key_0", "iris"),
				model.NewQueryResultCol("aggr__sample__top_values__count", int64(24)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__sample__count", 262),
				model.NewQueryResultCol("metric__sample__sample_count_col_0", 262),
				model.NewQueryResultCol("aggr__sample__top_values__parent_count", 262),
				model.NewQueryResultCol("aggr__sample__top_values__key_0", "pan"),
				model.NewQueryResultCol("aggr__sample__top_values__count", int64(24)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__sample__count", 262),
				model.NewQueryResultCol("metric__sample__sample_count_col_0", 262),
				model.NewQueryResultCol("aggr__sample__top_values__parent_count", 262),
				model.NewQueryResultCol("aggr__sample__top_values__key_0", "hades"),
				model.NewQueryResultCol("aggr__sample__top_values__count", int64(22)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__sample__count", 262),
				model.NewQueryResultCol("metric__sample__sample_count_col_0", 262),
				model.NewQueryResultCol("aggr__sample__top_values__parent_count", 262),
				model.NewQueryResultCol("aggr__sample__top_values__key_0", "hermes"),
				model.NewQueryResultCol("aggr__sample__top_values__count", int64(22)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__sample__count", 262),
				model.NewQueryResultCol("metric__sample__sample_count_col_0", 262),
				model.NewQueryResultCol("aggr__sample__top_values__parent_count", 262),
				model.NewQueryResultCol("aggr__sample__top_values__key_0", "persephone"),
				model.NewQueryResultCol("aggr__sample__top_values__count", int64(21)),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT sum(count(*)) OVER () AS "aggr__sample__count",
			  sum(count("host.name")) OVER () AS "metric__sample__sample_count_col_0",
			  sum(count(*)) OVER () AS "aggr__sample__top_values__parent_count",
			  "host.name" AS "aggr__sample__top_values__key_0",
			  count(*) AS "aggr__sample__top_values__count"
			FROM (
			  SELECT "host.name"
			  FROM __quesma_table_name
			  WHERE (("@timestamp">=fromUnixTimestamp64Milli(1706009236820) AND "@timestamp"
				<=fromUnixTimestamp64Milli(1706010136820)) AND
				"__quesma_fulltext_field_name" iLIKE '%user%')
			  LIMIT 8000)
			GROUP BY "host.name" AS "aggr__sample__top_values__key_0"
			ORDER BY "aggr__sample__top_values__count" DESC,
			  "aggr__sample__top_values__key_0" ASC
			LIMIT 11`,
	},
	{ // [12], "old" test, also can be found in testdata/requests.go TestAsyncSearch[3]
		// Copied it also here to be more sure we do not create some regression
		TestName: "date_histogram: regression test",
		QueryRequestJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"0": {
					"date_histogram": {
						"field": "@timestamp",
						"fixed_interval": "30s",
						"min_doc_count": 1
					}
				}
			},
			"fields": [
				{
					"field": "@timestamp",
					"format": "date_time"
				}
			],
			"query": {
				"bool": {
					"filter": [
						{
							"multi_match": {
								"lenient": true,
								"query": "user",
								"type": "best_fields"
							}
						},
						{
							"range": {
								"@timestamp": {
									"format": "strict_date_optional_time",
									"gte": "2024-01-23T14:43:19.481Z",
									"lte": "2024-01-23T14:58:19.481Z"
								}
							}
						}
					],
					"must": [],
					"must_not": [],
					"should": []
				}
			},
			"runtime_mappings": {},
			"script_fields": {},
			"size": 5,
			"stored_fields": [
				"*"
			],
			"track_total_hits": true
		}`,
		ExpectedResponse: `
		{
			"completion_time_in_millis": 1706021899595,
			"expiration_time_in_millis": 1706021959594,
			"id": "FjFQMlBUNnJmUU1pWml0WkllNmJWYXcdNVFvOUloYTBUZ3U0Q25MRTJtQTA0dzoyMTEyNzI=",
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
							{"doc_count": 2,  "key": 1706021670000, "key_as_string": "2024-01-23T14:54:30.000"},
							{"doc_count": 13, "key": 1706021700000, "key_as_string": "2024-01-23T14:55:00.000"},
							{"doc_count": 14, "key": 1706021730000, "key_as_string": "2024-01-23T14:55:30.000"},
							{"doc_count": 14, "key": 1706021760000, "key_as_string": "2024-01-23T14:56:00.000"},
							{"doc_count": 15, "key": 1706021790000, "key_as_string": "2024-01-23T14:56:30.000"},
							{"doc_count": 13, "key": 1706021820000, "key_as_string": "2024-01-23T14:57:00.000"},
							{"doc_count": 15, "key": 1706021850000, "key_as_string": "2024-01-23T14:57:30.000"},
							{"doc_count": 11, "key": 1706021880000, "key_as_string": "2024-01-23T14:58:00.000"}
						]
					}
				},
				"hits": {
					"hits": [],
					"max_score": null,
					"total": {
						"relation": "eq",
						"value": 97
					}
				},
				"timed_out": false,
				"took": 1
			},
			"start_time_in_millis": 1706021899594
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__key_0", int64(1706021670000/30000)),
				model.NewQueryResultCol("aggr__0__count", 2),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__key_0", int64(1706021700000/30000)),
				model.NewQueryResultCol("aggr__0__count", 13),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__key_0", int64(1706021730000/30000)),
				model.NewQueryResultCol("aggr__0__count", 14),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__key_0", int64(1706021760000/30000)),
				model.NewQueryResultCol("aggr__0__count", 14),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__key_0", int64(1706021790000/30000)),
				model.NewQueryResultCol("aggr__0__count", 15),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__key_0", int64(1706021820000/30000)),
				model.NewQueryResultCol("aggr__0__count", 13),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__key_0", int64(1706021850000/30000)),
				model.NewQueryResultCol("aggr__0__count", 15),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__key_0", int64(1706021880000/30000)),
				model.NewQueryResultCol("aggr__0__count", 11),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT toInt64(toUnixTimestamp64Milli("@timestamp") / 30000) AS "aggr__0__key_0"
			  , count(*) AS "aggr__0__count"
			FROM ` + TableName + `

			WHERE (` + fullTextFieldName + ` iLIKE '%user%' AND
              ("@timestamp">=fromUnixTimestamp64Milli(1706020999481) AND "@timestamp"<=fromUnixTimestamp64Milli(1706021899481)))
			GROUP BY toInt64(toUnixTimestamp64Milli("@timestamp") / 30000) AS
			  "aggr__0__key_0"
			ORDER BY "aggr__0__key_0" ASC`,
	},
	{ // [13], "old" test, also can be found in testdata/requests.go TestAsyncSearch[4]
		// Copied it also here to be more sure we do not create some regression
		TestName: "terms with date_histogram as subaggregation: regression test",
		QueryRequestJson: `
		{
			"query": {
				"range": {
					"@timestamp": {
						"gt": "2024-01-25T14:53:59.033Z",
						"lte": "2024-01-25T15:08:59.033Z",
						"format": "strict_date_optional_time"
					}
				}
			},
			"aggs": {
				"stats": {
					"terms": {
						"field": "event.dataset",
						"size": 4,
						"missing": "unknown"
					},
					"aggs": {
						"series": {
							"date_histogram": {
								"field": "@timestamp",
								"fixed_interval": "60s",
								"min_doc_count": 12
							}
						}
					}
				}
			},
			"size": 0,
			"track_total_hits": false
		}`,
		ExpectedResponse: `
		{
			"took": 180,
			"timed_out": false,
			"_shards": {
				"total": 1,
				"successful": 1,
				"skipped": 0,
				"failed": 0
			},
			"hits": {
				"total": {
					"value": 4675,
					"relation": "eq"
				},
				"max_score": null,
				"hits": []
			},
			"aggregations": {
				"stats": {
					"doc_count_error_upper_bound": 0,
					"sum_other_doc_count": 4139,
					"buckets": [
						{
							"key": "27",
							"doc_count": 348,
							"series": {
								"buckets": [
									{
										"key_as_string": "2024-04-18T00:00:00.000",
										"key": 1713398400000,
										"doc_count": 85
									},
									{
										"key_as_string": "2024-04-25T00:00:00.000",
										"key": 1714003200000,
										"doc_count": 79
									}
								]
							}
						},
						{
							"key": "52",
							"doc_count": 188,
							"series": {
								"buckets": [
									{
										"key_as_string": "2024-04-18T00:00:00.000",
										"key": 1713398400000,
										"doc_count": 35
									}
								]
							}
						}
					]
				}
			}
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__stats__parent_count", int64(4675)),
				model.NewQueryResultCol("aggr__stats__key_0", "27"),
				model.NewQueryResultCol("aggr__stats__count", int64(348)),
				model.NewQueryResultCol("aggr__stats__series__key_0", int64(1713398400000/60000)),
				model.NewQueryResultCol("aggr__stats__series__count", 85),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__stats__parent_count", int64(4675)),
				model.NewQueryResultCol("aggr__stats__key_0", "27"),
				model.NewQueryResultCol("aggr__stats__count", int64(348)),
				model.NewQueryResultCol("aggr__stats__series__key_0", int64(1714003200000/60000)),
				model.NewQueryResultCol("aggr__stats__series__count", 79),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__stats__parent_count", int64(4675)),
				model.NewQueryResultCol("aggr__stats__key_0", "52"),
				model.NewQueryResultCol("aggr__stats__count", int64(188)),
				model.NewQueryResultCol("aggr__stats__series__key_0", int64(1713398400000/60000)),
				model.NewQueryResultCol("aggr__stats__series__count", 35),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT "aggr__stats__parent_count", "aggr__stats__key_0", "aggr__stats__count",
			  "aggr__stats__series__key_0", "aggr__stats__series__count"
			FROM (
			  SELECT "aggr__stats__parent_count", "aggr__stats__key_0",
				"aggr__stats__count", "aggr__stats__series__key_0",
				"aggr__stats__series__count",
				dense_rank() OVER (ORDER BY "aggr__stats__count" DESC,
				"aggr__stats__key_0" ASC) AS "aggr__stats__order_1_rank",
				dense_rank() OVER (PARTITION BY "aggr__stats__key_0" ORDER BY
				"aggr__stats__series__key_0" ASC) AS "aggr__stats__series__order_1_rank"
			  FROM (
				SELECT sum(count(*)) OVER () AS "aggr__stats__parent_count",
				  COALESCE("event.dataset", 'unknown') AS "aggr__stats__key_0",
				  sum(count(*)) OVER (PARTITION BY "aggr__stats__key_0") AS
				  "aggr__stats__count",
				  toInt64(toUnixTimestamp64Milli("@timestamp") / 60000) AS
				  "aggr__stats__series__key_0", count(*) AS "aggr__stats__series__count"
				FROM ` + TableName + `
				WHERE ("@timestamp">fromUnixTimestamp64Milli(1706194439033) AND "@timestamp"<=fromUnixTimestamp64Milli(1706195339033))
				GROUP BY COALESCE("event.dataset", 'unknown') AS "aggr__stats__key_0",
				  toInt64(toUnixTimestamp64Milli("@timestamp") / 60000) AS
				  "aggr__stats__series__key_0"))
			WHERE "aggr__stats__order_1_rank"<=4
			ORDER BY "aggr__stats__order_1_rank" ASC,
			  "aggr__stats__series__order_1_rank" ASC`,
	},
	{ // [14], "old" test, also can be found in testdata/requests.go TestAsyncSearch[5]
		// Copied it also here to be more sure we do not create some regression
		TestName: "earliest/latest timestamp: regression test",
		QueryRequestJson: `
		{
			"aggs": {
				"earliest_timestamp": {
					"min": {
						"field": "@timestamp"
					}
				},
				"latest_timestamp": {
					"max": {
						"field": "@timestamp"
					}
				},
				"average_timestamp": {
					"avg": {
						"field": "@timestamp"
					}
				}
			},
			"query": {
				"bool": {
					"filter": [
						{
							"multi_match": {
								"lenient": true,
								"query": "posei",
								"type": "best_fields"
							}
						},
						{
							"match_phrase": {
								"message": "User logged out"
							}
						},
						{
							"match_phrase": {
								"host.name": "poseidon"
							}
						}
					],
					"must": [],
					"must_not": [],
					"should": []
				}
			},
			"size": 0,
			"track_total_hits": false
		}`,
		ExpectedResponse: `
		{
			"completion_time_in_millis": 1706551812667,
			"expiration_time_in_millis": 1706551872665,
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
					"earliest_timestamp": {
						"value": null
					},
					"latest_timestamp": {
						"value": null
					},
					"average_timestamp": {
						"value": null
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
				"took": 2
			},
			"start_time_in_millis": 1706551812665
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("metric__average_timestamp_col_0", nil),
				model.NewQueryResultCol("metric__earliest_timestamp_col_0", nil),
				model.NewQueryResultCol("metric__latest_timestamp_col_0", nil),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT avgOrNull("@timestamp") AS "metric__average_timestamp_col_0", minOrNull(
			  "@timestamp") AS "metric__earliest_timestamp_col_0", maxOrNull("@timestamp")
			  AS "metric__latest_timestamp_col_0"
			FROM ` + TableName + `
			WHERE ((` + fullTextFieldName + ` iLIKE '%posei%' AND ("message" __quesma_match 'User logged out')) AND
			  ("host.name" __quesma_match 'poseidon'))`,
	},
	{ // [15]
		TestName: "date_histogram: regression test",
		QueryRequestJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"0": {
					"aggs": {
						"1": {
							"sum": {
								"field": "taxful_total_price"
							}
						}
					},
					"date_histogram": {
						"calendar_interval": "1d",
						"field": "order_date"
					}
				}
			},
			"fields": [
				{
					"field": "@timestamp",
					"format": "date_time"
				},
				{
					"field": "order_date",
					"format": "date_time"
				}
			],
			"query": {
				"bool": {
					"filter": [
						{
							"range": {
								"order_date": {
									"format": "strict_date_optional_time",
									"gte": "2024-02-19T17:40:56.351Z",
									"lte": "2024-02-26T17:40:56.351Z"
								}
							}
						}
					],
					"must": [],
					"must_not": [],
					"should": []
				}
			},
			"runtime_mappings": {},
			"script_fields": {},
			"size": 2,
			"stored_fields": [
				"*"
			],
			"track_total_hits": 5
		}`,
		ExpectedResponse: `
		{
			"completion_time_in_millis": 1708969258827,
			"expiration_time_in_millis": 1708969318819,
			"id": "FlduNmpMRzJhU1p1dEV3bEhCbFdSaEEcVnRjbXJfX19RZk9wNjhid3IxWnhuZzoyMjAzOA==",
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
								"1": {
									"value": 2221.5625
								},
								"doc_count": 31,
								"key": 1708300800000,
								"key_as_string": "2024-02-19T00:00:00.000"
							},
							{
								"1": {
									"value": 11116.45703125
								},
								"doc_count": 158,
								"key": 1708387200000,
								"key_as_string": "2024-02-20T00:00:00.000"
							}
						]
					}
				},
				"hits": {
					"hits": [],
					"max_score": null,
					"total": {
						"relation": "eq",
						"value": 1049
					}
				},
				"timed_out": false,
				"took": 8
			},
			"start_time_in_millis": 1708969258819
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__key_0", int64(19772)),
				model.NewQueryResultCol("aggr__0__count", 31),
				model.NewQueryResultCol("metric__0__1_col_0", 2221.5625),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__key_0", int64(19773)),
				model.NewQueryResultCol("aggr__0__count", 158),
				model.NewQueryResultCol("metric__0__1_col_0", 11116.45703125),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT toInt64(toUnixTimestamp64Milli("order_date") / 86400000) AS
			  "aggr__0__key_0", count(*) AS "aggr__0__count",
			  sumOrNull("taxful_total_price") AS "metric__0__1_col_0"
			FROM ` + TableName + `
			WHERE ("order_date">=fromUnixTimestamp64Milli(1708364456351) AND "order_date"<=fromUnixTimestamp64Milli(1708969256351))
			GROUP BY toInt64(toUnixTimestamp64Milli("order_date") / 86400000) AS
			  "aggr__0__key_0"
			ORDER BY "aggr__0__key_0" ASC`,
	},
	{ // [16]
		TestName: "simple terms, seen at client's",
		QueryRequestJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"0": {
					"terms": {
						"field": "message",
						"order": {
							"_count": "desc"
						},
						"shard_size": 25,
						"size": 3
					}
				}
			},
			"query": {
				"bool": {
					"filter": [
						{
							"range": {
								"timestamp": {
									"format": "strict_date_optional_time",
									"gte": "2024-02-20T19:13:33.795Z",
									"lte": "2024-02-21T04:01:14.920Z"
								}
							}
						}
					],
					"must": [],
					"must_not": [],
					"should": []
				}
			},
			"runtime_mappings": {},
			"script_fields": {},
			"size": 5,
			"stored_fields": [
				"*"
			],
			"track_total_hits": 2
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
					"value": 15750,
					"relation": "eq"
				},
				"max_score": null,
				"hits": []
			},
			"aggregations": {
				"0": {
					"buckets": [
						{
							"doc_count": 1700,
							"key": "User created"
						},
						{
							"doc_count": 1781,
							"key": "User deleted"
						},
						{
							"doc_count": 1757,
							"key": "User logged in"
						}
					],
					"doc_count_error_upper_bound": 0,
					"sum_other_doc_count": 10512
				}
			}
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__parent_count", uint64(15750)),
				model.NewQueryResultCol("aggr__0__key_0", "User created"),
				model.NewQueryResultCol("aggr__0__count", int64(1700)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__parent_count", uint64(15750)),
				model.NewQueryResultCol("aggr__0__key_0", "User deleted"),
				model.NewQueryResultCol("aggr__0__count", int64(1781)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__parent_count", uint64(15750)),
				model.NewQueryResultCol("aggr__0__key_0", "User logged in"),
				model.NewQueryResultCol("aggr__0__count", int64(1757)),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT sum(count(*)) OVER () AS "aggr__0__parent_count",
			  "message" AS "aggr__0__key_0", count(*) AS "aggr__0__count"
			FROM __quesma_table_name
			WHERE ("timestamp">=fromUnixTimestamp64Milli(1708456413795) AND "timestamp"<=
			  fromUnixTimestamp64Milli(1708488074920))
			GROUP BY "message" AS "aggr__0__key_0"
			ORDER BY "aggr__0__count" DESC, "aggr__0__key_0" ASC
			LIMIT 4`,
	},
	{ // [17]
		TestName: "triple nested aggs",
		QueryRequestJson: `
		{
			"aggs": {
				"0": {
					"date_histogram": {
						"field": "order_date",
						"fixed_interval": "12h",
						"extended_bounds": {
							"min": 1708627654149,
							"max": 1708782454149
						}
					},
					"aggs": {
						"1-bucket": {
							"filter": {
								"bool": {
									"must": [
										{
											"query_string": {
												"query": "products.product_name:*watch*",
												"analyze_wildcard": true
											}
										}
									],
									"filter": [],
									"should": [],
									"must_not": []
								}
							},
							"aggs": {
								"1-metric": {
									"sum": {
										"field": "taxful_total_price"
									}
								}
							}
						}
					}
				}
			},
			"size": 2,
			"script_fields": {},
			"stored_fields": [
				"*"
			],
			"runtime_mappings": {},
			"_source": {
				"excludes": []
			},
			"query": {
				"bool": {
					"must": [],
					"filter": [
						{
							"range": {
								"order_date": {
									"format": "strict_date_optional_time",
									"gte": "2024-02-22T18:47:34.149Z",
									"lte": "2024-02-29T18:47:34.149Z"
								}
							}
						}
					],
					"should": [],
					"must_not": []
				}
			},
			"track_total_hits": true
		}`,
		ExpectedResponse: `
		{
			"completion_time_in_millis": 1709243857592,
			"expiration_time_in_millis": 1709243917570,
			"id": "FjI4Y1Q2cFNzUnJDVUc1d3NsaThCTHccRkVwTVBXQW1UOXE1cHl0MHpnT0ZVQTo4MDQxNw==",
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
								"1-bucket": {
									"1-metric": {
										"value": 0.0
									},
									"doc_count": 0
								},
								"doc_count": 10,
								"key": 1708603200000,
								"key_as_string": "2024-02-22T12:00:00.000"
							},
							{
								"1-bucket": {
									"1-metric": {
										"value": 1222.65625
									},
									"doc_count": 13
								},
								"doc_count": 83,
								"key": 1708646400000,
								"key_as_string": "2024-02-23T00:00:00.000"
							},
							{
								"1-bucket": {
									"1-metric": {
										"value": 931.96875
									},
									"doc_count": 9
								},
								"doc_count": 83,
								"key": 1708689600000,
								"key_as_string": "2024-02-23T12:00:00.000"
							},
							{
								"1-bucket": {
									"1-metric": {
										"value": null
									}
								},
								"doc_count": 0,
								"key": 1708732800000,
								"key_as_string": "2024-02-24T00:00:00.000"
							},
							{
								"1-bucket": {
									"1-metric": {
										"value": null
									}
								},
								"doc_count": 0,
								"key": 1708776000000,
								"key_as_string": "2024-02-24T12:00:00.000"
							}
						]
					}
				},
				"hits": {
					"hits": [],
					"max_score": null,
					"total": {
						"relation": "eq",
						"value": 1051
					}
				},
				"timed_out": false,
				"took": 22
			},
			"start_time_in_millis": 1709243857570
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__key_0", int64(39551)),
				model.NewQueryResultCol("aggr__0__count", uint64(10)),
				model.NewQueryResultCol("aggr__0__1-bucket__count", uint64(0)),
				model.NewQueryResultCol("metric__0__1-bucket__1-metric_col_0", 0.0),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__key_0", int64(39552)),
				model.NewQueryResultCol("aggr__0__count", uint64(83)),
				model.NewQueryResultCol("aggr__0__1-bucket__count", uint64(13)),
				model.NewQueryResultCol("metric__0__1-bucket__1-metric_col_0", 1222.65625),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__key_0", int64(39553)),
				model.NewQueryResultCol("aggr__0__count", uint64(83)),
				model.NewQueryResultCol("aggr__0__1-bucket__count", uint64(9)),
				model.NewQueryResultCol("metric__0__1-bucket__1-metric_col_0", 931.96875),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT toInt64(toUnixTimestamp64Milli("order_date") / 43200000) AS
			  "aggr__0__key_0", count(*) AS "aggr__0__count",
			  countIf("products.product_name" ILIKE '%watch%') AS
			  "aggr__0__1-bucket__count",
			  sumOrNullIf("taxful_total_price", "products.product_name" ILIKE '%watch%') AS
			  "metric__0__1-bucket__1-metric_col_0"
			FROM ` + TableName + `
			WHERE ("order_date">=fromUnixTimestamp64Milli(1708627654149) AND "order_date"<=fromUnixTimestamp64Milli(1709232454149))
			GROUP BY toInt64(toUnixTimestamp64Milli("order_date") / 43200000) AS
			  "aggr__0__key_0"
			ORDER BY "aggr__0__key_0" ASC`,
	},
	{ // [18]
		TestName: "complex filters",
		QueryRequestJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"time_offset_split": {
					"aggs": {
						"0": {
							"aggs": {
								"1": {
									"sum": {
										"field": "taxful_total_price"
									}
								},
								"2": {
									"sum": {
										"field": "taxful_total_price"
									}
								}
							},
							"date_histogram": {
								"calendar_interval": "1d",
								"field": "order_date"
							}
						}
					},
					"filters": {
						"filters": {
							"0": {
								"range": {
									"order_date": {
										"format": "strict_date_optional_time",
										"gte": "2024-02-22T21:57:36.376Z",
										"lte": "2024-02-29T21:57:36.376Z"
									}
								}
							},
							"604800000": {
								"range": {
									"order_date": {
										"format": "strict_date_optional_time",
										"gte": "2024-02-15T21:57:36.376Z",
										"lte": "2024-02-22T21:57:36.376Z"
									}
								}
							}
						}
					}
				}
			},
			"query": {
				"bool": {
					"filter": [
						{
							"bool": {
								"minimum_should_match": 1,
								"should": [
									{
										"bool": {
											"filter": [
												{
													"range": {
														"order_date": {
															"format": "strict_date_optional_time",
															"gte": "2024-02-22T21:57:36.376Z",
															"lte": "2024-02-29T21:57:36.376Z"
														}
													}
												}
											]
										}
									},
									{
										"bool": {
											"filter": [
												{
													"range": {
														"order_date": {
															"format": "strict_date_optional_time",
															"gte": "2024-02-15T21:57:36.376Z",
															"lte": "2024-02-22T21:57:36.376Z"
														}
													}
												}
											]
										}
									}
								]
							}
						}
					],
					"must": [],
					"must_not": [],
					"should": []
				}
			},
			"runtime_mappings": {},
			"script_fields": {},
			"size": 3,
			"stored_fields": [
				"*"
			],
			"track_total_hits": false
		}`,
		ExpectedResponse: `
		{
			"completion_time_in_millis": 1709243857589,
			"expiration_time_in_millis": 1709243917582,
			"id": "FnV5ZURIdDdHVGlPZ0xfdTJrQnc3MFEcRkVwTVBXQW1UOXE1cHl0MHpnT0ZVQTo4MDQ1Ng==",
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
					"time_offset_split": {
						"buckets": {
							"0": {
								"0": {
									"buckets": [
										{
											"1": {
												"value": 840.921875
											},
											"2": {
												"value": 841.921875
											},
											"doc_count": 10,
											"key": 1708560000000,
											"key_as_string": "2024-02-22T00:00:00.000"
										},
										{
											"1": {
												"value": 13902.15625
											},
											"2": {
												"value": 13903.15625
											},
											"doc_count": 166,
											"key": 1708646400000,
											"key_as_string": "2024-02-23T00:00:00.000"
										}
									]
								},
								"doc_count": 1051
							},
							"604800000": {
								"0": {
									"buckets": [
										{
											"1": {
												"value": 465.84375
											},
											"2": {
												"value": 466.84375
											},
											"doc_count": 7,
											"key": 1707955200000,
											"key_as_string": "2024-02-15T00:00:00.000"
										}
									]
								},
								"doc_count": 1026
							}
						}
					}
				},
				"hits": {
					"hits": [],
					"max_score": null,
					"total": {
						"relation": "eq",
						"value": 2077
					}
				},
				"timed_out": false,
				"took": 7
			},
			"start_time_in_millis": 1709243857582
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__time_offset_split__count", int64(1051)),
				model.NewQueryResultCol("aggr__time_offset_split__0__key_0", int64(1708560000000/86400000)),
				model.NewQueryResultCol("aggr__time_offset_split__0__count", int64(10)),
				model.NewQueryResultCol("metric__time_offset_split__0__1_col_0", 840.921875),
				model.NewQueryResultCol("metric__time_offset_split__0__2_col_0", 841.921875),
				//model.NewQueryResultCol("filter_1__aggr__time_offset_split__count", int64(1026)),
				//model.NewQueryResultCol("filter_1__aggr__time_offset_split__0__key_0", int64(1708560000000/86400000)),
				//model.NewQueryResultCol("filter_1__aggr__time_offset_split__0__count", int64(0)),
				//model.NewQueryResultCol("filter_1__metric__time_offset_split__0__1_col_0", nil),
				//model.NewQueryResultCol("filter_1__metric__time_offset_split__0__2_col_0", nil),
			}},

			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__time_offset_split__count", int64(1051)),
				model.NewQueryResultCol("aggr__time_offset_split__0__key_0", int64(1708646400000/86400000)),
				model.NewQueryResultCol("aggr__time_offset_split__0__count", int64(166)),
				model.NewQueryResultCol("metric__time_offset_split__0__1_col_0", 13902.156250),
				model.NewQueryResultCol("metric__time_offset_split__0__2_col_0", 13903.156250),
				//model.NewQueryResultCol("filter_1__aggr__time_offset_split__count", int64(1026)),
				//model.NewQueryResultCol("filter_1__aggr__time_offset_split__0__key_0", int64(1708646400000/86400000)),
				//model.NewQueryResultCol("filter_1__aggr__time_offset_split__0__count", int64(0)),
				//model.NewQueryResultCol("filter_1__metric__time_offset_split__0__1_col_0", nil),
				//model.NewQueryResultCol("filter_1__metric__time_offset_split__0__2_col_0", nil),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT sum(count(*)) OVER () AS "aggr__time_offset_split__count",
			  toInt64(toUnixTimestamp64Milli("order_date") / 86400000) AS
			  "aggr__time_offset_split__0__key_0",
			  count(*) AS "aggr__time_offset_split__0__count",
			  sumOrNull("taxful_total_price") AS "metric__time_offset_split__0__1_col_0",
			  sumOrNull("taxful_total_price") AS "metric__time_offset_split__0__2_col_0"
			FROM __quesma_table_name
			WHERE ((("order_date">=fromUnixTimestamp64Milli(1708639056376) AND "order_date"
			  <=fromUnixTimestamp64Milli(1709243856376)) OR ("order_date">=
			  fromUnixTimestamp64Milli(1708034256376) AND "order_date"<=
			  fromUnixTimestamp64Milli(1708639056376))) AND ("order_date">=
			  fromUnixTimestamp64Milli(1708639056376) AND "order_date"<=
			  fromUnixTimestamp64Milli(1709243856376)))
			GROUP BY toInt64(toUnixTimestamp64Milli("order_date") / 86400000) AS
			  "aggr__time_offset_split__0__key_0"
			ORDER BY "aggr__time_offset_split__0__key_0" ASC`,
		ExpectedAdditionalPancakeSQLs: []string{
			`SELECT sum(count(*)) OVER () AS "aggr__time_offset_split__count",
			  toInt64(toUnixTimestamp64Milli("order_date") / 86400000) AS
			  "aggr__time_offset_split__0__key_0",
			  count(*) AS "aggr__time_offset_split__0__count",
			  sumOrNull("taxful_total_price") AS "metric__time_offset_split__0__1_col_0",
			  sumOrNull("taxful_total_price") AS "metric__time_offset_split__0__2_col_0"
			FROM __quesma_table_name
			WHERE ((("order_date">=fromUnixTimestamp64Milli(1708639056376) AND
			  "order_date"<=fromUnixTimestamp64Milli(1709243856376)) OR
			  ("order_date">=fromUnixTimestamp64Milli(1708034256376) AND
			  "order_date"<=fromUnixTimestamp64Milli(1708639056376))) AND
			  ("order_date">=fromUnixTimestamp64Milli(1708034256376) AND
			  "order_date"<=fromUnixTimestamp64Milli(1708639056376)))
			GROUP BY toInt64(toUnixTimestamp64Milli("order_date") / 86400000) AS
			  "aggr__time_offset_split__0__key_0"
			ORDER BY "aggr__time_offset_split__0__key_0" ASC`,
		},
		ExpectedAdditionalPancakeResults: [][]model.QueryResultRow{{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__time_offset_split__count", int64(1026)),
				model.NewQueryResultCol("aggr__time_offset_split__0__key_0", int64(1707955200000/86400000)),
				model.NewQueryResultCol("aggr__time_offset_split__0__count", int64(7)),
				model.NewQueryResultCol("metric__time_offset_split__0__1_col_0", 465.843750),
				model.NewQueryResultCol("metric__time_offset_split__0__2_col_0", 466.843750),
			}}},
		},
	},
	{ // [19]
		TestName: "random sampler, from Explorer > Field statistics",
		QueryRequestJson: `
		{
			"aggs": {
				"sampler": {
					"aggs": {
						"eventRate": {
							"date_histogram": {
								"extended_bounds": {
									"min": 1709816824995,
									"max": 1709816834995
								},
								"field": "@timestamp",
								"fixed_interval": "15000ms",
								"min_doc_count": 0
							}
						}
					},
					"random_sampler": {
						"probability": 1e-06,
						"seed": "1225474982"
					}
				}
			},
			"query": {
				"bool": {
					"filter": [
						{
							"range": {
								"@timestamp": {
									"format": "epoch_millis",
									"gte": 1709815794995,
									"lte": 1709816694995
								}
							}
						},
						{
							"bool": {
								"filter": [],
								"must": [
									{
										"match_all": {}
									}
								],
								"must_not": []
							}
						}
					]
				}
			},
			"size": 0,
			"track_total_hits": false
		}`,
		ExpectedResponse: `
		{
			"completion_time_in_millis": 1709817695887,
			"expiration_time_in_millis": 1709817755884,
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
						"doc_count": 15,
						"eventRate": {
							"buckets": [
								{
									"doc_count": 2,
									"key": 1709816790000,
									"key_as_string": "2024-03-07T13:06:30.000"
								},
								{
									"doc_count": 1,
									"key": 1709816805000,
									"key_as_string": "2024-03-07T13:06:45.000"
								},
								{
									"doc_count": 0,
									"key": 1709816820000,
									"key_as_string": "2024-03-07T13:07:00.000"
								}
							]
						},
						"probability": 1.0,
						"seed": 1740377510
					}
				},
				"hits": {
					"hits": [],
					"max_score": null
				},
				"timed_out": false,
				"took": 3
			},
			"start_time_in_millis": 1709817695884
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__sampler__count", uint64(15)),
				model.NewQueryResultCol("aggr__sampler__eventRate__key_0", int64(1709816790000/15000)),
				model.NewQueryResultCol("aggr__sampler__eventRate__count", 2),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__sampler__count", uint64(15)),
				model.NewQueryResultCol("aggr__sampler__eventRate__key_0", int64(1709816805000/15000)),
				model.NewQueryResultCol("aggr__sampler__eventRate__count", 1),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT sum(count(*)) OVER () AS "aggr__sampler__count",
			  toInt64(toUnixTimestamp64Milli("@timestamp") / 15000) AS
			  "aggr__sampler__eventRate__key_0",
			  count(*) AS "aggr__sampler__eventRate__count"
			FROM (
			  SELECT "@timestamp"
			  FROM ` + TableName + `
			  WHERE ("@timestamp">=fromUnixTimestamp64Milli(1709815794995) AND "@timestamp"<=fromUnixTimestamp64Milli(1709816694995))
			  LIMIT 20000)
			GROUP BY toInt64(toUnixTimestamp64Milli("@timestamp") / 15000) AS
			  "aggr__sampler__eventRate__key_0"
			ORDER BY "aggr__sampler__eventRate__key_0" ASC`,
	},
	{ // [20]
		TestName: "Field statistics > summary for numeric fields",
		QueryRequestJson: `
		{
			"aggs": {
				"sample": {
					"aggs": {
						"bytes_gauge_field_stats": {
							"aggs": {
								"actual_stats": {
									"stats": {
										"field": "bytes_gauge"
									}
								}
							},
							"filter": {
								"exists": {
									"field": "bytes_gauge"
								}
							}
						},
						"bytes_gauge_percentiles": {
							"percentiles": {
								"field": "bytes_gauge",
								"keyed": false,
								"percents": [
									5, 10, 15, 20, 25, 30, 35, 40, 45, 50,
									55, 60, 65, 70, 75, 80, 85, 90, 95, 100
								]
							}
						},
						"bytes_gauge_percentiles_keyed_true": {
							"percentiles": {
								"field": "bytes_gauge",
								"percents": [
									5, 10, 15, 20, 25, 30, 35, 40, 45, 50,
									55, 60, 65, 70, 75, 80, 85, 90, 95, 100
								]
							}
						},
						"bytes_gauge_top": {
							"terms": {
								"field": "bytes_gauge",
								"order": {
									"_count": "desc"
								},
								"size": 10
							}
						}
					},
					"sampler": {
						"shard_size": 5000
					}
				}
			},
			"query": {
				"bool": {
					"filter": [
						{
							"range": {
								"timestamp": {
									"format": "epoch_millis",
									"gte": 1709932426749,
									"lte": 1711228426749
								}
							}
						},
						{
							"bool": {
								"filter": [],
								"must": [
									{
										"match_all": {}
									}
								],
								"must_not": []
							}
						}
					]
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
			"track_total_hits": true
		}`,
		ExpectedResponse: `
		{
			"is_partial": false,
			"is_running": false,
			"start_time_in_millis": 1711263722921,
			"expiration_time_in_millis": 1711695722921,
			"completion_time_in_millis": 1711263722955,
			"response": {
				"took": 34,
				"timed_out": false,
				"_shards": {
					"total": 1,
					"successful": 1,
					"skipped": 0,
					"failed": 0
				},
				"hits": {
					"total": {
						"value": 1634,
						"relation": "eq"
					},
					"max_score": null,
					"hits": []
				},
				"aggregations": {
					"sample": {
						"doc_count": 1634,
						"bytes_gauge_top": {
							"doc_count_error_upper_bound": 0,
							"sum_other_doc_count": 1570,
							"buckets": [
								{
									"key": 0,
									"doc_count": 53
								},
								{
									"key": 15035,
									"doc_count": 7
								},
								{
									"key": 3350,
									"doc_count": 4
								}
							]
						},
						"bytes_gauge_percentiles": {
							"values": [
								{"key": 5, "value": 349.95000000000005},
								{"key": 10, "value": 1600.2},
								{"key": 15, "value": 2104.85},
								{"key": 20, "value": 2653.6000000000004},
								{"key": 25, "value": 3118.75},
								{"key": 30, "value": 3599.7},
								{"key": 35, "value": 4142.75},
								{"key": 40, "value": 4605.4},
								{"key": 45, "value": 5090.650000000001},
								{"key": 50, "value": 5574.5},
								{"key": 55, "value": 6127.450000000001},
								{"key": 60, "value": 6562.799999999999},
								{"key": 65, "value": 7006.25},
								{"key": 70, "value": 7493.5},
								{"key": 75, "value": 8078.75},
								{"key": 80, "value": 8537.800000000001},
								{"key": 85, "value": 9021.3},
								{"key": 90, "value": 9609.4},
								{"key": 95, "value": 10931.049999999983},
								{"key": 100, "value": 19742}
							]
						},
						"bytes_gauge_percentiles_keyed_true": {
							"values": {
								"5.0": 349.95000000000005,
								"10.0": 1600.2,
								"15.0": 2104.85,
								"20.0": 2653.6000000000004,
								"25.0": 3118.75,
								"30.0": 3599.7,
								"35.0": 4142.75,
								"40.0": 4605.4,
								"45.0": 5090.650000000001,
								"50.0": 5574.5,
								"55.0": 6127.450000000001,
								"60.0": 6562.799999999999,
								"65.0": 7006.25,
								"70.0": 7493.5,
								"75.0": 8078.75,
								"80.0": 8537.800000000001,
								"85.0": 9021.3,
								"90.0": 9609.4,
								"95.0": 10931.049999999983,
								"100.0": 19742
							}
						},
						"bytes_gauge_field_stats": {
							"doc_count": 1634,
							"actual_stats": {
								"count": 1634,
								"min": 0,
								"max": 19742,
								"avg": 5750.900856793146,
								"sum": 9396972
							}
						}
					}
				}
			}
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__sample__count", int64(1634)),
				model.NewQueryResultCol("metric__sample__bytes_gauge_percentiles_col_0", []float64{349.95000000000005}),
				model.NewQueryResultCol("metric__sample__bytes_gauge_percentiles_col_1", []float64{1600.2}),
				model.NewQueryResultCol("metric__sample__bytes_gauge_percentiles_col_2", []float64{2104.85}),
				model.NewQueryResultCol("metric__sample__bytes_gauge_percentiles_col_3", []float64{2653.6000000000004}),
				model.NewQueryResultCol("metric__sample__bytes_gauge_percentiles_col_4", []float64{3118.75}),
				model.NewQueryResultCol("metric__sample__bytes_gauge_percentiles_col_5", []float64{3599.7}),
				model.NewQueryResultCol("metric__sample__bytes_gauge_percentiles_col_6", []float64{4142.75}),
				model.NewQueryResultCol("metric__sample__bytes_gauge_percentiles_col_7", []float64{4605.4}),
				model.NewQueryResultCol("metric__sample__bytes_gauge_percentiles_col_8", []float64{5090.650000000001}),
				model.NewQueryResultCol("metric__sample__bytes_gauge_percentiles_col_9", []float64{5574.5}),
				model.NewQueryResultCol("metric__sample__bytes_gauge_percentiles_col_10", []float64{6127.450000000001}),
				model.NewQueryResultCol("metric__sample__bytes_gauge_percentiles_col_11", []float64{6562.799999999999}),
				model.NewQueryResultCol("metric__sample__bytes_gauge_percentiles_col_12", []float64{7006.25}),
				model.NewQueryResultCol("metric__sample__bytes_gauge_percentiles_col_13", []float64{7493.5}),
				model.NewQueryResultCol("metric__sample__bytes_gauge_percentiles_col_14", []float64{8078.75}),
				model.NewQueryResultCol("metric__sample__bytes_gauge_percentiles_col_15", []float64{8537.800000000001}),
				model.NewQueryResultCol("metric__sample__bytes_gauge_percentiles_col_16", []float64{9021.3}),
				model.NewQueryResultCol("metric__sample__bytes_gauge_percentiles_col_17", []float64{9609.4}),
				model.NewQueryResultCol("metric__sample__bytes_gauge_percentiles_col_18", []float64{10931.049999999983}),
				model.NewQueryResultCol("metric__sample__bytes_gauge_percentiles_col_19", []float64{19742}),
				model.NewQueryResultCol("metric__sample__bytes_gauge_percentiles_keyed_true_col_0", []float64{349.95000000000005}),
				model.NewQueryResultCol("metric__sample__bytes_gauge_percentiles_keyed_true_col_1", []float64{1600.2}),
				model.NewQueryResultCol("metric__sample__bytes_gauge_percentiles_keyed_true_col_2", []float64{2104.85}),
				model.NewQueryResultCol("metric__sample__bytes_gauge_percentiles_keyed_true_col_3", []float64{2653.6000000000004}),
				model.NewQueryResultCol("metric__sample__bytes_gauge_percentiles_keyed_true_col_4", []float64{3118.75}),
				model.NewQueryResultCol("metric__sample__bytes_gauge_percentiles_keyed_true_col_5", []float64{3599.7}),
				model.NewQueryResultCol("metric__sample__bytes_gauge_percentiles_keyed_true_col_6", []float64{4142.75}),
				model.NewQueryResultCol("metric__sample__bytes_gauge_percentiles_keyed_true_col_7", []float64{4605.4}),
				model.NewQueryResultCol("metric__sample__bytes_gauge_percentiles_keyed_true_col_8", []float64{5090.650000000001}),
				model.NewQueryResultCol("metric__sample__bytes_gauge_percentiles_keyed_true_col_9", []float64{5574.5}),
				model.NewQueryResultCol("metric__sample__bytes_gauge_percentiles_keyed_true_col_10", []float64{6127.450000000001}),
				model.NewQueryResultCol("metric__sample__bytes_gauge_percentiles_keyed_true_col_11", []float64{6562.799999999999}),
				model.NewQueryResultCol("metric__sample__bytes_gauge_percentiles_keyed_true_col_12", []float64{7006.25}),
				model.NewQueryResultCol("metric__sample__bytes_gauge_percentiles_keyed_true_col_13", []float64{7493.5}),
				model.NewQueryResultCol("metric__sample__bytes_gauge_percentiles_keyed_true_col_14", []float64{8078.75}),
				model.NewQueryResultCol("metric__sample__bytes_gauge_percentiles_keyed_true_col_15", []float64{8537.800000000001}),
				model.NewQueryResultCol("metric__sample__bytes_gauge_percentiles_keyed_true_col_16", []float64{9021.3}),
				model.NewQueryResultCol("metric__sample__bytes_gauge_percentiles_keyed_true_col_17", []float64{9609.4}),
				model.NewQueryResultCol("metric__sample__bytes_gauge_percentiles_keyed_true_col_18", []float64{10931.049999999983}),
				model.NewQueryResultCol("metric__sample__bytes_gauge_percentiles_keyed_true_col_19", []float64{19742}),
				model.NewQueryResultCol("aggr__sample__bytes_gauge_field_stats__count", 1634),
				model.NewQueryResultCol("metric__sample__bytes_gauge_field_stats__actual_stats_col_0", 1634),
				model.NewQueryResultCol("metric__sample__bytes_gauge_field_stats__actual_stats_col_1", 0),
				model.NewQueryResultCol("metric__sample__bytes_gauge_field_stats__actual_stats_col_2", 19742),
				model.NewQueryResultCol("metric__sample__bytes_gauge_field_stats__actual_stats_col_3", 5750.900856793146),
				model.NewQueryResultCol("metric__sample__bytes_gauge_field_stats__actual_stats_col_4", 9396972),
			}},
		},
		ExpectedAdditionalPancakeResults: [][]model.QueryResultRow{
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("aggr__sample__count", int64(1634)),
					model.NewQueryResultCol("aggr__sample__bytes_gauge_top__parent_count", int64(1634)),
					model.NewQueryResultCol("aggr__sample__bytes_gauge_top__key_0", int64(0)),
					model.NewQueryResultCol("aggr__sample__bytes_gauge_top__count", int64(53)),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("aggr__sample__count", int64(1634)),
					model.NewQueryResultCol("aggr__sample__bytes_gauge_top__parent_count", int64(1634)),
					model.NewQueryResultCol("aggr__sample__bytes_gauge_top__key_0", int64(15035)),
					model.NewQueryResultCol("aggr__sample__bytes_gauge_top__count", int64(7)),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("aggr__sample__count", int64(1634)),
					model.NewQueryResultCol("aggr__sample__bytes_gauge_top__parent_count", int64(1634)),
					model.NewQueryResultCol("aggr__sample__bytes_gauge_top__key_0", int64(3350)),
					model.NewQueryResultCol("aggr__sample__bytes_gauge_top__count", int64(4)),
				}},
			},
		},
		ExpectedPancakeSQL: `
			SELECT count(*) AS "aggr__sample__count",
			  quantiles(0.050000)("bytes_gauge") AS
			  "metric__sample__bytes_gauge_percentiles_col_0",
			  quantiles(0.100000)("bytes_gauge") AS
			  "metric__sample__bytes_gauge_percentiles_col_1",
			  quantiles(0.150000)("bytes_gauge") AS
			  "metric__sample__bytes_gauge_percentiles_col_2",
			  quantiles(0.200000)("bytes_gauge") AS
			  "metric__sample__bytes_gauge_percentiles_col_3",
			  quantiles(0.250000)("bytes_gauge") AS
			  "metric__sample__bytes_gauge_percentiles_col_4",
			  quantiles(0.300000)("bytes_gauge") AS
			  "metric__sample__bytes_gauge_percentiles_col_5",
			  quantiles(0.350000)("bytes_gauge") AS
			  "metric__sample__bytes_gauge_percentiles_col_6",
			  quantiles(0.400000)("bytes_gauge") AS
			  "metric__sample__bytes_gauge_percentiles_col_7",
			  quantiles(0.450000)("bytes_gauge") AS
			  "metric__sample__bytes_gauge_percentiles_col_8",
			  quantiles(0.500000)("bytes_gauge") AS
			  "metric__sample__bytes_gauge_percentiles_col_9",
			  quantiles(0.550000)("bytes_gauge") AS
			  "metric__sample__bytes_gauge_percentiles_col_10",
			  quantiles(0.600000)("bytes_gauge") AS
			  "metric__sample__bytes_gauge_percentiles_col_11",
			  quantiles(0.650000)("bytes_gauge") AS
			  "metric__sample__bytes_gauge_percentiles_col_12",
			  quantiles(0.700000)("bytes_gauge") AS
			  "metric__sample__bytes_gauge_percentiles_col_13",
			  quantiles(0.750000)("bytes_gauge") AS
			  "metric__sample__bytes_gauge_percentiles_col_14",
			  quantiles(0.800000)("bytes_gauge") AS
			  "metric__sample__bytes_gauge_percentiles_col_15",
			  quantiles(0.850000)("bytes_gauge") AS
			  "metric__sample__bytes_gauge_percentiles_col_16",
			  quantiles(0.900000)("bytes_gauge") AS
			  "metric__sample__bytes_gauge_percentiles_col_17",
			  quantiles(0.950000)("bytes_gauge") AS
			  "metric__sample__bytes_gauge_percentiles_col_18",
			  quantiles(0.999999)("bytes_gauge") AS
			  "metric__sample__bytes_gauge_percentiles_col_19",
			  quantiles(0.050000)("bytes_gauge") AS
			  "metric__sample__bytes_gauge_percentiles_keyed_true_col_0",
			  quantiles(0.100000)("bytes_gauge") AS
			  "metric__sample__bytes_gauge_percentiles_keyed_true_col_1",
			  quantiles(0.150000)("bytes_gauge") AS
			  "metric__sample__bytes_gauge_percentiles_keyed_true_col_2",
			  quantiles(0.200000)("bytes_gauge") AS
			  "metric__sample__bytes_gauge_percentiles_keyed_true_col_3",
			  quantiles(0.250000)("bytes_gauge") AS
			  "metric__sample__bytes_gauge_percentiles_keyed_true_col_4",
			  quantiles(0.300000)("bytes_gauge") AS
			  "metric__sample__bytes_gauge_percentiles_keyed_true_col_5",
			  quantiles(0.350000)("bytes_gauge") AS
			  "metric__sample__bytes_gauge_percentiles_keyed_true_col_6",
			  quantiles(0.400000)("bytes_gauge") AS
			  "metric__sample__bytes_gauge_percentiles_keyed_true_col_7",
			  quantiles(0.450000)("bytes_gauge") AS
			  "metric__sample__bytes_gauge_percentiles_keyed_true_col_8",
			  quantiles(0.500000)("bytes_gauge") AS
			  "metric__sample__bytes_gauge_percentiles_keyed_true_col_9",
			  quantiles(0.550000)("bytes_gauge") AS
			  "metric__sample__bytes_gauge_percentiles_keyed_true_col_10",
			  quantiles(0.600000)("bytes_gauge") AS
			  "metric__sample__bytes_gauge_percentiles_keyed_true_col_11",
			  quantiles(0.650000)("bytes_gauge") AS
			  "metric__sample__bytes_gauge_percentiles_keyed_true_col_12",
			  quantiles(0.700000)("bytes_gauge") AS
			  "metric__sample__bytes_gauge_percentiles_keyed_true_col_13",
			  quantiles(0.750000)("bytes_gauge") AS
			  "metric__sample__bytes_gauge_percentiles_keyed_true_col_14",
			  quantiles(0.800000)("bytes_gauge") AS
			  "metric__sample__bytes_gauge_percentiles_keyed_true_col_15",
			  quantiles(0.850000)("bytes_gauge") AS
			  "metric__sample__bytes_gauge_percentiles_keyed_true_col_16",
			  quantiles(0.900000)("bytes_gauge") AS
			  "metric__sample__bytes_gauge_percentiles_keyed_true_col_17",
			  quantiles(0.950000)("bytes_gauge") AS
			  "metric__sample__bytes_gauge_percentiles_keyed_true_col_18",
			  quantiles(0.999999)("bytes_gauge") AS
			  "metric__sample__bytes_gauge_percentiles_keyed_true_col_19",
			  countIf("bytes_gauge" IS NOT NULL) AS
			  "aggr__sample__bytes_gauge_field_stats__count",
			  countIf("bytes_gauge" IS NOT NULL) AS
			  "metric__sample__bytes_gauge_field_stats__actual_stats_col_0",
			  minOrNullIf("bytes_gauge", "bytes_gauge" IS NOT NULL) AS
			  "metric__sample__bytes_gauge_field_stats__actual_stats_col_1",
			  maxOrNullIf("bytes_gauge", "bytes_gauge" IS NOT NULL) AS
			  "metric__sample__bytes_gauge_field_stats__actual_stats_col_2",
			  avgOrNullIf("bytes_gauge", "bytes_gauge" IS NOT NULL) AS
			  "metric__sample__bytes_gauge_field_stats__actual_stats_col_3",
			  sumOrNullIf("bytes_gauge", "bytes_gauge" IS NOT NULL) AS
			  "metric__sample__bytes_gauge_field_stats__actual_stats_col_4"
			FROM (
			  SELECT "bytes_gauge"
			  FROM __quesma_table_name
			  WHERE ("timestamp">=fromUnixTimestamp64Milli(1709932426749) AND "timestamp"<=fromUnixTimestamp64Milli(1711228426749))
			  LIMIT 20000)`,
		ExpectedAdditionalPancakeSQLs: []string{
			`SELECT sum(count(*)) OVER () AS "aggr__sample__count",
			  sum(count(*)) OVER () AS "aggr__sample__bytes_gauge_top__parent_count",
			  "bytes_gauge" AS "aggr__sample__bytes_gauge_top__key_0",
			  count(*) AS "aggr__sample__bytes_gauge_top__count"
			FROM (
			  SELECT "bytes_gauge"
			  FROM __quesma_table_name
			  WHERE ("timestamp">=fromUnixTimestamp64Milli(1709932426749) AND "timestamp"<=fromUnixTimestamp64Milli(1711228426749))
			  LIMIT 20000)
			GROUP BY "bytes_gauge" AS "aggr__sample__bytes_gauge_top__key_0"
			ORDER BY "aggr__sample__bytes_gauge_top__count" DESC,
			  "aggr__sample__bytes_gauge_top__key_0" ASC
			LIMIT 11`,
		},
	},
	{ // [21]
		TestName: "range bucket aggregation, both keyed and not",
		QueryRequestJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"2": {
					"range": {
						"field": "bytes_gauge",
						"keyed": true,
						"ranges": [
							{
								"from": 0,
								"to": 1000
							},
							{
								"from": 1000,
								"to": 2000
							},
							{
								"from": -5.5
							},
							{
								"to": 6.555
							},
							{
							}
						]
					}
				},
				"3": {
					"range": {
						"field": "bytes_gauge",
						"ranges": [
							{
								"from": 0,
								"to": 1000
							},
							{
								"from": 1000,
								"to": 2000
							},
							{
								"from": -5.5
							},
							{
								"to": 6.555
							},
							{
							}
						]
					}
				}
			},
			"docvalue_fields": [
				{
					"field": "epoch_time",
					"format": "date_time"
				},
				{
					"field": "ts_time_druid",
					"format": "date_time"
				}
			],
			"query": {
				"bool": {
					"filter": [
						{
							"match_all": {}
						},
						{
							"range": {
								"timestamp": {
									"format": "strict_date_optional_time",
									"gte": "2024-04-16T12:15:11.790Z",
									"lte": "2024-04-16T12:30:11.790Z"
								}
							}
						}
					],
					"must": [],
					"must_not": [],
					"should": []
				}
			},
			"script_fields": {},
			"size": 0,
			"stored_fields": [
				"*"
			],
			"track_total_hits": true
		}`,
		ExpectedResponse: `
		{
			"is_partial": false,
			"is_running": false,
			"start_time_in_millis": 1711263722921,
			"expiration_time_in_millis": 1711695722921,
			"completion_time_in_millis": 1711263722955,
			"response": {
				"_shards": {
					"failed": 0,
					"skipped": 0,
					"successful": 1,
					"total": 1
				},
				"aggregations": {
					"2": {
						"buckets": {
							"0.0-1000.0": {
								"doc_count": 1,
								"from": 0.0,
								"to": 1000.0
							},
							"1000.0-2000.0": {
								"doc_count": 0,
								"from": 1000.0,
								"to": 2000.0
							},
							"-5.5-*": {
								"doc_count": 5,
								"from": -5.5
							},
							"*-6.555": {
								"doc_count": 6,
								"to": 6.555
							},
							"*-*": {
								"doc_count": 10
							}
						}
					},
					"3": {
						"buckets": [
							{
								"doc_count": 1,
								"from": 0.0,
								"to": 1000.0,
								"key": "0.0-1000.0" 
							},
							{
								"doc_count": 0,
								"from": 1000.0,
								"to": 2000.0,
								"key": "1000.0-2000.0"
							},
							{
								"doc_count": 5,
								"from": -5.5,
								"key": "-5.5-*"
							},
							{
								"doc_count": 6,
								"to": 6.555,
								"key": "*-6.555"
							},
							{
								"doc_count": 10,
								"key": "*-*"
							}
						]
					}
				},
				"hits": {
					"hits": [],
					"max_score": null,
					"total": {
						"relation": "eq",
						"value": 1
					}
				},
				"timed_out": false,
				"took": 123
			}
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("range_0__aggr__2__count", uint64(1)),
				model.NewQueryResultCol("range_1__aggr__2__count", uint64(0)),
				model.NewQueryResultCol("range_2__aggr__2__count", uint64(5)),
				model.NewQueryResultCol("range_3__aggr__2__count", uint64(6)),
				model.NewQueryResultCol("range_4__aggr__2__count", uint64(10)),
			}},
		},
		ExpectedAdditionalPancakeResults: [][]model.QueryResultRow{
			{{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("range_0__aggr__3__count", uint64(1)),
				model.NewQueryResultCol("range_1__aggr__3__count", uint64(0)),
				model.NewQueryResultCol("range_2__aggr__3__count", uint64(5)),
				model.NewQueryResultCol("range_3__aggr__3__count", uint64(6)),
				model.NewQueryResultCol("range_4__aggr__3__count", uint64(10)),
			}}},
		},
		ExpectedPancakeSQL: `
			SELECT countIf(("bytes_gauge">=0 AND "bytes_gauge"<1000)) AS
			  "range_0__aggr__2__count",
			  countIf(("bytes_gauge">=1000 AND "bytes_gauge"<2000)) AS
			  "range_1__aggr__2__count",
			  countIf("bytes_gauge">=-5.5) AS "range_2__aggr__2__count",
			  countIf("bytes_gauge"<6.555) AS "range_3__aggr__2__count",
			  countIf("bytes_gauge" IS NOT NULL) AS "range_4__aggr__2__count"
			FROM __quesma_table_name
			WHERE ("timestamp">=fromUnixTimestamp64Milli(1713269711790) AND "timestamp"<=
			  fromUnixTimestamp64Milli(1713270611790))`,
		ExpectedAdditionalPancakeSQLs: []string{`
			SELECT countIf(("bytes_gauge">=0 AND "bytes_gauge"<1000)) AS
			  "range_0__aggr__3__count",
			  countIf(("bytes_gauge">=1000 AND "bytes_gauge"<2000)) AS
			  "range_1__aggr__3__count",
			  countIf("bytes_gauge">=-5.5) AS "range_2__aggr__3__count",
			  countIf("bytes_gauge"<6.555) AS "range_3__aggr__3__count",
			  countIf("bytes_gauge" IS NOT NULL) AS "range_4__aggr__3__count"
			FROM __quesma_table_name
			WHERE ("timestamp">=fromUnixTimestamp64Milli(1713269711790) AND "timestamp"<=
			  fromUnixTimestamp64Milli(1713270611790))`,
		},
	},
	{ // [22]
		TestName: "date_range aggregation",
		QueryRequestJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"2": {
					"date_range": {
						"field": "timestamp",
						"ranges": [
							{
								"to": "now"
							},
							{
								"from": "now-3w/d",
								"to": "now"
							},
							{
								"from": "2024-04-14"
							}
						],
						"time_zone": "Europe/Warsaw"
					}
				}
			},
			"docvalue_fields": [
				{
					"field": "customer_birth_date",
					"format": "date_time"
				},
				{
					"field": "order_date",
					"format": "date_time"
				},
				{
					"field": "products.created_on",
					"format": "date_time"
				}
			],
			"query": {
				"bool": {
					"filter": [
						{
							"match_all": {}
						},
						{
							"range": {
								"timestamp": {
									"format": "strict_date_optional_time",
									"gte": "2024-04-06T07:28:50.059Z",
									"lte": "2024-04-16T17:28:50.059Z"
								}
							}
						}
					],
					"must": [],
					"must_not": [],
					"should": []
				}
			},
			"script_fields": {},
			"size": 0,
			"stored_fields": [
				"*"
			],
			"track_total_hits": true
		}`,
		ExpectedResponse: `
		{
			"is_partial": false,
			"is_running": false,
			"start_time_in_millis": 1711263722921,
			"expiration_time_in_millis": 1711695722921,
			"completion_time_in_millis": 1711263722955,
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
								"doc_count": 1541,
								"key": "*-2024-04-16T17:28:50.000",
								"to": 1713288530000.0,
								"to_as_string": "2024-04-16T17:28:50.000"
							},
							{
								"doc_count": 1541,
								"from": 1711407600000.0,
								"from_as_string": "2024-03-25T23:00:00.000",
								"key": "2024-03-25T23:00:00.000-2024-04-16T17:28:50.000",
								"to": 1713288530000.0,
								"to_as_string": "2024-04-16T17:28:50.000"
							},
							{
								"doc_count": 414,
								"from": 1713045600000.0,
								"from_as_string": "2024-04-13T22:00:00.000",
								"key": "2024-04-13T22:00:00.000-*"
							}
						]
					}
				},
				"hits": {
					"hits": [],
					"max_score": null,
					"total": {
						"relation": "eq",
						"value": 1541
					}
				},
				"timed_out": false,
				"took": 13
			}
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("range_0__aggr__2__count", int64(1541)),
				model.NewQueryResultCol("range_1__aggr__2__count", int64(1541)),
				model.NewQueryResultCol("range_2__aggr__2__count", int(414)),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT countIf("timestamp"<toInt64(toUnixTimestamp(now()))) AS
			  "range_0__aggr__2__count",
			  countIf(("timestamp">=toInt64(toUnixTimestamp(toStartOfDay(subDate(now(),
			  INTERVAL 3 week)))) AND "timestamp"<toInt64(toUnixTimestamp(now())))) AS
			  "range_1__aggr__2__count",
			  countIf("timestamp">=toInt64(toUnixTimestamp('2024-04-14'))) AS
			  "range_2__aggr__2__count"
			FROM ` + TableName + ` 
			WHERE ("timestamp">=fromUnixTimestamp64Milli(1712388530059) AND "timestamp"<=fromUnixTimestamp64Milli(1713288530059))`,
	},
	{ // [23]
		TestName: "significant terms aggregation: same as terms for now",
		QueryRequestJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"2": {
					"significant_terms": {
						"field": "message",
						"size": 4
					}
				}
			},
			"docvalue_fields": [
				{
					"field": "epoch_time",
					"format": "date_time"
				},
				{
					"field": "ts_time_druid",
					"format": "date_time"
				}
			],
			"query": {
				"bool": {
					"filter": [],
					"must": [
						{
							"match_all": {}
						}
					],
					"must_not": [],
					"should": []
				}
			},
			"script_fields": {},
			"size": 0,
			"stored_fields": [
				"*"
			],
			"track_total_hits": true
		}`,
		ExpectedResponse: `
		{
			"is_partial": false,
			"is_running": false,
			"start_time_in_millis": 1711263722921,
			"expiration_time_in_millis": 1711695722921,
			"completion_time_in_millis": 1711263722955,
			"response": {
				"_shards": {
					"failed": 0,
					"skipped": 0,
					"successful": 1,
					"total": 1
				},
				"aggregations": {
					"2": {
						"bg_count": 825,
						"doc_count": 825,
						"buckets": [
							{
								"bg_count": 619,
								"doc_count": 619,
								"key": "a",
								"score": 619
							},
							{
								"bg_count": 206,
								"doc_count": 206,
								"key": "zip",
								"score": 206
							}
						]
					}
				},
				"hits": {
					"hits": [],
					"max_score": null,
					"total": {
						"relation": "eq",
						"value": 1608
					}
				},
				"timed_out": false,
				"took": 14
			}
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", uint64(825)),
				model.NewQueryResultCol("aggr__2__key_0", "a"),
				model.NewQueryResultCol("aggr__2__count", uint64(619)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", uint64(825)),
				model.NewQueryResultCol("aggr__2__key_0", "zip"),
				model.NewQueryResultCol("aggr__2__count", uint64(206)),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT sum(count(*)) OVER () AS "aggr__2__parent_count",
			  "message" AS "aggr__2__key_0", 
			  count(*) AS "aggr__2__count"
			FROM ` + TableName + `
			GROUP BY "message" AS "aggr__2__key_0"
			ORDER BY "aggr__2__count" DESC, "aggr__2__key_0" ASC
			LIMIT 5`,
	},
	{ // [24]
		TestName: "meta field in aggregation",
		QueryRequestJson: `
		{
			"aggs": {
				"timeseries": {
					"aggs": {
						"61ca57f2-469d-11e7-af02-69e470af7417": {
							"cardinality": {
								"field": "host.name"
							}
						}
					},
					"date_histogram": {
						"fixed_interval": "22h",
						"field": "@timestamp"
					},
					"meta": {
						"bucketSize": 3600,
						"intervalString": "3600s",
						"seriesId": "61ca57f1-469d-11e7-af02-69e470af7417",
						"timeField": "timestamp"
					}
				}
			},
			"query": {
				"bool": {
					"filter": []
				}
			},
			"size": 0,
			"timeout": "30000ms",
			"track_total_hits": true
		}`,
		ExpectedResponse: `
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
					"value": 1180,
					"relation": "eq"
				},
				"max_score": 0,
				"hits": []
			},
			"aggregations": {
				"timeseries": {
					"buckets": [
						{
							"61ca57f2-469d-11e7-af02-69e470af7417": {
								"value": 21
							},
							"doc_count": 1180,
							"key": 1713571200000,
							"key_as_string": "2024-04-20T00:00:00.000"
						}
					],
					"meta": {
						"bucketSize": 3600,
						"intervalString": "3600s",
						"seriesId": "61ca57f1-469d-11e7-af02-69e470af7417",
						"timeField": "timestamp"
					}
				}
			}
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__timeseries__key_0", int64(1713571200000/79200000)),
				model.NewQueryResultCol("aggr__timeseries__count", 1180),
				model.NewQueryResultCol("metric__timeseries__61ca57f2-469d-11e7-af02-69e470af7417_col_0", 21),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT toInt64(toUnixTimestamp64Milli("@timestamp") / 79200000) AS
			  "aggr__timeseries__key_0", count(*) AS "aggr__timeseries__count",
			  uniq("host.name") AS
			  "metric__timeseries__61ca57f2-469d-11e7-af02-69e470af7417_col_0"
			FROM ` + TableName + `
			GROUP BY toInt64(toUnixTimestamp64Milli("@timestamp") / 79200000) AS
			  "aggr__timeseries__key_0"
			ORDER BY "aggr__timeseries__key_0" ASC`,
	},
	{ // [25]
		TestName: "simple histogram, but min_doc_count: 0",
		QueryRequestJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"2": {
					"histogram": {
						"field": "bytes",
						"interval": 100,
						"min_doc_count": 0
					}
				}
			},
			"docvalue_fields": [
				{
					"field": "@timestamp",
					"format": "date_time"
				},
				{
					"field": "timestamp",
					"format": "date_time"
				},
				{
					"field": "utc_time",
					"format": "date_time"
				}
			],
			"query": {
				"bool": {
					"filter": [
						{
							"range": {
								"timestamp": {
									"format": "strict_date_optional_time",
									"gte": "2024-05-10T13:47:56.077Z",
									"lte": "2024-05-10T14:02:56.077Z"
								}
							}
						}
					],
					"must": [
						{
							"match_all": {}
						}
					],
					"must_not": [],
					"should": []
				}
			},
			"script_fields": {
				"hour_of_day": {
					"script": {
						"lang": "painless",
						"source": "doc['timestamp'].value.getHour()"
					}
				}
			},
			"size": 0,
			"stored_fields": [
				"*"
			],
			"track_total_hits": true
		}`,
		ExpectedResponse: `
		{
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
							"doc_count": 1,
							"key": 9100.0
						},
						{
							"doc_count": 0,
							"key": 9200.0
						},
						{
							"doc_count": 0,
							"key": 9300.0
						},
						{
							"doc_count": 0,
							"key": 9400.0
						},
						{
							"doc_count": 0,
							"key": 9500.0
						},
						{
							"doc_count": 0,
							"key": 9600.0
						},
						{
							"doc_count": 2,
							"key": 9700.0
						}
					]
				}
			},
			"hits": {
				"hits": [],
				"max_score": null,
				"total": {
					"relation": "eq",
					"value": 6
				}
			},
			"timed_out": false,
			"took": 10
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__key_0", 9100.0),
				model.NewQueryResultCol("aggr__2__count", 1),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__key_0", 9700.0),
				model.NewQueryResultCol("aggr__2__count", 2),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT floor("bytes"/100)*100 AS "aggr__2__key_0",
			  count(*) AS "aggr__2__count"
			FROM ` + TableName + `
			 WHERE ("timestamp">=fromUnixTimestamp64Milli(1715348876077) AND "timestamp"<=fromUnixTimestamp64Milli(1715349776077))
			GROUP BY floor("bytes"/100)*100 AS "aggr__2__key_0"
			ORDER BY "aggr__2__key_0" ASC`,
	},
	{ // [26]
		TestName: "simple date_histogram, but min_doc_count: 0",
		QueryRequestJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"2": {
					"date_histogram": {
						"field": "timestamp",
						"fixed_interval": "30s",
						"min_doc_count": 0
					}
				}
			},
			"docvalue_fields": [
				{
					"field": "@timestamp",
					"format": "date_time"
				},
				{
					"field": "timestamp",
					"format": "date_time"
				},
				{
					"field": "utc_time",
					"format": "date_time"
				}
			],
			"query": {
				"bool": {
					"filter": [
						{
							"range": {
								"timestamp": {
									"format": "strict_date_optional_time",
									"gte": "2024-05-10T14:29:02.900Z",
									"lte": "2024-05-10T14:44:02.900Z"
								}
							}
						}
					],
					"must": [
						{
							"match_all": {}
						}
					],
					"must_not": [],
					"should": []
				}
			},
			"script_fields": {
				"hour_of_day": {
					"script": {
						"lang": "painless",
						"source": "doc['timestamp'].value.getHour()"
					}
				}
			},
			"size": 0,
			"stored_fields": [
				"*"
			],
			"track_total_hits": true
		}`,
		ExpectedResponse: `
		{
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
							"doc_count": 1,
							"key": 1715351610000,
							"key_as_string": "2024-05-10T14:33:30.000"
						},
						{
							"doc_count": 1,
							"key": 1715351640000,
							"key_as_string": "2024-05-10T14:34:00.000"
						},
						{
							"doc_count": 0,
							"key": 1715351670000,
							"key_as_string": "2024-05-10T14:34:30.000"
						},
						{
							"doc_count": 0,
							"key": 1715351700000,
							"key_as_string": "2024-05-10T14:35:00.000"
						},
						{
							"doc_count": 1,
							"key": 1715351730000,
							"key_as_string": "2024-05-10T14:35:30.000"
						}
					]
				}
			},
			"hits": {
				"hits": [],
				"max_score": null,
				"total": {
					"relation": "eq",
					"value": 4
				}
			},
			"timed_out": false,
			"took": 146
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__key_0", int64(1715351610000/30000)),
				model.NewQueryResultCol("aggr__2__count", 1),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__key_0", int64(1715351640000/30000)),
				model.NewQueryResultCol("aggr__2__count", 1),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__key_0", int64(1715351730000/30000)),
				model.NewQueryResultCol("aggr__2__count", 1),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT toInt64(toUnixTimestamp64Milli("timestamp") / 30000) AS "aggr__2__key_0",
			  count(*) AS "aggr__2__count"
			FROM __quesma_table_name
			WHERE ("timestamp">=fromUnixTimestamp64Milli(1715351342900) AND "timestamp"<=
			  fromUnixTimestamp64Milli(1715352242900))
			GROUP BY toInt64(toUnixTimestamp64Milli("timestamp") / 30000) AS
			  "aggr__2__key_0"
			ORDER BY "aggr__2__key_0" ASC`,
	},
	{ // [27]
		TestName: "simple date_histogram, but min_doc_count: 0",
		QueryRequestJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"0": {
					"aggs": {
						"2": {
							"terms": {
								"field": "message",
								"size": 4
							}
						}
					},
					"histogram": {
						"extended_bounds": {
							"max": 10000,
							"min": 0
						},
						"field": "rspContentLen",
						"interval": 2000,
						"min_doc_count": 0
					}
				}
			},
			"docvalue_fields": [
				{
					"field": "@timestamp",
					"format": "date_time"
				},
				{
					"field": "timestamp",
					"format": "date_time"
				},
				{
					"field": "utc_time",
					"format": "date_time"
				}
			],
			"query": {
				"bool": {
					"filter": [],
					"must": [
						{
							"match_all": {}
						}
					],
					"must_not": [],
					"should": []
				}
			},
			"script_fields": {
				"hour_of_day": {
					"script": {
						"lang": "painless",
						"source": "doc['timestamp'].value.getHour()"
					}
				}
			},
			"size": 0,
			"stored_fields": [
				"*"
			]
		}`,
		ExpectedResponse: `
		{
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
							"doc_count": 3,
							"key": 0.0,
							"2": {
								"buckets": [
									{
										"doc_count": 2,
										"key": "a"
									},
									{
										"doc_count": 1,
										"key": "b"
									}
								],
								"sum_other_doc_count": 0
							}
						},
						{
							"doc_count": 0,
							"key": 2000.0
						},
						{
							"doc_count": 1,
							"key": 4000.0,
							"2": {
								"buckets": [
									{
										"doc_count": 1,
										"key": "c"
									}
								],
								"sum_other_doc_count": 0
							}
						}
					]
				}
			},
			"hits": {
				"hits": [],
				"max_score": null,
				"total": {
					"relation": "eq",
					"value": 6
				}
			},
			"timed_out": false,
			"took": 10
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__key_0", 0.0),
				model.NewQueryResultCol("aggr__0__count", 3),
				model.NewQueryResultCol("aggr__0__2__parent_count", 3),
				model.NewQueryResultCol("aggr__0__2__key_0", "a"),
				model.NewQueryResultCol("aggr__0__2__count", int64(2)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__key_0", 0.0),
				model.NewQueryResultCol("aggr__0__count", 3),
				model.NewQueryResultCol("aggr__0__2__parent_count", 3),
				model.NewQueryResultCol("aggr__0__2__key_0", "b"),
				model.NewQueryResultCol("aggr__0__2__count", int64(1)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__key_0", 4000.0),
				model.NewQueryResultCol("aggr__0__count", 1),
				model.NewQueryResultCol("aggr__0__2__parent_count", 1),
				model.NewQueryResultCol("aggr__0__2__key_0", "c"),
				model.NewQueryResultCol("aggr__0__2__count", int64(1)),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT "aggr__0__key_0", "aggr__0__count", "aggr__0__2__parent_count",
			  "aggr__0__2__key_0", "aggr__0__2__count"
			FROM (
			  SELECT "aggr__0__key_0", "aggr__0__count", "aggr__0__2__parent_count",
				"aggr__0__2__key_0", "aggr__0__2__count",
				dense_rank() OVER (ORDER BY "aggr__0__key_0" ASC) AS "aggr__0__order_1_rank"
				,
				dense_rank() OVER (PARTITION BY "aggr__0__key_0" ORDER BY
				"aggr__0__2__count" DESC, "aggr__0__2__key_0" ASC) AS
				"aggr__0__2__order_1_rank"
			  FROM (
				SELECT floor("rspContentLen"/2000)*2000 AS "aggr__0__key_0",
				  sum(count(*)) OVER (PARTITION BY "aggr__0__key_0") AS "aggr__0__count",
				  sum(count(*)) OVER (PARTITION BY "aggr__0__key_0") AS
				  "aggr__0__2__parent_count", "message" AS "aggr__0__2__key_0",
				  count(*) AS "aggr__0__2__count"
				FROM ` + TableName + `
				GROUP BY floor("rspContentLen"/2000)*2000 AS "aggr__0__key_0",
				  "message" AS "aggr__0__2__key_0"))
			WHERE "aggr__0__2__order_1_rank"<=5
			ORDER BY "aggr__0__order_1_rank" ASC, "aggr__0__2__order_1_rank" ASC`,
	},
	{ // [28]
		TestName: "Terms, completely different tree results from 2 queries - merging them didn't work before",
		QueryRequestJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"0": {
					"aggs": {
						"1-bucket": {
							"filter": {
								"bool": {
									"filter": [
										{
											"bool": {
												"minimum_should_match": 1,
												"should": [
													{
														"match": {
															"FlightDelay": true
														}
													}
												]
											}
										}
									],
									"must": [],
									"must_not": [],
									"should": []
								}
							}
						},
						"3-bucket": {
							"filter": {
								"bool": {
									"filter": [
										{
											"bool": {
												"minimum_should_match": 1,
												"should": [
													{
														"match": {
															"Cancelled": true
														}
													}
												]
											}
										}
									],
									"must": [],
									"must_not": [],
									"should": []
								}
							}
						}
					},
					"terms": {
						"field": "OriginCityName",
						"order": {
							"_key": "asc"
						},
						"size": 1000
					}
				}
			},
			"fields": [
				{
					"field": "@timestamp",
					"format": "date_time"
				},
				{
					"field": "timestamp",
					"format": "date_time"
				}
			],
			"query": {
				"bool": {
					"filter": [],
					"must": [],
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
			"script_fields": {},
			"size": 0,
			"stored_fields": [
				"*"
			],
			"track_total_hits": false
		}`,
		ExpectedResponse: `
		{
			"is_partial": false,
			"is_running": false,
			"start_time_in_millis": 1711785625800,
			"expiration_time_in_millis": 1712217625800,
			"completion_time_in_millis": 1711785625803,
			"response": {
				"took": 3,
				"timed_out": false,
				"_shards": {
					"total": 1,
					"successful": 1,
					"skipped": 0,
					"failed": 0
				},
				"hits": {
					"total": {
						"value": 2167,
						"relation": "eq"
					},
					"max_score": null,
					"hits": []
				},
				"aggregations": {
					"0": {
						"doc_count_error_upper_bound": 0,
						"sum_other_doc_count": 0,
						"buckets": [
							{
								"key": "Albuquerque",
								"doc_count": 4,
								"3-bucket": {
									"doc_count": 2
								},
								"1-bucket": {
									"doc_count": 1
								}
							},
							{
								"key": "Atlanta",
								"doc_count": 5,
								"3-bucket": {
									"doc_count": 0
								},
								"1-bucket": {
									"doc_count": 0
								}
							},
							{
								"key": "Baltimore",
								"doc_count": 5,
								"3-bucket": {
									"doc_count": 0
								},
								"1-bucket": {
									"doc_count": 2
								}
							}
						]
					}
				}
			}
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__parent_count", uint64(14)),
				model.NewQueryResultCol("aggr__0__key_0", "Albuquerque"),
				model.NewQueryResultCol("aggr__0__count", uint64(4)),
				model.NewQueryResultCol("aggr__0__1-bucket__count", uint64(1)),
				model.NewQueryResultCol("metric__0__3-bucket_col_0", uint64(2)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__parent_count", uint64(14)),
				model.NewQueryResultCol("aggr__0__key_0", "Atlanta"),
				model.NewQueryResultCol("aggr__0__count", uint64(5)),
				model.NewQueryResultCol("aggr__0__1-bucket__count", uint64(0)),
				model.NewQueryResultCol("metric__0__3-bucket_col_0", uint64(0)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__parent_count", uint64(14)),
				model.NewQueryResultCol("aggr__0__key_0", "Baltimore"),
				model.NewQueryResultCol("aggr__0__count", uint64(5)),
				model.NewQueryResultCol("aggr__0__1-bucket__count", uint64(2)),
				model.NewQueryResultCol("metric__0__3-bucket_col_0", uint64(0)),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT sum(count(*)) OVER () AS "aggr__0__parent_count",
			  "OriginCityName" AS "aggr__0__key_0", count(*) AS "aggr__0__count",
			  countIf("Cancelled"==true) AS "metric__0__3-bucket_col_0",
			  countIf("FlightDelay"==true) AS "aggr__0__1-bucket__count"
			FROM ` + TableName + `
			GROUP BY "OriginCityName" AS "aggr__0__key_0"
			ORDER BY "aggr__0__key_0" ASC
			LIMIT 1001`,
	},
	{ // [29]
		TestName: "Terms, completely different tree results from 2 queries - merging them didn't work before (logs) TODO add results",
		QueryRequestJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"3": {
					"aggs": {
						"1": {
							"sum": {
								"field": "memory"
							}
						},
						"2": {
							"aggs": {
								"1": {
									"sum": {
										"field": "memory"
									}
								}
							},
							"terms": {
								"field": "machine.os.keyword",
								"order": {
									"1": "desc"
								},
								"size": 5
							}
						}
					},
					"terms": {
						"field": "geo.src",
						"order": {
							"1": "desc"
						},
						"size": 5
					}
				}
			},
			"size": 0,
			"stored_fields": [
				"*"
			]
		}`,
		ExpectedResponse: `
		{
			"aggregations": {
				"3": {
					"buckets": [
						{
							"1": {
								"value": 79725689
							},
							"2": {
								"buckets": [
									{
										"1": {
											"value": 16537711
										},
										"doc_count": 2885,
										"key": "win xp"
									},
									{
										"1": {
											"value": 3
										},
										"doc_count": 2,
										"key": "win xd"
									}
								],
								"doc_count_error_upper_bound": 0,
								"sum_other_doc_count": 11187
							},
							"doc_count": 14074,
							"key": "US"
						},
						{
							"key": "PL",
							"doc_count": 1410,
							"1": {
								"value": null
							}
						},
						{
							"1": {
								"value": 1.1
							},
							"2": {
								"buckets": [
									{
										"1": {
											"value": 2.2
										},
										"doc_count": 28,
										"key": "win xp"
									}
								],
								"doc_count_error_upper_bound": 0,
								"sum_other_doc_count": 1
							},
							"doc_count": 29,
							"key": "DE"
						}
					],
					"doc_count_error_upper_bound": 0,
					"sum_other_doc_count": 44487
				}
			}
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__3__parent_count", uint64(60000)),
				model.NewQueryResultCol("aggr__3__key_0", "US"),
				model.NewQueryResultCol("aggr__3__count", uint64(14074)),
				model.NewQueryResultCol("metric__3__1_col_0", 79725689),
				model.NewQueryResultCol("aggr__3__2__parent_count", uint64(14074)),
				model.NewQueryResultCol("aggr__3__2__key_0", "win xp"),
				model.NewQueryResultCol("aggr__3__2__count", uint64(2885)),
				model.NewQueryResultCol("metric__3__2__1_col_0", 16537711),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__3__parent_count", uint64(60000)),
				model.NewQueryResultCol("aggr__3__key_0", "US"),
				model.NewQueryResultCol("aggr__3__count", uint64(14074)),
				model.NewQueryResultCol("metric__3__1_col_0", 79725689),
				model.NewQueryResultCol("aggr__3__2__parent_count", uint64(14074)),
				model.NewQueryResultCol("aggr__3__2__key_0", "win xd"),
				model.NewQueryResultCol("aggr__3__2__count", uint64(2)),
				model.NewQueryResultCol("metric__3__2__1_col_0", 3),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__3__parent_count", uint64(60000)),
				model.NewQueryResultCol("aggr__3__key_0", "PL"),
				model.NewQueryResultCol("aggr__3__count", uint64(1410)),
				model.NewQueryResultCol("metric__3__1_col_0", nil),
				model.NewQueryResultCol("aggr__3__2__parent_count", uint64(1410)),
				model.NewQueryResultCol("aggr__3__2__key_0", nil),
				model.NewQueryResultCol("aggr__3__2__count", uint64(2)),
				model.NewQueryResultCol("metric__3__2__1_col_0", nil),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__3__parent_count", uint64(60000)),
				model.NewQueryResultCol("aggr__3__key_0", "DE"),
				model.NewQueryResultCol("aggr__3__count", uint64(29)),
				model.NewQueryResultCol("metric__3__1_col_0", 1.1),
				model.NewQueryResultCol("aggr__3__2__parent_count", uint64(29)),
				model.NewQueryResultCol("aggr__3__2__key_0", "win xp"),
				model.NewQueryResultCol("aggr__3__2__count", uint64(28)),
				model.NewQueryResultCol("metric__3__2__1_col_0", 2.2),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__3__parent_count", uint64(60000)),
				model.NewQueryResultCol("aggr__3__key_0", "DE"),
				model.NewQueryResultCol("aggr__3__count", uint64(29)),
				model.NewQueryResultCol("metric__3__1_col_0", 1.1),
				model.NewQueryResultCol("aggr__3__2__parent_count", uint64(29)),
				model.NewQueryResultCol("aggr__3__2__key_0", nil),
				model.NewQueryResultCol("aggr__3__2__count", uint64(1)),
				model.NewQueryResultCol("metric__3__2__1_col_0", 2),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT "aggr__3__parent_count", "aggr__3__key_0", "aggr__3__count",
			  "metric__3__1_col_0", "aggr__3__2__parent_count", "aggr__3__2__key_0",
			  "aggr__3__2__count", "metric__3__2__1_col_0"
			FROM (
			  SELECT "aggr__3__parent_count", "aggr__3__key_0", "aggr__3__count",
				"metric__3__1_col_0", "aggr__3__2__parent_count", "aggr__3__2__key_0",
				"aggr__3__2__count", "metric__3__2__1_col_0",
				dense_rank() OVER (ORDER BY "metric__3__1_col_0" DESC, "aggr__3__key_0" ASC)
				AS "aggr__3__order_1_rank",
				dense_rank() OVER (PARTITION BY "aggr__3__key_0" ORDER BY
				"metric__3__2__1_col_0" DESC, "aggr__3__2__key_0" ASC) AS
				"aggr__3__2__order_1_rank"
			  FROM (
				SELECT sum(count(*)) OVER () AS "aggr__3__parent_count",
				  "geo.src" AS "aggr__3__key_0",
				  sum(count(*)) OVER (PARTITION BY "aggr__3__key_0") AS "aggr__3__count",
				  sumOrNull(sumOrNull("memory")) OVER (PARTITION BY "aggr__3__key_0") AS
				  "metric__3__1_col_0",
				  sum(count(*)) OVER (PARTITION BY "aggr__3__key_0") AS
				  "aggr__3__2__parent_count", "machine.os" AS "aggr__3__2__key_0",
				  count(*) AS "aggr__3__2__count",
				  sumOrNull("memory") AS "metric__3__2__1_col_0"
				FROM __quesma_table_name
				GROUP BY "geo.src" AS "aggr__3__key_0", "machine.os" AS "aggr__3__2__key_0"))
			WHERE ("aggr__3__order_1_rank"<=6 AND "aggr__3__2__order_1_rank"<=6)
			ORDER BY "aggr__3__order_1_rank" ASC, "aggr__3__2__order_1_rank" ASC`,
	},
	{ // [30]
		TestName: "Terms, completely different tree results from 2 queries - merging them didn't work before (logs). what when cardinality = 0?",
		QueryRequestJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"2": {
					"aggs": {
						"1": {
							"cardinality": {
								"field": "clientip"
							}
						}
					},
					"terms": {
						"field": "machine.os.keyword",
						"order": {
							"1": "desc"
						},
						"size": 5
					}
				}
			},
			"docvalue_fields": [
				{
					"field": "@timestamp",
					"format": "date_time"
				},
				{
					"field": "timestamp",
					"format": "date_time"
				},
				{
					"field": "utc_time",
					"format": "date_time"
				}
			],
			"query": {
				"bool": {
					"filter": [
						{
							"match_all": {}
						},
						{
							"range": {
								"timestamp": {
									"format": "strict_date_optional_time",
									"gte": "2024-05-10T06:22:39.037Z",
									"lte": "2024-05-10T21:22:39.037Z"
								}
							}
						}
					],
					"must": [],
					"must_not": [],
					"should": []
				}
			},
			"script_fields": {
				"hour_of_day": {
					"script": {
						"lang": "painless",
						"source": "doc['timestamp'].value.getHour()"
					}
				}
			},
			"size": 0,
			"stored_fields": [
				"*"
			]
		}`,
		ExpectedResponse: `
		{
			"is_partial": false,
			"is_running": false,
			"start_time_in_millis": 1711785625800,
			"expiration_time_in_millis": 1712217625800,
			"completion_time_in_millis": 1711785625803,
			"response": {
				"took": 3,
				"timed_out": false,
				"_shards": {
					"total": 1,
					"successful": 1,
					"skipped": 0,
					"failed": 0
				},
				"hits": {
					"total": {
						"value": 2167,
						"relation": "eq"
					},
					"max_score": null,
					"hits": []
				},
				"aggregations": {
					"2": {
						"doc_count_error_upper_bound": 0,
						"sum_other_doc_count": 0,
						"buckets": [
							{
								"key": "Albuquerque",
								"doc_count": 4,
								"1": {
									"value": 2
								}
							},
							{
								"key": "Atlanta",
								"doc_count": 5,
								"1": {
									"value": 0
								}
							},
							{
								"key": "Baltimore",
								"doc_count": 5,
								"1": {
									"value": 0
								}
							}
						]
					}
				}
			}
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", 14),
				model.NewQueryResultCol("aggr__2__key_0", "Albuquerque"),
				model.NewQueryResultCol("aggr__2__count", int64(4)),
				model.NewQueryResultCol("metric__2__1_col_0", int64(2)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", 14),
				model.NewQueryResultCol("aggr__2__key_0", "Atlanta"),
				model.NewQueryResultCol("aggr__2__count", int64(5)),
				model.NewQueryResultCol("metric__2__1_col_0", int64(0)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", 14),
				model.NewQueryResultCol("aggr__2__key_0", "Baltimore"),
				model.NewQueryResultCol("aggr__2__count", int64(5)),
				model.NewQueryResultCol("metric__2__1_col_0", int64(0)),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT sum(count(*)) OVER () AS "aggr__2__parent_count",
			  "machine.os" AS "aggr__2__key_0", count(*) AS "aggr__2__count",
			  uniq("clientip") AS "metric__2__1_col_0"
			FROM __quesma_table_name
			WHERE ("timestamp">=fromUnixTimestamp64Milli(1715322159037) AND "timestamp"<=fromUnixTimestamp64Milli(1715376159037))
			GROUP BY "machine.os" AS "aggr__2__key_0"
			ORDER BY "metric__2__1_col_0" DESC, "aggr__2__key_0" ASC
			LIMIT 6`,
	},
	// terms + histogram
	// histogram + terms
	// everything with some avg, cardinality, etc
	{ // [31]
		TestName: "Kibana Visualize -> Last Value. Used to panic",
		QueryRequestJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"0": {
					"aggs": {
						"1-bucket": {
							"aggs": {
								"1-metric": {
									"top_metrics": {
										"metrics": {
											"field": "message"
										},
										"size": 1,
										"sort": {
											"order_date": "desc"
										}
									}
								}
							},
							"filter": {
								"bool": {
									"filter": [
										{
											"bool": {
												"minimum_should_match": 1,
												"should": [
													{
														"exists": {
															"field": "message"
														}
													}
												]
											}
										}
									],
									"must": [],
									"must_not": [],
									"should": []
								}
							}
						}
					},
					"date_histogram": {
						"calendar_interval": "1d",
						"field": "@timestamp",
						"min_doc_count": 1
					}
				}
			},
			"fields": [
				{
					"field": "@timestamp",
					"format": "date_time"
				},
				{
					"field": "order_date",
					"format": "date_time"
				}
			],
			"query": {
				"bool": {
					"filter": [],
					"must": [],
					"must_not": [],
					"should": []
				}
			},
			"runtime_mappings": {},
			"script_fields": {},
			"size": 0,
			"stored_fields": [
				"*"
			],
			"track_total_hits": true
		}`,
		ExpectedResponse: `
		{
			"completion_status": 200,
			"completion_time_in_millis": 0,
			"expiration_time_in_millis": 0,
			"id": "quesma_async_17",
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
								"1-bucket": {
									"1-metric": {
										"top": [
											{
												"metrics": {
													"message": 5
												},
												"sort": [
													"2024-05-09T23:52:48Z"
												]
											}
										]
									},
									"doc_count": 146
								},
								"doc_count": 146,
								"key": 1715212800000,
								"key_as_string": "2024-05-09T00:00:00.000"
							},
							{
								"1-bucket": {
									"1-metric": {
										"top": [
											{
												"metrics": {
													"message": 30
												},
												"sort": [
													"2024-05-22T10:20:38Z"
												]
											}
										]
									},
									"doc_count": 58
								},
								"doc_count": 58,
								"key": 1716336000000,
								"key_as_string": "2024-05-22T00:00:00.000"
							}
						]
					}
				},
				"hits": {
					"hits": [],
					"max_score": null,
					"total": {
						"relation": "eq",
						"value": 1974
					}
				},
				"timed_out": false,
				"took": 0
			},
			"start_time_in_millis": 0
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__key_0", int64(1715212800000/86400000)),
				model.NewQueryResultCol("aggr__0__count", 146),
				model.NewQueryResultCol("aggr__0__1-bucket__count", 146),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__key_0", int64(1716336000000/86400000)),
				model.NewQueryResultCol("aggr__0__count", 58),
				model.NewQueryResultCol("aggr__0__1-bucket__count", 58),
			}},
		},
		ExpectedAdditionalPancakeResults: [][]model.QueryResultRow{
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("aggr__0__key_0", int64(1715212800000/86400000)),
					model.NewQueryResultCol("aggr__0__count", 146),
					model.NewQueryResultCol("aggr__0__1-bucket__count", 146),
					model.NewQueryResultCol("top_metrics__0__1-bucket__1-metric_col_0", 5),
					model.NewQueryResultCol("top_metrics__0__1-bucket__1-metric_col_1", "2024-05-09T23:52:48Z"),
					model.NewQueryResultCol("top_hits_rank", 1),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("aggr__0__key_0", int64(1716336000000/86400000)),
					model.NewQueryResultCol("aggr__0__count", 58),
					model.NewQueryResultCol("aggr__0__1-bucket__count", 58),
					model.NewQueryResultCol("top_metrics__0__1-bucket__1-metric_col_0", 30),
					model.NewQueryResultCol("top_metrics__0__1-bucket__1-metric_col_1", "2024-05-22T10:20:38Z"),
					model.NewQueryResultCol("top_hits_rank", 1),
				}},
			},
		},
		ExpectedPancakeSQL: `
			SELECT toInt64(toUnixTimestamp64Milli("@timestamp") / 86400000) AS
			  "aggr__0__key_0", count(*) AS "aggr__0__count",
			  countIf("message" IS NOT NULL) AS "aggr__0__1-bucket__count"
			FROM __quesma_table_name
			GROUP BY toInt64(toUnixTimestamp64Milli("@timestamp") / 86400000) AS
			  "aggr__0__key_0"
			ORDER BY "aggr__0__key_0" ASC`,
		ExpectedAdditionalPancakeSQLs: []string{`
			WITH quesma_top_hits_group_table AS (
			  SELECT toInt64(toUnixTimestamp64Milli("@timestamp") / 86400000) AS
				"aggr__0__key_0", count(*) AS "aggr__0__count",
				countIf("message" IS NOT NULL) AS "aggr__0__1-bucket__count"
			  FROM __quesma_table_name
			  GROUP BY toInt64(toUnixTimestamp64Milli("@timestamp") / 86400000) AS
				"aggr__0__key_0"
			  ORDER BY "aggr__0__key_0" ASC) ,
			quesma_top_hits_join AS (
			  SELECT "group_table"."aggr__0__key_0" AS "aggr__0__key_0",
				"group_table"."aggr__0__count" AS "aggr__0__count",
				"group_table"."aggr__0__1-bucket__count" AS "aggr__0__1-bucket__count",
				"hit_table"."message" AS "top_metrics__0__1-bucket__1-metric_col_0",
				"hit_table"."order_date" AS "top_metrics__0__1-bucket__1-metric_col_1",
				ROW_NUMBER() OVER (PARTITION BY "group_table"."aggr__0__key_0" ORDER BY
				"order_date" DESC) AS "top_hits_rank"
			  FROM quesma_top_hits_group_table AS "group_table" LEFT OUTER JOIN
				__quesma_table_name AS "hit_table" ON ("group_table"."aggr__0__key_0"=
				toInt64(toUnixTimestamp64Milli("@timestamp") / 86400000))
			  WHERE "message" IS NOT NULL)
			SELECT "aggr__0__key_0", "aggr__0__count", "aggr__0__1-bucket__count",
			  "top_metrics__0__1-bucket__1-metric_col_0",
			  "top_metrics__0__1-bucket__1-metric_col_1", "top_hits_rank"
			FROM "quesma_top_hits_join"
			WHERE "top_hits_rank"<=1
			ORDER BY "aggr__0__key_0" ASC, "top_hits_rank" ASC`},
	},
	{ // [32]
		TestName: "Standard deviation",
		QueryRequestJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"0": {
					"aggs": {
						"1": {
							"extended_stats": {
								"field": "bytes"
							}
						},
						"2": {
							"extended_stats": {
								"field": "bytes",
								"sigma": 3.0
							}
						}
					},
					"date_histogram": {
						"field": "timestamp",
						"fixed_interval": "10m",
						"min_doc_count": 1,
						"time_zone": "Europe/Warsaw"
					}
				}
			},
			"fields": [
				{
					"field": "@timestamp",
					"format": "date_time"
				},
				{
					"field": "timestamp",
					"format": "date_time"
				},
				{
					"field": "utc_time",
					"format": "date_time"
				}
			],
			"query": {
				"bool": {
					"filter": [
						{
							"range": {
								"timestamp": {
									"format": "strict_date_optional_time",
									"gte": "2024-05-21T21:35:34.210Z",
									"lte": "2024-05-22T12:35:34.210Z"
								}
							}
						}
					],
					"must": [],
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
			"script_fields": {},
			"size": 0,
			"stored_fields": [
				"*"
			],
			"track_total_hits": true
		}`,
		ExpectedResponse: `
		{
			"completion_time_in_millis": 1716381334370,
			"expiration_time_in_millis": 1716381394365,
			"id": "FkdNb3pESzBlUTEyZXB2UHRrZnRKR0EbOVkyZEhiNFZSaGlYbm9WaHlXdm9Xdzo3MDQ5",
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
								"1": {
									"avg": 7676.0,
									"count": 1,
									"max": 7676.0,
									"min": 7676.0,
									"std_deviation": 0.0,
									"std_deviation_bounds": {
										"lower": 7676.0,
										"lower_population": 7676.0,
										"lower_sampling": "NaN",
										"upper": 7676.0,
										"upper_population": 7676.0,
										"upper_sampling": "NaN"
									},
									"std_deviation_population": 0.0,
									"std_deviation_sampling": "NaN",
									"sum": 7676.0,
									"sum_of_squares": 58920976.0,
									"variance": 0.0,
									"variance_population": 0.0,
									"variance_sampling": "NaN"
								},
								"2": {
									"avg": 7676.0,
									"count": 1,
									"max": 7676.0,
									"min": 7676.0,
									"std_deviation": 0.0,
									"std_deviation_bounds": {
										"lower": 7676.0,
										"lower_population": 7676.0,
										"lower_sampling": "NaN",
										"upper": 7676.0,
										"upper_population": 7676.0,
										"upper_sampling": "NaN"
									},
									"std_deviation_population": 0.0,
									"std_deviation_sampling": "NaN",
									"sum": 7676.0,
									"sum_of_squares": 58920976.0,
									"variance": 0.0,
									"variance_population": 0.0,
									"variance_sampling": "NaN"
								},
								"doc_count": 1,
								"key": 1716326400000,
								"key_as_string": "2024-05-21T21:20:00.000"
							},
							{	
								"1": {
									"avg": 5754.375,
									"count": 8,
									"max": 7708.0,
									"min": 2426.0,
									"std_deviation": 1580.8501618986538,
									"std_deviation_bounds": {
										"lower": 2592.6746762026924,
										"lower_population": 2592.6746762026924,
										"lower_sampling": 2374.375179628069,
										"upper": 8916.075323797308,
										"upper_population": 8916.075323797308,
										"upper_sampling": 9134.374820371931
									},
									"std_deviation_population": 1580.8501618986538,
									"std_deviation_sampling": 1689.9999101859655,
									"sum": 46035.0,
									"sum_of_squares": 284895351.0,
									"variance": 2499087.234375,
									"variance_population": 2499087.234375,
									"variance_sampling": 2856099.6964285714
								},
								"2": {
									"avg": 5754.375,
									"count": 8,
									"max": 7708.0,
									"min": 2426.0,
									"std_deviation": 1580.8501618986538,
									"std_deviation_bounds": {
										"lower": 1011.8245143040385,
										"lower_population": 1011.8245143040385,
										"lower_sampling": 684.375269442103,
										"upper": 10496.925485695961,
										"upper_population": 10496.925485695961,
										"upper_sampling": 10824.374730557896
									},
									"std_deviation_population": 1580.8501618986538,
									"std_deviation_sampling": 1689.9999101859655,
									"sum": 46035.0,
									"sum_of_squares": 284895351.0,
									"variance": 2499087.234375,
									"variance_population": 2499087.234375,
									"variance_sampling": 2856099.6964285714
								},
								"doc_count": 8,
								"key": 1716370200000,
								"key_as_string": "2024-05-22T09:30:00.000"
							}
						]
					}
				},
				"hits": {
					"hits": [],
					"max_score": null,
					"total": {
						"relation": "eq",
						"value": 122
					}
				},
				"timed_out": false,
				"took": 5
			},
			"start_time_in_millis": 1716381334365
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__key_0", int64(1716333600000/600000)),
				model.NewQueryResultCol("aggr__0__count", 1),
				model.NewQueryResultCol("metric__0__1_col_0", 1),
				model.NewQueryResultCol("metric__0__1_col_1", 7676.0),
				model.NewQueryResultCol("metric__0__1_col_2", 7676.0),
				model.NewQueryResultCol("metric__0__1_col_3", 7676.0),
				model.NewQueryResultCol("metric__0__1_col_4", 7676.0),
				model.NewQueryResultCol("metric__0__1_col_5", 58920976.0),
				model.NewQueryResultCol("metric__0__1_col_6", 0.0),
				model.NewQueryResultCol("metric__0__1_col_7", math.NaN()),
				model.NewQueryResultCol("metric__0__1_col_8", 0.0),
				model.NewQueryResultCol("metric__0__1_col_9", math.NaN()),
				model.NewQueryResultCol("metric__0__2_col_0", 1),
				model.NewQueryResultCol("metric__0__2_col_1", 7676.0),
				model.NewQueryResultCol("metric__0__2_col_2", 7676.0),
				model.NewQueryResultCol("metric__0__2_col_3", 7676.0),
				model.NewQueryResultCol("metric__0__2_col_4", 7676.0),
				model.NewQueryResultCol("metric__0__2_col_5", 58920976.0),
				model.NewQueryResultCol("metric__0__2_col_6", 0.0),
				model.NewQueryResultCol("metric__0__2_col_7", nil),
				model.NewQueryResultCol("metric__0__2_col_8", 0.0),
				model.NewQueryResultCol("metric__0__2_col_9", nil),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__key_0", int64(1716377400000/600000)),
				model.NewQueryResultCol("aggr__0__count", 8),
				model.NewQueryResultCol("metric__0__1_col_0", 8),
				model.NewQueryResultCol("metric__0__1_col_1", 2426.0),
				model.NewQueryResultCol("metric__0__1_col_2", 7708.0),
				model.NewQueryResultCol("metric__0__1_col_3", 5754.375),
				model.NewQueryResultCol("metric__0__1_col_4", 46035.0),
				model.NewQueryResultCol("metric__0__1_col_5", 284895351.0),
				model.NewQueryResultCol("metric__0__1_col_6", 2499087.234375),
				model.NewQueryResultCol("metric__0__1_col_7", 2856099.6964285714),
				model.NewQueryResultCol("metric__0__1_col_8", 1580.8501618986538),
				model.NewQueryResultCol("metric__0__1_col_9", 1689.9999101859655),
				model.NewQueryResultCol("metric__0__2_col_0", 8),
				model.NewQueryResultCol("metric__0__2_col_1", 2426.0),
				model.NewQueryResultCol("metric__0__2_col_2", 7708.0),
				model.NewQueryResultCol("metric__0__2_col_3", 5754.375),
				model.NewQueryResultCol("metric__0__2_col_4", 46035.0),
				model.NewQueryResultCol("metric__0__2_col_5", 284895351.0),
				model.NewQueryResultCol("metric__0__2_col_6", 2499087.234375),
				model.NewQueryResultCol("metric__0__2_col_7", 2856099.6964285714),
				model.NewQueryResultCol("metric__0__2_col_8", 1580.8501618986538),
				model.NewQueryResultCol("metric__0__2_col_9", 1689.9999101859655),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT toInt64((toUnixTimestamp64Milli("timestamp")+timeZoneOffset(toTimezone(
			  "timestamp", 'Europe/Warsaw'))*1000) / 600000) AS "aggr__0__key_0",
			  count(*) AS "aggr__0__count", count("bytes") AS "metric__0__1_col_0",
			  minOrNull("bytes") AS "metric__0__1_col_1",
			  maxOrNull("bytes") AS "metric__0__1_col_2",
			  avgOrNull("bytes") AS "metric__0__1_col_3",
			  sumOrNull("bytes") AS "metric__0__1_col_4",
			  sumOrNull("bytes"*"bytes") AS "metric__0__1_col_5",
			  varPop("bytes") AS "metric__0__1_col_6",
			  varSamp("bytes") AS "metric__0__1_col_7",
			  stddevPop("bytes") AS "metric__0__1_col_8",
			  stddevSamp("bytes") AS "metric__0__1_col_9",
			  count("bytes") AS "metric__0__2_col_0",
			  minOrNull("bytes") AS "metric__0__2_col_1",
			  maxOrNull("bytes") AS "metric__0__2_col_2",
			  avgOrNull("bytes") AS "metric__0__2_col_3",
			  sumOrNull("bytes") AS "metric__0__2_col_4",
			  sumOrNull("bytes"*"bytes") AS "metric__0__2_col_5",
			  varPop("bytes") AS "metric__0__2_col_6",
			  varSamp("bytes") AS "metric__0__2_col_7",
			  stddevPop("bytes") AS "metric__0__2_col_8",
			  stddevSamp("bytes") AS "metric__0__2_col_9"
			FROM __quesma_table_name
			WHERE ("timestamp">=fromUnixTimestamp64Milli(1716327334210) AND "timestamp"<=
			  fromUnixTimestamp64Milli(1716381334210))
			GROUP BY toInt64((toUnixTimestamp64Milli("timestamp")+timeZoneOffset(toTimezone(
			  "timestamp", 'Europe/Warsaw'))*1000) / 600000) AS "aggr__0__key_0"
			ORDER BY "aggr__0__key_0" ASC`,
	},
	{ // [33]
		TestName: "0 result rows in 2x terms",
		QueryRequestJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"0": {
					"aggs": {
						"1": {
							"terms": {
								"field": "message",
								"order": {
			  						"_count": "desc"
								},
								"shard_size": 25,
								"size": 3
							}
						}
					},
					"terms": {
						"field": "host.name",
						"order": {
							"_count": "desc"
						},
						"shard_size": 25,
						"size": 10
					}
				}
			},
			"fields": [
				{
					"field": "@timestamp",
					"format": "date_time"
				},
				{
					"field": "reqTimeSec",
					"format": "date_time"
				}
			],
			"query": {
				"bool": {
					"filter": [
						{
							"exists": {
								"field": "message"
							}
						}
					],
					"must": [],
					"must_not": [
						{
							"match_phrase": {
								"message": "US"
							}
						}
					],
					"should": []
				}
			},
			"runtime_mappings": {},
			"script_fields": {},
			"size": 0,
			"stored_fields": [
				"*"
			],
			"track_total_hits": true
		}`,
		ExpectedResponse:       `{"response": {"aggregations":{}}}`,
		ExpectedPancakeResults: make([]model.QueryResultRow, 0),
		ExpectedPancakeSQL: `
			SELECT "aggr__0__parent_count", "aggr__0__key_0", "aggr__0__count",
			  "aggr__0__1__parent_count", "aggr__0__1__key_0", "aggr__0__1__count"
			FROM (
			  SELECT "aggr__0__parent_count", "aggr__0__key_0", "aggr__0__count",
				"aggr__0__1__parent_count", "aggr__0__1__key_0", "aggr__0__1__count",
				dense_rank() OVER (ORDER BY "aggr__0__count" DESC, "aggr__0__key_0" ASC) AS
				"aggr__0__order_1_rank",
				dense_rank() OVER (PARTITION BY "aggr__0__key_0" ORDER BY
				"aggr__0__1__count" DESC, "aggr__0__1__key_0" ASC) AS
				"aggr__0__1__order_1_rank"
			  FROM (
				SELECT sum(count(*)) OVER () AS "aggr__0__parent_count",
				  "host.name" AS "aggr__0__key_0",
				  sum(count(*)) OVER (PARTITION BY "aggr__0__key_0") AS "aggr__0__count",
				  sum(count(*)) OVER (PARTITION BY "aggr__0__key_0") AS
				  "aggr__0__1__parent_count", "message" AS "aggr__0__1__key_0",
				  count(*) AS "aggr__0__1__count"
				FROM __quesma_table_name
				WHERE ("message" IS NOT NULL AND NOT (("message" __quesma_match 'US')))
				GROUP BY "host.name" AS "aggr__0__key_0", "message" AS "aggr__0__1__key_0"))
			WHERE ("aggr__0__order_1_rank"<=11 AND "aggr__0__1__order_1_rank"<=4)
			ORDER BY "aggr__0__order_1_rank" ASC, "aggr__0__1__order_1_rank" ASC`,
	},
	{ // [34]
		TestName: "0 result rows in 3x terms",
		QueryRequestJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"0": {
					"aggs": {
						"1": {
							"terms": {
								"field": "message",
								"order": {
			  						"_count": "desc"
								},
								"shard_size": 25,
								"size": 3
							},
							"aggs": {
								"2": {
									"terms": {
										"field": "message",
										"order": {
											"_count": "desc"
										},
										"shard_size": 25,
										"size": 3
									}
								}
							},
						}
					},
					"terms": {
						"field": "host.name",
						"order": {
							"_count": "desc"
						},
						"shard_size": 25,
						"size": 10
					}
				}
			},
			"fields": [
				{
					"field": "@timestamp",
					"format": "date_time"
				},
				{
					"field": "reqTimeSec",
					"format": "date_time"
				}
			],
			"query": {
				"bool": {
					"filter": [
						{
							"exists": {
								"field": "message"
							}
						}
					],
					"must": [],
					"must_not": [
						{
							"match_phrase": {
								"message": "US"
							}
						}
					],
					"should": []
				}
			},
			"runtime_mappings": {},
			"script_fields": {},
			"size": 0,
			"stored_fields": [
				"*"
			],
			"track_total_hits": false
		}`,
		ExpectedResponse:       `{"response": {"aggregations":{}}}`,
		ExpectedPancakeResults: make([]model.QueryResultRow, 0),
		ExpectedPancakeSQL: `
			SELECT "aggr__0__parent_count", "aggr__0__key_0", "aggr__0__count",
			  "aggr__0__1__parent_count", "aggr__0__1__key_0", "aggr__0__1__count",
			  "aggr__0__1__2__parent_count", "aggr__0__1__2__key_0", "aggr__0__1__2__count"
			FROM (
			  SELECT "aggr__0__parent_count", "aggr__0__key_0", "aggr__0__count",
				"aggr__0__1__parent_count", "aggr__0__1__key_0", "aggr__0__1__count",
				"aggr__0__1__2__parent_count", "aggr__0__1__2__key_0",
				"aggr__0__1__2__count",
				dense_rank() OVER (ORDER BY "aggr__0__count" DESC, "aggr__0__key_0" ASC) AS
				"aggr__0__order_1_rank",
				dense_rank() OVER (PARTITION BY "aggr__0__key_0" ORDER BY
				"aggr__0__1__count" DESC, "aggr__0__1__key_0" ASC) AS
				"aggr__0__1__order_1_rank",
				dense_rank() OVER (PARTITION BY "aggr__0__key_0", "aggr__0__1__key_0" ORDER
				BY "aggr__0__1__2__count" DESC, "aggr__0__1__2__key_0" ASC) AS "aggr__0__1__2__order_1_rank"
			  FROM (
				SELECT sum(count(*)) OVER () AS "aggr__0__parent_count",
				  "host.name" AS "aggr__0__key_0",
				  sum(count(*)) OVER (PARTITION BY "aggr__0__key_0") AS "aggr__0__count",
				  sum(count(*)) OVER (PARTITION BY "aggr__0__key_0") AS
				  "aggr__0__1__parent_count", "message" AS "aggr__0__1__key_0",
				  sum(count(*)) OVER (PARTITION BY "aggr__0__key_0", "aggr__0__1__key_0") AS
				  "aggr__0__1__count",
				  sum(count(*)) OVER (PARTITION BY "aggr__0__key_0", "aggr__0__1__key_0") AS
				  "aggr__0__1__2__parent_count", "message" AS "aggr__0__1__2__key_0",
				  count(*) AS "aggr__0__1__2__count"
				FROM __quesma_table_name
				WHERE ("message" IS NOT NULL AND NOT (("message" __quesma_match 'US')))
				GROUP BY "host.name" AS "aggr__0__key_0", "message" AS "aggr__0__1__key_0",
				  "message" AS "aggr__0__1__2__key_0"))
			WHERE (("aggr__0__order_1_rank"<=11 AND "aggr__0__1__order_1_rank"<=4) AND
			  "aggr__0__1__2__order_1_rank"<=4)
			ORDER BY "aggr__0__order_1_rank" ASC, "aggr__0__1__order_1_rank" ASC,
			  "aggr__0__1__2__order_1_rank" ASC`,
	},
	{ // [35]
		TestName: "0 result rows in terms+histogram",
		QueryRequestJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"0": {
					"aggs": {
						"1": {
							"histogram": {
								"field": "FlightDelayMin",
								"interval": 1,
								"min_doc_count": 1
							}
						}
					},
					"terms": {
						"field": "host.name",
						"order": {
							"_count": "desc"
						},
						"shard_size": 25,
						"size": 8
					}
				}
			},
			"fields": [
				{
					"field": "@timestamp",
					"format": "date_time"
				},
				{
					"field": "reqTimeSec",
					"format": "date_time"
				}
			],
			"query": {
				"bool": {
					"filter": [
						{
							"exists": {
								"field": "message"
							}
						}
					],
					"must": [],
					"must_not": [
						{
							"match_phrase": {
								"message": "US"
							}
						}
					],
					"should": []
				}
			},
			"runtime_mappings": {},
			"script_fields": {},
			"size": 0,
			"stored_fields": [
				"*"
			],
			"track_total_hits": true
		}`,
		ExpectedResponse:       `{"response": {"aggregations":{}}}`,
		ExpectedPancakeResults: make([]model.QueryResultRow, 0),
		ExpectedPancakeSQL: `
			SELECT "aggr__0__parent_count", "aggr__0__key_0", "aggr__0__count",
			  "aggr__0__1__key_0", "aggr__0__1__count"
			FROM (
			  SELECT "aggr__0__parent_count", "aggr__0__key_0", "aggr__0__count",
				"aggr__0__1__key_0", "aggr__0__1__count",
				dense_rank() OVER (ORDER BY "aggr__0__count" DESC, "aggr__0__key_0" ASC)
				AS "aggr__0__order_1_rank",
				dense_rank() OVER (PARTITION BY "aggr__0__key_0" ORDER BY
				"aggr__0__1__key_0" ASC) AS "aggr__0__1__order_1_rank"
			  FROM (
				SELECT sum(count(*)) OVER () AS "aggr__0__parent_count",
				  "host.name" AS "aggr__0__key_0",
				  sum(count(*)) OVER (PARTITION BY "aggr__0__key_0") AS "aggr__0__count",
				  "FlightDelayMin" AS "aggr__0__1__key_0", count(*) AS "aggr__0__1__count"
				FROM ` + TableName + `
				WHERE ("message" IS NOT NULL AND NOT (("message" __quesma_match 'US')))
				GROUP BY "host.name" AS "aggr__0__key_0",
				  "FlightDelayMin" AS "aggr__0__1__key_0"))
			WHERE "aggr__0__order_1_rank"<=9
			ORDER BY "aggr__0__order_1_rank" ASC, "aggr__0__1__order_1_rank" ASC`,
	},
	{ // [36]
		TestName: "0 result rows in terms+histogram + meta field",
		QueryRequestJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"0": {
					"aggs": {
						"1": {
							"histogram": {
								"field": "FlightDelayMin",
								"interval": 1,
								"min_doc_count": 1
							}
						}
					},
					"terms": {
						"field": "host.name",
						"order": {
							"_count": "desc"
						},
						"shard_size": 25,
						"size": 10
					},
					"meta": {
						"bucketSize": 3600,
						"intervalString": "3600s",
						"seriesId": "61ca57f1-469d-11e7-af02-69e470af7417",
						"timeField": "timestamp"
					}
				}
			},
			"fields": [
				{
					"field": "@timestamp",
					"format": "date_time"
				},
				{
					"field": "reqTimeSec",
					"format": "date_time"
				}
			],
			"query": {
				"bool": {
					"filter": [
						{
							"exists": {
								"field": "message"
							}
						}
					],
					"must": [],
					"must_not": [
						{
							"match_phrase": {
								"message": "US"
							}
						}
					],
					"should": []
				}
			},
			"runtime_mappings": {},
			"script_fields": {},
			"size": 0,
			"stored_fields": [
				"*"
			],
			"track_total_hits": false
		}`,
		ExpectedResponse: `
		{
			"response": {
				"aggregations": {
					"0": {
						"meta": {
							"bucketSize":     3600,
							"intervalString": "3600s",
							"seriesId":       "61ca57f1-469d-11e7-af02-69e470af7417",
							"timeField":      "timestamp"
						}
					}
				}
			}
		}`,
		ExpectedPancakeResults: make([]model.QueryResultRow, 0),
		ExpectedPancakeSQL: `
			SELECT "aggr__0__parent_count", "aggr__0__key_0", "aggr__0__count",
			  "aggr__0__1__key_0", "aggr__0__1__count"
			FROM (
			  SELECT "aggr__0__parent_count", "aggr__0__key_0", "aggr__0__count",
				"aggr__0__1__key_0", "aggr__0__1__count",
				dense_rank() OVER (ORDER BY "aggr__0__count" DESC, "aggr__0__key_0" ASC)
				AS "aggr__0__order_1_rank",
				dense_rank() OVER (PARTITION BY "aggr__0__key_0" ORDER BY
				"aggr__0__1__key_0" ASC) AS "aggr__0__1__order_1_rank"
			  FROM (
				SELECT sum(count(*)) OVER () AS "aggr__0__parent_count",
				  "host.name" AS "aggr__0__key_0",
				  sum(count(*)) OVER (PARTITION BY "aggr__0__key_0") AS "aggr__0__count",
				  "FlightDelayMin" AS "aggr__0__1__key_0", count(*) AS "aggr__0__1__count"
				FROM ` + TableName + `
				WHERE ("message" IS NOT NULL AND NOT (("message" __quesma_match 'US')))
				GROUP BY "host.name" AS "aggr__0__key_0",
				  "FlightDelayMin" AS "aggr__0__1__key_0"))
			WHERE "aggr__0__order_1_rank"<=11
			ORDER BY "aggr__0__order_1_rank" ASC, "aggr__0__1__order_1_rank" ASC`,
	},
	{ // [37]
		// Now we don't copy, as it's nested. Tested with Elasticsearch.
		TestName: "0 result rows in terms+histogram + meta field, meta in subaggregation",
		QueryRequestJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"0": {
					"aggs": {
						"1": {
							"histogram": {
								"field": "FlightDelayMin",
								"interval": 1,
								"min_doc_count": 1
							},
							"meta": {
								"bucketSize": 3600,
								"intervalString": "3600s",
								"seriesId": "61ca57f1-469d-11e7-af02-69e470af7417",
								"timeField": "timestamp"
							}
						}
					},
					"terms": {
						"field": "host.name",
						"order": {
							"_count": "desc"
						},
						"shard_size": 25,
						"size": 10
					}
				}
			},
			"fields": [
				{
					"field": "@timestamp",
					"format": "date_time"
				},
				{
					"field": "reqTimeSec",
					"format": "date_time"
				}
			],
			"query": {
				"bool": {
					"filter": [
						{
							"exists": {
								"field": "message"
							}
						}
					],
					"must": [],
					"must_not": [
						{
							"match_phrase": {
								"message": "US"
							}
						}
					],
					"should": []
				}
			},
			"runtime_mappings": {},
			"script_fields": {},
			"size": 0,
			"stored_fields": [
				"*"
			],
			"track_total_hits": false
		}`,
		ExpectedResponse:       `{"response": {"aggregations":{}}}`,
		ExpectedPancakeResults: make([]model.QueryResultRow, 0),
		ExpectedPancakeSQL: `
			SELECT "aggr__0__parent_count", "aggr__0__key_0", "aggr__0__count",
			  "aggr__0__1__key_0", "aggr__0__1__count"
			FROM (
			  SELECT "aggr__0__parent_count", "aggr__0__key_0", "aggr__0__count",
				"aggr__0__1__key_0", "aggr__0__1__count",
				dense_rank() OVER (ORDER BY "aggr__0__count" DESC, "aggr__0__key_0" ASC) AS
				"aggr__0__order_1_rank",
				dense_rank() OVER (PARTITION BY "aggr__0__key_0" ORDER BY
				"aggr__0__1__key_0" ASC) AS "aggr__0__1__order_1_rank"
			  FROM (
				SELECT sum(count(*)) OVER () AS "aggr__0__parent_count",
				  "host.name" AS "aggr__0__key_0",
				  sum(count(*)) OVER (PARTITION BY "aggr__0__key_0") AS "aggr__0__count",
				  "FlightDelayMin" AS "aggr__0__1__key_0", count(*) AS "aggr__0__1__count"
				FROM __quesma_table_name
				WHERE ("message" IS NOT NULL AND NOT (("message" __quesma_match 'US')))
				GROUP BY "host.name" AS "aggr__0__key_0",
				  "FlightDelayMin" AS "aggr__0__1__key_0"))
			WHERE "aggr__0__order_1_rank"<=11
			ORDER BY "aggr__0__order_1_rank" ASC, "aggr__0__1__order_1_rank" ASC`,
	},
	{ // [38]
		TestName: "simplest top_metrics, no sort",
		QueryRequestJson: `
		{
			"aggs": {
				"tm_empty_result": {
					"top_metrics": {
						"metrics": {"field": "message"}
					}
				},
				"tm_with_result": {
					"top_metrics": {
						"metrics": {"field": "message"},
						"size": 2
					}
				}
			},
			"size": 0,
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
					"value": 6018,
					"relation": "eq"
				},
				"max_score": null,
				"hits": []
			},
			"aggregations": {
				"tm_with_result": {
					"top": [
						{
							"metrics": {
								"message": "User updated"
							},
							"sort": []
						}
					]
				}
			}
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{model.NewQueryResultCol("top_metrics__tm_with_result_col_0", "User updated")}},
		},
		ExpectedAdditionalPancakeResults: [][]model.QueryResultRow{
			{},
		},
		AdditionalAcceptableDifference: []string{"tm_empty_result"}, // TODO: check, but we should return empty result
		ExpectedPancakeSQL: `
			SELECT "message" AS "top_metrics__tm_with_result_col_0"
			FROM __quesma_table_name
			LIMIT 2`,
		ExpectedAdditionalPancakeSQLs: []string{`
			SELECT "message" AS "top_metrics__tm_empty_result_col_0"
			FROM __quesma_table_name
			LIMIT 1`},
	},
	{ // [39]
		TestName: "simplest top_metrics, with sort",
		QueryRequestJson: `
		{
			"aggs": {
				"tm_empty_result": {
					"top_metrics": {
						"metrics": {"field": "message"},
						"sort": {"timestamp": "desc"}
					}
				},
				"tm_with_result": {
					"top_metrics": {
						"metrics": {"field": "message"},
						"sort": {"timestamp": "desc"}
					}
				}
			},
			"size": 0,
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
					"value": 6018,
					"relation": "eq"
				},
				"max_score": null,
				"hits": []
			},
			"aggregations": {
				"tm_with_result": {
					"top": [
						{
							"metrics": {
								"message": "User updated"
							},
							"sort": [
								"stamp"
							]
						}
					]
				}
			}
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("top_metrics__tm_with_result_col_0", "User updated"),
				model.NewQueryResultCol("top_metrics__tm_with_result_col_1", "stamp"),
			},
			}},
		ExpectedAdditionalPancakeResults: [][]model.QueryResultRow{
			{},
		},
		AdditionalAcceptableDifference: []string{"tm_empty_result"}, // TODO: check, but we should return empty result
		ExpectedPancakeSQL: `
			SELECT "message" AS "top_metrics__tm_with_result_col_0",
			  "timestamp" AS "top_metrics__tm_with_result_col_1"
			FROM __quesma_table_name
			ORDER BY "timestamp" DESC
			LIMIT 1`,
		ExpectedAdditionalPancakeSQLs: []string{`
			SELECT "message" AS "top_metrics__tm_empty_result_col_0",
			  "timestamp" AS "top_metrics__tm_empty_result_col_1"
			FROM __quesma_table_name
			ORDER BY "timestamp" DESC
			LIMIT 1`},
	},
	{ // [40]
		TestName: "terms ordered by subaggregation",
		QueryRequestJson: `
		{
			"aggs": {
				"2": {
					"terms": {
						"field": "name",
						"order": {
							"1": "desc"
						},
						"size": 10,
						"shard_size": 3000
					},
					"aggs": {
						"1": {
							"sum": {
								"field": "total"
							}
						}
					}
				}
			},
			"size": 0,
			"fields": [],
			"script_fields": {},
			"stored_fields": [
				"*"
			],
			"runtime_mappings": {},
			"_source": {
				"excludes": []
			},
			"query": {
				"bool": {
					"must": [],
					"filter": [],
					"should": [],
					"must_not": [
						{
							"range": {
								"abc": {
			  						"gte": 0,
			  						"lt": 600
								}
							}
						},
						{
							"match_phrase": {
								"type": {
			  						"query": "def"
								}
							}
						}
					]
				}
			}
		}`,
		ExpectedResponse:       `{"aggregations": {}}`,
		ExpectedPancakeResults: make([]model.QueryResultRow, 0),
		ExpectedPancakeSQL: `
			SELECT sum(count(*)) OVER () AS "aggr__2__parent_count",
			  "name" AS "aggr__2__key_0", 
			  count(*) AS "aggr__2__count",
  			  sumOrNull("total") AS "metric__2__1_col_0"
			FROM ` + TableName + `
			WHERE NOT ((("abc">=0 AND "abc"<600) OR ("type" __quesma_match 'def')))
			GROUP BY "name" AS "aggr__2__key_0"
			ORDER BY "metric__2__1_col_0" DESC, "aggr__2__key_0" ASC
			LIMIT 11`,
	},
	{ // [41]
		TestName: "0 result rows in 2x terms",
		QueryRequestJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"0": {
					"aggs": {
						"1": {
							"terms": {
								"field": "DestAirportID",
								"order": {
			  						"_count": "desc"
								},
								"shard_size": 25,
								"size": 3
							}
						}
					},
					"terms": {
						"field": "OriginAirportID",
						"order": {
							"_count": "desc"
						},
						"shard_size": 25,
						"size": 10
					}
				}
			},
			"fields": [
				{
					"field": "@timestamp",
					"format": "date_time"
				},
				{
					"field": "reqTimeSec",
					"format": "date_time"
				}
			],
			"runtime_mappings": {},
			"script_fields": {},
			"size": 0,
			"stored_fields": [
				"*"
			],
			"track_total_hits": true
		}`,
		ExpectedResponse:       `{"response": {"aggregations":{}}}`,
		ExpectedPancakeResults: make([]model.QueryResultRow, 0),
		ExpectedPancakeSQL: `
			SELECT "aggr__0__parent_count", "aggr__0__key_0", "aggr__0__count",
			  "aggr__0__1__parent_count", "aggr__0__1__key_0", "aggr__0__1__count"
			FROM (
			  SELECT "aggr__0__parent_count", "aggr__0__key_0", "aggr__0__count",
				"aggr__0__1__parent_count", "aggr__0__1__key_0", "aggr__0__1__count",
				dense_rank() OVER (ORDER BY "aggr__0__count" DESC, "aggr__0__key_0" ASC) AS
				"aggr__0__order_1_rank",
				dense_rank() OVER (PARTITION BY "aggr__0__key_0" ORDER BY
				"aggr__0__1__count" DESC, "aggr__0__1__key_0" ASC) AS
				"aggr__0__1__order_1_rank"
			  FROM (
				SELECT sum(count(*)) OVER () AS "aggr__0__parent_count",
				  "OriginAirportID" AS "aggr__0__key_0",
				  sum(count(*)) OVER (PARTITION BY "aggr__0__key_0") AS "aggr__0__count",
				  sum(count(*)) OVER (PARTITION BY "aggr__0__key_0") AS
				  "aggr__0__1__parent_count", "DestAirportID" AS "aggr__0__1__key_0",
				  count(*) AS "aggr__0__1__count"
				FROM __quesma_table_name
				GROUP BY "OriginAirportID" AS "aggr__0__key_0",
				  "DestAirportID" AS "aggr__0__1__key_0"))
			WHERE ("aggr__0__order_1_rank"<=11 AND "aggr__0__1__order_1_rank"<=4)
			ORDER BY "aggr__0__order_1_rank" ASC, "aggr__0__1__order_1_rank" ASC`,
	},
}
