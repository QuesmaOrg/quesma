// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package testdata

import (
	"quesma/clickhouse"
	"quesma/model"
	"time"
)

var timestampGroupByClause = model.AsString(clickhouse.TimestampGroupBy(
	model.NewColumnRef("@timestamp"), clickhouse.DateTime64, 30*time.Second))

func groupBySQL(fieldName string, typ clickhouse.DateTimeType, groupByInterval time.Duration) string {
	return model.AsString(clickhouse.TimestampGroupBy(model.NewColumnRef(fieldName), typ, groupByInterval))
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
		ExpectedResults: [][]model.QueryResultRow{
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol("count()", uint64(2200))}}},
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol(`maxOrNull("AvgTicketPrice")`, 1199.72900390625)}}},
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol(`minOrNull("AvgTicketPrice")`, 100.14596557617188)}}},
		},
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("metric__maxAgg_col_0", 1199.72900390625),
				model.NewQueryResultCol("metric__minAgg_col_0", 100.14596557617188),
			}},
		},
		ExpectedSQLs: []string{
			`SELECT count() FROM ` + TableName + ` ` +
				`WHERE ("timestamp">=parseDateTime64BestEffort('2024-02-02T13:47:16.029Z') ` +
				`AND "timestamp"<=parseDateTime64BestEffort('2024-02-09T13:47:16.029Z'))`,
			`SELECT maxOrNull("AvgTicketPrice") FROM ` + TableName + ` ` +
				`WHERE ("timestamp">=parseDateTime64BestEffort('2024-02-02T13:47:16.029Z') ` +
				`AND "timestamp"<=parseDateTime64BestEffort('2024-02-09T13:47:16.029Z'))`,
			`SELECT minOrNull("AvgTicketPrice") FROM ` + TableName + ` ` +
				`WHERE ("timestamp">=parseDateTime64BestEffort('2024-02-02T13:47:16.029Z') ` +
				`AND "timestamp"<=parseDateTime64BestEffort('2024-02-09T13:47:16.029Z'))`,
		},
		ExpectedPancakeSQL: `SELECT maxOrNull("AvgTicketPrice") AS "metric__maxAgg_col_0", ` +
			`minOrNull("AvgTicketPrice") AS "metric__minAgg_col_0" ` +
			`FROM ` + TableName + ` ` +
			`WHERE ("timestamp">=parseDateTime64BestEffort('2024-02-02T13:47:16.029Z') ` +
			`AND "timestamp"<=parseDateTime64BestEffort('2024-02-09T13:47:16.029Z'))`,
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
		ExpectedResults: [][]model.QueryResultRow{
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("OriginCityName", "Abu Dhabi"),
					model.NewQueryResultCol("doc_count", 7),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("OriginCityName", "Adelaide"),
					model.NewQueryResultCol("doc_count", 3),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("OriginCityName", "Albuquerque"),
					model.NewQueryResultCol("doc_count", 0),
				}},
			},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("OriginCityName", "Abu Dhabi"),
					model.NewQueryResultCol("doc_count", 3),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("OriginCityName", "Adelaide"),
					model.NewQueryResultCol("doc_count", 2),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("OriginCityName", "Albuquerque"),
					model.NewQueryResultCol("doc_count", 2),
				}},
			},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("key", "Abu Dhabi"),
					model.NewQueryResultCol("doc_count", 23),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("key", "Adelaide"),
					model.NewQueryResultCol("doc_count", 20),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("key", "Albuquerque"),
					model.NewQueryResultCol("doc_count", 3),
				}},
			},
		},
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
		ExpectedSQLs: []string{
			`WITH cte_1 AS ` +
				`(SELECT "OriginCityName" AS "cte_1_1", count() AS "cte_1_cnt" ` +
				`FROM ` + TableName + ` ` +
				`WHERE (("timestamp">=parseDateTime64BestEffort('2024-02-02T13:47:16.029Z') ` +
				`AND "timestamp"<=parseDateTime64BestEffort('2024-02-09T13:47:16.029Z')) ` +
				`AND "OriginCityName" IS NOT NULL) ` +
				`GROUP BY "OriginCityName" ` +
				`ORDER BY "OriginCityName" ASC ` +
				`LIMIT 1000) ` +
				`SELECT "OriginCityName", count() ` +
				`FROM ` + TableName + ` ` +
				`INNER JOIN "cte_1" ON "OriginCityName" = "cte_1_1" ` +
				`WHERE ((("timestamp">=parseDateTime64BestEffort('2024-02-02T13:47:16.029Z') ` +
				`AND "timestamp"<=parseDateTime64BestEffort('2024-02-09T13:47:16.029Z')) ` +
				`AND "OriginCityName" IS NOT NULL) AND "FlightDelay"==true) ` +
				`GROUP BY "OriginCityName", cte_1_cnt ` +
				`ORDER BY "OriginCityName" ASC`,
			`WITH cte_1 AS ` +
				`(SELECT "OriginCityName" AS "cte_1_1", count() AS "cte_1_cnt" ` +
				`FROM ` + TableName + ` ` +
				`WHERE (("timestamp">=parseDateTime64BestEffort('2024-02-02T13:47:16.029Z') ` +
				`AND "timestamp"<=parseDateTime64BestEffort('2024-02-09T13:47:16.029Z')) ` +
				`AND "OriginCityName" IS NOT NULL) ` +
				`GROUP BY "OriginCityName" ` +
				`ORDER BY "OriginCityName" ASC ` +
				`LIMIT 1000) ` +
				`SELECT "OriginCityName", count() ` +
				`FROM ` + TableName + ` ` +
				`INNER JOIN "cte_1" ON "OriginCityName" = "cte_1_1" ` +
				`WHERE ((("timestamp">=parseDateTime64BestEffort('2024-02-02T13:47:16.029Z') ` +
				`AND "timestamp"<=parseDateTime64BestEffort('2024-02-09T13:47:16.029Z')) ` +
				`AND "OriginCityName" IS NOT NULL) ` +
				`AND "Cancelled"==true) ` +
				`GROUP BY "OriginCityName", cte_1_cnt ` +
				`ORDER BY "OriginCityName" ASC`,
			`SELECT "OriginCityName", count() FROM ` + TableName + ` ` +
				`WHERE (("timestamp">=parseDateTime64BestEffort('2024-02-02T13:47:16.029Z') ` +
				`AND "timestamp"<=parseDateTime64BestEffort('2024-02-09T13:47:16.029Z')) ` +
				`AND "OriginCityName" IS NOT NULL) ` +
				`GROUP BY "OriginCityName" ` +
				`ORDER BY "OriginCityName" ASC ` +
				`LIMIT 1000`,
		},
		ExpectedPancakeSQL: `
			SELECT sum(count(*)) OVER () AS "aggr__0__parent_count",
			  "OriginCityName" AS "aggr__0__key_0", count(*) AS "aggr__0__count",
			  countIf("Cancelled"==true) AS "metric__0__3-bucket_col_0",
			  countIf("FlightDelay"==true) AS "aggr__0__1-bucket__count"
			FROM ` + TableName + `
			WHERE ("timestamp">=parseDateTime64BestEffort('2024-02-02T13:47:16.029Z') AND
			  "timestamp"<=parseDateTime64BestEffort('2024-02-09T13:47:16.029Z'))
			GROUP BY "OriginCityName" AS "aggr__0__key_0"
			ORDER BY "aggr__0__key_0" ASC
			LIMIT 1001`,
	},
	{ // [2]
		TestName: "date_histogram",
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
								"extended_bounds": {
									"max": 1707486436029,
									"min": 1706881636029
								},
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
						"size": 10
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
											"key": 1706875200000,
											"key_as_string": "2024-02-02T12:00:00.000"
										},
										{
											"doc_count": 27,
											"key": 1706886000000,
											"key_as_string": "2024-02-02T15:00:00.000"
										},
										{
											"doc_count": 34,
											"key": 1706896800000,
											"key_as_string": "2024-02-02T18:00:00.000"
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
											"key": 1706875200000,
											"key_as_string": "2024-02-02T12:00:00.000"
										},
										{
											"doc_count": 2,
											"key": 1706886000000,
											"key_as_string": "2024-02-02T15:00:00.000"
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
		ExpectedResults: [][]model.QueryResultRow{
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol("value", uint64(2200))}}},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("FlightDelayType", "No Delay"),
					model.NewQueryResultCol("key", int64(1706875200000/1000/60/60/3)), // / 3h
					model.NewQueryResultCol("doc_count", uint64(2)),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("FlightDelayType", "No Delay"),
					model.NewQueryResultCol("key", int64(1706886000000/1000/60/60/3)),
					model.NewQueryResultCol("doc_count", uint64(27)),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("FlightDelayType", "No Delay"),
					model.NewQueryResultCol("key", int64(1706896800000/1000/60/60/3)),
					model.NewQueryResultCol("doc_count", uint64(34)),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("FlightDelayType", "Security Delay"),
					model.NewQueryResultCol("key", int64(1706875200000/1000/60/60/3)),
					model.NewQueryResultCol("doc_count", uint64(0)),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("FlightDelayType", "Security Delay"),
					model.NewQueryResultCol("key", int64(1706886000000/1000/60/60/3)),
					model.NewQueryResultCol("doc_count", uint64(2)),
				}},
			},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("key", "No Delay"),
					model.NewQueryResultCol("doc_count", uint64(1647)),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("key", "Security Delay"),
					model.NewQueryResultCol("doc_count", uint64(45)),
				}},
			},
		},
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__parent_count", uint64(2200)),
				model.NewQueryResultCol("aggr__0__key_0", "No Delay"),
				model.NewQueryResultCol("aggr__0__count", uint64(1647)),
				model.NewQueryResultCol("aggr__0__order_1", uint64(1647)),
				model.NewQueryResultCol("aggr__0__1__key_0", int64(1706875200000/1000/60/60/3)),
				model.NewQueryResultCol("aggr__0__1__count", uint64(2)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__parent_count", uint64(2200)),
				model.NewQueryResultCol("aggr__0__key_0", "No Delay"),
				model.NewQueryResultCol("aggr__0__count", uint64(1647)),
				model.NewQueryResultCol("aggr__0__order_1", uint64(1647)),
				model.NewQueryResultCol("aggr__0__1__key_0", int64(1706886000000/1000/60/60/3)),
				model.NewQueryResultCol("aggr__0__1__count", uint64(27)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__parent_count", uint64(2200)),
				model.NewQueryResultCol("aggr__0__key_0", "No Delay"),
				model.NewQueryResultCol("aggr__0__count", uint64(1647)),
				model.NewQueryResultCol("aggr__0__order_1", uint64(1647)),
				model.NewQueryResultCol("aggr__0__1__key_0", int64(1706896800000/1000/60/60/3)),
				model.NewQueryResultCol("aggr__0__1__count", uint64(34)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__parent_count", uint64(2200)),
				model.NewQueryResultCol("aggr__0__key_0", "Security Delay"),
				model.NewQueryResultCol("aggr__0__count", uint64(45)),
				model.NewQueryResultCol("aggr__0__order_1", uint64(45)),
				model.NewQueryResultCol("aggr__0__1__key_0", int64(1706875200000/1000/60/60/3)),
				model.NewQueryResultCol("aggr__0__1__count", uint64(0)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__parent_count", uint64(2200)),
				model.NewQueryResultCol("aggr__0__key_0", "Security Delay"),
				model.NewQueryResultCol("aggr__0__count", uint64(45)),
				model.NewQueryResultCol("aggr__0__order_1", uint64(45)),
				model.NewQueryResultCol("aggr__0__1__key_0", int64(1706886000000/1000/60/60/3)),
				model.NewQueryResultCol("aggr__0__1__count", uint64(2)),
			}},
		},
		ExpectedSQLs: []string{
			`SELECT count() FROM (SELECT 1 FROM ` + TableName + ` ` +
				`WHERE ("timestamp">=parseDateTime64BestEffort('2024-02-02T13:47:16.029Z') ` +
				`AND "timestamp"<=parseDateTime64BestEffort('2024-02-09T13:47:16.029Z')) ` +
				`LIMIT 12)`,
			`WITH cte_1 AS ` +
				`(SELECT "FlightDelayType" AS "cte_1_1", count() AS "cte_1_cnt" ` +
				`FROM ` + TableName + ` ` +
				`WHERE (("timestamp">=parseDateTime64BestEffort('2024-02-02T13:47:16.029Z') AND ` +
				`"timestamp"<=parseDateTime64BestEffort('2024-02-09T13:47:16.029Z')) ` +
				`AND "FlightDelayType" IS NOT NULL) ` +
				`GROUP BY "FlightDelayType" ` +
				`ORDER BY count() DESC, "FlightDelayType" ` +
				`LIMIT 10) ` +
				`SELECT "FlightDelayType", toInt64(toUnixTimestamp64Milli("timestamp") / 10800000), count() ` +
				`FROM ` + TableName + ` ` +
				`INNER JOIN "cte_1" ON "FlightDelayType" = "cte_1_1" ` +
				`WHERE (("timestamp">=parseDateTime64BestEffort('2024-02-02T13:47:16.029Z') AND ` +
				`"timestamp"<=parseDateTime64BestEffort('2024-02-09T13:47:16.029Z')) ` +
				`AND "FlightDelayType" IS NOT NULL) ` +
				`GROUP BY "FlightDelayType", toInt64(toUnixTimestamp64Milli("timestamp") / 10800000), cte_1_cnt ` +
				`ORDER BY cte_1_cnt DESC, "FlightDelayType", toInt64(toUnixTimestamp64Milli("timestamp") / 10800000)`,
			`SELECT "FlightDelayType", count() FROM ` + TableName + ` ` +
				`WHERE (("timestamp">=parseDateTime64BestEffort('2024-02-02T13:47:16.029Z') ` +
				`AND "timestamp"<=parseDateTime64BestEffort('2024-02-09T13:47:16.029Z')) ` +
				`AND "FlightDelayType" IS NOT NULL) ` +
				`GROUP BY "FlightDelayType" ` +
				`ORDER BY count() DESC, "FlightDelayType" ` +
				`LIMIT 10`,
		},
		ExpectedPancakeSQL: `
			SELECT "aggr__0__parent_count", "aggr__0__key_0", "aggr__0__count",
			  "aggr__0__order_1", "aggr__0__1__key_0", "aggr__0__1__count"
			FROM (
			  SELECT "aggr__0__parent_count", "aggr__0__key_0", "aggr__0__count",
				"aggr__0__order_1", "aggr__0__1__key_0", "aggr__0__1__count",
				dense_rank() OVER (ORDER BY "aggr__0__order_1" DESC, "aggr__0__key_0" ASC)
				AS "aggr__0__order_1_rank",
				dense_rank() OVER (PARTITION BY "aggr__0__key_0" ORDER BY
				"aggr__0__1__key_0" ASC) AS "aggr__0__1__order_1_rank"
			  FROM (
				SELECT sum(count(*)) OVER () AS "aggr__0__parent_count",
				  "FlightDelayType" AS "aggr__0__key_0",
				  sum(count(*)) OVER (PARTITION BY "aggr__0__key_0") AS "aggr__0__count",
				  sum(count()) OVER (PARTITION BY "aggr__0__key_0") AS "aggr__0__order_1",
				  toInt64(toUnixTimestamp64Milli("timestamp") / 10800000) AS
				  "aggr__0__1__key_0", count(*) AS "aggr__0__1__count"
				FROM ` + TableName + `
				WHERE ("timestamp">=parseDateTime64BestEffort('2024-02-02T13:47:16.029Z')
				  AND "timestamp"<=parseDateTime64BestEffort('2024-02-09T13:47:16.029Z'))
				GROUP BY "FlightDelayType" AS "aggr__0__key_0",
				  toInt64(toUnixTimestamp64Milli("timestamp") / 10800000) AS
				  "aggr__0__1__key_0"))
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
		ExpectedResults: [][]model.QueryResultRow{
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol("value", uint64(1043))}}},
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol("value", 76631.67578125)}}},
		},
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("metric__0_col_0", 76631.67578125),
			}},
		},
		ExpectedSQLs: []string{
			`SELECT count() ` +
				`FROM ` + TableName + ` ` +
				`WHERE ("order_date">=parseDateTime64BestEffort('2024-02-06T09:59:57.034Z') ` +
				`AND "order_date"<=parseDateTime64BestEffort('2024-02-13T09:59:57.034Z'))`,
			`SELECT sumOrNull("taxful_total_price") ` +
				`FROM ` + TableName + ` ` +
				`WHERE ("order_date">=parseDateTime64BestEffort('2024-02-06T09:59:57.034Z') ` +
				`AND "order_date"<=parseDateTime64BestEffort('2024-02-13T09:59:57.034Z'))`,
		},
		ExpectedPancakeSQL: `SELECT sumOrNull("taxful_total_price") AS "metric__0_col_0" ` +
			`FROM ` + TableName + ` ` +
			`WHERE ("order_date">=parseDateTime64BestEffort('2024-02-06T09:59:57.034Z') ` +
			`AND "order_date"<=parseDateTime64BestEffort('2024-02-13T09:59:57.034Z'))`,
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
		ExpectedResults: [][]model.QueryResultRow{
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol("value", uint64(2200))}}},
			{
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", "Rome"), model.NewQueryResultCol("doc_count", 73)}},
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", "Bogota"), model.NewQueryResultCol("doc_count", 44)}},
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", "Milan"), model.NewQueryResultCol("doc_count", 32)}},
			},
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol("value", 143)}}},
		},
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("metric__unique_terms_col_0", 143),
				model.NewQueryResultCol("aggr__suggestions__parent_count", uint64(2200)),
				model.NewQueryResultCol("aggr__suggestions__key_0", "Rome"),
				model.NewQueryResultCol("aggr__suggestions__count", uint64(73)),
				model.NewQueryResultCol("aggr__suggestions__order_1", uint64(73)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("metric__unique_terms_col_0", 143),
				model.NewQueryResultCol("aggr__suggestions__parent_count", uint64(2200)),
				model.NewQueryResultCol("aggr__suggestions__key_0", "Bogota"),
				model.NewQueryResultCol("aggr__suggestions__count", uint64(44)),
				model.NewQueryResultCol("aggr__suggestions__order_1", uint64(44)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("metric__unique_terms_col_0", 143),
				model.NewQueryResultCol("aggr__suggestions__parent_count", uint64(2200)),
				model.NewQueryResultCol("aggr__suggestions__key_0", "Milan"),
				model.NewQueryResultCol("aggr__suggestions__count", uint64(32)),
				model.NewQueryResultCol("aggr__suggestions__order_1", uint64(32)),
			}},
		},
		ExpectedSQLs: []string{
			`SELECT count() ` +
				`FROM ` + TableName + ` ` +
				`WHERE ("timestamp">=parseDateTime64BestEffort('2024-02-02T13:47:16.029Z') ` +
				`AND "timestamp"<=parseDateTime64BestEffort('2024-02-09T13:47:16.029Z'))`,
			`SELECT "OriginCityName", count() ` +
				`FROM ` + TableName + ` ` +
				`WHERE (("timestamp">=parseDateTime64BestEffort('2024-02-02T13:47:16.029Z') ` +
				`AND "timestamp"<=parseDateTime64BestEffort('2024-02-09T13:47:16.029Z')) ` +
				`AND "OriginCityName" IS NOT NULL) ` +
				`GROUP BY "OriginCityName" ` +
				`ORDER BY count() DESC, "OriginCityName" ` +
				`LIMIT 10`,
			`SELECT count(DISTINCT "OriginCityName") ` +
				`FROM ` + TableName + ` ` +
				`WHERE ("timestamp">=parseDateTime64BestEffort('2024-02-02T13:47:16.029Z') ` +
				`AND "timestamp"<=parseDateTime64BestEffort('2024-02-09T13:47:16.029Z'))`,
		},
		ExpectedPancakeSQL: `
			SELECT uniqMerge(uniqState("OriginCityName")) OVER () AS
			  "metric__unique_terms_col_0",
			  sum(count(*)) OVER () AS "aggr__suggestions__parent_count",
			  "OriginCityName" AS "aggr__suggestions__key_0",
			  count(*) AS "aggr__suggestions__count",
			  count() AS "aggr__suggestions__order_1"
			FROM ` + TableName + `
			WHERE ("timestamp">=parseDateTime64BestEffort('2024-02-02T13:47:16.029Z') AND
			  "timestamp"<=parseDateTime64BestEffort('2024-02-09T13:47:16.029Z'))
			GROUP BY "OriginCityName" AS "aggr__suggestions__key_0"
			ORDER BY "aggr__suggestions__order_1" DESC, "aggr__suggestions__key_0" ASC
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
		ExpectedResults: [][]model.QueryResultRow{
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol("value", uint64(2200))}}},
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol("doc_count", uint64(553))}}},
		},
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0-bucket__count", uint64(553)),
			}},
		},
		ExpectedSQLs: []string{
			`SELECT count() ` +
				`FROM ` + TableName + ` ` +
				`WHERE ("timestamp">=parseDateTime64BestEffort('2024-02-02T13:47:16.029Z') ` +
				`AND "timestamp"<=parseDateTime64BestEffort('2024-02-09T13:47:16.029Z'))`,
			`SELECT count() ` +
				`FROM ` + TableName + ` ` +
				`WHERE (("timestamp">=parseDateTime64BestEffort('2024-02-02T13:47:16.029Z') ` +
				`AND "timestamp"<=parseDateTime64BestEffort('2024-02-09T13:47:16.029Z')) ` +
				`AND "FlightDelay"==true)`,
		},
		ExpectedPancakeSQL: `
			SELECT countIf("FlightDelay"==true) AS "aggr__0-bucket__count"
			FROM ` + TableName + `
			WHERE ("timestamp">=parseDateTime64BestEffort('2024-02-02T13:47:16.029Z') AND
			  "timestamp"<=parseDateTime64BestEffort('2024-02-09T13:47:16.029Z'))`,
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
		ExpectedResults: [][]model.QueryResultRow{
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol("value", uint64(904))}}},
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol("doc_count", 553)}}},
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol("doc_count", 351)}}},
		},
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("filter_0__aggr__time_offset_split__count", uint64(553)),
				model.NewQueryResultCol("filter_1__aggr__time_offset_split__count", uint64(351)),
			}},
		},
		ExpectedSQLs: []string{
			`SELECT count() FROM ` + TableName + ` WHERE ("FlightDelay"==true AND (("timestamp">=parseDateTime64BestEffort('2024-02-02T13:47:16.029Z') AND "timestamp"<=parseDateTime64BestEffort('2024-02-09T13:47:16.029Z')) OR ("timestamp">=parseDateTime64BestEffort('2024-01-26T13:47:16.029Z') AND "timestamp"<=parseDateTime64BestEffort('2024-02-02T13:47:16.029Z'))))`,
			`SELECT count() FROM ` + TableName + ` WHERE (("FlightDelay"==true ` +
				`AND (("timestamp">=parseDateTime64BestEffort('2024-02-02T13:47:16.029Z') AND "timestamp"<=parseDateTime64BestEffort('2024-02-09T13:47:16.029Z')) ` +
				`OR ("timestamp">=parseDateTime64BestEffort('2024-01-26T13:47:16.029Z') AND "timestamp"<=parseDateTime64BestEffort('2024-02-02T13:47:16.029Z')))) ` +
				`AND ("timestamp">=parseDateTime64BestEffort('2024-02-02T13:47:16.029Z') AND "timestamp"<=parseDateTime64BestEffort('2024-02-09T13:47:16.029Z')))`,
			`SELECT count() FROM ` + TableName + ` WHERE (("FlightDelay"==true ` +
				`AND (("timestamp">=parseDateTime64BestEffort('2024-02-02T13:47:16.029Z') AND "timestamp"<=parseDateTime64BestEffort('2024-02-09T13:47:16.029Z')) ` +
				`OR ("timestamp">=parseDateTime64BestEffort('2024-01-26T13:47:16.029Z') AND "timestamp"<=parseDateTime64BestEffort('2024-02-02T13:47:16.029Z')))) ` +
				`AND ("timestamp">=parseDateTime64BestEffort('2024-01-26T13:47:16.029Z') AND "timestamp"<=parseDateTime64BestEffort('2024-02-02T13:47:16.029Z')))`,
		},
		ExpectedPancakeSQL: `
			SELECT countIf(("timestamp">=parseDateTime64BestEffort(
			  '2024-02-02T13:47:16.029Z') AND "timestamp"<=parseDateTime64BestEffort(
			  '2024-02-09T13:47:16.029Z'))) AS "filter_0__aggr__time_offset_split__count",
			  countIf(("timestamp">=parseDateTime64BestEffort('2024-01-26T13:47:16.029Z')
			  AND "timestamp"<=parseDateTime64BestEffort('2024-02-02T13:47:16.029Z'))) AS
			  "filter_1__aggr__time_offset_split__count"
			FROM ` + TableName + `
			WHERE ("FlightDelay"==true AND (("timestamp">=parseDateTime64BestEffort(
			  '2024-02-02T13:47:16.029Z') AND "timestamp"<=parseDateTime64BestEffort(
			  '2024-02-09T13:47:16.029Z')) OR ("timestamp">=parseDateTime64BestEffort(
			  '2024-01-26T13:47:16.029Z') AND "timestamp"<=parseDateTime64BestEffort(
			  '2024-02-02T13:47:16.029Z'))))`,
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
									"sum_other_doc_count": 0
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
									"sum_other_doc_count": 0,
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
								"doc_count": 15,
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
						"sum_other_doc_count": 12716
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
		ExpectedResults: [][]model.QueryResultRow{
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol("value", uint64(13014))}}},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("OriginAirportID", "UIO"), model.NewQueryResultCol("DestAirportID", "EZE"),
					model.NewQueryResultCol("lat", "-34.8222"), model.NewQueryResultCol("lon", "-58.5358"),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("OriginAirportID", "UIO"), model.NewQueryResultCol("DestAirportID", "UIO"),
					model.NewQueryResultCol("lat", "-0.129166667"), model.NewQueryResultCol("lon", "-78.3575"),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("OriginAirportID", "DLH"), model.NewQueryResultCol("DestAirportID", "YUL"),
					model.NewQueryResultCol("lat", "45.47060013"), model.NewQueryResultCol("lon", "-73.74079895"),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("OriginAirportID", "DLH"), model.NewQueryResultCol("DestAirportID", "EZE"),
					model.NewQueryResultCol("lat", "46.84209824"), model.NewQueryResultCol("lon", "-92.19360352"),
				}},
			},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("OriginAirportID", "UIO"), model.NewQueryResultCol("DestAirportID", "EZE"),
					model.NewQueryResultCol("doc_count", 21), model.NewQueryResultCol("key", "EZE"),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("OriginAirportID", "UIO"), model.NewQueryResultCol("DestAirportID", "UIO"),
					model.NewQueryResultCol("doc_count", 12), model.NewQueryResultCol("key", "UI"),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("OriginAirportID", "DLH"), model.NewQueryResultCol("DestAirportID", "YUL"),
					model.NewQueryResultCol("doc_count", 11), model.NewQueryResultCol("key", "YUL"),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("OriginAirportID", "DLH"), model.NewQueryResultCol("DestAirportID", "EZE"),
					model.NewQueryResultCol("doc_count", 10), model.NewQueryResultCol("key", "EZE"),
				}},
			},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("Origin", "Mariscal Sucre International Airport"),
					model.NewQueryResultCol("lat", "-0.129166667"), model.NewQueryResultCol("lon", "-78.3575"),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("Origin", "Duluth International Airport"),
					model.NewQueryResultCol("lat", "46.84209824"), model.NewQueryResultCol("lon", "-92.19360352"),
				}},
			},
			{
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("doc_count", 283), model.NewQueryResultCol("key", "UIO")}},
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("doc_count", 15), model.NewQueryResultCol("key", "DLH")}},
			},
		},
		ExpectedPancakeResults: []model.QueryResultRow{
			{
				Cols: []model.QueryResultCol{
					model.NewQueryResultCol("aggr__origins__parent_count", uint64(13014)),
					model.NewQueryResultCol("aggr__origins__key_0", "UIO"),
					model.NewQueryResultCol("aggr__origins__count", int64(283)),
					model.NewQueryResultCol("aggr__origins__order_1", int64(283)),
					model.NewQueryResultCol("aggr__origins__distinations__key_0", "EZE"),
					model.NewQueryResultCol("aggr__origins__distinations__count", int64(21)),
					model.NewQueryResultCol("aggr__origins__distinations__order_1", int64(21)),
				},
			},
			{
				Cols: []model.QueryResultCol{
					model.NewQueryResultCol("aggr__origins__parent_count", uint64(13014)),
					model.NewQueryResultCol("aggr__origins__key_0", "UIO"),
					model.NewQueryResultCol("aggr__origins__count", int64(283)),
					model.NewQueryResultCol("aggr__origins__order_1", int64(283)),
					model.NewQueryResultCol("aggr__origins__distinations__key_0", "UIO"),
					model.NewQueryResultCol("aggr__origins__distinations__count", int64(12)),
					model.NewQueryResultCol("aggr__origins__distinations__order_1", int64(12)),
				},
			},
			{
				Cols: []model.QueryResultCol{
					model.NewQueryResultCol("aggr__origins__parent_count", uint64(13014)),
					model.NewQueryResultCol("aggr__origins__key_0", "DLH"),
					model.NewQueryResultCol("aggr__origins__count", int64(15)),
					model.NewQueryResultCol("aggr__origins__order_1", int64(15)),
					model.NewQueryResultCol("aggr__origins__distinations__key_0", "YUL"),
					model.NewQueryResultCol("aggr__origins__distinations__count", int64(11)),
					model.NewQueryResultCol("aggr__origins__distinations__order_1", int64(11)),
				},
			},
			{
				Cols: []model.QueryResultCol{
					model.NewQueryResultCol("aggr__origins__parent_count", uint64(13014)),
					model.NewQueryResultCol("aggr__origins__key_0", "DLH"),
					model.NewQueryResultCol("aggr__origins__count", int64(15)),
					model.NewQueryResultCol("aggr__origins__order_1", int64(15)),
					model.NewQueryResultCol("aggr__origins__distinations__key_0", "EZE"),
					model.NewQueryResultCol("aggr__origins__distinations__count", int64(10)),
					model.NewQueryResultCol("aggr__origins__distinations__order_1", int64(10)),
				},
			},
		},
		ExpectedAdditionalPancakeResults: [][]model.QueryResultRow{
			{
				{
					Cols: []model.QueryResultCol{
						model.NewQueryResultCol("aggr__origins__parent_count", uint64(13014)),
						model.NewQueryResultCol("aggr__origins__key_0", "UIO"),
						model.NewQueryResultCol("aggr__origins__count", int64(283)),
						model.NewQueryResultCol("aggr__origins__order_1", int64(283)),
						model.NewQueryResultCol("top_hits_1", "Mariscal Sucre International Airport"),
						model.NewQueryResultCol("top_hits_2", `"OriginLocation": {
														"lat": "-0.129166667",
														"lon": "-78.3575"
													}`),
						model.NewQueryResultCol("top_hits_rank", int64(1)),
					},
				},
				{
					Cols: []model.QueryResultCol{
						model.NewQueryResultCol("aggr__origins__parent_count", uint64(13014)),
						model.NewQueryResultCol("aggr__origins__key_0", "DLH"),
						model.NewQueryResultCol("aggr__origins__count", int64(15)),
						model.NewQueryResultCol("aggr__origins__order_1", int64(15)),
						model.NewQueryResultCol("top_hits_1", "Duluth International Airport"),
						model.NewQueryResultCol("top_hits_2", `"OriginLocation": {
														"lat": "46.84209824",
														"lon": "-92.19360352"
													}`),
						model.NewQueryResultCol("top_hits_rank", int64(1)),
					},
				},
			},
			{
				{
					Cols: []model.QueryResultCol{
						model.NewQueryResultCol("aggr__origins__parent_count", uint64(13014)),
						model.NewQueryResultCol("aggr__origins__key_0", "UIO"),
						model.NewQueryResultCol("aggr__origins__count", int64(283)),
						model.NewQueryResultCol("aggr__origins__order_1", int64(283)),
						model.NewQueryResultCol("aggr__origins__distinations__key_0", "EZE"),
						model.NewQueryResultCol("aggr__origins__distinations__count", int64(21)),
						model.NewQueryResultCol("aggr__origins__distinations__order_1", int64(21)),
						model.NewQueryResultCol("top_hits_1", "TODO"),
						model.NewQueryResultCol("top_hits_rank", int64(1)),
					},
				},
				{
					Cols: []model.QueryResultCol{
						model.NewQueryResultCol("aggr__origins__parent_count", uint64(13014)),
						model.NewQueryResultCol("aggr__origins__key_0", "UIO"),
						model.NewQueryResultCol("aggr__origins__count", int64(283)),
						model.NewQueryResultCol("aggr__origins__order_1", int64(283)),
						model.NewQueryResultCol("aggr__origins__distinations__key_0", "UIO"),
						model.NewQueryResultCol("aggr__origins__distinations__count", int64(12)),
						model.NewQueryResultCol("aggr__origins__distinations__order_1", int64(12)),
						model.NewQueryResultCol("top_hits_1", "TODO"),
						model.NewQueryResultCol("top_hits_rank", int64(1)),
					},
				},
				{
					Cols: []model.QueryResultCol{
						model.NewQueryResultCol("aggr__origins__parent_count", uint64(13014)),
						model.NewQueryResultCol("aggr__origins__key_0", "DLH"),
						model.NewQueryResultCol("aggr__origins__count", int64(15)),
						model.NewQueryResultCol("aggr__origins__order_1", int64(15)),
						model.NewQueryResultCol("aggr__origins__distinations__key_0", "YUL"),
						model.NewQueryResultCol("aggr__origins__distinations__count", int64(11)),
						model.NewQueryResultCol("aggr__origins__distinations__order_1", int64(11)),
						model.NewQueryResultCol("top_hits_1", "TODO"),
						model.NewQueryResultCol("top_hits_rank", int64(1)),
					},
				},
				{
					Cols: []model.QueryResultCol{
						model.NewQueryResultCol("aggr__origins__parent_count", uint64(13014)),
						model.NewQueryResultCol("aggr__origins__key_0", "DLH"),
						model.NewQueryResultCol("aggr__origins__count", int64(15)),
						model.NewQueryResultCol("aggr__origins__order_1", int64(15)),
						model.NewQueryResultCol("aggr__origins__distinations__key_0", "EZE"),
						model.NewQueryResultCol("aggr__origins__distinations__count", int64(10)),
						model.NewQueryResultCol("aggr__origins__distinations__order_1", int64(10)),
						model.NewQueryResultCol("top_hits_1", "TODO"),
						model.NewQueryResultCol("top_hits_rank", int64(1)),
					},
				},
			},
		},
		ExpectedSQLs: []string{
			`SELECT count() ` +
				`FROM ` + TableName + ` `,
			``,
			``,
			``,
		},
		ExpectedPancakeSQL: `
			SELECT "aggr__origins__parent_count", "aggr__origins__key_0",
			  "aggr__origins__count", "aggr__origins__order_1",
			  "aggr__origins__distinations__parent_count",
			  "aggr__origins__distinations__key_0", "aggr__origins__distinations__count",
			  "aggr__origins__distinations__order_1"
			FROM (
			  SELECT "aggr__origins__parent_count", "aggr__origins__key_0",
				"aggr__origins__count", "aggr__origins__order_1",
				"aggr__origins__distinations__parent_count",
				"aggr__origins__distinations__key_0", "aggr__origins__distinations__count",
				"aggr__origins__distinations__order_1",
				dense_rank() OVER (ORDER BY "aggr__origins__order_1" DESC,
				"aggr__origins__key_0" ASC) AS "aggr__origins__order_1_rank",
				dense_rank() OVER (PARTITION BY "aggr__origins__key_0" ORDER BY
				"aggr__origins__distinations__order_1" DESC,
				"aggr__origins__distinations__key_0" ASC) AS
				"aggr__origins__distinations__order_1_rank"
			  FROM (
				SELECT sum(count(*)) OVER () AS "aggr__origins__parent_count",
				  "OriginAirportID" AS "aggr__origins__key_0",
				  sum(count(*)) OVER (PARTITION BY "aggr__origins__key_0") AS
				  "aggr__origins__count",
				  sum(count()) OVER (PARTITION BY "aggr__origins__key_0") AS
				  "aggr__origins__order_1",
				  sum(count(*)) OVER (PARTITION BY "aggr__origins__key_0") AS
				  "aggr__origins__distinations__parent_count",
				  "DestAirportID" AS "aggr__origins__distinations__key_0",
				  count(*) AS "aggr__origins__distinations__count",
				  count() AS "aggr__origins__distinations__order_1"
				FROM __quesma_table_name
				GROUP BY "OriginAirportID" AS "aggr__origins__key_0",
				  "DestAirportID" AS "aggr__origins__distinations__key_0"))
			WHERE ("aggr__origins__order_1_rank"<=10001 AND
			  "aggr__origins__distinations__order_1_rank"<=10001)
			ORDER BY "aggr__origins__order_1_rank" ASC,
			  "aggr__origins__distinations__order_1_rank" ASC`,
		ExpectedAdditionalPancakeSQLs: []string{`
			WITH quesma_top_hits_group_table AS (
			  SELECT sum(count(*)) OVER () AS "aggr__origins__parent_count",
				"OriginAirportID" AS "aggr__origins__key_0",
				count(*) AS "aggr__origins__count", count() AS "aggr__origins__order_1"
			  FROM __quesma_table_name
			  GROUP BY "OriginAirportID" AS "aggr__origins__key_0"
			  ORDER BY "aggr__origins__order_1" DESC, "aggr__origins__key_0" ASC
			  LIMIT 10001) ,
			quesma_top_hits_join AS (
			  SELECT "group_table"."aggr__origins__parent_count" AS
				"aggr__origins__parent_count",
				"group_table"."aggr__origins__key_0" AS "aggr__origins__key_0",
				"group_table"."aggr__origins__count" AS "aggr__origins__count",
				"group_table"."aggr__origins__order_1" AS "aggr__origins__order_1",
				"hit_table"."OriginLocation" AS "top_hits_1",
				"hit_table"."Origin" AS "top_hits_2",
				ROW_NUMBER() OVER (PARTITION BY "group_table"."aggr__origins__key_0") AS
				"top_hits_rank"
			  FROM quesma_top_hits_group_table AS "group_table" LEFT OUTER JOIN
				__quesma_table_name AS "hit_table" ON ("group_table"."aggr__origins__key_0"=
				"hit_table"."OriginAirportID"))
			SELECT "aggr__origins__parent_count", "aggr__origins__key_0",
			  "aggr__origins__count", "aggr__origins__order_1", "top_hits_1", "top_hits_2",
			  "top_hits_rank"
			FROM quesma_top_hits_join
			WHERE top_hits_rank<=1`, `
			WITH quesma_top_hits_group_table AS (
			  SELECT "aggr__origins__parent_count", "aggr__origins__key_0",
				"aggr__origins__count", "aggr__origins__order_1",
				"aggr__origins__distinations__parent_count",
				"aggr__origins__distinations__key_0", "aggr__origins__distinations__count",
				"aggr__origins__distinations__order_1"
			  FROM (
				SELECT "aggr__origins__parent_count", "aggr__origins__key_0",
				  "aggr__origins__count", "aggr__origins__order_1",
				  "aggr__origins__distinations__parent_count",
				  "aggr__origins__distinations__key_0",
				  "aggr__origins__distinations__count",
				  "aggr__origins__distinations__order_1",
				  dense_rank() OVER (ORDER BY "aggr__origins__order_1" DESC,
				  "aggr__origins__key_0" ASC) AS "aggr__origins__order_1_rank",
				  dense_rank() OVER (PARTITION BY "aggr__origins__key_0" ORDER BY
				  "aggr__origins__distinations__order_1" DESC,
				  "aggr__origins__distinations__key_0" ASC) AS
				  "aggr__origins__distinations__order_1_rank"
				FROM (
				  SELECT sum(count(*)) OVER () AS "aggr__origins__parent_count",
					"OriginAirportID" AS "aggr__origins__key_0",
					sum(count(*)) OVER (PARTITION BY "aggr__origins__key_0") AS
					"aggr__origins__count",
					sum(count()) OVER (PARTITION BY "aggr__origins__key_0") AS
					"aggr__origins__order_1",
					sum(count(*)) OVER (PARTITION BY "aggr__origins__key_0") AS
					"aggr__origins__distinations__parent_count",
					"DestAirportID" AS "aggr__origins__distinations__key_0",
					count(*) AS "aggr__origins__distinations__count",
					count() AS "aggr__origins__distinations__order_1"
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
				"group_table"."aggr__origins__order_1" AS "aggr__origins__order_1",
				"group_table"."aggr__origins__distinations__parent_count" AS
				"aggr__origins__distinations__parent_count",
				"group_table"."aggr__origins__distinations__key_0" AS
				"aggr__origins__distinations__key_0",
				"group_table"."aggr__origins__distinations__count" AS
				"aggr__origins__distinations__count",
				"group_table"."aggr__origins__distinations__order_1" AS
				"aggr__origins__distinations__order_1",
				"hit_table"."DestLocation" AS "top_hits_1",
				ROW_NUMBER() OVER (PARTITION BY "group_table"."aggr__origins__key_0",
				"group_table"."aggr__origins__distinations__key_0") AS "top_hits_rank"
			  FROM quesma_top_hits_group_table AS "group_table" LEFT OUTER JOIN
				__quesma_table_name AS "hit_table" ON (("group_table"."aggr__origins__key_0"
				="hit_table"."OriginAirportID" AND
				"group_table"."aggr__origins__distinations__key_0"=
				"hit_table"."DestAirportID")))
			SELECT "aggr__origins__parent_count", "aggr__origins__key_0",
			  "aggr__origins__count", "aggr__origins__order_1",
			  "aggr__origins__distinations__parent_count",
			  "aggr__origins__distinations__key_0", "aggr__origins__distinations__count",
			  "aggr__origins__distinations__order_1", "top_hits_1", "top_hits_rank"
			FROM quesma_top_hits_join
			WHERE top_hits_rank<=1`},
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
		ExpectedResults: [][]model.QueryResultRow{
			{
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", 15.0), model.NewQueryResultCol("doc_count", 21)}},
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", 30.0), model.NewQueryResultCol("doc_count", 22)}},
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", 345.0), model.NewQueryResultCol("doc_count", 13)}},
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", 360.0), model.NewQueryResultCol("doc_count", 22)}},
			},
		},
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
		ExpectedSQLs: []string{
			`SELECT "FlightDelayMin", count() ` +
				`FROM ` + TableName + ` ` +
				`WHERE (("timestamp">=parseDateTime64BestEffort('2024-02-02T13:47:16.029Z') ` +
				`AND "timestamp"<=parseDateTime64BestEffort('2024-02-09T13:47:16.029Z')) ` +
				`AND NOT ("FlightDelayMin"==0)) ` +
				`GROUP BY "FlightDelayMin" ` +
				`ORDER BY "FlightDelayMin"`,
		},
		ExpectedPancakeSQL: `
			SELECT "FlightDelayMin" AS "aggr__0__key_0", count(*) AS "aggr__0__count"
			FROM ` + TableName + `
			WHERE (("timestamp">=parseDateTime64BestEffort('2024-02-02T13:47:16.029Z') AND
			  "timestamp"<=parseDateTime64BestEffort('2024-02-09T13:47:16.029Z')) AND NOT (
			  "FlightDelayMin"==0))
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
											"key": 1707480000000,
											"key_as_string": "2024-02-09T12:00:00.000"
										},
										{
											"doc_count": 80,
											"key": 1707490800000,
											"key_as_string": "2024-02-09T15:00:00.000"
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
											"key": 1707480000000,
											"key_as_string": "2024-02-09T12:00:00.000"
										},
										{
											"doc_count": 32,
											"key": 1707490800000,
											"key_as_string": "2024-02-09T15:00:00.000"
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
											"key": 1707480000000,
											"key_as_string": "2024-02-09T12:00:00.000"
										},
										{
											"doc_count": 11,
											"key": 1707490800000,
											"key_as_string": "2024-02-09T15:00:00.000"
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
		ExpectedResults: [][]model.QueryResultRow{
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol("hits", uint64(198))}}},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("key", "info"),
					model.NewQueryResultCol("key", int64(1707480000000/1000/60/60/3)), // divide by 3h
					model.NewQueryResultCol("doc_count", 22),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("key", "info"),
					model.NewQueryResultCol("key", int64(1707490800000/1000/60/60/3)), // divide by 3h
					model.NewQueryResultCol("doc_count", 80),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("key", "debug"),
					model.NewQueryResultCol("key", int64(1707480000000/1000/60/60/3)), // divide by 3h
					model.NewQueryResultCol("doc_count", 17),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("key", "debug"),
					model.NewQueryResultCol("key", int64(1707490800000/1000/60/60/3)), // divide by 3h
					model.NewQueryResultCol("doc_count", 32),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("key", "critical"),
					model.NewQueryResultCol("key", int64(1707480000000/1000/60/60/3)), // divide by 3h
					model.NewQueryResultCol("doc_count", 5),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("key", "critical"),
					model.NewQueryResultCol("key", int64(1707490800000/1000/60/60/3)), // divide by 3h
					model.NewQueryResultCol("doc_count", 11),
				}},
			},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("key", "info"),
					model.NewQueryResultCol("doc_count", 102),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("key", "debug"),
					model.NewQueryResultCol("doc_count", 49),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("key", "critical"),
					model.NewQueryResultCol("doc_count", 16),
				}},
			},
		},
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__parent_count", 167),
				model.NewQueryResultCol("aggr__0__key_0", "info"),
				model.NewQueryResultCol("aggr__0__count", int64(102)),
				model.NewQueryResultCol("aggr__0__order_1", 102),
				model.NewQueryResultCol("aggr__0__1__key_0", int64(1707480000000/1000/60/60/3)),
				model.NewQueryResultCol("aggr__0__1__count", 22),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__parent_count", 167),
				model.NewQueryResultCol("aggr__0__key_0", "info"),
				model.NewQueryResultCol("aggr__0__count", int64(102)),
				model.NewQueryResultCol("aggr__0__order_1", 102),
				model.NewQueryResultCol("aggr__0__1__key_0", int64(1707490800000/1000/60/60/3)),
				model.NewQueryResultCol("aggr__0__1__count", 80),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__parent_count", 167),
				model.NewQueryResultCol("aggr__0__key_0", "debug"),
				model.NewQueryResultCol("aggr__0__count", int64(49)),
				model.NewQueryResultCol("aggr__0__order_1", 49),
				model.NewQueryResultCol("aggr__0__1__key_0", int64(1707480000000/1000/60/60/3)),
				model.NewQueryResultCol("aggr__0__1__count", 17),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__parent_count", 167),
				model.NewQueryResultCol("aggr__0__key_0", "debug"),
				model.NewQueryResultCol("aggr__0__count", int64(49)),
				model.NewQueryResultCol("aggr__0__order_1", 49),
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
				model.NewQueryResultCol("aggr__0__order_1", 16),
				model.NewQueryResultCol("aggr__0__1__key_0", int64(1707490800000/1000/60/60/3)),
				model.NewQueryResultCol("aggr__0__1__count", 11),
			}},
		},
		ExpectedSQLs: []string{
			`SELECT count() ` +
				`FROM ` + TableName + ` ` +
				`WHERE ("host.name" iLIKE '%prometheus%' ` +
				`AND ("@timestamp"<=parseDateTime64BestEffort('2024-02-09T16:36:49.940Z') ` +
				`AND "@timestamp">=parseDateTime64BestEffort('2024-02-02T16:36:49.940Z')))`,
			`WITH cte_1 AS ` +
				`(SELECT "severity" AS "cte_1_1", count() AS "cte_1_cnt" ` +
				`FROM ` + TableName + ` ` +
				`WHERE (("host.name" iLIKE '%prometheus%' AND ("@timestamp">=parseDateTime64BestEffort('2024-02-02T16:36:49.940Z') ` +
				`AND "@timestamp"<=parseDateTime64BestEffort('2024-02-09T16:36:49.940Z'))) ` +
				`AND "severity" IS NOT NULL) ` +
				`GROUP BY "severity" ` +
				`ORDER BY count() DESC, "severity" ` +
				`LIMIT 3) ` +
				`SELECT "severity", toInt64(toUnixTimestamp64Milli("@timestamp") / 10800000), count() ` +
				`FROM ` + TableName + ` ` +
				`INNER JOIN "cte_1" ON "severity" = "cte_1_1" ` +
				`WHERE (("host.name" iLIKE '%prometheus%' AND ("@timestamp">=parseDateTime64BestEffort('2024-02-02T16:36:49.940Z') ` +
				`AND "@timestamp"<=parseDateTime64BestEffort('2024-02-09T16:36:49.940Z'))) ` +
				`AND "severity" IS NOT NULL) ` +
				`GROUP BY "severity", toInt64(toUnixTimestamp64Milli("@timestamp") / 10800000), cte_1_cnt ` +
				`ORDER BY cte_1_cnt DESC, "severity", toInt64(toUnixTimestamp64Milli("@timestamp") / 10800000)`,
			`SELECT "severity", count() ` +
				`FROM ` + TableName + ` ` +
				`WHERE (("host.name" iLIKE '%prometheus%' ` +
				`AND ("@timestamp">=parseDateTime64BestEffort('2024-02-02T16:36:49.940Z') ` +
				`AND "@timestamp"<=parseDateTime64BestEffort('2024-02-09T16:36:49.940Z'))) ` +
				`AND "severity" IS NOT NULL) ` +
				`GROUP BY "severity" ` +
				`ORDER BY count() DESC, "severity" ` +
				`LIMIT 3`,
		},
		ExpectedPancakeSQL: `
			SELECT "aggr__0__parent_count", "aggr__0__key_0", "aggr__0__count",
			  "aggr__0__order_1", "aggr__0__1__key_0", "aggr__0__1__count"
			FROM (
			  SELECT "aggr__0__parent_count", "aggr__0__key_0", "aggr__0__count",
				"aggr__0__order_1", "aggr__0__1__key_0", "aggr__0__1__count",
				dense_rank() OVER (ORDER BY "aggr__0__order_1" DESC, "aggr__0__key_0" ASC)
				AS "aggr__0__order_1_rank",
				dense_rank() OVER (PARTITION BY "aggr__0__key_0" ORDER BY
				"aggr__0__1__key_0" ASC) AS "aggr__0__1__order_1_rank"
			  FROM (
				SELECT sum(count(*)) OVER () AS "aggr__0__parent_count",
				  "severity" AS "aggr__0__key_0",
				  sum(count(*)) OVER (PARTITION BY "aggr__0__key_0") AS "aggr__0__count",
				  sum(count()) OVER (PARTITION BY "aggr__0__key_0") AS "aggr__0__order_1",
				  toInt64(toUnixTimestamp64Milli("@timestamp") / 10800000) AS
				  "aggr__0__1__key_0", count(*) AS "aggr__0__1__count"
				FROM ` + TableName + `
				WHERE ("host.name" iLIKE '%prometheus%' AND ("@timestamp">=
				  parseDateTime64BestEffort('2024-02-02T16:36:49.940Z') AND "@timestamp"<=
				  parseDateTime64BestEffort('2024-02-09T16:36:49.940Z')))
				GROUP BY "severity" AS "aggr__0__key_0",
				  toInt64(toUnixTimestamp64Milli("@timestamp") / 10800000) AS
				  "aggr__0__1__key_0"))
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
											"key": 1707480000000,
											"key_as_string": "2024-02-09T12:00:00.000"
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
											"key": 1707739200000,
											"key_as_string": "2024-02-12T12:00:00.000"
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
											"key": 1707782400000,
											"key_as_string": "2024-02-13T00:00:00.000"
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
		ExpectedResults: [][]model.QueryResultRow{
			{
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", int64(1707480000000/1000/60/60/12)), model.NewQueryResultCol("order_date", "2024-02-09T17:16:48.000Z"), model.NewQueryResultCol("order_date", "2024-02-09T17:16:48.000Z")}},
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", int64(1707480000000/1000/60/60/12)), model.NewQueryResultCol("order_date", "2024-02-09T21:34:34.000Z"), model.NewQueryResultCol("order_date", "2024-02-09T21:34:34.000Z")}},
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", int64(1707739200000/1000/60/60/12)), model.NewQueryResultCol("order_date", "2024-02-12T11:38:24.000Z"), model.NewQueryResultCol("order_date", "2024-02-12T11:38:24.000Z")}},
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", int64(1707782400000/1000/60/60/12)), model.NewQueryResultCol("order_date", "2024-02-13T03:50:24.000Z"), model.NewQueryResultCol("order_date", "2024-02-13T03:50:24.000Z")}},
			},
			{
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", int64(1707480000000/1000/60/60/12)), model.NewQueryResultCol("taxful_total_price", 310.0), model.NewQueryResultCol("order_date", "2024-02-09T17:16:48.000Z")}},
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", int64(1707480000000/1000/60/60/12)), model.NewQueryResultCol("taxful_total_price", 393.0), model.NewQueryResultCol("order_date", "2024-02-09T21:34:34.000Z")}},
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", int64(1707739200000/1000/60/60/12)), model.NewQueryResultCol("taxful_total_price", 283.0), model.NewQueryResultCol("order_date", "2024-02-12T11:38:24.000Z")}},
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", int64(1707782400000/1000/60/60/12)), model.NewQueryResultCol("taxful_total_price", 301.0), model.NewQueryResultCol("order_date", "2024-02-13T03:50:24.000Z")}},
			},
			{
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", int64(1707480000000/1000/60/60/12)), model.NewQueryResultCol("doc_count", 2)}},
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", int64(1707739200000/1000/60/60/12)), model.NewQueryResultCol("doc_count", 1)}},
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", int64(1707782400000/1000/60/60/12)), model.NewQueryResultCol("doc_count", 1)}},
			},
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol("hits", uint64(4))}}},
		},
		ExpectedPancakeResults: make([]model.QueryResultRow, 0),
		ExpectedSQLs: []string{
			`SELECT toInt64(toUnixTimestamp64Milli("order_date") / 43200000), maxOrNull("order_date") AS "windowed_order_date", ` +
				`maxOrNull("order_date") AS "windowed_order_date" FROM ` +
				`(SELECT "order_date", "order_date", ROW_NUMBER() OVER ` +
				`(PARTITION BY toInt64(toUnixTimestamp64Milli("order_date") / 43200000) ` +
				`ORDER BY "order_date" ASC) AS "row_number", "taxful_total_price" FROM ` + TableName + " " +
				`WHERE (("order_date">=parseDateTime64BestEffort('2024-02-06T09:59:57.034Z') AND ` +
				`"order_date"<=parseDateTime64BestEffort('2024-02-13T09:59:57.034Z')) AND "taxful_total_price" > '250')) ` +
				`WHERE ((("order_date">=parseDateTime64BestEffort('2024-02-06T09:59:57.034Z') AND ` +
				`"order_date"<=parseDateTime64BestEffort('2024-02-13T09:59:57.034Z')) AND "taxful_total_price" > '250') AND "row_number"<=10) ` +
				`GROUP BY toInt64(toUnixTimestamp64Milli("order_date") / 43200000) ` +
				`ORDER BY toInt64(toUnixTimestamp64Milli("order_date") / 43200000)`,
			`SELECT toInt64(toUnixTimestamp64Milli("order_date") / 43200000), maxOrNull("taxful_total_price") AS "windowed_taxful_total_price", ` +
				`maxOrNull("order_date") AS "windowed_order_date" FROM ` +
				`(SELECT "taxful_total_price", "order_date", ROW_NUMBER() OVER ` +
				`(PARTITION BY toInt64(toUnixTimestamp64Milli("order_date") / 43200000) ` +
				`ORDER BY "order_date" ASC) AS "row_number" FROM ` + TableName + " " +
				`WHERE (("order_date">=parseDateTime64BestEffort('2024-02-06T09:59:57.034Z') AND ` +
				`"order_date"<=parseDateTime64BestEffort('2024-02-13T09:59:57.034Z')) AND "taxful_total_price" > '250')) ` +
				`WHERE ((("order_date">=parseDateTime64BestEffort('2024-02-06T09:59:57.034Z') AND ` +
				`"order_date"<=parseDateTime64BestEffort('2024-02-13T09:59:57.034Z')) AND "taxful_total_price" > '250') AND "row_number"<=10) ` +
				`GROUP BY toInt64(toUnixTimestamp64Milli("order_date") / 43200000) ` +
				`ORDER BY toInt64(toUnixTimestamp64Milli("order_date") / 43200000)`,
			`SELECT toInt64(toUnixTimestamp64Milli("order_date") / 43200000), count() FROM ` + TableName + " " +
				`WHERE (("order_date">=parseDateTime64BestEffort('2024-02-06T09:59:57.034Z') AND ` +
				`"order_date"<=parseDateTime64BestEffort('2024-02-13T09:59:57.034Z')) AND "taxful_total_price" > '250') ` +
				`GROUP BY toInt64(toUnixTimestamp64Milli("order_date") / 43200000) ` +
				`ORDER BY toInt64(toUnixTimestamp64Milli("order_date") / 43200000)`,
			`SELECT count() FROM ` + TableName + ` WHERE (("order_date">=parseDateTime64BestEffort('2024-02-06T09:59:57.034Z') ` +
				`AND "order_date"<=parseDateTime64BestEffort('2024-02-13T09:59:57.034Z')) AND "taxful_total_price" > '250')`,
		},
		ExpectedPancakeSQL: "TODO",
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
		ExpectedResults: [][]model.QueryResultRow{
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol("hits", uint64(262))}}},
			{
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", "hephaestus"), model.NewQueryResultCol("doc_count", uint64(30))}},
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", "poseidon"), model.NewQueryResultCol("doc_count", uint64(29))}},
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", "jupiter"), model.NewQueryResultCol("doc_count", uint64(28))}},
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", "selen"), model.NewQueryResultCol("doc_count", uint64(26))}},
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", "demeter"), model.NewQueryResultCol("doc_count", uint64(24))}},
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", "iris"), model.NewQueryResultCol("doc_count", uint64(24))}},
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", "pan"), model.NewQueryResultCol("doc_count", uint64(24))}},
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", "hades"), model.NewQueryResultCol("doc_count", uint64(22))}},
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", "hermes"), model.NewQueryResultCol("doc_count", uint64(22))}},
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", "persephone"), model.NewQueryResultCol("doc_count", uint64(21))}},
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", "below-top-10"), model.NewQueryResultCol("doc_count", uint64(12))}},
			},
		},
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__sample__count", 262),
				model.NewQueryResultCol("metric__sample__sample_count_col_0", 262),
				model.NewQueryResultCol("aggr__sample__top_values__parent_count", 262),
				model.NewQueryResultCol("aggr__sample__top_values__key_0", "hephaestus"),
				model.NewQueryResultCol("aggr__sample__top_values__count", int64(30)),
				model.NewQueryResultCol("aggr__sample__top_values__order_1", 30),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__sample__count", 262),
				model.NewQueryResultCol("metric__sample__sample_count_col_0", 262),
				model.NewQueryResultCol("aggr__sample__top_values__parent_count", 262),
				model.NewQueryResultCol("aggr__sample__top_values__key_0", "poseidon"),
				model.NewQueryResultCol("aggr__sample__top_values__count", int64(29)),
				model.NewQueryResultCol("aggr__sample__top_values__order_1", 29),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__sample__count", 262),
				model.NewQueryResultCol("metric__sample__sample_count_col_0", 262),
				model.NewQueryResultCol("aggr__sample__top_values__parent_count", 262),
				model.NewQueryResultCol("aggr__sample__top_values__key_0", "jupiter"),
				model.NewQueryResultCol("aggr__sample__top_values__count", int64(28)),
				model.NewQueryResultCol("aggr__sample__top_values__order_1", 28),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__sample__count", 262),
				model.NewQueryResultCol("metric__sample__sample_count_col_0", 262),
				model.NewQueryResultCol("aggr__sample__top_values__parent_count", 262),
				model.NewQueryResultCol("aggr__sample__top_values__key_0", "selen"),
				model.NewQueryResultCol("aggr__sample__top_values__count", int64(26)),
				model.NewQueryResultCol("aggr__sample__top_values__order_1", 26),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__sample__count", 262),
				model.NewQueryResultCol("metric__sample__sample_count_col_0", 262),
				model.NewQueryResultCol("aggr__sample__top_values__parent_count", 262),
				model.NewQueryResultCol("aggr__sample__top_values__key_0", "demeter"),
				model.NewQueryResultCol("aggr__sample__top_values__count", int64(24)),
				model.NewQueryResultCol("aggr__sample__top_values__order_1", 24),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__sample__count", 262),
				model.NewQueryResultCol("metric__sample__sample_count_col_0", 262),
				model.NewQueryResultCol("aggr__sample__top_values__parent_count", 262),
				model.NewQueryResultCol("aggr__sample__top_values__key_0", "iris"),
				model.NewQueryResultCol("aggr__sample__top_values__count", int64(24)),
				model.NewQueryResultCol("aggr__sample__top_values__order_1", 24),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__sample__count", 262),
				model.NewQueryResultCol("metric__sample__sample_count_col_0", 262),
				model.NewQueryResultCol("aggr__sample__top_values__parent_count", 262),
				model.NewQueryResultCol("aggr__sample__top_values__key_0", "pan"),
				model.NewQueryResultCol("aggr__sample__top_values__count", int64(24)),
				model.NewQueryResultCol("aggr__sample__top_values__order_1", 24),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__sample__count", 262),
				model.NewQueryResultCol("metric__sample__sample_count_col_0", 262),
				model.NewQueryResultCol("aggr__sample__top_values__parent_count", 262),
				model.NewQueryResultCol("aggr__sample__top_values__key_0", "hades"),
				model.NewQueryResultCol("aggr__sample__top_values__count", int64(22)),
				model.NewQueryResultCol("aggr__sample__top_values__order_1", 22),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__sample__count", 262),
				model.NewQueryResultCol("metric__sample__sample_count_col_0", 262),
				model.NewQueryResultCol("aggr__sample__top_values__parent_count", 262),
				model.NewQueryResultCol("aggr__sample__top_values__key_0", "hermes"),
				model.NewQueryResultCol("aggr__sample__top_values__count", int64(22)),
				model.NewQueryResultCol("aggr__sample__top_values__order_1", 22),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__sample__count", 262),
				model.NewQueryResultCol("metric__sample__sample_count_col_0", 262),
				model.NewQueryResultCol("aggr__sample__top_values__parent_count", 262),
				model.NewQueryResultCol("aggr__sample__top_values__key_0", "persephone"),
				model.NewQueryResultCol("aggr__sample__top_values__count", int64(21)),
				model.NewQueryResultCol("aggr__sample__top_values__order_1", 21),
			}},
		},
		ExpectedSQLs: []string{
			`SELECT count() FROM (SELECT 1 ` +
				`FROM ` + TableName + ` ` +
				`WHERE (("@timestamp">=parseDateTime64BestEffort('2024-01-23T11:27:16.820Z') ` +
				`AND "@timestamp"<=parseDateTime64BestEffort('2024-01-23T11:42:16.820Z')) ` +
				`AND ` + fullTextFieldName + ` iLIKE '%user%') LIMIT 3)`,
			`SELECT "host.name" AS "key", count() AS "doc_count" ` +
				`FROM (SELECT "host.name" FROM ` + TableName + ` ` +
				`WHERE (("@timestamp">=parseDateTime64BestEffort('2024-01-23T11:27:16.820Z') ` +
				`AND "@timestamp"<=parseDateTime64BestEffort('2024-01-23T11:42:16.820Z')) ` +
				`AND ` + fullTextFieldName + ` iLIKE '%user%') ` +
				`LIMIT 20000) ` +
				`GROUP BY "host.name" ` +
				`ORDER BY count() DESC`,
		},
		ExpectedPancakeSQL: `
			SELECT sum(count(*)) OVER () AS "aggr__sample__count",
			  sum(count("host.name")) OVER () AS "metric__sample__sample_count_col_0",
			  sum(count(*)) OVER () AS "aggr__sample__top_values__parent_count",
			  "host.name" AS "aggr__sample__top_values__key_0",
			  count(*) AS "aggr__sample__top_values__count",
			  count() AS "aggr__sample__top_values__order_1"
			FROM (
			  SELECT "host.name"
			  FROM ` + TableName + `
			  WHERE (("@timestamp">=parseDateTime64BestEffort('2024-01-23T11:27:16.820Z')
				AND "@timestamp"<=parseDateTime64BestEffort('2024-01-23T11:42:16.820Z')) AND
				` + fullTextFieldName + ` iLIKE '%user%')
			  LIMIT 8000)
			GROUP BY "host.name" AS "aggr__sample__top_values__key_0"
			ORDER BY "aggr__sample__top_values__order_1" DESC,
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
						"min_doc_count": 1,
						"time_zone": "Europe/Warsaw"
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
		ExpectedResults: [][]model.QueryResultRow{
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol("hits", uint64(97))}}},
			{}, // TODO non-aggregation query, maybe fill in results later: now we don't check them
			{
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", int64(1706021670000/30000)), model.NewQueryResultCol("doc_count", 2)}},
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", int64(1706021700000/30000)), model.NewQueryResultCol("doc_count", 13)}},
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", int64(1706021730000/30000)), model.NewQueryResultCol("doc_count", 14)}},
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", int64(1706021760000/30000)), model.NewQueryResultCol("doc_count", 14)}},
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", int64(1706021790000/30000)), model.NewQueryResultCol("doc_count", 15)}},
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", int64(1706021820000/30000)), model.NewQueryResultCol("doc_count", 13)}},
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", int64(1706021850000/30000)), model.NewQueryResultCol("doc_count", 15)}},
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", int64(1706021880000/30000)), model.NewQueryResultCol("doc_count", 11)}},
			},
		},
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
		ExpectedSQLs: []string{
			`SELECT count() ` +
				`FROM ` + TableName + ` ` +
				`WHERE (` + fullTextFieldName + ` iLIKE '%user%' ` +
				`AND ("@timestamp">=parseDateTime64BestEffort('2024-01-23T14:43:19.481Z') ` +
				`AND "@timestamp"<=parseDateTime64BestEffort('2024-01-23T14:58:19.481Z')))`,
			`SELECT "@timestamp" ` +
				`FROM ` + TableName + ` ` +
				`WHERE (` + fullTextFieldName + ` iLIKE '%user%' AND ("@timestamp">=parseDateTime64BestEffort('2024-01-23T14:43:19.481Z') ` +
				`AND "@timestamp"<=parseDateTime64BestEffort('2024-01-23T14:58:19.481Z'))) ` +
				`LIMIT 5`,
			`SELECT ` + timestampGroupByClause + `, count() ` +
				`FROM ` + TableName + ` ` +
				`WHERE (` + fullTextFieldName + ` iLIKE '%user%' ` +
				`AND ("@timestamp">=parseDateTime64BestEffort('2024-01-23T14:43:19.481Z') ` +
				`AND "@timestamp"<=parseDateTime64BestEffort('2024-01-23T14:58:19.481Z'))) ` +
				`GROUP BY ` + timestampGroupByClause + ` ` +
				`ORDER BY ` + timestampGroupByClause,
		},
		ExpectedPancakeSQL: `
			SELECT toInt64(toUnixTimestamp64Milli("@timestamp") / 30000) AS "aggr__0__key_0"
			  , count(*) AS "aggr__0__count"
			FROM ` + TableName + `

			WHERE (` + fullTextFieldName + ` iLIKE '%user%' AND ("@timestamp">=parseDateTime64BestEffort(
			  '2024-01-23T14:43:19.481Z') AND "@timestamp"<=parseDateTime64BestEffort(
			  '2024-01-23T14:58:19.481Z')))
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
								"fixed_interval": "60s"
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
		ExpectedResults: [][]model.QueryResultRow{
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("event.dataset", "27"),
					model.NewQueryResultCol("timestamp", int64(1713398400000/60000)),
					model.NewQueryResultCol("doc_count", 85),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("event.dataset", "27"),
					model.NewQueryResultCol("timestamp", int64(1714003200000/60000)),
					model.NewQueryResultCol("doc_count", 79),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("event.dataset", "52"),
					model.NewQueryResultCol("key_as_string", int64(1713398400000/60000)),
					model.NewQueryResultCol("doc_count", 35),
				}},
			},
			{
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", "27"), model.NewQueryResultCol("doc_count", 348)}},
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", "52"), model.NewQueryResultCol("doc_count", 188)}},
			},
		},
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__stats__parent_count", int64(4675)),
				model.NewQueryResultCol("aggr__stats__key_0", "27"),
				model.NewQueryResultCol("aggr__stats__count", int64(348)),
				model.NewQueryResultCol("aggr__stats__order_1", 348),
				model.NewQueryResultCol("aggr__stats__series__key_0", int64(1713398400000/60000)),
				model.NewQueryResultCol("aggr__stats__series__count", 85),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__stats__parent_count", int64(4675)),
				model.NewQueryResultCol("aggr__stats__key_0", "27"),
				model.NewQueryResultCol("aggr__stats__count", int64(348)),
				model.NewQueryResultCol("aggr__stats__order_1", 348),
				model.NewQueryResultCol("aggr__stats__series__key_0", int64(1714003200000/60000)),
				model.NewQueryResultCol("aggr__stats__series__count", 79),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__stats__parent_count", int64(4675)),
				model.NewQueryResultCol("aggr__stats__key_0", "52"),
				model.NewQueryResultCol("aggr__stats__count", int64(188)),
				model.NewQueryResultCol("aggr__stats__order_1", 188),
				model.NewQueryResultCol("aggr__stats__series__key_0", int64(1713398400000/60000)),
				model.NewQueryResultCol("aggr__stats__series__count", 35),
			}},
		},
		ExpectedSQLs: []string{
			`WITH cte_1 AS ` +
				`(SELECT COALESCE("event.dataset",'unknown') AS "cte_1_1", count() AS "cte_1_cnt" ` +
				`FROM ` + TableName + ` ` +
				`WHERE ("@timestamp">parseDateTime64BestEffort('2024-01-25T14:53:59.033Z') ` +
				`AND "@timestamp"<=parseDateTime64BestEffort('2024-01-25T15:08:59.033Z')) ` +
				`GROUP BY COALESCE("event.dataset",'unknown') ` +
				`ORDER BY count() DESC, COALESCE("event.dataset",'unknown') ` +
				`LIMIT 4) ` +
				`SELECT COALESCE("event.dataset",'unknown'), toInt64(toUnixTimestamp64Milli("@timestamp") / 60000), count() ` +
				`FROM ` + TableName + ` ` +
				`INNER JOIN "cte_1" ON COALESCE("event.dataset",'unknown') = "cte_1_1" ` +
				`WHERE ("@timestamp">parseDateTime64BestEffort('2024-01-25T14:53:59.033Z') ` +
				`AND "@timestamp"<=parseDateTime64BestEffort('2024-01-25T15:08:59.033Z')) ` +
				`GROUP BY COALESCE("event.dataset",'unknown'), toInt64(toUnixTimestamp64Milli("@timestamp") / 60000), cte_1_cnt ` +
				`ORDER BY cte_1_cnt DESC, COALESCE("event.dataset",'unknown'), toInt64(toUnixTimestamp64Milli("@timestamp") / 60000)`,
			`SELECT COALESCE("event.dataset",'unknown'), count() FROM ` + TableName + ` ` +
				`WHERE ("@timestamp">parseDateTime64BestEffort('2024-01-25T14:53:59.033Z') ` +
				`AND "@timestamp"<=parseDateTime64BestEffort('2024-01-25T15:08:59.033Z')) ` +
				`GROUP BY COALESCE("event.dataset",'unknown') ` +
				`ORDER BY count() DESC, COALESCE("event.dataset",'unknown') ` +
				`LIMIT 4`,
		},
		ExpectedPancakeSQL: `
			SELECT "aggr__stats__parent_count", "aggr__stats__key_0", "aggr__stats__count",
			  "aggr__stats__order_1", "aggr__stats__series__key_0",
			  "aggr__stats__series__count"
			FROM (
			  SELECT "aggr__stats__parent_count", "aggr__stats__key_0",
				"aggr__stats__count", "aggr__stats__order_1", "aggr__stats__series__key_0",
				"aggr__stats__series__count",
				dense_rank() OVER (ORDER BY "aggr__stats__order_1" DESC,
				"aggr__stats__key_0" ASC) AS "aggr__stats__order_1_rank",
				dense_rank() OVER (PARTITION BY "aggr__stats__key_0" ORDER BY
				"aggr__stats__series__key_0" ASC) AS "aggr__stats__series__order_1_rank"
			  FROM (
				SELECT sum(count(*)) OVER () AS "aggr__stats__parent_count",
				  COALESCE("event.dataset", 'unknown') AS "aggr__stats__key_0",
				  sum(count(*)) OVER (PARTITION BY "aggr__stats__key_0") AS
				  "aggr__stats__count",
				  sum(count()) OVER (PARTITION BY "aggr__stats__key_0") AS
				  "aggr__stats__order_1",
				  toInt64(toUnixTimestamp64Milli("@timestamp") / 60000) AS
				  "aggr__stats__series__key_0", count(*) AS "aggr__stats__series__count"
				FROM ` + TableName + `
				WHERE ("@timestamp">parseDateTime64BestEffort('2024-01-25T14:53:59.033Z')
				  AND "@timestamp"<=parseDateTime64BestEffort('2024-01-25T15:08:59.033Z'))
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
		ExpectedResults: [][]model.QueryResultRow{
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol(`minOrNull("@timestamp")`, nil)}}},
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol(`maxOrNull("@timestamp")`, nil)}}},
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol(`maxOrNull("@timestamp")`, nil)}}},
		},
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("metric__average_timestamp_col_0", nil),
				model.NewQueryResultCol("metric__earliest_timestamp_col_0", nil),
				model.NewQueryResultCol("metric__latest_timestamp_col_0", nil),
			}},
		},
		ExpectedSQLs: []string{
			`SELECT avgOrNull("@timestamp") ` +
				`FROM ` + TableName + ` ` +
				`WHERE ((` + fullTextFieldName + ` iLIKE '%posei%' AND "message" iLIKE '%User logged out%') AND "host.name" iLIKE '%poseidon%')`,
			`SELECT minOrNull("@timestamp") ` +
				`FROM ` + TableName + ` ` +
				`WHERE ((` + fullTextFieldName + ` iLIKE '%posei%' AND "message" iLIKE '%User logged out%') AND "host.name" iLIKE '%poseidon%')`,
			`SELECT maxOrNull("@timestamp") ` +
				`FROM ` + TableName + ` ` +
				`WHERE ((` + fullTextFieldName + ` iLIKE '%posei%' AND "message" iLIKE '%User logged out%') AND "host.name" iLIKE '%poseidon%')`,
		},
		ExpectedPancakeSQL: `
			SELECT avgOrNull("@timestamp") AS "metric__average_timestamp_col_0", minOrNull(
			  "@timestamp") AS "metric__earliest_timestamp_col_0", maxOrNull("@timestamp")
			  AS "metric__latest_timestamp_col_0"
			FROM ` + TableName + `
			WHERE ((` + fullTextFieldName + ` iLIKE '%posei%' AND "message" iLIKE '%User logged out%') AND
			  "host.name" iLIKE '%poseidon%')`,
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
						"extended_bounds": {
							"max": 1708969256351,
							"min": 1708364456351
						},
						"field": "order_date",
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
		ExpectedResults: [][]model.QueryResultRow{
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", uint64(1049))}}},
			{}, // TODO non-aggregation, but we can fill in results
			{
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", int64(19772)), model.NewQueryResultCol("1", 2221.5625)}},
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", int64(19773)), model.NewQueryResultCol("1", 11116.45703125)}},
			},
			{
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", int64(19772)), model.NewQueryResultCol("doc_count", uint64(31))}},
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", int64(19773)), model.NewQueryResultCol("doc_count", uint64(158))}},
			},
		},
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
		ExpectedSQLs: []string{
			`SELECT count() FROM (SELECT 1 ` +
				`FROM ` + TableName + ` ` +
				`WHERE ("order_date">=parseDateTime64BestEffort('2024-02-19T17:40:56.351Z') ` +
				`AND "order_date"<=parseDateTime64BestEffort('2024-02-26T17:40:56.351Z')) ` +
				`LIMIT 5)`,
			`SELECT * ` +
				`FROM ` + TableName + ` ` +
				`WHERE ("order_date">=parseDateTime64BestEffort('2024-02-19T17:40:56.351Z') ` +
				`AND "order_date"<=parseDateTime64BestEffort('2024-02-26T17:40:56.351Z')) ` +
				`LIMIT 2`,
			`SELECT ` + groupBySQL("order_date", clickhouse.DateTime64, 24*time.Hour) + `, ` +
				`sumOrNull("taxful_total_price") ` +
				`FROM ` + TableName + ` ` +
				`WHERE ("order_date">=parseDateTime64BestEffort('2024-02-19T17:40:56.351Z') ` +
				`AND "order_date"<=parseDateTime64BestEffort('2024-02-26T17:40:56.351Z')) ` +
				`GROUP BY ` + groupBySQL("order_date", clickhouse.DateTime64, 24*time.Hour) + ` ` +
				`ORDER BY ` + groupBySQL("order_date", clickhouse.DateTime64, 24*time.Hour),
			`SELECT ` + groupBySQL("order_date", clickhouse.DateTime64, 24*time.Hour) + `, count() ` +
				`FROM ` + TableName + ` ` +
				`WHERE ("order_date">=parseDateTime64BestEffort('2024-02-19T17:40:56.351Z') ` +
				`AND "order_date"<=parseDateTime64BestEffort('2024-02-26T17:40:56.351Z')) ` +
				`GROUP BY ` + groupBySQL("order_date", clickhouse.DateTime64, 24*time.Hour) + ` ` +
				`ORDER BY ` + groupBySQL("order_date", clickhouse.DateTime64, 24*time.Hour),
		},
		ExpectedPancakeSQL: `
			SELECT toInt64(toUnixTimestamp64Milli("order_date") / 86400000) AS
			  "aggr__0__key_0", count(*) AS "aggr__0__count",
			  sumOrNull("taxful_total_price") AS "metric__0__1_col_0"
			FROM ` + TableName + `
			WHERE ("order_date">=parseDateTime64BestEffort('2024-02-19T17:40:56.351Z') AND
			  "order_date"<=parseDateTime64BestEffort('2024-02-26T17:40:56.351Z'))
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
		ExpectedResults: [][]model.QueryResultRow{
			{}, // TODO non-aggregation query
			{
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", "User created"), model.NewQueryResultCol("doc_count", uint64(1700))}},
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", "User deleted"), model.NewQueryResultCol("doc_count", uint64(1781))}},
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", "User logged in"), model.NewQueryResultCol("doc_count", uint64(1757))}},
			},
		},
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__parent_count", uint64(15750)),
				model.NewQueryResultCol("aggr__0__key_0", "User created"),
				model.NewQueryResultCol("aggr__0__count", int64(1700)),
				model.NewQueryResultCol("aggr__0__order_1", 1700),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__parent_count", uint64(15750)),
				model.NewQueryResultCol("aggr__0__key_0", "User deleted"),
				model.NewQueryResultCol("aggr__0__count", int64(1781)),
				model.NewQueryResultCol("aggr__0__order_1", 1781),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__parent_count", uint64(15750)),
				model.NewQueryResultCol("aggr__0__key_0", "User logged in"),
				model.NewQueryResultCol("aggr__0__count", int64(1757)),
				model.NewQueryResultCol("aggr__0__order_1", 1757),
			}},
		},
		ExpectedSQLs: []string{
			`SELECT * ` +
				`FROM ` + TableName + ` ` +
				`WHERE ("timestamp">=parseDateTime64BestEffort('2024-02-20T19:13:33.795Z') ` +
				`AND "timestamp"<=parseDateTime64BestEffort('2024-02-21T04:01:14.920Z')) ` +
				`LIMIT 5`,
			`SELECT "message", count() ` +
				`FROM ` + TableName + ` ` +
				`WHERE (("timestamp"<=parseDateTime64BestEffort('2024-02-21T04:01:14.920Z') ` +
				`AND "timestamp">=parseDateTime64BestEffort('2024-02-20T19:13:33.795Z')) ` +
				`AND "message" IS NOT NULL) ` +
				`GROUP BY "message" ` +
				`ORDER BY count() DESC, "message" ` +
				`LIMIT 3`,
		},
		ExpectedPancakeSQL: `
			SELECT sum(count(*)) OVER () AS "aggr__0__parent_count",
			  "message" AS "aggr__0__key_0", count(*) AS "aggr__0__count",
			  count() AS "aggr__0__order_1"
			FROM ` + TableName + `
			WHERE ("timestamp">=parseDateTime64BestEffort('2024-02-20T19:13:33.795Z') AND
			  "timestamp"<=parseDateTime64BestEffort('2024-02-21T04:01:14.920Z'))
			GROUP BY "message" AS "aggr__0__key_0"
			ORDER BY "aggr__0__order_1" DESC, "aggr__0__key_0" ASC
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
						"time_zone": "Europe/Warsaw",
						"extended_bounds": {
							"min": 1708627654149,
							"max": 1709232454149
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
												"analyze_wildcard": true,
												"time_zone": "Europe/Warsaw"
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
		ExpectedResults: [][]model.QueryResultRow{
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol("hits", uint64(1051))}}},
			{},
			{
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", int64(39551)), model.NewQueryResultCol("1-metric", 0.0)}},
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", int64(39552)), model.NewQueryResultCol("1-metric", 1222.65625)}},
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", int64(39553)), model.NewQueryResultCol("1-metric", 931.96875)}},
			},
			{
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", int64(39551)), model.NewQueryResultCol("doc_count", uint64(0))}},
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", int64(39552)), model.NewQueryResultCol("doc_count", uint64(13))}},
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", int64(39553)), model.NewQueryResultCol("doc_count", uint64(9))}},
			},
			{
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", int64(39551)), model.NewQueryResultCol("doc_count", uint64(10))}},
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", int64(39552)), model.NewQueryResultCol("doc_count", uint64(83))}},
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", int64(39553)), model.NewQueryResultCol("doc_count", uint64(83))}},
			},
		},
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
		ExpectedSQLs: []string{
			`SELECT count() ` +
				`FROM ` + TableName + ` ` +
				`WHERE ("order_date">=parseDateTime64BestEffort('2024-02-22T18:47:34.149Z') ` +
				`AND "order_date"<=parseDateTime64BestEffort('2024-02-29T18:47:34.149Z'))`,
			`SELECT * ` +
				`FROM ` + TableName + ` ` +
				`WHERE ("order_date">=parseDateTime64BestEffort('2024-02-22T18:47:34.149Z') ` +
				`AND "order_date"<=parseDateTime64BestEffort('2024-02-29T18:47:34.149Z')) ` +
				`LIMIT 2`,
			`SELECT ` + groupBySQL("order_date", clickhouse.DateTime64, 12*time.Hour) + `, ` +
				`sumOrNull("taxful_total_price") ` +
				`FROM ` + TableName + ` ` +
				`WHERE (("order_date">=parseDateTime64BestEffort('2024-02-22T18:47:34.149Z') ` +
				`AND "order_date"<=parseDateTime64BestEffort('2024-02-29T18:47:34.149Z')) ` +
				`AND "products.product_name" ILIKE '%watch%') ` +
				`GROUP BY ` + groupBySQL("order_date", clickhouse.DateTime64, 12*time.Hour) + ` ` +
				`ORDER BY ` + groupBySQL("order_date", clickhouse.DateTime64, 12*time.Hour),
			`SELECT ` + groupBySQL("order_date", clickhouse.DateTime64, 12*time.Hour) + `, count() ` +
				`FROM ` + TableName + ` ` +
				`WHERE (("order_date">=parseDateTime64BestEffort('2024-02-22T18:47:34.149Z') ` +
				`AND "order_date"<=parseDateTime64BestEffort('2024-02-29T18:47:34.149Z')) ` +
				`AND "products.product_name" ILIKE '%watch%') ` +
				`GROUP BY ` + groupBySQL("order_date", clickhouse.DateTime64, 12*time.Hour) + ` ` +
				`ORDER BY ` + groupBySQL("order_date", clickhouse.DateTime64, 12*time.Hour),
			`SELECT ` + groupBySQL("order_date", clickhouse.DateTime64, 12*time.Hour) + `, count() ` +
				`FROM ` + TableName + ` ` +
				`WHERE ("order_date">=parseDateTime64BestEffort('2024-02-22T18:47:34.149Z') ` +
				`AND "order_date"<=parseDateTime64BestEffort('2024-02-29T18:47:34.149Z')) ` +
				`GROUP BY ` + groupBySQL("order_date", clickhouse.DateTime64, 12*time.Hour) + ` ` +
				`ORDER BY ` + groupBySQL("order_date", clickhouse.DateTime64, 12*time.Hour),
		},
		ExpectedPancakeSQL: `
			SELECT toInt64(toUnixTimestamp64Milli("order_date") / 43200000) AS
			  "aggr__0__key_0", count(*) AS "aggr__0__count",
			  countIf("products.product_name" ILIKE '%watch%') AS
			  "aggr__0__1-bucket__count",
			  sumOrNullIf("taxful_total_price", "products.product_name" ILIKE '%watch%') AS
			  "metric__0__1-bucket__1-metric_col_0"
			FROM ` + TableName + `
			WHERE ("order_date">=parseDateTime64BestEffort('2024-02-22T18:47:34.149Z') AND
			  "order_date"<=parseDateTime64BestEffort('2024-02-29T18:47:34.149Z'))
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
								"field": "order_date",
								"time_zone": "Europe/Warsaw"
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
		ExpectedResults: [][]model.QueryResultRow{
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol("hits", uint64(2077))}}},
			{
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", int64(19775)), model.NewQueryResultCol("value", 840.921875)}},
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", int64(19776)), model.NewQueryResultCol("value", 13902.15625)}},
			},
			{
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", int64(19775)), model.NewQueryResultCol("value", 841.921875)}},
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", int64(19776)), model.NewQueryResultCol("value", 13903.15625)}},
			},
			{
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", int64(19775)), model.NewQueryResultCol("doc_count", 10)}},
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", int64(19776)), model.NewQueryResultCol("doc_count", 166)}},
			},
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol("hits", uint64(1051))}}},
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", int64(19768)), model.NewQueryResultCol("value", 465.84375)}}},
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", int64(19768)), model.NewQueryResultCol("value", 466.84375)}}},
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", int64(19768)), model.NewQueryResultCol("doc_count", 7)}}},
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol("hits", uint64(1026))}}},
		},
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("filter_0__aggr__time_offset_split__count", int64(1051)),
				model.NewQueryResultCol("filter_0__aggr__time_offset_split__0__key_0", int64(1708560000000/86400000)),
				model.NewQueryResultCol("filter_0__aggr__time_offset_split__0__count", int64(10)),
				model.NewQueryResultCol("filter_0__metric__time_offset_split__0__1_col_0", 840.921875),
				model.NewQueryResultCol("filter_0__metric__time_offset_split__0__2_col_0", 841.921875),
				model.NewQueryResultCol("filter_1__aggr__time_offset_split__count", int64(1026)),
				model.NewQueryResultCol("filter_1__aggr__time_offset_split__0__key_0", int64(1708560000000/86400000)),
				model.NewQueryResultCol("filter_1__aggr__time_offset_split__0__count", int64(0)),
				model.NewQueryResultCol("filter_1__metric__time_offset_split__0__1_col_0", nil),
				model.NewQueryResultCol("filter_1__metric__time_offset_split__0__2_col_0", nil),
			}},

			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("filter_0__aggr__time_offset_split__count", int64(1051)),
				model.NewQueryResultCol("filter_0__aggr__time_offset_split__0__key_0", int64(1708646400000/86400000)),
				model.NewQueryResultCol("filter_0__aggr__time_offset_split__0__count", int64(166)),
				model.NewQueryResultCol("filter_0__metric__time_offset_split__0__1_col_0", 13902.156250),
				model.NewQueryResultCol("filter_0__metric__time_offset_split__0__2_col_0", 13903.156250),
				model.NewQueryResultCol("filter_1__aggr__time_offset_split__count", int64(1026)),
				model.NewQueryResultCol("filter_1__aggr__time_offset_split__0__key_0", int64(1708646400000/86400000)),
				model.NewQueryResultCol("filter_1__aggr__time_offset_split__0__count", int64(0)),
				model.NewQueryResultCol("filter_1__metric__time_offset_split__0__1_col_0", nil),
				model.NewQueryResultCol("filter_1__metric__time_offset_split__0__2_col_0", nil),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("filter_0__aggr__time_offset_split__count", int64(1051)),
				model.NewQueryResultCol("filter_0__aggr__time_offset_split__0__key_0", int64(1707955200000/86400000)),
				model.NewQueryResultCol("filter_0__aggr__time_offset_split__0__count", int64(0)),
				model.NewQueryResultCol("filter_0__metric__time_offset_split__0__1_col_0", nil),
				model.NewQueryResultCol("filter_0__metric__time_offset_split__0__2_col_0", nil),
				model.NewQueryResultCol("filter_1__aggr__time_offset_split__count", int64(1026)),
				model.NewQueryResultCol("filter_1__aggr__time_offset_split__0__key_0", int64(1707955200000/86400000)),
				model.NewQueryResultCol("filter_1__aggr__time_offset_split__0__count", int64(7)),
				model.NewQueryResultCol("filter_1__metric__time_offset_split__0__1_col_0", 465.843750),
				model.NewQueryResultCol("filter_1__metric__time_offset_split__0__2_col_0", 466.843750),
			}},
		},
		ExpectedSQLs: []string{
			`SELECT * ` +
				`FROM ` + TableName + ` ` +
				`WHERE (("order_date">=parseDateTime64BestEffort('2024-02-22T21:57:36.376Z') ` +
				`AND "order_date"<=parseDateTime64BestEffort('2024-02-29T21:57:36.376Z')) ` +
				`OR ("order_date">=parseDateTime64BestEffort('2024-02-15T21:57:36.376Z') ` +
				`AND "order_date"<=parseDateTime64BestEffort('2024-02-22T21:57:36.376Z'))) ` +
				`LIMIT 3`,
			`SELECT toInt64(toUnixTimestamp64Milli("order_date") / 86400000), sumOrNull("taxful_total_price") ` +
				`FROM ` + TableName + ` ` +
				`WHERE ((("order_date">=parseDateTime64BestEffort('2024-02-22T21:57:36.376Z') ` +
				`AND "order_date"<=parseDateTime64BestEffort('2024-02-29T21:57:36.376Z')) ` +
				`OR ("order_date">=parseDateTime64BestEffort('2024-02-15T21:57:36.376Z') ` +
				`AND "order_date"<=parseDateTime64BestEffort('2024-02-22T21:57:36.376Z'))) ` +
				`AND ("order_date">=parseDateTime64BestEffort('2024-02-22T21:57:36.376Z') ` +
				`AND "order_date"<=parseDateTime64BestEffort('2024-02-29T21:57:36.376Z'))) ` +
				`GROUP BY toInt64(toUnixTimestamp64Milli("order_date") / 86400000) ` +
				`ORDER BY toInt64(toUnixTimestamp64Milli("order_date") / 86400000)`,
			`SELECT toInt64(toUnixTimestamp64Milli("order_date") / 86400000), ` +
				`sumOrNull("taxful_total_price") ` +
				`FROM ` + TableName + ` ` +
				`WHERE ((("order_date">=parseDateTime64BestEffort('2024-02-22T21:57:36.376Z') ` +
				`AND "order_date"<=parseDateTime64BestEffort('2024-02-29T21:57:36.376Z')) ` +
				`OR ("order_date">=parseDateTime64BestEffort('2024-02-15T21:57:36.376Z') ` +
				`AND "order_date"<=parseDateTime64BestEffort('2024-02-22T21:57:36.376Z'))) ` +
				`AND ("order_date">=parseDateTime64BestEffort('2024-02-22T21:57:36.376Z') ` +
				`AND "order_date"<=parseDateTime64BestEffort('2024-02-29T21:57:36.376Z'))) ` +
				`GROUP BY toInt64(toUnixTimestamp64Milli("order_date") / 86400000) ` +
				`ORDER BY toInt64(toUnixTimestamp64Milli("order_date") / 86400000)`,
			`SELECT toInt64(toUnixTimestamp64Milli("order_date") / 86400000), count() ` +
				`FROM ` + TableName + ` ` +
				`WHERE ((("order_date">=parseDateTime64BestEffort('2024-02-22T21:57:36.376Z') ` +
				`AND "order_date"<=parseDateTime64BestEffort('2024-02-29T21:57:36.376Z')) ` +
				`OR ("order_date"<=parseDateTime64BestEffort('2024-02-22T21:57:36.376Z') ` +
				`AND "order_date">=parseDateTime64BestEffort('2024-02-15T21:57:36.376Z'))) ` +
				`AND ("order_date">=parseDateTime64BestEffort('2024-02-22T21:57:36.376Z') ` +
				`AND "order_date"<=parseDateTime64BestEffort('2024-02-29T21:57:36.376Z'))) ` +
				`GROUP BY toInt64(toUnixTimestamp64Milli("order_date") / 86400000) ` +
				`ORDER BY toInt64(toUnixTimestamp64Milli("order_date") / 86400000)`,
			`SELECT count() ` +
				`FROM ` + TableName + ` ` +
				`WHERE ((("order_date">=parseDateTime64BestEffort('2024-02-22T21:57:36.376Z') ` +
				`AND "order_date"<=parseDateTime64BestEffort('2024-02-29T21:57:36.376Z')) ` +
				`OR ("order_date">=parseDateTime64BestEffort('2024-02-15T21:57:36.376Z') ` +
				`AND "order_date"<=parseDateTime64BestEffort('2024-02-22T21:57:36.376Z'))) ` +
				`AND ("order_date"<=parseDateTime64BestEffort('2024-02-29T21:57:36.376Z') ` +
				`AND "order_date">=parseDateTime64BestEffort('2024-02-22T21:57:36.376Z')))`,
			`SELECT toInt64(toUnixTimestamp64Milli("order_date") / 86400000), ` +
				`sumOrNull("taxful_total_price") ` +
				`FROM ` + TableName + ` ` +
				`WHERE ((("order_date">=parseDateTime64BestEffort('2024-02-22T21:57:36.376Z') ` +
				`AND "order_date"<=parseDateTime64BestEffort('2024-02-29T21:57:36.376Z')) ` +
				`OR ("order_date">=parseDateTime64BestEffort('2024-02-15T21:57:36.376Z') ` +
				`AND "order_date"<=parseDateTime64BestEffort('2024-02-22T21:57:36.376Z'))) ` +
				`AND ("order_date">=parseDateTime64BestEffort('2024-02-15T21:57:36.376Z') ` +
				`AND "order_date"<=parseDateTime64BestEffort('2024-02-22T21:57:36.376Z'))) ` +
				`GROUP BY toInt64(toUnixTimestamp64Milli("order_date") / 86400000) ` +
				`ORDER BY toInt64(toUnixTimestamp64Milli("order_date") / 86400000)`,
			`SELECT toInt64(toUnixTimestamp64Milli("order_date") / 86400000), ` +
				`sumOrNull("taxful_total_price") ` +
				`FROM ` + TableName + ` ` +
				`WHERE ((("order_date"<=parseDateTime64BestEffort('2024-02-29T21:57:36.376Z') ` +
				`AND "order_date">=parseDateTime64BestEffort('2024-02-22T21:57:36.376Z')) ` +
				`OR ("order_date">=parseDateTime64BestEffort('2024-02-15T21:57:36.376Z') ` +
				`AND "order_date"<=parseDateTime64BestEffort('2024-02-22T21:57:36.376Z'))) ` +
				`AND ("order_date">=parseDateTime64BestEffort('2024-02-15T21:57:36.376Z') ` +
				`AND "order_date"<=parseDateTime64BestEffort('2024-02-22T21:57:36.376Z'))) ` +
				`GROUP BY toInt64(toUnixTimestamp64Milli("order_date") / 86400000) ` +
				`ORDER BY toInt64(toUnixTimestamp64Milli("order_date") / 86400000)`,
			`SELECT toInt64(toUnixTimestamp64Milli("order_date") / 86400000), count() ` +
				`FROM ` + TableName + ` ` +
				`WHERE ((("order_date">=parseDateTime64BestEffort('2024-02-22T21:57:36.376Z') ` +
				`AND "order_date"<=parseDateTime64BestEffort('2024-02-29T21:57:36.376Z')) ` +
				`OR ("order_date">=parseDateTime64BestEffort('2024-02-15T21:57:36.376Z') ` +
				`AND "order_date"<=parseDateTime64BestEffort('2024-02-22T21:57:36.376Z'))) ` +
				`AND ("order_date">=parseDateTime64BestEffort('2024-02-15T21:57:36.376Z') ` +
				`AND "order_date"<=parseDateTime64BestEffort('2024-02-22T21:57:36.376Z'))) ` +
				`GROUP BY toInt64(toUnixTimestamp64Milli("order_date") / 86400000) ` +
				`ORDER BY toInt64(toUnixTimestamp64Milli("order_date") / 86400000)`,
			`SELECT count() ` +
				`FROM ` + TableName + ` ` +
				`WHERE ((("order_date">=parseDateTime64BestEffort('2024-02-22T21:57:36.376Z') ` +
				`AND "order_date"<=parseDateTime64BestEffort('2024-02-29T21:57:36.376Z')) ` +
				`OR ("order_date"<=parseDateTime64BestEffort('2024-02-22T21:57:36.376Z') ` +
				`AND "order_date">=parseDateTime64BestEffort('2024-02-15T21:57:36.376Z'))) ` +
				`AND ("order_date">=parseDateTime64BestEffort('2024-02-15T21:57:36.376Z') ` +
				`AND "order_date"<=parseDateTime64BestEffort('2024-02-22T21:57:36.376Z')))`,
		},
		ExpectedPancakeSQL: `
			SELECT sum(countIf(("order_date">=parseDateTime64BestEffort(
			  '2024-02-22T21:57:36.376Z') AND "order_date"<=parseDateTime64BestEffort(
			  '2024-02-29T21:57:36.376Z')))) OVER () AS
			  "filter_0__aggr__time_offset_split__count",
			  toInt64(toUnixTimestamp64Milli("order_date") / 86400000) AS
			  "filter_0__aggr__time_offset_split__0__key_0",
			  countIf(("order_date">=parseDateTime64BestEffort('2024-02-22T21:57:36.376Z')
			  AND "order_date"<=parseDateTime64BestEffort('2024-02-29T21:57:36.376Z'))) AS
			  "filter_0__aggr__time_offset_split__0__count",
			  sumOrNullIf("taxful_total_price", ("order_date">=parseDateTime64BestEffort(
			  '2024-02-22T21:57:36.376Z') AND "order_date"<=parseDateTime64BestEffort(
			  '2024-02-29T21:57:36.376Z'))) AS
			  "filter_0__metric__time_offset_split__0__1_col_0",
			  sumOrNullIf("taxful_total_price", ("order_date">=parseDateTime64BestEffort(
			  '2024-02-22T21:57:36.376Z') AND "order_date"<=parseDateTime64BestEffort(
			  '2024-02-29T21:57:36.376Z'))) AS
			  "filter_0__metric__time_offset_split__0__2_col_0",
			  sum(countIf(("order_date">=parseDateTime64BestEffort(
			  '2024-02-15T21:57:36.376Z') AND "order_date"<=parseDateTime64BestEffort(
			  '2024-02-22T21:57:36.376Z')))) OVER () AS
			  "filter_1__aggr__time_offset_split__count",
			  toInt64(toUnixTimestamp64Milli("order_date") / 86400000) AS
			  "filter_1__aggr__time_offset_split__0__key_0",
			  countIf(("order_date">=parseDateTime64BestEffort('2024-02-15T21:57:36.376Z')
			  AND "order_date"<=parseDateTime64BestEffort('2024-02-22T21:57:36.376Z'))) AS
			  "filter_1__aggr__time_offset_split__0__count",
			  sumOrNullIf("taxful_total_price", ("order_date">=parseDateTime64BestEffort(
			  '2024-02-15T21:57:36.376Z') AND "order_date"<=parseDateTime64BestEffort(
			  '2024-02-22T21:57:36.376Z'))) AS
			  "filter_1__metric__time_offset_split__0__1_col_0",
			  sumOrNullIf("taxful_total_price", ("order_date">=parseDateTime64BestEffort(
			  '2024-02-15T21:57:36.376Z') AND "order_date"<=parseDateTime64BestEffort(
			  '2024-02-22T21:57:36.376Z'))) AS
			  "filter_1__metric__time_offset_split__0__2_col_0"
			FROM ` + TableName + `
			WHERE (("order_date">=parseDateTime64BestEffort('2024-02-22T21:57:36.376Z') AND
			  "order_date"<=parseDateTime64BestEffort('2024-02-29T21:57:36.376Z')) OR (
			  "order_date">=parseDateTime64BestEffort('2024-02-15T21:57:36.376Z') AND
			  "order_date"<=parseDateTime64BestEffort('2024-02-22T21:57:36.376Z')))
			GROUP BY toInt64(toUnixTimestamp64Milli("order_date") / 86400000) AS
			  "aggr__time_offset_split__0__key_0"
			ORDER BY "aggr__time_offset_split__0__key_0" ASC`,
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
									"max": 1709816694995,
									"min": 1709815794995
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
									"doc_count": 0,
									"key": 1709816790000,
									"key_as_string": "2024-03-07T13:06:30.000"
								},
								{
									"doc_count": 0,
									"key": 1709816805000,
									"key_as_string": "2024-03-07T13:06:45.000"
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
		ExpectedResults: [][]model.QueryResultRow{
			{
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", int64(1709816790000/15000)), model.NewQueryResultCol("doc_count", uint64(0))}},
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", int64(1709816805000/15000)), model.NewQueryResultCol("doc_count", uint64(0))}},
			},
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol("doc_count", uint64(15))}}},
		},
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__sampler__count", uint64(15)),
				model.NewQueryResultCol("aggr__sampler__eventRate__key_0", int64(1709816790000/15000)),
				model.NewQueryResultCol("aggr__sampler__eventRate__count", 0),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__sampler__count", uint64(15)),
				model.NewQueryResultCol("aggr__sampler__eventRate__key_0", int64(1709816805000/15000)),
				model.NewQueryResultCol("aggr__sampler__eventRate__count", 0),
			}},
		},
		ExpectedSQLs: []string{
			`SELECT ` + groupBySQL("@timestamp", clickhouse.DateTime64, 15*time.Second) + `, count() ` +
				`FROM ` + TableName + ` ` +
				`WHERE (toUnixTimestamp64Milli("@timestamp")>=1.709815794995e+12 ` +
				`AND toUnixTimestamp64Milli("@timestamp")<=1.709816694995e+12) ` +
				`GROUP BY ` + groupBySQL("@timestamp", clickhouse.DateTime64, 15*time.Second) + ` ` +
				`ORDER BY ` + groupBySQL("@timestamp", clickhouse.DateTime64, 15*time.Second),
			`SELECT count() FROM ` + TableName + ` ` +
				`WHERE (toUnixTimestamp64Milli("@timestamp")>=1.709815794995e+12 ` +
				`AND toUnixTimestamp64Milli("@timestamp")<=1.709816694995e+12)`,
		},
		ExpectedPancakeSQL: `
			SELECT sum(count(*)) OVER () AS "aggr__sampler__count",
			  toInt64(toUnixTimestamp64Milli("@timestamp") / 15000) AS
			  "aggr__sampler__eventRate__key_0",
			  count(*) AS "aggr__sampler__eventRate__count"
			FROM (
			  SELECT "@timestamp"
			  FROM ` + TableName + `
			  WHERE (toUnixTimestamp64Milli("@timestamp")>=1.709815794995e+12 AND
				toUnixTimestamp64Milli("@timestamp")<=1.709816694995e+12)
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
		ExpectedResults: [][]model.QueryResultRow{
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol("hits", uint64(1634))}}},
			{{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("count(bytes_gauge)", 1634),
				model.NewQueryResultCol("minOrNull(bytes_gauge)", 0),
				model.NewQueryResultCol("maxOrNull(bytes_gauge)", 19742),
				model.NewQueryResultCol("avgOrNull(bytes_gauge)", 5750.900856793146),
				model.NewQueryResultCol("sumOrNull(bytes_gauge)", 9396972),
			}}},
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol("hits", uint64(1634))}}},
			{{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("quantile_5", []float64{349.95000000000005}),
				model.NewQueryResultCol("quantile_10", []float64{1600.2}),
				model.NewQueryResultCol("quantile_15", []float64{2104.85}),
				model.NewQueryResultCol("quantile_20", []float64{2653.6000000000004}),
				model.NewQueryResultCol("quantile_25", []float64{3118.75}),
				model.NewQueryResultCol("quantile_30", []float64{3599.7}),
				model.NewQueryResultCol("quantile_35", []float64{4142.75}),
				model.NewQueryResultCol("quantile_40", []float64{4605.4}),
				model.NewQueryResultCol("quantile_45", []float64{5090.650000000001}),
				model.NewQueryResultCol("quantile_50", []float64{5574.5}),
				model.NewQueryResultCol("quantile_55", []float64{6127.450000000001}),
				model.NewQueryResultCol("quantile_60", []float64{6562.799999999999}),
				model.NewQueryResultCol("quantile_65", []float64{7006.25}),
				model.NewQueryResultCol("quantile_70", []float64{7493.5}),
				model.NewQueryResultCol("quantile_75", []float64{8078.75}),
				model.NewQueryResultCol("quantile_80", []float64{8537.800000000001}),
				model.NewQueryResultCol("quantile_85", []float64{9021.3}),
				model.NewQueryResultCol("quantile_90", []float64{9609.4}),
				model.NewQueryResultCol("quantile_95", []float64{10931.049999999983}),
				model.NewQueryResultCol("quantile_100", []float64{19742}),
			}}},
			{{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("quantile_5", []float64{349.95000000000005}),
				model.NewQueryResultCol("quantile_10", []float64{1600.2}),
				model.NewQueryResultCol("quantile_15", []float64{2104.85}),
				model.NewQueryResultCol("quantile_20", []float64{2653.6000000000004}),
				model.NewQueryResultCol("quantile_25", []float64{3118.75}),
				model.NewQueryResultCol("quantile_30", []float64{3599.7}),
				model.NewQueryResultCol("quantile_35", []float64{4142.75}),
				model.NewQueryResultCol("quantile_40", []float64{4605.4}),
				model.NewQueryResultCol("quantile_45", []float64{5090.650000000001}),
				model.NewQueryResultCol("quantile_50", []float64{5574.5}),
				model.NewQueryResultCol("quantile_55", []float64{6127.450000000001}),
				model.NewQueryResultCol("quantile_60", []float64{6562.799999999999}),
				model.NewQueryResultCol("quantile_65", []float64{7006.25}),
				model.NewQueryResultCol("quantile_70", []float64{7493.5}),
				model.NewQueryResultCol("quantile_75", []float64{8078.75}),
				model.NewQueryResultCol("quantile_80", []float64{8537.800000000001}),
				model.NewQueryResultCol("quantile_85", []float64{9021.3}),
				model.NewQueryResultCol("quantile_90", []float64{9609.4}),
				model.NewQueryResultCol("quantile_95", []float64{10931.049999999983}),
				model.NewQueryResultCol("quantile_100", []float64{19742}),
			}}},
			{
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", int64(0)), model.NewQueryResultCol("doc_count", uint64(53))}},
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", int64(15035)), model.NewQueryResultCol("doc_count", uint64(7))}},
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", int64(3350)), model.NewQueryResultCol("doc_count", uint64(4))}},
			},
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol("hits", uint64(1634))}}},
		},
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
					model.NewQueryResultCol("aggr__sample__bytes_gauge_top__order_1", int64(53)),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("aggr__sample__count", int64(1634)),
					model.NewQueryResultCol("aggr__sample__bytes_gauge_top__parent_count", int64(1634)),
					model.NewQueryResultCol("aggr__sample__bytes_gauge_top__key_0", int64(15035)),
					model.NewQueryResultCol("aggr__sample__bytes_gauge_top__count", int64(7)),
					model.NewQueryResultCol("aggr__sample__bytes_gauge_top__order_1", int64(7)),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("aggr__sample__count", int64(1634)),
					model.NewQueryResultCol("aggr__sample__bytes_gauge_top__parent_count", int64(1634)),
					model.NewQueryResultCol("aggr__sample__bytes_gauge_top__key_0", int64(3350)),
					model.NewQueryResultCol("aggr__sample__bytes_gauge_top__count", int64(4)),
					model.NewQueryResultCol("aggr__sample__bytes_gauge_top__order_1", int64(4)),
				}},
			},
		},
		ExpectedSQLs: []string{
			`SELECT count() ` +
				`FROM ` + TableName + ` ` +
				`WHERE toUnixTimestamp64Milli("timestamp")<=1.711228426749e+12 ` +
				`AND toUnixTimestamp64Milli("timestamp")>=1.709932426749e+12`,
			"SELECT count(`bytes_gauge`), minOrNull(`bytes_gauge`), maxOrNull(`bytes_gauge`), " +
				"avgOrNull(`bytes_gauge`), sumOrNull(`bytes_gauge`) " +
				`FROM ` + TableName + ` ` +
				`WHERE (toUnixTimestamp64Milli("timestamp")>=1.709932426749e+12 ` +
				`AND toUnixTimestamp64Milli("timestamp")<=1.711228426749e+12) ` +
				`AND "bytes_gauge" IS NOT NULL`,
			`SELECT count() ` +
				`FROM ` + TableName + ` ` +
				`WHERE (toUnixTimestamp64Milli("timestamp")>=1.709932426749e+12 ` +
				`AND toUnixTimestamp64Milli("timestamp")<=1.711228426749e+12) ` +
				`AND "bytes_gauge" IS NOT NULL`,
			"TODO", // too tiresome to implement the check, so for now this SQL for quantiles isn't tested
			"TODO", // too tiresome to implement the check, so for now this SQL for quantiles isn't tested
			`SELECT "bytes_gauge", count() ` +
				`FROM ` + TableName + ` ` +
				`WHERE toUnixTimestamp64Milli("timestamp")<=1.711228426749e+12 ` +
				`AND toUnixTimestamp64Milli("timestamp")>=1.709932426749e+12 ` +
				`GROUP BY "bytes_gauge" ` +
				`ORDER BY "bytes_gauge"`,
			`SELECT count() ` +
				`FROM ` + TableName + ` ` +
				`WHERE toUnixTimestamp64Milli("timestamp")>=1.709932426749e+12 ` +
				`AND toUnixTimestamp64Milli("timestamp")<=1.711228426749e+12`,
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
			  WHERE (toUnixTimestamp64Milli("timestamp")>=1.709932426749e+12 AND
				toUnixTimestamp64Milli("timestamp")<=1.711228426749e+12)
			  LIMIT 20000)`,
		ExpectedAdditionalPancakeSQLs: []string{
			`SELECT sum(count(*)) OVER () AS "aggr__sample__count",
			  sum(count(*)) OVER () AS "aggr__sample__bytes_gauge_top__parent_count",
			  "bytes_gauge" AS "aggr__sample__bytes_gauge_top__key_0",
			  count(*) AS "aggr__sample__bytes_gauge_top__count",
			  count() AS "aggr__sample__bytes_gauge_top__order_1"
			FROM (
			  SELECT "bytes_gauge"
			  FROM __quesma_table_name
			  WHERE (toUnixTimestamp64Milli("timestamp")>=1.709932426749e+12 AND
				toUnixTimestamp64Milli("timestamp")<=1.711228426749e+12)
			  LIMIT 20000)
			GROUP BY "bytes_gauge" AS "aggr__sample__bytes_gauge_top__key_0"
			ORDER BY "aggr__sample__bytes_gauge_top__order_1" DESC,
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
		ExpectedResults: [][]model.QueryResultRow{
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol("hits", uint64(1634))}}},
			{{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("value", 1),
				model.NewQueryResultCol("value", 0),
				model.NewQueryResultCol("value", 5),
				model.NewQueryResultCol("value", 6),
				model.NewQueryResultCol("value", 10),
				model.NewQueryResultCol("value", 1),
			}}},
			{{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("value", 1),
				model.NewQueryResultCol("value", 0),
				model.NewQueryResultCol("value", 5),
				model.NewQueryResultCol("value", 6),
				model.NewQueryResultCol("value", 10),
				model.NewQueryResultCol("value", 1),
			}}},
		},
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
		ExpectedSQLs: []string{
			`SELECT count() ` +
				`FROM ` + TableName + ` ` +
				`WHERE ("timestamp">=parseDateTime64BestEffort('2024-04-16T12:15:11.790Z') ` +
				`AND "timestamp"<=parseDateTime64BestEffort('2024-04-16T12:30:11.790Z'))`,
			`SELECT count(if(("bytes_gauge">=0.000000 AND "bytes_gauge"<1000.000000),1,NULL)), ` +
				`count(if(("bytes_gauge">=1000.000000 AND "bytes_gauge"<2000.000000),1,NULL)), ` +
				`count(if("bytes_gauge">=-5.500000,1,NULL)), ` +
				`count(if("bytes_gauge"<6.555000,1,NULL)), ` +
				`count(), count() FROM ` + TableName + ` WHERE ("timestamp">=parseDateTime64BestEffort('2024-04-16T12:15:11.790Z') ` +
				`AND "timestamp"<=parseDateTime64BestEffort('2024-04-16T12:30:11.790Z'))`,
			`SELECT count(if(("bytes_gauge">=0.000000 AND "bytes_gauge"<1000.000000),1,NULL)), ` +
				`count(if(("bytes_gauge">=1000.000000 AND "bytes_gauge"<2000.000000),1,NULL)), ` +
				`count(if("bytes_gauge">=-5.500000,1,NULL)), ` +
				`count(if("bytes_gauge"<6.555000,1,NULL)), ` +
				`count(), count() FROM ` + TableName + ` WHERE ("timestamp">=parseDateTime64BestEffort('2024-04-16T12:15:11.790Z') ` +
				`AND "timestamp"<=parseDateTime64BestEffort('2024-04-16T12:30:11.790Z'))`,
		},
		ExpectedPancakeSQL: `
			SELECT countIf(("bytes_gauge">=0 AND "bytes_gauge"<1000)) AS
			  "range_0__aggr__2__count",
			  countIf(("bytes_gauge">=1000 AND "bytes_gauge"<2000)) AS
			  "range_1__aggr__2__count",
			  countIf("bytes_gauge">=-5.5) AS "range_2__aggr__2__count",
			  countIf("bytes_gauge"<6.555) AS "range_3__aggr__2__count",
			  countIf("bytes_gauge" IS NOT NULL) AS "range_4__aggr__2__count"
			FROM ` + TableName + `
			WHERE ("timestamp">=parseDateTime64BestEffort('2024-04-16T12:15:11.790Z') AND
			  "timestamp"<=parseDateTime64BestEffort('2024-04-16T12:30:11.790Z'))`,
		ExpectedAdditionalPancakeSQLs: []string{`
			SELECT countIf(("bytes_gauge">=0 AND "bytes_gauge"<1000)) AS
			  "range_0__aggr__3__count",
			  countIf(("bytes_gauge">=1000 AND "bytes_gauge"<2000)) AS
			  "range_1__aggr__3__count",
			  countIf("bytes_gauge">=-5.5) AS "range_2__aggr__3__count",
			  countIf("bytes_gauge"<6.555) AS "range_3__aggr__3__count",
			  countIf("bytes_gauge" IS NOT NULL) AS "range_4__aggr__3__count"
			FROM ` + TableName + `
			WHERE ("timestamp">=parseDateTime64BestEffort('2024-04-16T12:15:11.790Z') AND
			  "timestamp"<=parseDateTime64BestEffort('2024-04-16T12:30:11.790Z'))`,
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
		ExpectedResults: [][]model.QueryResultRow{
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol("hits", uint64(1541))}}},
			{{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("doc_count", 1541),
				model.NewQueryResultCol("to", int64(1713288530)),
				model.NewQueryResultCol("doc_count", 1541),
				model.NewQueryResultCol("from", int64(1711407600)),
				model.NewQueryResultCol("to", int64(1713288530)),
				model.NewQueryResultCol("doc_count", 414),
				model.NewQueryResultCol("from", int64(1713045600)),
				model.NewQueryResultCol("value", 1541),
			}}},
		},
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("range_0__aggr__2__count", int64(1541)),
				model.NewQueryResultCol("range_1__aggr__2__count", int64(1541)),
				model.NewQueryResultCol("range_2__aggr__2__count", int(414)),
			}},
		},
		ExpectedSQLs: []string{
			`SELECT count() ` +
				`FROM ` + TableName + ` ` +
				`WHERE ("timestamp">=parseDateTime64BestEffort('2024-04-06T07:28:50.059Z') ` +
				`AND "timestamp"<=parseDateTime64BestEffort('2024-04-16T17:28:50.059Z'))`,
			`SELECT count(if("timestamp" < now(),1,NULL)), toInt64(toUnixTimestamp(now())), ` +
				`count(if(("timestamp" >= toStartOfDay(subDate(now(), INTERVAL 3 week)) AND "timestamp" < now()),1,NULL)), ` +
				`toInt64(toUnixTimestamp(toStartOfDay(subDate(now(), INTERVAL 3 week)))), ` +
				`toInt64(toUnixTimestamp(now())), count(if("timestamp" >= '2024-04-14',1,NULL)), toInt64(toUnixTimestamp('2024-04-14')), ` +
				`count() FROM ` + TableName + ` WHERE ("timestamp"<=parseDateTime64BestEffort('2024-04-16T17:28:50.059Z') ` +
				`AND "timestamp">=parseDateTime64BestEffort('2024-04-06T07:28:50.059Z'))`,
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
			WHERE ("timestamp">=parseDateTime64BestEffort('2024-04-06T07:28:50.059Z') AND
			  "timestamp"<=parseDateTime64BestEffort('2024-04-16T17:28:50.059Z'))`,
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
		ExpectedResults: [][]model.QueryResultRow{
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol("hits", uint64(1608))}}},
			{
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", "a"), model.NewQueryResultCol("doc_count", uint64(619))}},
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", "zip"), model.NewQueryResultCol("doc_count", uint64(206))}},
			},
		},
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", uint64(825)),
				model.NewQueryResultCol("aggr__2__key_0", "a"),
				model.NewQueryResultCol("aggr__2__count", uint64(619)),
				model.NewQueryResultCol("aggr__2__order_1", uint64(619)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", uint64(825)),
				model.NewQueryResultCol("aggr__2__key_0", "zip"),
				model.NewQueryResultCol("aggr__2__count", uint64(206)),
				model.NewQueryResultCol("aggr__2__order_1", uint64(206)),
			}},
		},
		ExpectedSQLs: []string{
			`SELECT count() ` +
				`FROM ` + TableName,
			`SELECT "message", count() ` +
				`FROM ` + TableName + ` ` +
				`WHERE "message" IS NOT NULL ` +
				`GROUP BY "message" ` +
				`ORDER BY count() DESC, "message" ` +
				`LIMIT 4`,
		},
		ExpectedPancakeSQL: `
			SELECT sum(count(*)) OVER () AS "aggr__2__parent_count",
			  "message" AS "aggr__2__key_0", 
			  count(*) AS "aggr__2__count",
			  count() AS "aggr__2__order_1"
			FROM ` + TableName + `
			GROUP BY "message" AS "aggr__2__key_0"
			ORDER BY "aggr__2__order_1" DESC, "aggr__2__key_0" ASC
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
		ExpectedResults: [][]model.QueryResultRow{
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol("hits", uint64(1180))}}},
			{{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("key", int64(1713571200000/79200000)),
				model.NewQueryResultCol("doc_count", 21),
			}}},
			{{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("key", int64(1713571200000/79200000)),
				model.NewQueryResultCol("doc_count", uint64(1180)),
			}}},
		},
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__timeseries__key_0", int64(1713571200000/79200000)),
				model.NewQueryResultCol("aggr__timeseries__count", 1180),
				model.NewQueryResultCol("metric__timeseries__61ca57f2-469d-11e7-af02-69e470af7417_col_0", 21),
			}},
		},
		ExpectedSQLs: []string{
			`SELECT count() FROM ` + TableName,
			`SELECT toInt64(toUnixTimestamp64Milli("@timestamp") / 79200000), count(DISTINCT "host.name") ` +
				`FROM ` + TableName + " " +
				`GROUP BY toInt64(toUnixTimestamp64Milli("@timestamp") / 79200000) ` +
				`ORDER BY toInt64(toUnixTimestamp64Milli("@timestamp") / 79200000)`,
			`SELECT toInt64(toUnixTimestamp64Milli("@timestamp") / 79200000), count() ` +
				`FROM ` + TableName + " " +
				`GROUP BY toInt64(toUnixTimestamp64Milli("@timestamp") / 79200000) ` +
				`ORDER BY toInt64(toUnixTimestamp64Milli("@timestamp") / 79200000)`,
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
		ExpectedResults: [][]model.QueryResultRow{
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol("hits", uint64(6))}}},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("key", 9100.0),
					model.NewQueryResultCol("doc_count", 1),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("key", 9700.0),
					model.NewQueryResultCol("doc_count", 2),
				}},
			},
		},
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
		ExpectedSQLs: []string{
			`SELECT count() ` +
				`FROM ` + TableName + ` ` +
				`WHERE ("timestamp">=parseDateTime64BestEffort('2024-05-10T13:47:56.077Z') ` +
				`AND "timestamp"<=parseDateTime64BestEffort('2024-05-10T14:02:56.077Z'))`,
			`SELECT floor("bytes"/100.000000)*100.000000, count() ` +
				`FROM ` + TableName + ` ` +
				`WHERE ("timestamp">=parseDateTime64BestEffort('2024-05-10T13:47:56.077Z') ` +
				`AND "timestamp"<=parseDateTime64BestEffort('2024-05-10T14:02:56.077Z')) ` +
				`GROUP BY floor("bytes"/100.000000)*100.000000 ` +
				`ORDER BY floor("bytes"/100.000000)*100.000000`,
		},
		ExpectedPancakeSQL: `
			SELECT floor("bytes"/100.000000)*100.000000 AS "aggr__2__key_0",
			  count(*) AS "aggr__2__count"
			FROM ` + TableName + `
			WHERE ("timestamp">=parseDateTime64BestEffort('2024-05-10T13:47:56.077Z') AND
			  "timestamp"<=parseDateTime64BestEffort('2024-05-10T14:02:56.077Z'))
			GROUP BY floor("bytes"/100.000000)*100.000000 AS "aggr__2__key_0"
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
						"min_doc_count": 0,
						"time_zone": "Europe/Warsaw"
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
		ExpectedResults: [][]model.QueryResultRow{
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol("hits", uint64(4))}}},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("key", int64(1715351610000/30000)),
					model.NewQueryResultCol("doc_count", 1),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("key", int64(1715351730000/30000)),
					model.NewQueryResultCol("doc_count", 2),
				}},
			},
		},
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
		ExpectedSQLs: []string{
			`SELECT count() ` +
				`FROM ` + TableName + ` ` +
				`WHERE ("timestamp">=parseDateTime64BestEffort('2024-05-10T14:29:02.900Z') ` +
				`AND "timestamp"<=parseDateTime64BestEffort('2024-05-10T14:44:02.900Z'))`,
			`SELECT toInt64(toUnixTimestamp64Milli("timestamp") / 30000), count() ` +
				`FROM ` + TableName + ` ` +
				`WHERE ("timestamp">=parseDateTime64BestEffort('2024-05-10T14:29:02.900Z') ` +
				`AND "timestamp"<=parseDateTime64BestEffort('2024-05-10T14:44:02.900Z')) ` +
				`GROUP BY toInt64(toUnixTimestamp64Milli("timestamp") / 30000) ` +
				`ORDER BY toInt64(toUnixTimestamp64Milli("timestamp") / 30000)`,
		},
		ExpectedPancakeSQL: `
			SELECT toInt64(toUnixTimestamp64Milli("timestamp") / 30000) AS "aggr__2__key_0",
			  count(*) AS "aggr__2__count"
			FROM ` + TableName + `
			WHERE ("timestamp">=parseDateTime64BestEffort('2024-05-10T14:29:02.900Z') AND
			  "timestamp"<=parseDateTime64BestEffort('2024-05-10T14:44:02.900Z'))
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
		ExpectedResults: [][]model.QueryResultRow{
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol("hits", uint64(6))}}},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol(`floor("rspContentLen" / 2000.000000) * 2000.000000`, 0.0),
					model.NewQueryResultCol("message", "a"),
					model.NewQueryResultCol("doc_count", 2),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol(`floor("rspContentLen" / 2000.000000) * 2000.000000`, 0.0),
					model.NewQueryResultCol("message", "b"),
					model.NewQueryResultCol("doc_count", 1),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol(`floor("rspContentLen" / 2000.000000) * 2000.000000`, 4000.0),
					model.NewQueryResultCol("message", "c"),
					model.NewQueryResultCol("doc_count", 1),
				}},
			},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol(`floor("rspContentLen" / 2000.000000) * 2000.000000`, 0.0),
					model.NewQueryResultCol("doc_count", 3),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol(`floor("rspContentLen" / 2000.000000) * 2000.000000`, 4000.0),
					model.NewQueryResultCol("doc_count", 1),
				}},
			},
		},
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__key_0", 0.0),
				model.NewQueryResultCol("aggr__0__count", 3),
				model.NewQueryResultCol("aggr__0__2__parent_count", 3),
				model.NewQueryResultCol("aggr__0__2__key_0", "a"),
				model.NewQueryResultCol("aggr__0__2__count", int64(2)),
				model.NewQueryResultCol("aggr__0__2__order_1", 2),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__key_0", 0.0),
				model.NewQueryResultCol("aggr__0__count", 3),
				model.NewQueryResultCol("aggr__0__2__parent_count", 3),
				model.NewQueryResultCol("aggr__0__2__key_0", "b"),
				model.NewQueryResultCol("aggr__0__2__count", int64(1)),
				model.NewQueryResultCol("aggr__0__2__order_1", 1),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__key_0", 4000.0),
				model.NewQueryResultCol("aggr__0__count", 1),
				model.NewQueryResultCol("aggr__0__2__parent_count", 1),
				model.NewQueryResultCol("aggr__0__2__key_0", "c"),
				model.NewQueryResultCol("aggr__0__2__count", int64(1)),
				model.NewQueryResultCol("aggr__0__2__order_1", 1),
			}},
		},
		ExpectedSQLs: []string{
			`SELECT count() ` +
				`FROM ` + TableName,
			`SELECT floor("rspContentLen" / 2000.000000) * 2000.000000, "message", count() ` +
				`FROM ` + TableName + ` ` +
				`GROUP BY floor("rspContentLen" / 2000.000000) * 2000.000000, "message" ` +
				`ORDER BY floor("rspContentLen" / 2000.000000) * 2000.000000, "message"`,
			`SELECT floor("rspContentLen" / 2000.000000) * 2000.000000, count() ` +
				`FROM ` + TableName + ` ` +
				`GROUP BY floor("rspContentLen" / 2000.000000) * 2000.000000 ` +
				`ORDER BY floor("rspContentLen" / 2000.000000) * 2000.000000`,
		},
		ExpectedPancakeSQL: `
			SELECT "aggr__0__key_0", "aggr__0__count", "aggr__0__2__parent_count",
			  "aggr__0__2__key_0", "aggr__0__2__count", "aggr__0__2__order_1"
			FROM (
			  SELECT "aggr__0__key_0", "aggr__0__count", "aggr__0__2__parent_count",
				"aggr__0__2__key_0", "aggr__0__2__count", "aggr__0__2__order_1",
				dense_rank() OVER (ORDER BY "aggr__0__key_0" ASC) AS "aggr__0__order_1_rank"
				,
				dense_rank() OVER (PARTITION BY "aggr__0__key_0" ORDER BY
				"aggr__0__2__order_1" DESC, "aggr__0__2__key_0" ASC) AS
				"aggr__0__2__order_1_rank"
			  FROM (
				SELECT floor("rspContentLen"/2000.000000)*2000.000000 AS "aggr__0__key_0",
				  sum(count(*)) OVER (PARTITION BY "aggr__0__key_0") AS "aggr__0__count",
				  sum(count(*)) OVER (PARTITION BY "aggr__0__key_0") AS
				  "aggr__0__2__parent_count", "message" AS "aggr__0__2__key_0",
				  count(*) AS "aggr__0__2__count", count() AS "aggr__0__2__order_1"
				FROM ` + TableName + `
				GROUP BY floor("rspContentLen"/2000.000000)*2000.000000 AS "aggr__0__key_0",
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
		ExpectedResults: [][]model.QueryResultRow{
			{
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", "Albuquerque"), model.NewQueryResultCol("doc_count", 1)}},
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", "Baltimore"), model.NewQueryResultCol("doc_count", 2)}},
			},
			{
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", "Albuquerque"), model.NewQueryResultCol("doc_count", 2)}},
			},
			{
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", "Albuquerque"), model.NewQueryResultCol("doc_count", 4)}},
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", "Atlanta"), model.NewQueryResultCol("doc_count", 5)}},
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", "Baltimore"), model.NewQueryResultCol("doc_count", 5)}},
			},
		},
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
		ExpectedSQLs: []string{
			`WITH cte_1 AS ` +
				`(SELECT "OriginCityName" AS "cte_1_1", count() AS "cte_1_cnt" ` +
				`FROM ` + TableName + ` ` +
				`WHERE "OriginCityName" IS NOT NULL ` +
				`GROUP BY "OriginCityName" ` +
				`ORDER BY "OriginCityName" ASC ` +
				`LIMIT 1000) ` +
				`SELECT "OriginCityName", count() ` +
				`FROM ` + TableName + ` ` +
				`INNER JOIN "cte_1" ON "OriginCityName" = "cte_1_1" ` +
				`WHERE ("OriginCityName" IS NOT NULL AND "FlightDelay"==true) ` +
				`GROUP BY "OriginCityName", cte_1_cnt ` +
				`ORDER BY "OriginCityName" ASC`,
			`WITH cte_1 AS ` +
				`(SELECT "OriginCityName" AS "cte_1_1", count() AS "cte_1_cnt" ` +
				`FROM ` + TableName + ` ` +
				`WHERE "OriginCityName" IS NOT NULL ` +
				`GROUP BY "OriginCityName" ` +
				`ORDER BY "OriginCityName" ASC ` +
				`LIMIT 1000) ` +
				`SELECT "OriginCityName", count() ` +
				`FROM ` + TableName + ` ` +
				`INNER JOIN "cte_1" ON "OriginCityName" = "cte_1_1" ` +
				`WHERE ("OriginCityName" IS NOT NULL AND "Cancelled"==true) ` +
				`GROUP BY "OriginCityName", cte_1_cnt ` +
				`ORDER BY "OriginCityName" ASC`,
			`SELECT "OriginCityName", count() ` +
				`FROM ` + TableName + ` ` +
				`WHERE "OriginCityName" IS NOT NULL ` +
				`GROUP BY "OriginCityName" ` +
				`ORDER BY "OriginCityName" ASC ` +
				`LIMIT 1000`,
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
									"gte": "2024-05-10T06:15:26.167Z",
									"lte": "2024-05-10T21:15:26.167Z"
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
		ExpectedResults: [][]model.QueryResultRow{
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol("hits", uint64(2167))}}},
			{
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", "Albuquerque"), model.NewQueryResultCol("doc_count", 4)}},
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", "Atlanta"), model.NewQueryResultCol("doc_count", 5)}},
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", "Baltimore"), model.NewQueryResultCol("doc_count", 5)}},
			},
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", "Albuquerque"), model.NewQueryResultCol("doc_count", 2)}}},
			{
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", "Albuquerque"), model.NewQueryResultCol("doc_count", 1)}},
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", "Baltimore"), model.NewQueryResultCol("doc_count", 2)}},
			},
			{},
		},
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__3__key_0", "a"),
				model.NewQueryResultCol("aggr__3__count", uint64(619)),
				model.NewQueryResultCol("aggr__3__order_1", uint64(619)),
				model.NewQueryResultCol("metric__3__1_col_0", uint64(619)),
				model.NewQueryResultCol("aggr__3__2__key_0", "a"),
				model.NewQueryResultCol("aggr__3__2__count", uint64(619)),
				model.NewQueryResultCol("aggr__3__2__order_1", uint64(619)),
				model.NewQueryResultCol("metric__3__2__1_col_0", uint64(619)),
			}},
		},
		ExpectedSQLs: []string{
			`SELECT count() ` +
				`FROM ` +
				`(SELECT 1 ` +
				`FROM ` + TableName + ` ` +
				`WHERE ("timestamp">=parseDateTime64BestEffort('2024-05-10T06:15:26.167Z') ` +
				`AND "timestamp"<=parseDateTime64BestEffort('2024-05-10T21:15:26.167Z')) ` +
				`LIMIT 10000)`,
			`WITH cte_1 AS ` +
				`(SELECT "geo.src" AS "cte_1_1", sumOrNull("memory") AS "cte_1_cnt" ` +
				`FROM ` + TableName + ` ` +
				`WHERE (("timestamp">=parseDateTime64BestEffort('2024-05-10T06:15:26.167Z') ` +
				`AND "timestamp"<=parseDateTime64BestEffort('2024-05-10T21:15:26.167Z')) ` +
				`AND "geo.src" IS NOT NULL) ` +
				`GROUP BY "geo.src" ` +
				`ORDER BY sumOrNull("memory") DESC, "geo.src" ` +
				`LIMIT 5) ` +
				`SELECT "geo.src", sumOrNull("memory") ` +
				`FROM ` + TableName + ` ` +
				`INNER JOIN "cte_1" ON "geo.src" = "cte_1_1" ` +
				`WHERE (("timestamp">=parseDateTime64BestEffort('2024-05-10T06:15:26.167Z') ` +
				`AND "timestamp"<=parseDateTime64BestEffort('2024-05-10T21:15:26.167Z')) ` +
				`AND "geo.src" IS NOT NULL) ` +
				`GROUP BY "geo.src", cte_1_cnt ` +
				`ORDER BY cte_1_cnt DESC, "geo.src"`,
			`WITH cte_1 AS ` +
				`(SELECT "geo.src" AS "cte_1_1", sumOrNull("memory") AS "cte_1_cnt" ` +
				`FROM ` + TableName + ` ` +
				`WHERE (("timestamp">=parseDateTime64BestEffort('2024-05-10T06:15:26.167Z') ` +
				`AND "timestamp"<=parseDateTime64BestEffort('2024-05-10T21:15:26.167Z')) ` +
				`AND "geo.src" IS NOT NULL) ` +
				`GROUP BY "geo.src" ` +
				`ORDER BY sumOrNull("memory") DESC, "geo.src" ` +
				`LIMIT 5), ` +
				`cte_2 AS ` +
				`(SELECT "geo.src" AS "cte_2_1", "machine.os" AS "cte_2_2", sumOrNull("memory") AS "cte_2_cnt" ` +
				`FROM ` + TableName + ` ` +
				`WHERE ((("timestamp">=parseDateTime64BestEffort('2024-05-10T06:15:26.167Z') ` +
				`AND "timestamp"<=parseDateTime64BestEffort('2024-05-10T21:15:26.167Z')) ` +
				`AND "geo.src" IS NOT NULL) ` +
				`AND "machine.os" IS NOT NULL) ` +
				`GROUP BY "geo.src", "machine.os" ` +
				`ORDER BY sumOrNull("memory") DESC, "machine.os" ` +
				`LIMIT 5 BY "geo.src") ` +
				`SELECT "geo.src", "machine.os", sumOrNull("memory") ` +
				`FROM ` + TableName + ` ` +
				`INNER JOIN "cte_1" ON "geo.src" = "cte_1_1" ` +
				`INNER JOIN "cte_2" ON "geo.src" = "cte_2_1" AND "machine.os" = "cte_2_2" ` +
				`WHERE ((("timestamp">=parseDateTime64BestEffort('2024-05-10T06:15:26.167Z') ` +
				`AND "timestamp"<=parseDateTime64BestEffort('2024-05-10T21:15:26.167Z')) ` +
				`AND "geo.src" IS NOT NULL) ` +
				`AND "machine.os" IS NOT NULL) ` +
				`GROUP BY "geo.src", "machine.os", cte_1_cnt, cte_2_cnt ` +
				`ORDER BY cte_1_cnt DESC, "geo.src", cte_2_cnt DESC, "machine.os"`,
			`WITH cte_1 AS ` +
				`(SELECT "geo.src" AS "cte_1_1", sumOrNull("memory") AS "cte_1_cnt" ` +
				`FROM ` + TableName + ` ` +
				`WHERE (("timestamp">=parseDateTime64BestEffort('2024-05-10T06:15:26.167Z') ` +
				`AND "timestamp"<=parseDateTime64BestEffort('2024-05-10T21:15:26.167Z')) ` +
				`AND "geo.src" IS NOT NULL) ` +
				`GROUP BY "geo.src" ` +
				`ORDER BY sumOrNull("memory") DESC, "geo.src" ` +
				`LIMIT 5) ` +
				`SELECT "geo.src", "machine.os", count() ` +
				`FROM ` + TableName + ` ` +
				`INNER JOIN "cte_1" ON "geo.src" = "cte_1_1" ` +
				`WHERE ((("timestamp">=parseDateTime64BestEffort('2024-05-10T06:15:26.167Z') ` +
				`AND "timestamp"<=parseDateTime64BestEffort('2024-05-10T21:15:26.167Z')) ` +
				`AND "geo.src" IS NOT NULL) ` +
				`AND "machine.os" IS NOT NULL) ` +
				`GROUP BY "geo.src", "machine.os", cte_1_cnt ` +
				`ORDER BY cte_1_cnt DESC, "geo.src", sumOrNull("memory") DESC, "machine.os" ` +
				`LIMIT 5 BY "geo.src"`,
			`SELECT "geo.src", count() ` +
				`FROM ` + TableName + ` ` +
				`WHERE (("timestamp">=parseDateTime64BestEffort('2024-05-10T06:15:26.167Z') ` +
				`AND "timestamp"<=parseDateTime64BestEffort('2024-05-10T21:15:26.167Z')) ` +
				`AND "geo.src" IS NOT NULL) ` +
				`GROUP BY "geo.src" ` +
				`ORDER BY sumOrNull("memory") DESC, "geo.src" ` +
				`LIMIT 5`,
		},
		ExpectedPancakeSQL: `
			SELECT "aggr__3__key_0", "aggr__3__count", "aggr__3__order_1",
			  "metric__3__1_col_0", "aggr__3__2__key_0", "aggr__3__2__count",
			  "aggr__3__2__order_1", "metric__3__2__1_col_0"
			FROM (
			  SELECT "aggr__3__key_0", "aggr__3__count", "aggr__3__order_1",
				"metric__3__1_col_0", "aggr__3__2__key_0", "aggr__3__2__count",
				"aggr__3__2__order_1", "metric__3__2__1_col_0", dense_rank() OVER (PARTITION
				 BY 1
			  ORDER BY "aggr__3__order_1" DESC, "aggr__3__key_0" ASC) AS
				"aggr__3__order_1_rank", dense_rank() OVER (PARTITION BY "aggr__3__key_0"
			  ORDER BY "aggr__3__2__order_1" DESC, "aggr__3__2__key_0" ASC) AS
				"aggr__3__2__order_1_rank"
			  FROM (
				SELECT "geo.src" AS "aggr__3__key_0", sum("aggr__3__count_part") OVER
				  (PARTITION BY "aggr__3__key_0") AS "aggr__3__count",
				  sumOrNull("aggr__3__order_1_part") OVER (PARTITION BY "aggr__3__key_0") AS
				  "aggr__3__order_1", sumOrNull("metric__3__1_col_0_part") OVER (PARTITION
				  BY "aggr__3__key_0") AS "metric__3__1_col_0", "machine.os" AS
				  "aggr__3__2__key_0", count(*) AS "aggr__3__2__count", sumOrNull("memory")
				  AS "aggr__3__2__order_1", sumOrNull("memory") AS "metric__3__2__1_col_0",
				  count(*) AS "aggr__3__count_part", sumOrNull("memory") AS
				  "aggr__3__order_1_part", sumOrNull("memory") AS "metric__3__1_col_0_part"
				FROM ` + TableName + `
				WHERE ("timestamp">=parseDateTime64BestEffort('2024-05-10T06:15:26.167Z')
				  AND "timestamp"<=parseDateTime64BestEffort('2024-05-10T21:15:26.167Z'))
				GROUP BY "geo.src" AS "aggr__3__key_0", "machine.os" AS "aggr__3__2__key_0"))
			WHERE ("aggr__3__order_1_rank"<=5 AND "aggr__3__2__order_1_rank"<=5)
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
		ExpectedResults: [][]model.QueryResultRow{
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol("hits", uint64(2167))}}},
			{
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", "Albuquerque"), model.NewQueryResultCol("doc_count", 4)}},
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", "Atlanta"), model.NewQueryResultCol("doc_count", 5)}},
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", "Baltimore"), model.NewQueryResultCol("doc_count", 5)}},
			},
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", "Albuquerque"), model.NewQueryResultCol("doc_count", 2)}}},
			{
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", "Albuquerque"), model.NewQueryResultCol("doc_count", 1)}},
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", "Baltimore"), model.NewQueryResultCol("doc_count", 2)}},
			},
		},
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__key_0", "Albuquerque"),
				model.NewQueryResultCol("aggr__2__count", 4),
				model.NewQueryResultCol("aggr__2__order_1", uint64(619)),
				model.NewQueryResultCol("metric__2__1_col_0", uint64(619)),
			}},
		},
		ExpectedSQLs: []string{
			`SELECT count() ` +
				`FROM ` + TableName + ` ` +
				`WHERE "timestamp">=parseDateTime64BestEffort('2024-03-23T07:32:06.246Z') ` +
				`AND "timestamp"<=parseDateTime64BestEffort('2024-03-30T07:32:06.246Z')`,
			``,
			``,
			``,
		},
		ExpectedPancakeSQL: `
			SELECT "machine.os" AS "aggr__2__key_0", count(*) AS "aggr__2__count",
			  count(DISTINCT "clientip") AS "aggr__2__order_1", count(DISTINCT "clientip") AS
			  "metric__2__1_col_0"
			FROM ` + TableName + `
			WHERE ("timestamp">=parseDateTime64BestEffort('2024-05-10T06:22:39.037Z') AND
			  "timestamp"<=parseDateTime64BestEffort('2024-05-10T21:22:39.037Z'))
			GROUP BY "machine.os" AS "aggr__2__key_0"
			ORDER BY "aggr__2__order_1" DESC, "aggr__2__key_0" ASC
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
		ExpectedResults: [][]model.QueryResultRow{
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol("hits", uint64(2167))}}},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol(`toInt64(toUnixTimestamp64Milli("@timestamp") / 86400000)`, int64(1715212800000/86400000)),
					model.NewQueryResultCol(`"windowed_message"`, 5),
					model.NewQueryResultCol(`minOrNull("order_date")`, "2024-05-09T23:52:48Z"),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol(`toInt64(toUnixTimestamp64Milli("@timestamp") / 86400000)`, int64(1716336000000/86400000)),
					model.NewQueryResultCol(`windowed_message`, 30),
					model.NewQueryResultCol(`minOrNull("order_date")`, "2024-05-22T10:20:38Z"),
				}},
			},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol(`toInt64(toUnixTimestamp64Milli("@timestamp") / 86400000)`, int64(1715212800000/86400000)),
					model.NewQueryResultCol(`count()`, 146),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol(`toInt64(toUnixTimestamp64Milli("@timestamp") / 86400000)`, int64(1716336000000/86400000)),
					model.NewQueryResultCol(`count()`, 58),
				}},
			},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol(`toInt64(toUnixTimestamp64Milli("@timestamp") / 86400000)`, int64(1715212800000/86400000)),
					model.NewQueryResultCol(`count()`, 146),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol(`toInt64(toUnixTimestamp64Milli("@timestamp") / 86400000)`, int64(1716336000000/86400000)),
					model.NewQueryResultCol(`count()`, 58),
				}},
			},
		},
		ExpectedPancakeResults: make([]model.QueryResultRow, 0),
		ExpectedSQLs: []string{
			`SELECT count() ` +
				`FROM ` + TableName,
			`SELECT toInt64(toUnixTimestamp64Milli("@timestamp") / 86400000), ` +
				`minOrNull("message") AS "windowed_message", ` +
				`minOrNull("order_date") AS "windowed_order_date" ` +
				`FROM (SELECT "message", "order_date", ROW_NUMBER() OVER ` +
				`(PARTITION BY toInt64(toUnixTimestamp64Milli("@timestamp") / 86400000) ` +
				`ORDER BY "order_date" DESC) ` +
				`AS "row_number" ` +
				`FROM ` + TableName + ` ` +
				`WHERE "message" IS NOT NULL) ` +
				`WHERE ("message" IS NOT NULL ` +
				`AND "row_number"<=1) ` +
				`GROUP BY toInt64(toUnixTimestamp64Milli("@timestamp") / 86400000) ` +
				`ORDER BY toInt64(toUnixTimestamp64Milli("@timestamp") / 86400000)`,
			`SELECT toInt64(toUnixTimestamp64Milli("@timestamp") / 86400000), ` +
				"count() " +
				`FROM ` + TableName + ` ` +
				`WHERE "message" IS NOT NULL ` +
				`GROUP BY toInt64(toUnixTimestamp64Milli("@timestamp") / 86400000) ` +
				`ORDER BY toInt64(toUnixTimestamp64Milli("@timestamp") / 86400000)`,
			`SELECT toInt64(toUnixTimestamp64Milli("@timestamp") / 86400000), ` +
				"count() " +
				`FROM ` + TableName + ` ` +
				`GROUP BY toInt64(toUnixTimestamp64Milli("@timestamp") / 86400000) ` +
				`ORDER BY toInt64(toUnixTimestamp64Milli("@timestamp") / 86400000)`,
		},
		ExpectedPancakeSQL: "TODO",
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
								"key": 1716333600000,
								"key_as_string": "2024-05-21T23:20:00.000"
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
								"key": 1716377400000,
								"key_as_string": "2024-05-22T11:30:00.000"
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
		ExpectedResults: [][]model.QueryResultRow{
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol("hits", uint64(122))}}},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("key", int64(1716333600000/600000)),
					model.NewQueryResultCol(`count("bytes")`, 1),
					model.NewQueryResultCol(`minOrNull("bytes")`, 7676.0),
					model.NewQueryResultCol(`maxOrNull("bytes")`, 7676.0),
					model.NewQueryResultCol(`avgOrNull("bytes")`, 7676.0),
					model.NewQueryResultCol(`sumOrNull("bytes")`, 7676.0),
					model.NewQueryResultCol(`sumOrNull("bytes"*"bytes")`, 58920976.0),
					model.NewQueryResultCol(`varPop("bytes")`, 0.0),
					model.NewQueryResultCol(`varSamp("bytes")`, nil),
					model.NewQueryResultCol(`stddevPop("bytes")`, 0.0),
					model.NewQueryResultCol(`stddevSamp("bytes")`, nil),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("key", int64(1716377400000/600000)),
					model.NewQueryResultCol(`count("bytes")`, 8),
					model.NewQueryResultCol(`minOrNull("bytes")`, 2426.0),
					model.NewQueryResultCol(`maxOrNull("bytes")`, 7708.0),
					model.NewQueryResultCol(`avgOrNull("bytes")`, 5754.375),
					model.NewQueryResultCol(`sumOrNull("bytes")`, 46035.0),
					model.NewQueryResultCol(`sumOrNull("bytes"*"bytes")`, 284895351.0),
					model.NewQueryResultCol(`varPop("bytes")`, 2499087.234375),
					model.NewQueryResultCol(`varSamp("bytes")`, 2856099.6964285714),
					model.NewQueryResultCol(`stddevPop("bytes")`, 1580.8501618986538),
					model.NewQueryResultCol(`stddevSamp("bytes")`, 1689.9999101859655),
				}},
			},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("key", int64(1716333600000/600000)),
					model.NewQueryResultCol(`count("bytes")`, 1),
					model.NewQueryResultCol(`minOrNull("bytes")`, 7676.0),
					model.NewQueryResultCol(`maxOrNull("bytes")`, 7676.0),
					model.NewQueryResultCol(`avgOrNull("bytes")`, 7676.0),
					model.NewQueryResultCol(`sumOrNull("bytes")`, 7676.0),
					model.NewQueryResultCol(`sumOrNull("bytes"*"bytes")`, 58920976.0),
					model.NewQueryResultCol(`varPop("bytes")`, 0.0),
					model.NewQueryResultCol(`varSamp("bytes")`, nil),
					model.NewQueryResultCol(`stddevPop("bytes")`, 0.0),
					model.NewQueryResultCol(`stddevSamp("bytes")`, nil),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("key", int64(1716377400000/600000)),
					model.NewQueryResultCol(`count("bytes")`, 8),
					model.NewQueryResultCol(`minOrNull("bytes")`, 2426.0),
					model.NewQueryResultCol(`maxOrNull("bytes")`, 7708.0),
					model.NewQueryResultCol(`avgOrNull("bytes")`, 5754.375),
					model.NewQueryResultCol(`sumOrNull("bytes")`, 46035.0),
					model.NewQueryResultCol(`sumOrNull("bytes"*"bytes")`, 284895351.0),
					model.NewQueryResultCol(`varPop("bytes")`, 2499087.234375),
					model.NewQueryResultCol(`varSamp("bytes")`, 2856099.6964285714),
					model.NewQueryResultCol(`stddevPop("bytes")`, 1580.8501618986538),
					model.NewQueryResultCol(`stddevSamp("bytes")`, 1689.9999101859655),
				}},
			},
			{
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", int64(1716333600000/600000)), model.NewQueryResultCol("count()", 1)}},
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", int64(1716377400000/600000)), model.NewQueryResultCol("count()", 8)}},
			},
		},
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
				model.NewQueryResultCol("metric__0__1_col_7", nil),
				model.NewQueryResultCol("metric__0__1_col_8", 0.0),
				model.NewQueryResultCol("metric__0__1_col_9", nil),
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
		ExpectedSQLs: []string{
			`SELECT count() ` +
				`FROM ` + TableName + ` ` +
				`WHERE ("timestamp">=parseDateTime64BestEffort('2024-05-21T21:35:34.210Z') ` +
				`AND "timestamp"<=parseDateTime64BestEffort('2024-05-22T12:35:34.210Z'))`,
			`SELECT toInt64(toUnixTimestamp64Milli("timestamp") / 600000), ` +
				`count("bytes"), ` +
				`minOrNull("bytes"), ` +
				`maxOrNull("bytes"), ` +
				`avgOrNull("bytes"), ` +
				`sumOrNull("bytes"), ` +
				`sumOrNull("bytes"*"bytes"), ` +
				`varPop("bytes"), ` +
				`varSamp("bytes"), ` +
				`stddevPop("bytes"), ` +
				`stddevSamp("bytes") ` +
				`FROM ` + TableName + ` ` +
				`WHERE ("timestamp">=parseDateTime64BestEffort('2024-05-21T21:35:34.210Z') ` +
				`AND "timestamp"<=parseDateTime64BestEffort('2024-05-22T12:35:34.210Z')) ` +
				`GROUP BY toInt64(toUnixTimestamp64Milli("timestamp") / 600000) ` +
				`ORDER BY toInt64(toUnixTimestamp64Milli("timestamp") / 600000)`,
			`SELECT toInt64(toUnixTimestamp64Milli("timestamp") / 600000), ` +
				`count("bytes"), ` +
				`minOrNull("bytes"), ` +
				`maxOrNull("bytes"), ` +
				`avgOrNull("bytes"), ` +
				`sumOrNull("bytes"), ` +
				`sumOrNull("bytes"*"bytes"), ` +
				`varPop("bytes"), ` +
				`varSamp("bytes"), ` +
				`stddevPop("bytes"), ` +
				`stddevSamp("bytes") ` +
				`FROM ` + TableName + ` ` +
				`WHERE ("timestamp">=parseDateTime64BestEffort('2024-05-21T21:35:34.210Z') ` +
				`AND "timestamp"<=parseDateTime64BestEffort('2024-05-22T12:35:34.210Z')) ` +
				`GROUP BY toInt64(toUnixTimestamp64Milli("timestamp") / 600000) ` +
				`ORDER BY toInt64(toUnixTimestamp64Milli("timestamp") / 600000)`,
			`SELECT toInt64(toUnixTimestamp64Milli("timestamp") / 600000), ` +
				`count() ` +
				`FROM ` + TableName + ` ` +
				`WHERE ("timestamp">=parseDateTime64BestEffort('2024-05-21T21:35:34.210Z') ` +
				`AND "timestamp"<=parseDateTime64BestEffort('2024-05-22T12:35:34.210Z')) ` +
				`GROUP BY toInt64(toUnixTimestamp64Milli("timestamp") / 600000) ` +
				`ORDER BY toInt64(toUnixTimestamp64Milli("timestamp") / 600000)`,
		},
		ExpectedPancakeSQL: `
			SELECT toInt64(toUnixTimestamp64Milli("timestamp") / 600000) AS "aggr__0__key_0"
			  , count(*) AS "aggr__0__count", count("bytes") AS "metric__0__1_col_0",
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
			FROM ` + TableName + `
			WHERE ("timestamp">=parseDateTime64BestEffort('2024-05-21T21:35:34.210Z') AND
			  "timestamp"<=parseDateTime64BestEffort('2024-05-22T12:35:34.210Z'))
			GROUP BY toInt64(toUnixTimestamp64Milli("timestamp") / 600000) AS
			  "aggr__0__key_0"
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
		ExpectedResponse: `{"response": {"aggregations":{}}}`,
		ExpectedResults: [][]model.QueryResultRow{
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol("hits", uint64(122))}}},
			{},
			{},
		},
		ExpectedPancakeResults: make([]model.QueryResultRow, 0),
		ExpectedSQLs: []string{
			`SELECT count() ` +
				`FROM ` + TableName + ` ` +
				`WHERE ("message" IS NOT NULL AND NOT ("message" iLIKE '%US%'))`,
			`WITH cte_1 AS ` +
				`(SELECT "host.name" AS "cte_1_1", count() AS "cte_1_cnt" ` +
				`FROM ` + TableName + ` ` +
				`WHERE (("message" IS NOT NULL AND NOT ("message" iLIKE '%US%')) ` +
				`AND "host.name" IS NOT NULL) ` +
				`GROUP BY "host.name" ` +
				`ORDER BY count() DESC, "host.name" ` +
				`LIMIT 10) ` +
				`SELECT "host.name", "message", count() ` +
				`FROM ` + TableName + ` ` +
				`INNER JOIN "cte_1" ON "host.name" = "cte_1_1" ` +
				`WHERE ((("message" IS NOT NULL AND NOT ("message" iLIKE '%US%')) ` +
				`AND "host.name" IS NOT NULL) ` +
				`AND "message" IS NOT NULL) ` +
				`GROUP BY "host.name", "message", cte_1_cnt ` +
				`ORDER BY cte_1_cnt DESC, "host.name", count() DESC, "message" ` +
				`LIMIT 3 BY "host.name"`,
			`SELECT "host.name", count() ` +
				`FROM ` + TableName + ` ` +
				`WHERE (("message" IS NOT NULL AND NOT ("message" iLIKE '%US%')) ` +
				`AND "host.name" IS NOT NULL) ` +
				`GROUP BY "host.name" ` +
				`ORDER BY count() DESC, "host.name" ` +
				`LIMIT 10`,
		},
		ExpectedPancakeSQL: `
			SELECT "aggr__0__parent_count", "aggr__0__key_0", "aggr__0__count",
			  "aggr__0__order_1", "aggr__0__1__parent_count", "aggr__0__1__key_0",
			  "aggr__0__1__count", "aggr__0__1__order_1"
			FROM (
			  SELECT "aggr__0__parent_count", "aggr__0__key_0", "aggr__0__count",
				"aggr__0__order_1", "aggr__0__1__parent_count", "aggr__0__1__key_0",
				"aggr__0__1__count", "aggr__0__1__order_1",
				dense_rank() OVER (ORDER BY "aggr__0__order_1" DESC, "aggr__0__key_0" ASC)
				AS "aggr__0__order_1_rank",
				dense_rank() OVER (PARTITION BY "aggr__0__key_0" ORDER BY
				"aggr__0__1__order_1" DESC, "aggr__0__1__key_0" ASC) AS
				"aggr__0__1__order_1_rank"
			  FROM (
				SELECT sum(count(*)) OVER () AS "aggr__0__parent_count",
				  "host.name" AS "aggr__0__key_0",
				  sum(count(*)) OVER (PARTITION BY "aggr__0__key_0") AS "aggr__0__count",
				  sum(count()) OVER (PARTITION BY "aggr__0__key_0") AS "aggr__0__order_1",
				  sum(count(*)) OVER (PARTITION BY "aggr__0__key_0") AS
				  "aggr__0__1__parent_count", "message" AS "aggr__0__1__key_0",
				  count(*) AS "aggr__0__1__count", count() AS "aggr__0__1__order_1"
				FROM ` + TableName + `
				WHERE ("message" IS NOT NULL AND NOT ("message" iLIKE '%US%'))
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
		ExpectedResponse: `{"response": {"aggregations":{}}}`,
		ExpectedResults: [][]model.QueryResultRow{
			{},
			{},
			{},
		},
		ExpectedPancakeResults: make([]model.QueryResultRow, 0),
		ExpectedSQLs: []string{
			`WITH cte_1 AS ` +
				`(SELECT "host.name" AS "cte_1_1", count() AS "cte_1_cnt" ` +
				`FROM ` + TableName + ` ` +
				`WHERE (("message" IS NOT NULL AND NOT ("message" iLIKE '%US%')) ` +
				`AND "host.name" IS NOT NULL) ` +
				`GROUP BY "host.name" ` +
				`ORDER BY count() DESC, "host.name" ` +
				`LIMIT 10), ` +
				`cte_2 AS ` +
				`(SELECT "host.name" AS "cte_2_1", "message" AS "cte_2_2", count() AS "cte_2_cnt" ` +
				`FROM ` + TableName + ` ` +
				`WHERE ((("message" IS NOT NULL AND NOT ("message" iLIKE '%US%')) ` +
				`AND "host.name" IS NOT NULL) ` +
				`AND "message" IS NOT NULL) ` +
				`GROUP BY "host.name", "message" ` +
				`ORDER BY count() DESC, "message" ` +
				`LIMIT 3 BY "host.name") ` +
				`SELECT "host.name", "message", "message", count() ` +
				`FROM ` + TableName + ` ` +
				`INNER JOIN "cte_1" ON "host.name" = "cte_1_1" ` +
				`INNER JOIN "cte_2" ON "host.name" = "cte_2_1" AND "message" = "cte_2_2" ` +
				`WHERE (((("message" IS NOT NULL AND NOT ("message" iLIKE '%US%')) ` +
				`AND "host.name" IS NOT NULL) ` +
				`AND "message" IS NOT NULL) ` +
				`AND "message" IS NOT NULL) ` +
				`GROUP BY "host.name", "message", "message", cte_1_cnt, cte_2_cnt ` +
				`ORDER BY cte_1_cnt DESC, "host.name", cte_2_cnt DESC, "message", count() DESC, "message" ` +
				`LIMIT 3 BY "host.name", "message"`,
			`WITH cte_1 AS ` +
				`(SELECT "host.name" AS "cte_1_1", count() AS "cte_1_cnt" ` +
				`FROM ` + TableName + ` ` +
				`WHERE (("message" IS NOT NULL AND NOT ("message" iLIKE '%US%')) ` +
				`AND "host.name" IS NOT NULL) ` +
				`GROUP BY "host.name" ` +
				`ORDER BY count() DESC, "host.name" ` +
				`LIMIT 10) ` +
				`SELECT "host.name", "message", count() ` +
				`FROM ` + TableName + ` ` +
				`INNER JOIN "cte_1" ON "host.name" = "cte_1_1" ` +
				`WHERE ((("message" IS NOT NULL AND NOT ("message" iLIKE '%US%')) ` +
				`AND "host.name" IS NOT NULL) ` +
				`AND "message" IS NOT NULL) ` +
				`GROUP BY "host.name", "message", cte_1_cnt ` +
				`ORDER BY cte_1_cnt DESC, "host.name", count() DESC, "message" ` +
				`LIMIT 3 BY "host.name"`,
			`SELECT "host.name", count() ` +
				`FROM ` + TableName + ` ` +
				`WHERE (("message" IS NOT NULL AND NOT ("message" iLIKE '%US%')) ` +
				`AND "host.name" IS NOT NULL) ` +
				`GROUP BY "host.name" ` +
				`ORDER BY count() DESC, "host.name" ` +
				`LIMIT 10`,
		},
		ExpectedPancakeSQL: `
			SELECT "aggr__0__parent_count", "aggr__0__key_0", "aggr__0__count",
			  "aggr__0__order_1", "aggr__0__1__parent_count", "aggr__0__1__key_0",
			  "aggr__0__1__count", "aggr__0__1__order_1", "aggr__0__1__2__parent_count",
			  "aggr__0__1__2__key_0", "aggr__0__1__2__count", "aggr__0__1__2__order_1"
			FROM (
			  SELECT "aggr__0__parent_count", "aggr__0__key_0", "aggr__0__count",
				"aggr__0__order_1", "aggr__0__1__parent_count", "aggr__0__1__key_0",
				"aggr__0__1__count", "aggr__0__1__order_1", "aggr__0__1__2__parent_count",
				"aggr__0__1__2__key_0", "aggr__0__1__2__count", "aggr__0__1__2__order_1",
				dense_rank() OVER (ORDER BY "aggr__0__order_1" DESC, "aggr__0__key_0" ASC)
				AS "aggr__0__order_1_rank",
				dense_rank() OVER (PARTITION BY "aggr__0__key_0" ORDER BY
				"aggr__0__1__order_1" DESC, "aggr__0__1__key_0" ASC) AS
				"aggr__0__1__order_1_rank",
				dense_rank() OVER (PARTITION BY "aggr__0__key_0", "aggr__0__1__key_0" ORDER
				BY "aggr__0__1__2__order_1" DESC, "aggr__0__1__key_0" ASC,
				"aggr__0__1__2__key_0" ASC) AS "aggr__0__1__2__order_1_rank"
			  FROM (
				SELECT sum(count(*)) OVER () AS "aggr__0__parent_count",
				  "host.name" AS "aggr__0__key_0",
				  sum(count(*)) OVER (PARTITION BY "aggr__0__key_0") AS "aggr__0__count",
				  sum(count()) OVER (PARTITION BY "aggr__0__key_0") AS "aggr__0__order_1",
				  sum(count(*)) OVER (PARTITION BY "aggr__0__key_0") AS
				  "aggr__0__1__parent_count", "message" AS "aggr__0__1__key_0",
				  sum(count(*)) OVER (PARTITION BY "aggr__0__key_0", "aggr__0__1__key_0") AS
				  "aggr__0__1__count",
				  sum(count()) OVER (PARTITION BY "aggr__0__key_0", "aggr__0__1__key_0") AS
				  "aggr__0__1__order_1",
				  sum(count(*)) OVER (PARTITION BY "aggr__0__key_0", "aggr__0__1__key_0") AS
				  "aggr__0__1__2__parent_count", "message" AS "aggr__0__1__2__key_0",
				  count(*) AS "aggr__0__1__2__count", count() AS "aggr__0__1__2__order_1"
				FROM ` + TableName + `
				WHERE ("message" IS NOT NULL AND NOT ("message" iLIKE '%US%'))
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
		ExpectedResponse: `{"response": {"aggregations":{}}}`,
		ExpectedResults: [][]model.QueryResultRow{
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol("hits", uint64(122))}}},
			{},
			{},
		},
		ExpectedPancakeResults: make([]model.QueryResultRow, 0),
		ExpectedSQLs: []string{
			`SELECT count() ` +
				`FROM ` + TableName + ` ` +
				`WHERE ("message" IS NOT NULL AND NOT ("message" iLIKE '%US%'))`,
			`WITH cte_1 AS ` +
				`(SELECT "host.name" AS "cte_1_1", count() AS "cte_1_cnt" ` +
				`FROM ` + TableName + ` ` +
				`WHERE (("message" IS NOT NULL AND NOT ("message" iLIKE '%US%')) ` +
				`AND "host.name" IS NOT NULL) ` +
				`GROUP BY "host.name" ` +
				`ORDER BY count() DESC, "host.name" ` +
				`LIMIT 8) ` +
				`SELECT "host.name", "FlightDelayMin", count() ` +
				`FROM ` + TableName + ` ` +
				`INNER JOIN "cte_1" ON "host.name" = "cte_1_1" ` +
				`WHERE (("message" IS NOT NULL AND NOT ("message" iLIKE '%US%')) ` +
				`AND "host.name" IS NOT NULL) ` +
				`GROUP BY "host.name", "FlightDelayMin", cte_1_cnt ` +
				`ORDER BY cte_1_cnt DESC, "host.name", "FlightDelayMin"`,
			`SELECT "host.name", count() ` +
				`FROM ` + TableName + ` ` +
				`WHERE (("message" IS NOT NULL AND NOT ("message" iLIKE '%US%')) ` +
				`AND "host.name" IS NOT NULL) ` +
				`GROUP BY "host.name" ` +
				`ORDER BY count() DESC, "host.name" ` +
				`LIMIT 8`,
		},
		ExpectedPancakeSQL: `
			SELECT "aggr__0__parent_count", "aggr__0__key_0", "aggr__0__count",
			  "aggr__0__order_1", "aggr__0__1__key_0", "aggr__0__1__count"
			FROM (
			  SELECT "aggr__0__parent_count", "aggr__0__key_0", "aggr__0__count",
				"aggr__0__order_1", "aggr__0__1__key_0", "aggr__0__1__count",
				dense_rank() OVER (ORDER BY "aggr__0__order_1" DESC, "aggr__0__key_0" ASC)
				AS "aggr__0__order_1_rank",
				dense_rank() OVER (PARTITION BY "aggr__0__key_0" ORDER BY
				"aggr__0__1__key_0" ASC) AS "aggr__0__1__order_1_rank"
			  FROM (
				SELECT sum(count(*)) OVER () AS "aggr__0__parent_count",
				  "host.name" AS "aggr__0__key_0",
				  sum(count(*)) OVER (PARTITION BY "aggr__0__key_0") AS "aggr__0__count",
				  sum(count()) OVER (PARTITION BY "aggr__0__key_0") AS "aggr__0__order_1",
				  "FlightDelayMin" AS "aggr__0__1__key_0", count(*) AS "aggr__0__1__count"
				FROM ` + TableName + `
				WHERE ("message" IS NOT NULL AND NOT ("message" iLIKE '%US%'))
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
							"bucketSize":     3600.000000,
							"intervalString": "3600s",
							"seriesId":       "61ca57f1-469d-11e7-af02-69e470af7417",
							"timeField":      "timestamp"
						}
					}
				}
			}
		}`,
		ExpectedResults: [][]model.QueryResultRow{
			{},
			{},
		},
		ExpectedPancakeResults: make([]model.QueryResultRow, 0),
		ExpectedSQLs: []string{
			`WITH cte_1 AS ` +
				`(SELECT "host.name" AS "cte_1_1", count() AS "cte_1_cnt" ` +
				`FROM ` + TableName + ` ` +
				`WHERE (("message" IS NOT NULL AND NOT ("message" iLIKE '%US%')) ` +
				`AND "host.name" IS NOT NULL) ` +
				`GROUP BY "host.name" ` +
				`ORDER BY count() DESC, "host.name" ` +
				`LIMIT 10) ` +
				`SELECT "host.name", "FlightDelayMin", count() ` +
				`FROM ` + TableName + ` ` +
				`INNER JOIN "cte_1" ON "host.name" = "cte_1_1" ` +
				`WHERE (("message" IS NOT NULL AND NOT ("message" iLIKE '%US%')) ` +
				`AND "host.name" IS NOT NULL) ` +
				`GROUP BY "host.name", "FlightDelayMin", cte_1_cnt ` +
				`ORDER BY cte_1_cnt DESC, "host.name", "FlightDelayMin"`,
			`SELECT "host.name", count() ` +
				`FROM ` + TableName + ` ` +
				`WHERE (("message" IS NOT NULL AND NOT ("message" iLIKE '%US%')) ` +
				`AND "host.name" IS NOT NULL) ` +
				`GROUP BY "host.name" ` +
				`ORDER BY count() DESC, "host.name" ` +
				`LIMIT 10`,
		},
		ExpectedPancakeSQL: `
			SELECT "aggr__0__parent_count", "aggr__0__key_0", "aggr__0__count",
			  "aggr__0__order_1", "aggr__0__1__key_0", "aggr__0__1__count"
			FROM (
			  SELECT "aggr__0__parent_count", "aggr__0__key_0", "aggr__0__count",
				"aggr__0__order_1", "aggr__0__1__key_0", "aggr__0__1__count",
				dense_rank() OVER (ORDER BY "aggr__0__order_1" DESC, "aggr__0__key_0" ASC)
				AS "aggr__0__order_1_rank",
				dense_rank() OVER (PARTITION BY "aggr__0__key_0" ORDER BY
				"aggr__0__1__key_0" ASC) AS "aggr__0__1__order_1_rank"
			  FROM (
				SELECT sum(count(*)) OVER () AS "aggr__0__parent_count",
				  "host.name" AS "aggr__0__key_0",
				  sum(count(*)) OVER (PARTITION BY "aggr__0__key_0") AS "aggr__0__count",
				  sum(count()) OVER (PARTITION BY "aggr__0__key_0") AS "aggr__0__order_1",
				  "FlightDelayMin" AS "aggr__0__1__key_0", count(*) AS "aggr__0__1__count"
				FROM ` + TableName + `
				WHERE ("message" IS NOT NULL AND NOT ("message" iLIKE '%US%'))
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
		ExpectedResponse: `{"response": {"aggregations":{}}}`,
		ExpectedResults: [][]model.QueryResultRow{
			{},
			{},
		},
		ExpectedPancakeResults: make([]model.QueryResultRow, 0),
		ExpectedSQLs: []string{
			`WITH cte_1 AS ` +
				`(SELECT "host.name" AS "cte_1_1", count() AS "cte_1_cnt" ` +
				`FROM ` + TableName + ` ` +
				`WHERE (("message" IS NOT NULL AND NOT ("message" iLIKE '%US%')) ` +
				`AND "host.name" IS NOT NULL) ` +
				`GROUP BY "host.name" ` +
				`ORDER BY count() DESC, "host.name" ` +
				`LIMIT 10) ` +
				`SELECT "host.name", "FlightDelayMin", count() ` +
				`FROM ` + TableName + ` ` +
				`INNER JOIN "cte_1" ON "host.name" = "cte_1_1" ` +
				`WHERE (("message" IS NOT NULL AND NOT ("message" iLIKE '%US%')) ` +
				`AND "host.name" IS NOT NULL) ` +
				`GROUP BY "host.name", "FlightDelayMin", cte_1_cnt ` +
				`ORDER BY cte_1_cnt DESC, "host.name", "FlightDelayMin"`,
			`SELECT "host.name", count() ` +
				`FROM ` + TableName + ` ` +
				`WHERE (("message" IS NOT NULL AND NOT ("message" iLIKE '%US%')) ` +
				`AND "host.name" IS NOT NULL) ` +
				`GROUP BY "host.name" ` +
				`ORDER BY count() DESC, "host.name" ` +
				`LIMIT 10`,
		},
		ExpectedPancakeSQL: `
			SELECT "aggr__0__parent_count", "aggr__0__key_0", "aggr__0__count",
			  "aggr__0__order_1", "aggr__0__1__key_0", "aggr__0__1__count"
			FROM (
			  SELECT "aggr__0__parent_count", "aggr__0__key_0", "aggr__0__count",
				"aggr__0__order_1", "aggr__0__1__key_0", "aggr__0__1__count",
				dense_rank() OVER (ORDER BY "aggr__0__order_1" DESC, "aggr__0__key_0" ASC)
				AS "aggr__0__order_1_rank",
				dense_rank() OVER (PARTITION BY "aggr__0__key_0" ORDER BY
				"aggr__0__1__key_0" ASC) AS "aggr__0__1__order_1_rank"
			  FROM (
				SELECT sum(count(*)) OVER () AS "aggr__0__parent_count",
				  "host.name" AS "aggr__0__key_0",
				  sum(count(*)) OVER (PARTITION BY "aggr__0__key_0") AS "aggr__0__count",
				  sum(count()) OVER (PARTITION BY "aggr__0__key_0") AS "aggr__0__order_1",
				  "FlightDelayMin" AS "aggr__0__1__key_0", count(*) AS "aggr__0__1__count"
				FROM ` + TableName + `
				WHERE ("message" IS NOT NULL AND NOT ("message" iLIKE '%US%'))
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
		ExpectedResults: [][]model.QueryResultRow{
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol("hits", uint64(6018))}}},
			{},
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol("message", "User updated")}}},
		},
		ExpectedPancakeResults: make([]model.QueryResultRow, 0),
		ExpectedSQLs: []string{
			`SELECT count() ` +
				`FROM ` + TableName,
			`SELECT "message" ` +
				`FROM ` + TableName + ` ` +
				`LIMIT 1`,
			`SELECT "message" ` +
				`FROM ` + TableName + ` ` +
				`LIMIT 2`,
		},
		ExpectedPancakeSQL: "TODO",
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
		ExpectedResults: [][]model.QueryResultRow{
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol("hits", uint64(6018))}}},
			{},
			{{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("message", "User updated"),
				model.NewQueryResultCol("timestamp", "stamp"),
			}}},
		},
		ExpectedPancakeResults: make([]model.QueryResultRow, 0),
		ExpectedSQLs: []string{
			`SELECT count() ` +
				`FROM ` + TableName,
			`SELECT "message", "timestamp" ` +
				`FROM ` + TableName + ` ` +
				`ORDER BY "timestamp" DESC ` +
				`LIMIT 1`,
			`SELECT "message", "timestamp" ` +
				`FROM ` + TableName + ` ` +
				`ORDER BY "timestamp" DESC ` +
				`LIMIT 1`,
		},
		ExpectedPancakeSQL: "TODO",
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
		ExpectedResponse: `{"aggregations": {}}`,
		ExpectedResults: [][]model.QueryResultRow{
			{},
			{},
			{},
		},
		ExpectedPancakeResults: make([]model.QueryResultRow, 0),
		ExpectedSQLs: []string{
			`SELECT count() ` +
				`FROM (SELECT 1 ` +
				`FROM ` + TableName + ` ` +
				`WHERE NOT ((("abc">=0 AND "abc"<600) OR "type" iLIKE '%def%')) ` +
				`LIMIT 10000)`,
			`WITH cte_1 AS ` +
				`(SELECT "name" AS "cte_1_1", sumOrNull("total") AS "cte_1_cnt" ` +
				`FROM ` + TableName + ` ` +
				`WHERE (NOT ((("abc">=0 AND "abc"<600) OR "type" iLIKE '%def%')) ` +
				`AND "name" IS NOT NULL) ` +
				`GROUP BY "name" ` +
				`ORDER BY sumOrNull("total") DESC, "name" ` +
				`LIMIT 10) ` +
				`SELECT "name", sumOrNull("total") ` +
				`FROM ` + TableName + ` ` +
				`INNER JOIN "cte_1" ON "name" = "cte_1_1" ` +
				`WHERE (NOT ((("abc">=0 AND "abc"<600) OR "type" iLIKE '%def%')) ` +
				`AND "name" IS NOT NULL) ` +
				`GROUP BY "name", cte_1_cnt ` +
				`ORDER BY cte_1_cnt DESC, "name"`,
			`SELECT "name", count() ` +
				`FROM ` + TableName + ` ` +
				`WHERE (NOT ((("abc">=0 AND "abc"<600) OR "type" iLIKE '%def%')) ` +
				`AND "name" IS NOT NULL) ` +
				`GROUP BY "name" ` +
				`ORDER BY sumOrNull("total") DESC, "name" ` +
				`LIMIT 10`,
		},
		ExpectedPancakeSQL: `
			SELECT sum(count(*)) OVER () AS "aggr__2__parent_count",
			  "name" AS "aggr__2__key_0", 
			  count(*) AS "aggr__2__count",
  			  sumOrNull("total") AS "aggr__2__order_1",
  			  sumOrNull("total") AS "metric__2__1_col_0"
			FROM ` + TableName + `
			WHERE NOT ((("abc">=0 AND "abc"<600) OR "type" iLIKE '%def%'))
			GROUP BY "name" AS "aggr__2__key_0"
			ORDER BY "aggr__2__order_1" DESC, "aggr__2__key_0" ASC
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
		ExpectedResponse: `{"response": {"aggregations":{}}}`,
		ExpectedResults: [][]model.QueryResultRow{
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol("hits", uint64(122))}}},
			{},
			{},
		},
		ExpectedPancakeResults: make([]model.QueryResultRow, 0),
		ExpectedSQLs: []string{
			`SELECT count() ` +
				`FROM ` + TableName,
			`WITH cte_1 AS ` +
				`(SELECT "OriginAirportID" AS "cte_1_1", count() AS "cte_1_cnt" ` +
				`FROM ` + TableName + ` ` +
				`WHERE "OriginAirportID" IS NOT NULL ` +
				`GROUP BY "OriginAirportID" ` +
				`ORDER BY count() DESC, "OriginAirportID" ` +
				`LIMIT 10) ` +
				`SELECT "OriginAirportID", "DestAirportID", count() ` +
				`FROM ` + TableName + ` ` +
				`INNER JOIN "cte_1" ON "OriginAirportID" = "cte_1_1" ` +
				`WHERE ("OriginAirportID" IS NOT NULL AND "DestAirportID" IS NOT NULL) ` +
				`GROUP BY "OriginAirportID", "DestAirportID", cte_1_cnt ` +
				`ORDER BY cte_1_cnt DESC, "OriginAirportID", count() DESC, "DestAirportID" ` +
				`LIMIT 3 BY "OriginAirportID"`,
			`SELECT "OriginAirportID", count() ` +
				`FROM ` + TableName + ` ` +
				`WHERE "OriginAirportID" IS NOT NULL ` +
				`GROUP BY "OriginAirportID" ` +
				`ORDER BY count() DESC, "OriginAirportID" ` +
				`LIMIT 10`,
		},
		ExpectedPancakeSQL: `
			SELECT "aggr__0__parent_count", "aggr__0__key_0", "aggr__0__count",
			  "aggr__0__order_1", "aggr__0__1__parent_count", "aggr__0__1__key_0",
			  "aggr__0__1__count", "aggr__0__1__order_1"
			FROM (
			  SELECT "aggr__0__parent_count", "aggr__0__key_0", "aggr__0__count",
				"aggr__0__order_1", "aggr__0__1__parent_count", "aggr__0__1__key_0",
				"aggr__0__1__count", "aggr__0__1__order_1",
				dense_rank() OVER (ORDER BY "aggr__0__order_1" DESC, "aggr__0__key_0" ASC)
				AS "aggr__0__order_1_rank",
				dense_rank() OVER (PARTITION BY "aggr__0__key_0" ORDER BY
				"aggr__0__1__order_1" DESC, "aggr__0__1__key_0" ASC) AS
				"aggr__0__1__order_1_rank"
			  FROM (
				SELECT sum(count(*)) OVER () AS "aggr__0__parent_count",
				  "OriginAirportID" AS "aggr__0__key_0",
				  sum(count(*)) OVER (PARTITION BY "aggr__0__key_0") AS "aggr__0__count",
				  sum(count()) OVER (PARTITION BY "aggr__0__key_0") AS "aggr__0__order_1",
				  sum(count(*)) OVER (PARTITION BY "aggr__0__key_0") AS
				  "aggr__0__1__parent_count", "DestAirportID" AS "aggr__0__1__key_0",
				  count(*) AS "aggr__0__1__count", count() AS "aggr__0__1__order_1"
				FROM ` + TableName + `
				GROUP BY "OriginAirportID" AS "aggr__0__key_0",
				  "DestAirportID" AS "aggr__0__1__key_0"))
			WHERE ("aggr__0__order_1_rank"<=11 AND "aggr__0__1__order_1_rank"<=4)
			ORDER BY "aggr__0__order_1_rank" ASC, "aggr__0__1__order_1_rank" ASC`,
	},
}
