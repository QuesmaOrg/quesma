package testdata

import (
	"mitmproxy/quesma/clickhouse"
	"mitmproxy/quesma/model"
	"time"
)

var timestampGroupByClause = clickhouse.TimestampGroupBy("@timestamp", clickhouse.DateTime64, 30*time.Second)

var AggregationTests = []AggregationTestCase{
	{ // [0]
		"simple max/min aggregation as 2 siblings",
		`{
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
		`{
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
		[][]model.QueryResultRow{
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol("value", uint64(2200))}}},
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol("value", 1199.72900390625)}}},
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol("value", 100.14596557617188)}}},
		},
		[]string{
			`SELECT count() FROM ` + QuotedTableName + ` ` +
				`WHERE ("timestamp">=parseDateTime64BestEffort('2024-02-02T13:47:16.029Z') ` +
				`AND "timestamp"<=parseDateTime64BestEffort('2024-02-09T13:47:16.029Z'))`,
			`SELECT maxOrNull("AvgTicketPrice") FROM ` + QuotedTableName + ` ` +
				`WHERE ("timestamp">=parseDateTime64BestEffort('2024-02-02T13:47:16.029Z') ` +
				`AND "timestamp"<=parseDateTime64BestEffort('2024-02-09T13:47:16.029Z'))`,
			`SELECT minOrNull("AvgTicketPrice") FROM ` + QuotedTableName + ` ` +
				`WHERE ("timestamp">=parseDateTime64BestEffort('2024-02-02T13:47:16.029Z') ` +
				`AND "timestamp"<=parseDateTime64BestEffort('2024-02-09T13:47:16.029Z'))`,
		},
	},
	{ // [1]
		"2 sibling count aggregations",
		`{
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
			"track_total_hits": true
		}`,
		`{
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
		[][]model.QueryResultRow{
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol("value", uint64(2200))}}},
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
		[]string{
			`SELECT count() FROM ` + QuotedTableName + ` ` +
				`WHERE ("timestamp"<=parseDateTime64BestEffort('2024-02-09T13:47:16.029Z') ` +
				`AND "timestamp">=parseDateTime64BestEffort('2024-02-02T13:47:16.029Z'))`,
			`SELECT "OriginCityName", count() FROM ` + QuotedTableName + ` ` +
				`WHERE (("timestamp">=parseDateTime64BestEffort('2024-02-02T13:47:16.029Z') ` +
				`AND "timestamp"<=parseDateTime64BestEffort('2024-02-09T13:47:16.029Z')) ` +
				`AND "FlightDelay"==true) ` +
				`GROUP BY "OriginCityName" ` +
				`ORDER BY "OriginCityName"`,
			`SELECT "OriginCityName", count() FROM ` + QuotedTableName + ` ` +
				`WHERE (("timestamp">=parseDateTime64BestEffort('2024-02-02T13:47:16.029Z') ` +
				`AND "timestamp"<=parseDateTime64BestEffort('2024-02-09T13:47:16.029Z')) ` +
				`AND "Cancelled"==true) ` +
				`GROUP BY "OriginCityName" ` +
				`ORDER BY "OriginCityName"`,
			`SELECT "OriginCityName", count() FROM ` + QuotedTableName + ` ` +
				`WHERE ("timestamp">=parseDateTime64BestEffort('2024-02-02T13:47:16.029Z') ` +
				`AND "timestamp"<=parseDateTime64BestEffort('2024-02-09T13:47:16.029Z')) ` +
				`GROUP BY "OriginCityName" ` +
				`ORDER BY "OriginCityName"`,
		},
	},
	{ // [2] needs some more work - double/3x/4x/... aggregation ([]buckets: []buckets ([]buckets...) doesn't work)
		"date_histogram",
		`{
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
			"track_total_hits": true
		}`,
		`{
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
				"took": 7
			},
			"start_time_in_millis": 1707486436405
		}`,
		[][]model.QueryResultRow{
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
		[]string{
			`SELECT count() FROM ` + QuotedTableName + ` ` +
				`WHERE ("timestamp">=parseDateTime64BestEffort('2024-02-02T13:47:16.029Z') ` +
				`AND "timestamp"<=parseDateTime64BestEffort('2024-02-09T13:47:16.029Z'))`,
			`SELECT "FlightDelayType", ` + clickhouse.TimestampGroupBy("timestamp", clickhouse.DateTime64, 3*time.Hour) + `, count() ` +
				`FROM ` + QuotedTableName + ` ` +
				`WHERE ("timestamp">=parseDateTime64BestEffort('2024-02-02T13:47:16.029Z') ` +
				`AND "timestamp"<=parseDateTime64BestEffort('2024-02-09T13:47:16.029Z')) ` +
				`GROUP BY "FlightDelayType", ` + clickhouse.TimestampGroupBy("timestamp", clickhouse.DateTime64, 3*time.Hour) + ` ` +
				`ORDER BY "FlightDelayType", ` + clickhouse.TimestampGroupBy("timestamp", clickhouse.DateTime64, 3*time.Hour),
			`SELECT "FlightDelayType", count() FROM ` + QuotedTableName + ` ` +
				`WHERE ("timestamp">=parseDateTime64BestEffort('2024-02-02T13:47:16.029Z') ` +
				`AND "timestamp"<=parseDateTime64BestEffort('2024-02-09T13:47:16.029Z')) ` +
				`GROUP BY "FlightDelayType" ` +
				`ORDER BY "FlightDelayType"`,
		},
	},
	{ // [3]
		"Sum",
		`{
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
		`{
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
		[][]model.QueryResultRow{
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol("value", uint64(1043))}}},
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol("value", 76631.67578125)}}},
		},
		[]string{
			`SELECT count() FROM "` + TableName + `" WHERE ("order_date">=parseDateTime64BestEffort('2024-02-06T09:59:57.034Z') AND "order_date"<=parseDateTime64BestEffort('2024-02-13T09:59:57.034Z'))`,
			`SELECT sumOrNull("taxful_total_price") FROM "` + TableName + `" WHERE ("order_date">=parseDateTime64BestEffort('2024-02-06T09:59:57.034Z') AND "order_date"<=parseDateTime64BestEffort('2024-02-13T09:59:57.034Z'))`,
		},
	},
	{ // [4]
		"cardinality",
		`{
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
			"timeout": "1000ms"
		}`, // missing entire response below, just "response" field.
		`{
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
						"sum_other_doc_count": 1706
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
		[][]model.QueryResultRow{
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol("value", uint64(2200))}}},
			{
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", "Rome"), model.NewQueryResultCol("doc_count", 73)}},
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", "Bogota"), model.NewQueryResultCol("doc_count", 44)}},
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", "Milan"), model.NewQueryResultCol("doc_count", 32)}},
			},
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol("value", 143)}}},
		},
		[]string{
			`SELECT count() FROM ` + QuotedTableName + ` ` +
				`WHERE ("timestamp">=parseDateTime64BestEffort('2024-02-02T13:47:16.029Z') ` +
				`AND "timestamp"<=parseDateTime64BestEffort('2024-02-09T13:47:16.029Z'))`,
			`SELECT "OriginCityName", count() FROM ` + QuotedTableName + ` ` +
				`WHERE ("timestamp">=parseDateTime64BestEffort('2024-02-02T13:47:16.029Z') ` +
				`AND "timestamp"<=parseDateTime64BestEffort('2024-02-09T13:47:16.029Z')) ` +
				`GROUP BY "OriginCityName" ` +
				`ORDER BY count() DESC ` +
				`LIMIT 10`,
			`SELECT count(DISTINCT "OriginCityName") FROM ` + QuotedTableName + ` ` +
				`WHERE ("timestamp">=parseDateTime64BestEffort('2024-02-02T13:47:16.029Z') ` +
				`AND "timestamp"<=parseDateTime64BestEffort('2024-02-09T13:47:16.029Z'))`,
		},
	},
	{ // [5]
		"simple filter/count",
		`{
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
			"stored_xfields": [
				"*"
			],
			"track_total_hits": true
		}`,
		`{
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
		[][]model.QueryResultRow{
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol("value", uint64(2200))}}},
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol("doc_count", uint64(553))}}},
		},
		[]string{
			`SELECT count() FROM "` + TableName + `" WHERE ("timestamp">=parseDateTime64BestEffort('2024-02-02T13:47:16.029Z') AND "timestamp"<=parseDateTime64BestEffort('2024-02-09T13:47:16.029Z'))`,
			`SELECT count() FROM "` + TableName + `" WHERE (("timestamp">=parseDateTime64BestEffort('2024-02-02T13:47:16.029Z') AND "timestamp"<=parseDateTime64BestEffort('2024-02-09T13:47:16.029Z')) AND "FlightDelay"==true)`,
		},
	},
	{ // [6]
		"idk",
		`{
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
		`{
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
		[][]model.QueryResultRow{
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol("value", uint64(904))}}},
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol("doc_count", 553)}}},
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol("doc_count", 351)}}},
		},
		[]string{
			`SELECT count() FROM "` + TableName + `" WHERE ("FlightDelay"==true AND (("timestamp">=parseDateTime64BestEffort('2024-02-02T13:47:16.029Z') AND "timestamp"<=parseDateTime64BestEffort('2024-02-09T13:47:16.029Z')) OR ("timestamp">=parseDateTime64BestEffort('2024-01-26T13:47:16.029Z') AND "timestamp"<=parseDateTime64BestEffort('2024-02-02T13:47:16.029Z'))))`,
			`SELECT count() FROM "` + TableName + `" WHERE (("FlightDelay"==true ` +
				`AND (("timestamp">=parseDateTime64BestEffort('2024-02-02T13:47:16.029Z') AND "timestamp"<=parseDateTime64BestEffort('2024-02-09T13:47:16.029Z')) ` +
				`OR ("timestamp">=parseDateTime64BestEffort('2024-01-26T13:47:16.029Z') AND "timestamp"<=parseDateTime64BestEffort('2024-02-02T13:47:16.029Z')))) ` +
				`AND ("timestamp">=parseDateTime64BestEffort('2024-02-02T13:47:16.029Z') AND "timestamp"<=parseDateTime64BestEffort('2024-02-09T13:47:16.029Z')))`,
			`SELECT count() FROM "` + TableName + `" WHERE (("FlightDelay"==true ` +
				`AND (("timestamp">=parseDateTime64BestEffort('2024-02-02T13:47:16.029Z') AND "timestamp"<=parseDateTime64BestEffort('2024-02-09T13:47:16.029Z')) ` +
				`OR ("timestamp">=parseDateTime64BestEffort('2024-01-26T13:47:16.029Z') AND "timestamp"<=parseDateTime64BestEffort('2024-02-02T13:47:16.029Z')))) ` +
				`AND ("timestamp">=parseDateTime64BestEffort('2024-01-26T13:47:16.029Z') AND "timestamp"<=parseDateTime64BestEffort('2024-02-02T13:47:16.029Z')))`,
		},
	},
	{ // [7]
		"top hits, quite complex",
		`{
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
			"track_total_hits": true
		}`,
		`{
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
						"sum_other_doc_count": 0
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
		[][]model.QueryResultRow{
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
		[]string{
			`SELECT count() FROM "` + TableName + `"`,
			``,
			``,
			``,
		},
	},
	{ // [8]
		"histogram, different field than timestamp",
		`{
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
			"track_total_hits": true
		}`,
		`{
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
		[][]model.QueryResultRow{
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol("value", uint64(553))}}},
			{
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", 15.0), model.NewQueryResultCol("doc_count", 21)}},
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", 30.0), model.NewQueryResultCol("doc_count", 22)}},
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", 345.0), model.NewQueryResultCol("doc_count", 13)}},
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", 360.0), model.NewQueryResultCol("doc_count", 22)}},
			},
		},
		[]string{
			`SELECT count() FROM ` + QuotedTableName + ` ` +
				`WHERE (("timestamp">=parseDateTime64BestEffort('2024-02-02T13:47:16.029Z') ` +
				`AND "timestamp"<=parseDateTime64BestEffort('2024-02-09T13:47:16.029Z')) ` +
				`AND NOT ("FlightDelayMin"==0))`,
			`SELECT "FlightDelayMin", count() FROM ` + QuotedTableName + ` ` +
				`WHERE (("timestamp">=parseDateTime64BestEffort('2024-02-02T13:47:16.029Z') ` +
				`AND "timestamp"<=parseDateTime64BestEffort('2024-02-09T13:47:16.029Z')) ` +
				`AND NOT ("FlightDelayMin"==0)) ` +
				`GROUP BY "FlightDelayMin" ` +
				`ORDER BY "FlightDelayMin"`,
		},
	},
	{ // [9]
		"double aggregation with histogram + harder query",
		`{
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
		`{
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
						"sum_other_doc_count": 31
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
		[][]model.QueryResultRow{
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
		[]string{
			`SELECT count() FROM ` + QuotedTableName + ` ` +
				`WHERE ("host.name" iLIKE '%prometheus%' ` +
				`AND ("@timestamp"<=parseDateTime64BestEffort('2024-02-09T16:36:49.940Z') ` +
				`AND "@timestamp">=parseDateTime64BestEffort('2024-02-02T16:36:49.940Z')))`,
			`SELECT "severity", toInt64(toUnixTimestamp64Milli(` + "`@timestamp`" + `)/10800000), count() FROM ` + QuotedTableName + ` ` +
				`WHERE ("host.name" iLIKE '%prometheus%' ` +
				`AND ("@timestamp">=parseDateTime64BestEffort('2024-02-02T16:36:49.940Z') ` +
				`AND "@timestamp"<=parseDateTime64BestEffort('2024-02-09T16:36:49.940Z'))) ` +
				`GROUP BY "severity", toInt64(toUnixTimestamp64Milli(` + "`@timestamp`)/10800000)" + ` ` +
				`ORDER BY "severity", toInt64(toUnixTimestamp64Milli(` + "`@timestamp`)/10800000)",
			`SELECT "severity", count() FROM ` + QuotedTableName + ` ` +
				`WHERE ("host.name" iLIKE '%prometheus%' ` +
				`AND ("@timestamp">=parseDateTime64BestEffort('2024-02-02T16:36:49.940Z') ` +
				`AND "@timestamp"<=parseDateTime64BestEffort('2024-02-09T16:36:49.940Z'))) ` +
				`GROUP BY "severity" ` +
				`ORDER BY "severity"`,
		},
	},
	{ // [10]
		"very long: multiple top_metrics + histogram",
		`{
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
			"track_total_hits": true
		}`,
		`{
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
		[][]model.QueryResultRow{
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol("hits", uint64(1043))}}},
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
		[]string{
			`SELECT count() FROM ` + QuotedTableName + ` WHERE ("order_date">=parseDateTime64BestEffort('2024-02-06T09:59:57.034Z') ` +
				`AND "order_date"<=parseDateTime64BestEffort('2024-02-13T09:59:57.034Z'))`,
			"SELECT toInt64(toUnixTimestamp64Milli(`order_date`)/43200000), " + `maxOrNull("order_date") AS "windowed_order_date", ` +
				`maxOrNull("order_date") AS "windowed_order_date" FROM ` +
				`(SELECT "order_date", "order_date", ROW_NUMBER() OVER ` +
				"(PARTITION BY toInt64(toUnixTimestamp64Milli(`order_date`)/43200000) " +
				`ORDER BY "order_date" asc) AS row_number FROM ` + QuotedTableName + " " +
				`WHERE (("order_date">=parseDateTime64BestEffort('2024-02-06T09:59:57.034Z') AND ` +
				`"order_date"<=parseDateTime64BestEffort('2024-02-13T09:59:57.034Z')) AND "taxful_total_price" > '250')) ` +
				`WHERE ((("order_date">=parseDateTime64BestEffort('2024-02-06T09:59:57.034Z') AND ` +
				`"order_date"<=parseDateTime64BestEffort('2024-02-13T09:59:57.034Z')) AND "taxful_total_price" > '250') AND "row_number"<=10) ` +
				"GROUP BY toInt64(toUnixTimestamp64Milli(`order_date`)/43200000) " +
				"ORDER BY toInt64(toUnixTimestamp64Milli(`order_date`)/43200000)",
			"SELECT toInt64(toUnixTimestamp64Milli(`order_date`)/43200000), " + `maxOrNull("taxful_total_price") AS "windowed_taxful_total_price", ` +
				`maxOrNull("order_date") AS "windowed_order_date" FROM ` +
				`(SELECT "taxful_total_price", "order_date", ROW_NUMBER() OVER ` +
				"(PARTITION BY toInt64(toUnixTimestamp64Milli(`order_date`)/43200000) " +
				`ORDER BY "order_date" asc) AS row_number FROM ` + QuotedTableName + " " +
				`WHERE (("order_date">=parseDateTime64BestEffort('2024-02-06T09:59:57.034Z') AND ` +
				`"order_date"<=parseDateTime64BestEffort('2024-02-13T09:59:57.034Z')) AND "taxful_total_price" > '250')) ` +
				`WHERE ((("order_date">=parseDateTime64BestEffort('2024-02-06T09:59:57.034Z') AND ` +
				`"order_date"<=parseDateTime64BestEffort('2024-02-13T09:59:57.034Z')) AND "taxful_total_price" > '250') AND "row_number"<=10) ` +
				"GROUP BY toInt64(toUnixTimestamp64Milli(`order_date`)/43200000) " +
				"ORDER BY toInt64(toUnixTimestamp64Milli(`order_date`)/43200000)",
			"SELECT toInt64(toUnixTimestamp64Milli(`order_date`)/43200000), count() FROM " + QuotedTableName + " " +
				`WHERE (("order_date">=parseDateTime64BestEffort('2024-02-06T09:59:57.034Z') AND ` +
				`"order_date"<=parseDateTime64BestEffort('2024-02-13T09:59:57.034Z')) AND "taxful_total_price" > '250') ` +
				"GROUP BY toInt64(toUnixTimestamp64Milli(`order_date`)/43200000) " +
				"ORDER BY toInt64(toUnixTimestamp64Milli(`order_date`)/43200000)",
			`SELECT count() FROM ` + QuotedTableName + ` WHERE (("order_date">=parseDateTime64BestEffort('2024-02-06T09:59:57.034Z') ` +
				`AND "order_date"<=parseDateTime64BestEffort('2024-02-13T09:59:57.034Z')) AND "taxful_total_price" > '250')`,
		},
	},
	{ // [11], "old" test, also can be found in testdata/requests.go TestAsyncSearch[0]
		// Copied it also here to be more sure we do not create some regression
		"value_count + top_values: regression test",
		`{
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
						"shard_size": 5000
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
			"size": 0,
			"track_total_hits": true
		}`,
		`{
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
						"doc_count": 442,
						"sample_count": {
							"value": 442
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
							"sum_other_doc_count": 192
						}
					}
				},
				"hits": {
					"hits": [],
					"max_score": null,
					"total": {
						"relation": "eq",
						"value": 442
					}
				},
				"timed_out": false,
				"took": 3
			},
			"start_time_in_millis": 1706010201964
		}`,
		[][]model.QueryResultRow{
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol("hits", uint64(442))}}},
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol("doc_count", 442)}}},
			{
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", "hephaestus"), model.NewQueryResultCol("doc_count", 30)}},
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", "poseidon"), model.NewQueryResultCol("doc_count", 29)}},
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", "jupiter"), model.NewQueryResultCol("doc_count", 28)}},
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", "selen"), model.NewQueryResultCol("doc_count", 26)}},
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", "demeter"), model.NewQueryResultCol("doc_count", 24)}},
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", "iris"), model.NewQueryResultCol("doc_count", 24)}},
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", "pan"), model.NewQueryResultCol("doc_count", 24)}},
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", "hades"), model.NewQueryResultCol("doc_count", 22)}},
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", "hermes"), model.NewQueryResultCol("doc_count", 22)}},
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", "persephone"), model.NewQueryResultCol("doc_count", 21)}},
			},
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol("doc_count", 442)}}},
		},
		[]string{
			`SELECT count() FROM ` + QuotedTableName + ` ` +
				`WHERE (("@timestamp">=parseDateTime64BestEffort('2024-01-23T11:27:16.820Z') ` +
				`AND "@timestamp"<=parseDateTime64BestEffort('2024-01-23T11:42:16.820Z')) ` +
				`AND "message" iLIKE '%user%')`,
			`SELECT count() FROM ` + QuotedTableName + ` ` +
				`WHERE (("@timestamp">=parseDateTime64BestEffort('2024-01-23T11:27:16.820Z') ` +
				`AND "@timestamp"<=parseDateTime64BestEffort('2024-01-23T11:42:16.820Z')) ` +
				`AND "message" iLIKE '%user%')`,
			`SELECT "host.name", count() FROM ` + QuotedTableName + ` ` +
				`WHERE (("@timestamp">=parseDateTime64BestEffort('2024-01-23T11:27:16.820Z') ` +
				`AND "@timestamp"<=parseDateTime64BestEffort('2024-01-23T11:42:16.820Z')) ` +
				`AND "message" iLIKE '%user%') ` +
				`GROUP BY "host.name" ` +
				`ORDER BY count() DESC LIMIT 10`,
			`SELECT count() FROM ` + QuotedTableName + ` ` +
				`WHERE (("@timestamp">=parseDateTime64BestEffort('2024-01-23T11:27:16.820Z') ` +
				`AND "@timestamp"<=parseDateTime64BestEffort('2024-01-23T11:42:16.820Z')) ` +
				`AND "message" iLIKE '%user%')`,
		},
	},
	{ // [12], "old" test, also can be found in testdata/requests.go TestAsyncSearch[3]
		// Copied it also here to be more sure we do not create some regression
		"date_histogram: regression test",
		`{
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
			"size": 0,
			"stored_fields": [
				"*"
			],
			"track_total_hits": true
		}`,
		`{
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
		[][]model.QueryResultRow{
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol("hits", uint64(97))}}},
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
		[]string{
			`SELECT count() FROM ` + QuotedTableName + ` ` +
				`WHERE ("message" iLIKE '%user%' ` +
				`AND ("@timestamp">=parseDateTime64BestEffort('2024-01-23T14:43:19.481Z') ` +
				`AND "@timestamp"<=parseDateTime64BestEffort('2024-01-23T14:58:19.481Z')))`,
			`SELECT ` + timestampGroupByClause + `, count() FROM ` + QuotedTableName + ` ` +
				`WHERE ("message" iLIKE '%user%' ` +
				`AND ("@timestamp">=parseDateTime64BestEffort('2024-01-23T14:43:19.481Z') ` +
				`AND "@timestamp"<=parseDateTime64BestEffort('2024-01-23T14:58:19.481Z'))) ` +
				`GROUP BY ` + timestampGroupByClause + ` ` +
				`ORDER BY ` + timestampGroupByClause,
		},
	},
	{ // [13], "old" test, also can be found in testdata/requests.go TestAsyncSearch[4]
		// Copied it also here to be more sure we do not create some regression
		TestName: "terms with date_histogram as subaggregation: regression test",
		QueryRequestJson: `
		{
			"size": 0,
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
			"track_total_hits": true
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
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol("hits", uint64(4675))}}},
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
		ExpectedSQLs: []string{
			`SELECT count() FROM ` + QuotedTableName + ` ` +
				`WHERE ("@timestamp">parseDateTime64BestEffort('2024-01-25T14:53:59.033Z') ` +
				`AND "@timestamp"<=parseDateTime64BestEffort('2024-01-25T15:08:59.033Z'))`,
			`SELECT "event.dataset", ` + "toInt64(toUnixTimestamp64Milli(`@timestamp`)/60000), count() " +
				`FROM ` + QuotedTableName + ` ` +
				`WHERE ("@timestamp">parseDateTime64BestEffort('2024-01-25T14:53:59.033Z') AND "@timestamp"<=parseDateTime64BestEffort('2024-01-25T15:08:59.033Z')) ` +
				`GROUP BY "event.dataset", ` + "toInt64(toUnixTimestamp64Milli(`@timestamp`)/60000) " +
				`ORDER BY "event.dataset", ` + "toInt64(toUnixTimestamp64Milli(`@timestamp`)/60000)",
			`SELECT "event.dataset", count() FROM ` + QuotedTableName + ` ` +
				`WHERE ("@timestamp">parseDateTime64BestEffort('2024-01-25T14:53:59.033Z') ` +
				`AND "@timestamp"<=parseDateTime64BestEffort('2024-01-25T15:08:59.033Z')) ` +
				`GROUP BY "event.dataset" ` +
				`ORDER BY "event.dataset"`,
		},
	},
	{ // [14], "old" test, also can be found in testdata/requests.go TestAsyncSearch[5]
		// Copied it also here to be more sure we do not create some regression
		"earliest/latest timestamp: regression test",
		`{
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
			"track_total_hits": true
		}`,
		`{
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
		[][]model.QueryResultRow{
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol("hits", uint64(0))}}},
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol(`minOrNull("@timestamp")`, nil)}}},
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol(`maxOrNull("@timestamp")`, nil)}}},
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol(`maxOrNull("@timestamp")`, nil)}}},
		},
		[]string{
			`SELECT count() FROM "` + TableName + `" WHERE (("message" iLIKE '%posei%' AND "message" iLIKE '%User logged out%') AND "host.name" iLIKE '%poseidon%')`,
			`SELECT avgOrNull("@timestamp") FROM "` + TableName + `" WHERE (("message" iLIKE '%posei%' AND "message" iLIKE '%User logged out%') AND "host.name" iLIKE '%poseidon%')`,
			`SELECT minOrNull("@timestamp") FROM "` + TableName + `" WHERE (("message" iLIKE '%posei%' AND "message" iLIKE '%User logged out%') AND "host.name" iLIKE '%poseidon%')`,
			`SELECT maxOrNull("@timestamp") FROM "` + TableName + `" WHERE (("message" iLIKE '%posei%' AND "message" iLIKE '%User logged out%') AND "host.name" iLIKE '%poseidon%')`,
		},
	},
	{ // [15]
		"date_histogram: regression test",
		`{
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
			"size": 0,
			"stored_fields": [
				"*"
			],
			"track_total_hits": true
		}`,
		`{
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
		[][]model.QueryResultRow{
			{
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", uint64(1049))}},
			},
			{
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", int64(19772)), model.NewQueryResultCol("1", 2221.5625)}},
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", int64(19773)), model.NewQueryResultCol("1", 11116.45703125)}},
			},
			{
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", int64(19772)), model.NewQueryResultCol("doc_count", uint64(31))}},
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", int64(19773)), model.NewQueryResultCol("doc_count", uint64(158))}},
			},
		},
		[]string{
			`SELECT count() FROM ` + QuotedTableName + ` ` +
				`WHERE ("order_date">=parseDateTime64BestEffort('2024-02-19T17:40:56.351Z') ` +
				`AND "order_date"<=parseDateTime64BestEffort('2024-02-26T17:40:56.351Z'))`,
			`SELECT ` + clickhouse.TimestampGroupBy("order_date", clickhouse.DateTime64, 24*time.Hour) + `, ` +
				`sumOrNull("taxful_total_price") ` +
				`FROM ` + QuotedTableName + ` ` +
				`WHERE ("order_date">=parseDateTime64BestEffort('2024-02-19T17:40:56.351Z') ` +
				`AND "order_date"<=parseDateTime64BestEffort('2024-02-26T17:40:56.351Z')) ` +
				`GROUP BY ` + clickhouse.TimestampGroupBy("order_date", clickhouse.DateTime64, 24*time.Hour) + ` ` +
				`ORDER BY ` + clickhouse.TimestampGroupBy("order_date", clickhouse.DateTime64, 24*time.Hour),
			`SELECT ` + clickhouse.TimestampGroupBy("order_date", clickhouse.DateTime64, 24*time.Hour) + `, count() ` +
				`FROM ` + QuotedTableName + ` ` +
				`WHERE ("order_date">=parseDateTime64BestEffort('2024-02-19T17:40:56.351Z') ` +
				`AND "order_date"<=parseDateTime64BestEffort('2024-02-26T17:40:56.351Z')) ` +
				`GROUP BY ` + clickhouse.TimestampGroupBy("order_date", clickhouse.DateTime64, 24*time.Hour) + ` ` +
				`ORDER BY ` + clickhouse.TimestampGroupBy("order_date", clickhouse.DateTime64, 24*time.Hour),
		},
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
			"size": 0,
			"stored_fields": [
				"*"
			],
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
					]
				}
			}
		}`,
		ExpectedResults: [][]model.QueryResultRow{
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol("hits", uint64(15750))}}},
			{
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", "User created"), model.NewQueryResultCol("doc_count", uint64(1700))}},
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", "User deleted"), model.NewQueryResultCol("doc_count", uint64(1781))}},
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", "User logged in"), model.NewQueryResultCol("doc_count", uint64(1757))}},
			},
		},
		ExpectedSQLs: []string{
			`SELECT count() FROM ` + QuotedTableName + ` ` +
				`WHERE ("timestamp"<=parseDateTime64BestEffort('2024-02-21T04:01:14.920Z') ` +
				`AND "timestamp">=parseDateTime64BestEffort('2024-02-20T19:13:33.795Z'))`,
			`SELECT "message", count() FROM ` + QuotedTableName + ` ` +
				`WHERE ("timestamp"<=parseDateTime64BestEffort('2024-02-21T04:01:14.920Z') ` +
				`AND "timestamp">=parseDateTime64BestEffort('2024-02-20T19:13:33.795Z')) ` +
				`GROUP BY "message" ` +
				`ORDER BY count() DESC LIMIT 3`,
		},
	},
	{ // [17]
		"triple nested aggs",
		`{
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
			"size": 0,
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
		`{
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
		[][]model.QueryResultRow{
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol("hits", uint64(1051))}}},
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
		[]string{
			`SELECT count() FROM ` + QuotedTableName + ` ` +
				`WHERE ("order_date">=parseDateTime64BestEffort('2024-02-22T18:47:34.149Z') ` +
				`AND "order_date"<=parseDateTime64BestEffort('2024-02-29T18:47:34.149Z'))`,
			`SELECT ` + clickhouse.TimestampGroupBy("order_date", clickhouse.DateTime64, 12*time.Hour) + `, ` +
				`sumOrNull("taxful_total_price") ` +
				`FROM ` + QuotedTableName + ` ` +
				`WHERE (("order_date">=parseDateTime64BestEffort('2024-02-22T18:47:34.149Z') ` +
				`AND "order_date"<=parseDateTime64BestEffort('2024-02-29T18:47:34.149Z')) ` +
				`AND "products.product_name" ILIKE '%watch%') ` +
				`GROUP BY ` + clickhouse.TimestampGroupBy("order_date", clickhouse.DateTime64, 12*time.Hour) + ` ` +
				`ORDER BY ` + clickhouse.TimestampGroupBy("order_date", clickhouse.DateTime64, 12*time.Hour),
			`SELECT ` + clickhouse.TimestampGroupBy("order_date", clickhouse.DateTime64, 12*time.Hour) + `, count() ` +
				`FROM ` + QuotedTableName + ` ` +
				`WHERE (("order_date">=parseDateTime64BestEffort('2024-02-22T18:47:34.149Z') ` +
				`AND "order_date"<=parseDateTime64BestEffort('2024-02-29T18:47:34.149Z')) ` +
				`AND "products.product_name" ILIKE '%watch%') ` +
				`GROUP BY ` + clickhouse.TimestampGroupBy("order_date", clickhouse.DateTime64, 12*time.Hour) + ` ` +
				`ORDER BY ` + clickhouse.TimestampGroupBy("order_date", clickhouse.DateTime64, 12*time.Hour),
			`SELECT ` + clickhouse.TimestampGroupBy("order_date", clickhouse.DateTime64, 12*time.Hour) + `, count() ` +
				`FROM ` + QuotedTableName + ` ` +
				`WHERE ("order_date">=parseDateTime64BestEffort('2024-02-22T18:47:34.149Z') ` +
				`AND "order_date"<=parseDateTime64BestEffort('2024-02-29T18:47:34.149Z')) ` +
				`GROUP BY ` + clickhouse.TimestampGroupBy("order_date", clickhouse.DateTime64, 12*time.Hour) + ` ` +
				`ORDER BY ` + clickhouse.TimestampGroupBy("order_date", clickhouse.DateTime64, 12*time.Hour),
		},
	},
	{ // [18]
		"",
		`{
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
    "size": 0,
    "stored_fields": [
        "*"
    ],
    "track_total_hits": true
}`,
		`{
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
		[][]model.QueryResultRow{
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
		[]string{
			`SELECT count() FROM ` + QuotedTableName + ` ` +
				`WHERE (("order_date">=parseDateTime64BestEffort('2024-02-22T21:57:36.376Z') ` +
				`AND "order_date"<=parseDateTime64BestEffort('2024-02-29T21:57:36.376Z')) ` +
				`OR ("order_date">=parseDateTime64BestEffort('2024-02-15T21:57:36.376Z') ` +
				`AND "order_date"<=parseDateTime64BestEffort('2024-02-22T21:57:36.376Z')))`,
			"SELECT toInt64(toUnixTimestamp64Milli(`order_date`)/86400000), " + `sumOrNull("taxful_total_price") ` +
				`FROM ` + QuotedTableName + ` ` +
				`WHERE ((("order_date">=parseDateTime64BestEffort('2024-02-22T21:57:36.376Z') ` +
				`AND "order_date"<=parseDateTime64BestEffort('2024-02-29T21:57:36.376Z')) ` +
				`OR ("order_date">=parseDateTime64BestEffort('2024-02-15T21:57:36.376Z') ` +
				`AND "order_date"<=parseDateTime64BestEffort('2024-02-22T21:57:36.376Z'))) ` +
				`AND ("order_date">=parseDateTime64BestEffort('2024-02-22T21:57:36.376Z') ` +
				`AND "order_date"<=parseDateTime64BestEffort('2024-02-29T21:57:36.376Z'))) ` +
				"GROUP BY toInt64(toUnixTimestamp64Milli(`order_date`)/86400000) " +
				"ORDER BY toInt64(toUnixTimestamp64Milli(`order_date`)/86400000)",
			"SELECT toInt64(toUnixTimestamp64Milli(`order_date`)/86400000), " +
				`sumOrNull("taxful_total_price") ` +
				`FROM ` + QuotedTableName + ` ` +
				`WHERE ((("order_date">=parseDateTime64BestEffort('2024-02-22T21:57:36.376Z') ` +
				`AND "order_date"<=parseDateTime64BestEffort('2024-02-29T21:57:36.376Z')) ` +
				`OR ("order_date">=parseDateTime64BestEffort('2024-02-15T21:57:36.376Z') ` +
				`AND "order_date"<=parseDateTime64BestEffort('2024-02-22T21:57:36.376Z'))) ` +
				`AND ("order_date">=parseDateTime64BestEffort('2024-02-22T21:57:36.376Z') ` +
				`AND "order_date"<=parseDateTime64BestEffort('2024-02-29T21:57:36.376Z'))) ` +
				"GROUP BY toInt64(toUnixTimestamp64Milli(`order_date`)/86400000) " +
				"ORDER BY toInt64(toUnixTimestamp64Milli(`order_date`)/86400000)",
			"SELECT toInt64(toUnixTimestamp64Milli(`order_date`)/86400000), count() " +
				`FROM ` + QuotedTableName + ` ` +
				`WHERE ((("order_date">=parseDateTime64BestEffort('2024-02-22T21:57:36.376Z') ` +
				`AND "order_date"<=parseDateTime64BestEffort('2024-02-29T21:57:36.376Z')) ` +
				`OR ("order_date"<=parseDateTime64BestEffort('2024-02-22T21:57:36.376Z') ` +
				`AND "order_date">=parseDateTime64BestEffort('2024-02-15T21:57:36.376Z'))) ` +
				`AND ("order_date">=parseDateTime64BestEffort('2024-02-22T21:57:36.376Z') ` +
				`AND "order_date"<=parseDateTime64BestEffort('2024-02-29T21:57:36.376Z'))) ` +
				"GROUP BY toInt64(toUnixTimestamp64Milli(`order_date`)/86400000) " +
				"ORDER BY toInt64(toUnixTimestamp64Milli(`order_date`)/86400000)",
			`SELECT count() ` +
				`FROM ` + QuotedTableName + ` ` +
				`WHERE ((("order_date">=parseDateTime64BestEffort('2024-02-22T21:57:36.376Z') ` +
				`AND "order_date"<=parseDateTime64BestEffort('2024-02-29T21:57:36.376Z')) ` +
				`OR ("order_date">=parseDateTime64BestEffort('2024-02-15T21:57:36.376Z') ` +
				`AND "order_date"<=parseDateTime64BestEffort('2024-02-22T21:57:36.376Z'))) ` +
				`AND ("order_date"<=parseDateTime64BestEffort('2024-02-29T21:57:36.376Z') ` +
				`AND "order_date">=parseDateTime64BestEffort('2024-02-22T21:57:36.376Z')))`,
			"SELECT toInt64(toUnixTimestamp64Milli(`order_date`)/86400000), " +
				`sumOrNull("taxful_total_price") ` +
				`FROM ` + QuotedTableName + ` ` +
				`WHERE ((("order_date">=parseDateTime64BestEffort('2024-02-22T21:57:36.376Z') ` +
				`AND "order_date"<=parseDateTime64BestEffort('2024-02-29T21:57:36.376Z')) ` +
				`OR ("order_date">=parseDateTime64BestEffort('2024-02-15T21:57:36.376Z') ` +
				`AND "order_date"<=parseDateTime64BestEffort('2024-02-22T21:57:36.376Z'))) ` +
				`AND ("order_date">=parseDateTime64BestEffort('2024-02-15T21:57:36.376Z') ` +
				`AND "order_date"<=parseDateTime64BestEffort('2024-02-22T21:57:36.376Z'))) ` +
				"GROUP BY toInt64(toUnixTimestamp64Milli(`order_date`)/86400000) " +
				"ORDER BY toInt64(toUnixTimestamp64Milli(`order_date`)/86400000)",
			"SELECT toInt64(toUnixTimestamp64Milli(`order_date`)/86400000), " +
				`sumOrNull("taxful_total_price") ` +
				`FROM ` + QuotedTableName + ` ` +
				`WHERE ((("order_date"<=parseDateTime64BestEffort('2024-02-29T21:57:36.376Z') ` +
				`AND "order_date">=parseDateTime64BestEffort('2024-02-22T21:57:36.376Z')) ` +
				`OR ("order_date">=parseDateTime64BestEffort('2024-02-15T21:57:36.376Z') ` +
				`AND "order_date"<=parseDateTime64BestEffort('2024-02-22T21:57:36.376Z'))) ` +
				`AND ("order_date">=parseDateTime64BestEffort('2024-02-15T21:57:36.376Z') ` +
				`AND "order_date"<=parseDateTime64BestEffort('2024-02-22T21:57:36.376Z'))) ` +
				"GROUP BY toInt64(toUnixTimestamp64Milli(`order_date`)/86400000) " +
				"ORDER BY toInt64(toUnixTimestamp64Milli(`order_date`)/86400000)",
			"SELECT toInt64(toUnixTimestamp64Milli(`order_date`)/86400000), count() " +
				`FROM ` + QuotedTableName + ` ` +
				`WHERE ((("order_date">=parseDateTime64BestEffort('2024-02-22T21:57:36.376Z') ` +
				`AND "order_date"<=parseDateTime64BestEffort('2024-02-29T21:57:36.376Z')) ` +
				`OR ("order_date">=parseDateTime64BestEffort('2024-02-15T21:57:36.376Z') ` +
				`AND "order_date"<=parseDateTime64BestEffort('2024-02-22T21:57:36.376Z'))) ` +
				`AND ("order_date">=parseDateTime64BestEffort('2024-02-15T21:57:36.376Z') ` +
				`AND "order_date"<=parseDateTime64BestEffort('2024-02-22T21:57:36.376Z'))) ` +
				"GROUP BY toInt64(toUnixTimestamp64Milli(`order_date`)/86400000) " +
				"ORDER BY toInt64(toUnixTimestamp64Milli(`order_date`)/86400000)",
			`SELECT count() ` +
				`FROM ` + QuotedTableName + ` ` +
				`WHERE ((("order_date">=parseDateTime64BestEffort('2024-02-22T21:57:36.376Z') ` +
				`AND "order_date"<=parseDateTime64BestEffort('2024-02-29T21:57:36.376Z')) ` +
				`OR ("order_date"<=parseDateTime64BestEffort('2024-02-22T21:57:36.376Z') ` +
				`AND "order_date">=parseDateTime64BestEffort('2024-02-15T21:57:36.376Z'))) ` +
				`AND ("order_date">=parseDateTime64BestEffort('2024-02-15T21:57:36.376Z') ` +
				`AND "order_date"<=parseDateTime64BestEffort('2024-02-22T21:57:36.376Z')))`,
		},
	},
	{ // [19]
		"random sampler, from Explorer > Field statistics",
		`{
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
		`{
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
		[][]model.QueryResultRow{
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol("hits", uint64(15))}}},
			{
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", int64(1709816790000/15000)), model.NewQueryResultCol("doc_count", uint64(0))}},
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", int64(1709816805000/15000)), model.NewQueryResultCol("doc_count", uint64(0))}},
			},
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol("doc_count", uint64(15))}}},
		},
		[]string{
			`SELECT count() FROM ` + QuotedTableName + ` ` +
				`WHERE (toUnixTimestamp64Milli("@timestamp")>=1.709815794995e+12 ` +
				`AND toUnixTimestamp64Milli("@timestamp")<=1.709816694995e+12)`,
			`SELECT ` + clickhouse.TimestampGroupBy("@timestamp", clickhouse.DateTime64, 15*time.Second) + `, count() ` +
				`FROM ` + QuotedTableName + ` ` +
				`WHERE (toUnixTimestamp64Milli("@timestamp")>=1.709815794995e+12 ` +
				`AND toUnixTimestamp64Milli("@timestamp")<=1.709816694995e+12) ` +
				`GROUP BY ` + clickhouse.TimestampGroupBy("@timestamp", clickhouse.DateTime64, 15*time.Second) + ` ` +
				`ORDER BY ` + clickhouse.TimestampGroupBy("@timestamp", clickhouse.DateTime64, 15*time.Second),
			`SELECT count() FROM ` + QuotedTableName + ` ` +
				`WHERE (toUnixTimestamp64Milli("@timestamp")>=1.709815794995e+12 ` +
				`AND toUnixTimestamp64Milli("@timestamp")<=1.709816694995e+12)`,
		},
	},
	{ // [20]
		"Field statistics > summary for numeric fields",
		`{
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
		`{
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
							"sum_other_doc_count": 1549,
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
		[][]model.QueryResultRow{
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
		[]string{
			`SELECT count() FROM ` + QuotedTableName + ` WHERE toUnixTimestamp64Milli("timestamp")<=1.711228426749e+12 AND toUnixTimestamp64Milli("timestamp")>=1.709932426749e+12`,
			"SELECT count(`bytes_gauge`), minOrNull(`bytes_gauge`), maxOrNull(`bytes_gauge`), avgOrNull(`bytes_gauge`), sumOrNull(`bytes_gauge`) FROM " + QuotedTableName + ` WHERE (toUnixTimestamp64Milli("timestamp")>=1.709932426749e+12 AND toUnixTimestamp64Milli("timestamp")<=1.711228426749e+12) AND "bytes_gauge" IS NOT NULL`,
			`SELECT count() FROM ` + QuotedTableName + ` WHERE (toUnixTimestamp64Milli("timestamp")>=1.709932426749e+12 AND toUnixTimestamp64Milli("timestamp")<=1.711228426749e+12) AND "bytes_gauge" IS NOT NULL`,
			"TODO", // too tiresome to implement the check, so for now this SQL for quantiles isn't tested
			"TODO", // too tiresome to implement the check, so for now this SQL for quantiles isn't tested
			`SELECT "bytes_gauge", count() FROM ` + QuotedTableName + ` WHERE toUnixTimestamp64Milli("timestamp")<=1.711228426749e+12 AND toUnixTimestamp64Milli("timestamp")>=1.709932426749e+12 GROUP BY "bytes_gauge") ORDER BY "bytes_gauge")`,
			`SELECT count() FROM ` + QuotedTableName + ` WHERE toUnixTimestamp64Milli("timestamp")>=1.709932426749e+12 AND toUnixTimestamp64Milli("timestamp")<=1.711228426749e+12`,
		},
	},
	{ // [21]
		TestName: `range bucket aggregation, both keyed and not`,
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
			]
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
		ExpectedSQLs: []string{
			`SELECT count() FROM ` + QuotedTableName + ` WHERE ("timestamp">=parseDateTime64BestEffort('2024-04-16T12:15:11.790Z') ` +
				`AND "timestamp"<=parseDateTime64BestEffort('2024-04-16T12:30:11.790Z'))`,
			`SELECT count(if("bytes_gauge">=0 AND "bytes_gauge"<1000, 1, NULL)), ` +
				`count(if("bytes_gauge">=1000 AND "bytes_gauge"<2000, 1, NULL)), ` +
				`count(if("bytes_gauge">=-5.5, 1, NULL)), ` +
				`count(if("bytes_gauge"<6.555, 1, NULL)), ` +
				`count(), count() FROM ` + QuotedTableName + ` WHERE ("timestamp">=parseDateTime64BestEffort('2024-04-16T12:15:11.790Z') ` +
				`AND "timestamp"<=parseDateTime64BestEffort('2024-04-16T12:30:11.790Z'))`,
			`SELECT count(if("bytes_gauge">=0 AND "bytes_gauge"<1000, 1, NULL)), ` +
				`count(if("bytes_gauge">=1000 AND "bytes_gauge"<2000, 1, NULL)), ` +
				`count(if("bytes_gauge">=-5.5, 1, NULL)), ` +
				`count(if("bytes_gauge"<6.555, 1, NULL)), ` +
				`count(), count() FROM ` + QuotedTableName + ` WHERE ("timestamp">=parseDateTime64BestEffort('2024-04-16T12:15:11.790Z') ` +
				`AND "timestamp"<=parseDateTime64BestEffort('2024-04-16T12:30:11.790Z'))`,
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
			]
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
		ExpectedSQLs: []string{
			`SELECT count() FROM ` + QuotedTableName + ` WHERE ("timestamp">=parseDateTime64BestEffort('2024-04-06T07:28:50.059Z') ` +
				`AND "timestamp"<=parseDateTime64BestEffort('2024-04-16T17:28:50.059Z'))`,
			`SELECT count(if("timestamp" < now(), 1, NULL)), toInt64(toUnixTimestamp(now())), ` +
				`count(if("timestamp" >= toStartOfDay(subDate(now(), INTERVAL 3 week)) AND "timestamp" < now(), 1, NULL)), ` +
				`toInt64(toUnixTimestamp(toStartOfDay(subDate(now(), INTERVAL 3 week)))), ` +
				`toInt64(toUnixTimestamp(now())), count(if("timestamp" >= '2024-04-14', 1, NULL)), toInt64(toUnixTimestamp('2024-04-14')), ` +
				`count() FROM "logs-generic-default" WHERE ("timestamp"<=parseDateTime64BestEffort('2024-04-16T17:28:50.059Z') ` +
				`AND "timestamp">=parseDateTime64BestEffort('2024-04-06T07:28:50.059Z'))`,
		},
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
			]
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
						"bg_count": 14074,
						"buckets": [
							{
								"bg_count": 619,
								"doc_count": 619,
								"key": "",
								"score": 619
							},
							{
								"bg_count": 206,
								"doc_count": 206,
								"key": "zip",
								"score": 206
							}
						],
						"doc_count": 1608
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
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", ""), model.NewQueryResultCol("doc_count", uint64(619))}},
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", "zip"), model.NewQueryResultCol("doc_count", uint64(206))}},
			},
		},
		ExpectedSQLs: []string{
			`SELECT count() FROM ` + QuotedTableName,
			`SELECT "message", count() FROM ` + QuotedTableName + ` GROUP BY "message" ORDER BY count() DESC LIMIT 4`,
		},
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
						"calendar_interval": "22h",
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
			"timeout": "30000ms"
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
		ExpectedSQLs: []string{
			`SELECT count() FROM ` + QuotedTableName,
			"SELECT toInt64(toUnixTimestamp64Milli(`@timestamp`)/79200000), " + `count(DISTINCT "host.name") ` +
				`FROM ` + QuotedTableName + " " +
				"GROUP BY toInt64(toUnixTimestamp64Milli(`@timestamp`)/79200000) " +
				"ORDER BY toInt64(toUnixTimestamp64Milli(`@timestamp`)/79200000)",
			"SELECT toInt64(toUnixTimestamp64Milli(`@timestamp`)/79200000), count() " +
				`FROM ` + QuotedTableName + " " +
				"GROUP BY toInt64(toUnixTimestamp64Milli(`@timestamp`)/79200000) " +
				"ORDER BY toInt64(toUnixTimestamp64Milli(`@timestamp`)/79200000)",
		},
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
		ExpectedSQLs: []string{
			`SELECT count() FROM ` + QuotedTableName + ` ` +
				`WHERE ("timestamp">=parseDateTime64BestEffort('2024-05-10T13:47:56.077Z') ` +
				`AND "timestamp"<=parseDateTime64BestEffort('2024-05-10T14:02:56.077Z'))`,
			`SELECT floor("bytes" / 100.000000) * 100.000000, count() ` +
				`FROM ` + QuotedTableName + ` ` +
				`WHERE ("timestamp">=parseDateTime64BestEffort('2024-05-10T13:47:56.077Z') ` +
				`AND "timestamp"<=parseDateTime64BestEffort('2024-05-10T14:02:56.077Z')) ` +
				`GROUP BY floor("bytes" / 100.000000) * 100.000000 ` +
				`ORDER BY floor("bytes" / 100.000000) * 100.000000`,
		},
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
		ExpectedSQLs: []string{
			`SELECT count() FROM ` + QuotedTableName + ` ` +
				`WHERE ("timestamp">=parseDateTime64BestEffort('2024-05-10T14:29:02.900Z') ` +
				`AND "timestamp"<=parseDateTime64BestEffort('2024-05-10T14:44:02.900Z'))`,
			"SELECT toInt64(toUnixTimestamp64Milli(`timestamp`)/30000), count() " +
				`FROM ` + QuotedTableName + ` ` +
				`WHERE ("timestamp">=parseDateTime64BestEffort('2024-05-10T14:29:02.900Z') ` +
				`AND "timestamp"<=parseDateTime64BestEffort('2024-05-10T14:44:02.900Z')) ` +
				"GROUP BY toInt64(toUnixTimestamp64Milli(`timestamp`)/30000) " +
				"ORDER BY toInt64(toUnixTimestamp64Milli(`timestamp`)/30000)",
		},
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
		ExpectedSQLs: []string{
			`SELECT count() FROM ` + QuotedTableName,
			`SELECT floor("rspContentLen" / 2000.000000) * 2000.000000, "message", count() ` +
				`FROM ` + QuotedTableName + ` ` +
				`GROUP BY floor("rspContentLen" / 2000.000000) * 2000.000000, "message" ` +
				`ORDER BY floor("rspContentLen" / 2000.000000) * 2000.000000, "message"`,
			`SELECT floor("rspContentLen" / 2000.000000) * 2000.000000, count() ` +
				`FROM ` + QuotedTableName + ` ` +
				`GROUP BY floor("rspContentLen" / 2000.000000) * 2000.000000 ` +
				`ORDER BY floor("rspContentLen" / 2000.000000) * 2000.000000`,
		},
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
			"track_total_hits": true
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
		ExpectedSQLs: []string{
			`SELECT count() FROM ` + QuotedTableName,
			`SELECT "OriginCityName", count() ` +
				`FROM ` + QuotedTableName + ` ` +
				`WHERE "FlightDelay" == true ` +
				`GROUP BY "OriginCityName" ` +
				`ORDER BY "OriginCityName"`,
			`SELECT "OriginCityName", count() ` +
				`FROM ` + QuotedTableName + ` ` +
				`WHERE "Cancelled" == true ` +
				`GROUP BY "OriginCityName" ` +
				`ORDER BY "OriginCityName"`,
			`SELECT "OriginCityName", count() ` +
				`FROM ` + QuotedTableName + ` ` +
				`GROUP BY "OriginCityName" ` +
				`ORDER BY "OriginCityName"`,
		},
	},
	{ // [29]
		TestName: "Terms, completely different tree results from 2 queries - merging them didn't work before (logs)",
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
		},
		ExpectedSQLs: []string{
			`SELECT count() FROM ` + QuotedTableName + ` WHERE "timestamp">=parseDateTime64BestEffort('2024-03-23T07:32:06.246Z') AND "timestamp"<=parseDateTime64BestEffort('2024-03-30T07:32:06.246Z')`,
			``,
			``,
			``,
		},
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
		ExpectedSQLs: []string{
			`SELECT count() FROM ` + QuotedTableName + ` WHERE "timestamp">=parseDateTime64BestEffort('2024-03-23T07:32:06.246Z') AND "timestamp"<=parseDateTime64BestEffort('2024-03-30T07:32:06.246Z')`,
			``,
			``,
			``,
		},
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
			"id": "quesma_async_search_id_17",
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
					model.NewQueryResultCol("toInt64(toUnixTimestamp64Milli(`@timestamp`)/86400000)", int64(1715212800000/86400000)),
					model.NewQueryResultCol(`"windowed_message"`, 5),
					model.NewQueryResultCol(`minOrNull("order_date")`, "2024-05-09T23:52:48Z"),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("toInt64(toUnixTimestamp64Milli(`@timestamp`)/86400000)", int64(1716336000000/86400000)),
					model.NewQueryResultCol(`windowed_message`, 30),
					model.NewQueryResultCol(`minOrNull("order_date")`, "2024-05-22T10:20:38Z"),
				}},
			},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("toInt64(toUnixTimestamp64Milli(`@timestamp`)/86400000)", int64(1715212800000/86400000)),
					model.NewQueryResultCol(`count()`, 146),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("toInt64(toUnixTimestamp64Milli(`@timestamp`)/86400000)", int64(1716336000000/86400000)),
					model.NewQueryResultCol(`count()`, 58),
				}},
			},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("toInt64(toUnixTimestamp64Milli(`@timestamp`)/86400000)", int64(1715212800000/86400000)),
					model.NewQueryResultCol(`count()`, 146),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("toInt64(toUnixTimestamp64Milli(`@timestamp`)/86400000)", int64(1716336000000/86400000)),
					model.NewQueryResultCol(`count()`, 58),
				}},
			},
		},
		ExpectedSQLs: []string{
			`SELECT count() ` +
				`FROM ` + QuotedTableName,
			"SELECT toInt64(toUnixTimestamp64Milli(`@timestamp`)/86400000), " +
				`minOrNull("message") AS "windowed_message", ` +
				`minOrNull("order_date") AS "windowed_order_date" ` +
				`FROM (SELECT "message", "order_date", ROW_NUMBER() OVER ` +
				"(PARTITION BY toInt64(toUnixTimestamp64Milli(`@timestamp`)/86400000) " +
				`ORDER BY "order_date" desc) ` +
				`AS row_number ` +
				`FROM ` + QuotedTableName + ` ` +
				`WHERE "message" IS NOT NULL) ` +
				`WHERE ("message" IS NOT NULL ` +
				`AND "row_number"<=1) ` +
				"GROUP BY toInt64(toUnixTimestamp64Milli(`@timestamp`)/86400000) " +
				"ORDER BY toInt64(toUnixTimestamp64Milli(`@timestamp`)/86400000)",
			"SELECT toInt64(toUnixTimestamp64Milli(`@timestamp`)/86400000), " +
				"count() " +
				`FROM ` + QuotedTableName + ` ` +
				`WHERE "message" IS NOT NULL ` +
				"GROUP BY toInt64(toUnixTimestamp64Milli(`@timestamp`)/86400000) " +
				"ORDER BY toInt64(toUnixTimestamp64Milli(`@timestamp`)/86400000)",
			"SELECT toInt64(toUnixTimestamp64Milli(`@timestamp`)/86400000), " +
				"count() " +
				`FROM ` + QuotedTableName + ` ` +
				"GROUP BY toInt64(toUnixTimestamp64Milli(`@timestamp`)/86400000) " +
				"ORDER BY toInt64(toUnixTimestamp64Milli(`@timestamp`)/86400000)",
		},
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
		ExpectedSQLs: []string{
			`SELECT count() ` +
				`FROM ` + QuotedTableName + ` ` +
				`WHERE ("timestamp">=parseDateTime64BestEffort('2024-05-21T21:35:34.210Z') ` +
				`AND "timestamp"<=parseDateTime64BestEffort('2024-05-22T12:35:34.210Z'))`,
			"SELECT toInt64(toUnixTimestamp64Milli(`timestamp`)/600000), " +
				`count("bytes"), ` +
				`minOrNull("bytes"), ` +
				`maxOrNull("bytes"), ` +
				`avgOrNull("bytes"), ` +
				`sumOrNull("bytes"), ` +
				`sumOrNull("bytes" * "bytes"), ` +
				`varPop("bytes"), ` +
				`varSamp("bytes"), ` +
				`stddevPop("bytes"), ` +
				`stddevSamp("bytes") ` +
				`FROM ` + QuotedTableName + ` ` +
				`WHERE ("timestamp">=parseDateTime64BestEffort('2024-05-21T21:35:34.210Z') ` +
				`AND "timestamp"<=parseDateTime64BestEffort('2024-05-22T12:35:34.210Z')) ` +
				"GROUP BY toInt64(toUnixTimestamp64Milli(`timestamp`)/600000) " +
				"ORDER BY toInt64(toUnixTimestamp64Milli(`timestamp`)/600000)",
			"SELECT toInt64(toUnixTimestamp64Milli(`timestamp`)/600000), " +
				`count("bytes"), ` +
				`minOrNull("bytes"), ` +
				`maxOrNull("bytes"), ` +
				`avgOrNull("bytes"), ` +
				`sumOrNull("bytes"), ` +
				`sumOrNull("bytes" * "bytes"), ` +
				`varPop("bytes"), ` +
				`varSamp("bytes"), ` +
				`stddevPop("bytes"), ` +
				`stddevSamp("bytes") ` +
				`FROM ` + QuotedTableName + ` ` +
				`WHERE ("timestamp">=parseDateTime64BestEffort('2024-05-21T21:35:34.210Z') ` +
				`AND "timestamp"<=parseDateTime64BestEffort('2024-05-22T12:35:34.210Z')) ` +
				"GROUP BY toInt64(toUnixTimestamp64Milli(`timestamp`)/600000) " +
				"ORDER BY toInt64(toUnixTimestamp64Milli(`timestamp`)/600000)",
			"SELECT toInt64(toUnixTimestamp64Milli(`timestamp`)/600000), " +
				`count() ` +
				`FROM ` + QuotedTableName + ` ` +
				`WHERE ("timestamp">=parseDateTime64BestEffort('2024-05-21T21:35:34.210Z') ` +
				`AND "timestamp"<=parseDateTime64BestEffort('2024-05-22T12:35:34.210Z')) ` +
				"GROUP BY toInt64(toUnixTimestamp64Milli(`timestamp`)/600000) " +
				"ORDER BY toInt64(toUnixTimestamp64Milli(`timestamp`)/600000)",
		},
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
		ExpectedSQLs: []string{
			`SELECT count() ` +
				`FROM ` + QuotedTableName + ` ` +
				`WHERE ("message" IS NOT NULL AND NOT ("message" iLIKE '%US%'))`,
			`SELECT "host.name", "message", count() ` +
				`FROM ` + QuotedTableName + ` ` +
				`WHERE ("message" IS NOT NULL AND NOT ("message" iLIKE '%US%')) ` +
				`GROUP BY "host.name", "message" ` +
				`ORDER BY "host.name", "message"`,
			`SELECT "host.name", count() ` +
				`FROM ` + QuotedTableName + ` ` +
				`WHERE ("message" IS NOT NULL AND NOT ("message" iLIKE '%US%')) ` +
				`GROUP BY "host.name" ` +
				`ORDER BY "host.name"`,
		},
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
			"track_total_hits": true
		}`,
		ExpectedResponse: `{"response": {"aggregations":{}}}`,
		ExpectedResults: [][]model.QueryResultRow{
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol("hits", uint64(122))}}},
			{},
			{},
			{},
		},
		ExpectedSQLs: []string{
			`SELECT count() ` +
				`FROM ` + QuotedTableName + ` ` +
				`WHERE ("message" IS NOT NULL AND NOT ("message" iLIKE '%US%'))`,
			`SELECT "host.name", "message", "message", count() ` +
				`FROM ` + QuotedTableName + ` ` +
				`WHERE ("message" IS NOT NULL AND NOT ("message" iLIKE '%US%')) ` +
				`GROUP BY "host.name", "message", "message" ` +
				`ORDER BY "host.name", "message", "message"`,
			`SELECT "host.name", "message", count() ` +
				`FROM ` + QuotedTableName + ` ` +
				`WHERE ("message" IS NOT NULL AND NOT ("message" iLIKE '%US%')) ` +
				`GROUP BY "host.name", "message" ` +
				`ORDER BY "host.name", "message"`,
			`SELECT "host.name", count() ` +
				`FROM ` + QuotedTableName + ` ` +
				`WHERE ("message" IS NOT NULL AND NOT ("message" iLIKE '%US%')) ` +
				`GROUP BY "host.name" ` +
				`ORDER BY "host.name"`,
		},
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
		ExpectedSQLs: []string{
			`SELECT count() ` +
				`FROM ` + QuotedTableName + ` ` +
				`WHERE ("message" IS NOT NULL AND NOT ("message" iLIKE '%US%'))`,
			`SELECT "host.name", "FlightDelayMin", count() ` +
				`FROM ` + QuotedTableName + ` ` +
				`WHERE ("message" IS NOT NULL AND NOT ("message" iLIKE '%US%')) ` +
				`GROUP BY "host.name", "FlightDelayMin" ` +
				`ORDER BY "host.name", "FlightDelayMin"`,
			`SELECT "host.name", count() ` +
				`FROM ` + QuotedTableName + ` ` +
				`WHERE ("message" IS NOT NULL AND NOT ("message" iLIKE '%US%')) ` +
				`GROUP BY "host.name" ` +
				`ORDER BY "host.name"`,
		},
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
			"track_total_hits": true
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
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol("hits", uint64(122))}}},
			{},
			{},
		},
		ExpectedSQLs: []string{
			`SELECT count() ` +
				`FROM ` + QuotedTableName + ` ` +
				`WHERE ("message" IS NOT NULL AND NOT ("message" iLIKE '%US%'))`,
			`SELECT "host.name", "FlightDelayMin", count() ` +
				`FROM ` + QuotedTableName + ` ` +
				`WHERE ("message" IS NOT NULL AND NOT ("message" iLIKE '%US%')) ` +
				`GROUP BY "host.name", "FlightDelayMin" ` +
				`ORDER BY "host.name", "FlightDelayMin"`,
			`SELECT "host.name", count() ` +
				`FROM ` + QuotedTableName + ` ` +
				`WHERE ("message" IS NOT NULL AND NOT ("message" iLIKE '%US%')) ` +
				`GROUP BY "host.name" ` +
				`ORDER BY "host.name"`,
		},
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
			"track_total_hits": true
		}`,
		ExpectedResponse: `{"response": {"aggregations":{}}}`,
		ExpectedResults: [][]model.QueryResultRow{
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol("hits", uint64(122))}}},
			{},
			{},
		},
		ExpectedSQLs: []string{
			`SELECT count() ` +
				`FROM ` + QuotedTableName + ` ` +
				`WHERE ("message" IS NOT NULL AND NOT ("message" iLIKE '%US%'))`,
			`SELECT "host.name", "FlightDelayMin", count() ` +
				`FROM ` + QuotedTableName + ` ` +
				`WHERE ("message" IS NOT NULL AND NOT ("message" iLIKE '%US%')) ` +
				`GROUP BY "host.name", "FlightDelayMin" ` +
				`ORDER BY "host.name", "FlightDelayMin"`,
			`SELECT "host.name", count() ` +
				`FROM ` + QuotedTableName + ` ` +
				`WHERE ("message" IS NOT NULL AND NOT ("message" iLIKE '%US%')) ` +
				`GROUP BY "host.name" ` +
				`ORDER BY "host.name"`,
		},
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
		ExpectedSQLs: []string{
			`SELECT count() FROM ` + QuotedTableName,
			`SELECT "message" FROM ` + QuotedTableName + ` LIMIT 1`,
			`SELECT "message" FROM ` + QuotedTableName + ` LIMIT 2`,
		},
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
		ExpectedSQLs: []string{
			`SELECT count() FROM ` + QuotedTableName,
			`SELECT "message", "timestamp" FROM ` + QuotedTableName + ` ORDER BY "timestamp" DESC LIMIT 1`,
			`SELECT "message", "timestamp" FROM ` + QuotedTableName + ` ORDER BY "timestamp" DESC LIMIT 1`,
		},
	},
}
