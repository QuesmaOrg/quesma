package testdata

import (
	"mitmproxy/quesma/clickhouse"
	"mitmproxy/quesma/model"
	"time"
)

var timestampGroupByClause = clickhouse.TimestampGroupBy("@timestamp", clickhouse.DateTime64, 30*time.Second)

type AggregationTestCase struct {
	TestName         string
	QueryRequestJson string                   // JSON query request, just like received from Kibana
	ExpectedResponse string                   // JSON response, just like Elastic would respond to the query request
	ExpectedResults  [][]model.QueryResultRow // [0] = result for first aggregation, [1] = result for second aggregation, etc.
	ExpectedSQLs     []string                 // [0] = translated SQLs for first aggregation, [1] = translated SQL for second aggregation, etc.
}

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
			`SELECT count() FROM "` + TableName + `" WHERE "timestamp">=parseDateTime64BestEffort('2024-02-02T13:47:16.029Z') AND "timestamp"<=parseDateTime64BestEffort('2024-02-09T13:47:16.029Z') `,
			`SELECT max("AvgTicketPrice") FROM "` + TableName + `" WHERE "timestamp">=parseDateTime64BestEffort('2024-02-02T13:47:16.029Z') AND "timestamp"<=parseDateTime64BestEffort('2024-02-09T13:47:16.029Z') `,
			`SELECT min("AvgTicketPrice") FROM "` + TableName + `" WHERE "timestamp">=parseDateTime64BestEffort('2024-02-02T13:47:16.029Z') AND "timestamp"<=parseDateTime64BestEffort('2024-02-09T13:47:16.029Z') `,
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
			`SELECT count() FROM "` + TableName + `" WHERE "timestamp"<=parseDateTime64BestEffort('2024-02-09T13:47:16.029Z') AND "timestamp">=parseDateTime64BestEffort('2024-02-02T13:47:16.029Z') `,
			`SELECT "OriginCityName", count() FROM "` + TableName + `" WHERE "FlightDelay" == true  GROUP BY ("OriginCityName")`,
			`SELECT "OriginCityName", count() FROM "` + TableName + `" WHERE "Cancelled" == true  GROUP BY ("OriginCityName")`,
			`SELECT "OriginCityName", count() FROM "` + TableName + `" WHERE "timestamp">=parseDateTime64BestEffort('2024-02-02T13:47:16.029Z') AND "timestamp"<=parseDateTime64BestEffort('2024-02-09T13:47:16.029Z')  GROUP BY ("OriginCityName")`,
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
											"key": 1706871600000,
											"key_as_string": "2024-02-02T12:00:00.000+01:00"
										},
										{
											"doc_count": 27,
											"key": 1706882400000,
											"key_as_string": "2024-02-02T15:00:00.000+01:00"
										},
										{
											"doc_count": 34,
											"key": 1706893200000,
											"key_as_string": "2024-02-02T18:00:00.000+01:00"
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
											"key_as_string": "2024-02-02T12:00:00.000+01:00"
										},
										{
											"doc_count": 2,
											"key": 1706882400000,
											"key_as_string": "2024-02-02T15:00:00.000+01:00"
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
					model.NewQueryResultCol("key", int64(1706871600000/1000/60/60/3)), // / 3h
					model.NewQueryResultCol("doc_count", uint64(2)),
					model.NewQueryResultCol("key_as_string", "2024-02-02T12:00:00.000+01:00"),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("FlightDelayType", "No Delay"),
					model.NewQueryResultCol("key", int64(1706882400000/1000/60/60/3)),
					model.NewQueryResultCol("doc_count", uint64(27)),
					model.NewQueryResultCol("key_as_string", "2024-02-02T15:00:00.000+01:00"),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("FlightDelayType", "No Delay"),
					model.NewQueryResultCol("key", int64(1706893200000/1000/60/60/3)),
					model.NewQueryResultCol("doc_count", uint64(34)),
					model.NewQueryResultCol("key_as_string", "2024-02-02T18:00:00.000+01:00"),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("FlightDelayType", "Security Delay"),
					model.NewQueryResultCol("key", int64(1706871600000/1000/60/60/3)),
					model.NewQueryResultCol("doc_count", uint64(0)),
					model.NewQueryResultCol("key_as_string", "2024-02-02T12:00:00.000+01:00"),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("FlightDelayType", "Security Delay"),
					model.NewQueryResultCol("key", int64(1706882400000/1000/60/60/3)),
					model.NewQueryResultCol("doc_count", uint64(2)),
					model.NewQueryResultCol("key_as_string", "2024-02-02T15:00:00.000+01:00"),
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
			`SELECT count() FROM "` + TableName + `" WHERE "timestamp">=parseDateTime64BestEffort('2024-02-02T13:47:16.029Z') AND "timestamp"<=parseDateTime64BestEffort('2024-02-09T13:47:16.029Z') `,
			`SELECT "FlightDelayType", "timestamp", count() FROM "` + TableName + `" WHERE "timestamp">=parseDateTime64BestEffort('2024-02-02T13:47:16.029Z') AND "timestamp"<=parseDateTime64BestEffort('2024-02-09T13:47:16.029Z')  GROUP BY ("FlightDelayType", "timestamp")`,
			`SELECT "FlightDelayType", count() FROM "` + TableName + `" WHERE "timestamp">=parseDateTime64BestEffort('2024-02-02T13:47:16.029Z') AND "timestamp"<=parseDateTime64BestEffort('2024-02-09T13:47:16.029Z')  GROUP BY ("FlightDelayType")`,
		},
	},
	{ // [3] works
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
			`SELECT sum("taxful_total_price") FROM "` + TableName + `" WHERE "order_date">=parseDateTime64BestEffort('2024-02-06T09:59:57.034Z') AND "order_date"<=parseDateTime64BestEffort('2024-02-13T09:59:57.034Z') `,
		},
	},
	{ // [4] works
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
			`SELECT "OriginCityName", count() FROM "` + TableName + `" WHERE "timestamp">=parseDateTime64BestEffort('2024-02-02T13:47:16.029Z') AND "timestamp"<=parseDateTime64BestEffort('2024-02-09T13:47:16.029Z')  GROUP BY ("OriginCityName")`,
			`SELECT COUNT(DISTINCT "OriginCityName") FROM "` + TableName + `" WHERE "timestamp">=parseDateTime64BestEffort('2024-02-02T13:47:16.029Z') AND "timestamp"<=parseDateTime64BestEffort('2024-02-09T13:47:16.029Z') `,
		},
	},
	{ // [5] works
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
			"stored_fields": [
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
			`SELECT count() FROM "` + TableName + `" WHERE ("timestamp">=parseDateTime64BestEffort('2024-02-02T13:47:16.029Z') AND "timestamp"<=parseDateTime64BestEffort('2024-02-09T13:47:16.029Z')) AND "FlightDelay" == true `,
		},
	},
	{ // [6] works but buckets are [], not {} like in response :(
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
			{{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("", nil), // nil aggregation
				model.NewQueryResultCol("doc_count", 553),
			}}},
			{{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("", nil), // nil aggregation
				model.NewQueryResultCol("doc_count", 351),
			}}},
		},
		[]string{
			`SELECT count() FROM "` + TableName + `" WHERE ("FlightDelay" == true ` +
				`AND (("timestamp">=parseDateTime64BestEffort('2024-02-02T13:47:16.029Z') AND "timestamp"<=parseDateTime64BestEffort('2024-02-09T13:47:16.029Z')) ` +
				`OR ("timestamp">=parseDateTime64BestEffort('2024-01-26T13:47:16.029Z') AND "timestamp"<=parseDateTime64BestEffort('2024-02-02T13:47:16.029Z')))) ` +
				`AND ("timestamp">=parseDateTime64BestEffort('2024-02-02T13:47:16.029Z') AND "timestamp"<=parseDateTime64BestEffort('2024-02-09T13:47:16.029Z')) `,
			`SELECT count() FROM "` + TableName + `" WHERE ("FlightDelay" == true ` +
				`AND (("timestamp">=parseDateTime64BestEffort('2024-02-02T13:47:16.029Z') AND "timestamp"<=parseDateTime64BestEffort('2024-02-09T13:47:16.029Z')) ` +
				`OR ("timestamp">=parseDateTime64BestEffort('2024-01-26T13:47:16.029Z') AND "timestamp"<=parseDateTime64BestEffort('2024-02-02T13:47:16.029Z')))) ` +
				`AND ("timestamp">=parseDateTime64BestEffort('2024-01-26T13:47:16.029Z') AND "timestamp"<=parseDateTime64BestEffort('2024-02-02T13:47:16.029Z')) `,
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
			``,
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
			`SELECT count() FROM "` + TableName + `" WHERE ("timestamp">=parseDateTime64BestEffort('2024-02-02T13:47:16.029Z') AND "timestamp"<=parseDateTime64BestEffort('2024-02-09T13:47:16.029Z')) AND NOT "FlightDelayMin" == 0 `,
			`SELECT "FlightDelayMin", count() FROM "` + TableName + `" WHERE ("timestamp">=parseDateTime64BestEffort('2024-02-02T13:47:16.029Z') AND "timestamp"<=parseDateTime64BestEffort('2024-02-09T13:47:16.029Z')) AND NOT "FlightDelayMin" == 0  GROUP BY (FlightDelayMin)`,
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
											"key": 1707476400000,
											"key_as_string": "2024-02-09T12:00:00.000+01:00"
										},
										{
											"doc_count": 80,
											"key": 1707487200000,
											"key_as_string": "2024-02-09T15:00:00.000+01:00"
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
											"key_as_string": "2024-02-09T12:00:00.000+01:00"
										},
										{
											"doc_count": 32,
											"key": 1707487200000,
											"key_as_string": "2024-02-09T15:00:00.000+01:00"
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
											"key_as_string": "2024-02-09T12:00:00.000+01:00"
										},
										{
											"doc_count": 11,
											"key": 1707487200000,
											"key_as_string": "2024-02-09T15:00:00.000+01:00"
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
					model.NewQueryResultCol("key", int64(1707476400000/1000/60/60/3)), // divide by 3h
					model.NewQueryResultCol("doc_count", 22),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("key", "info"),
					model.NewQueryResultCol("key", int64(1707487200000/1000/60/60/3)), // divide by 3h
					model.NewQueryResultCol("doc_count", 80),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("key", "debug"),
					model.NewQueryResultCol("key", int64(1707476400000/1000/60/60/3)), // divide by 3h
					model.NewQueryResultCol("doc_count", 17),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("key", "debug"),
					model.NewQueryResultCol("key", int64(1707487200000/1000/60/60/3)), // divide by 3h
					model.NewQueryResultCol("doc_count", 32),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("key", "critical"),
					model.NewQueryResultCol("key", int64(1707476400000/1000/60/60/3)), // divide by 3h
					model.NewQueryResultCol("doc_count", 5),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("key", "critical"),
					model.NewQueryResultCol("key", int64(1707487200000/1000/60/60/3)), // divide by 3h
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
			`SELECT count() FROM "` + TableName + `" WHERE "host.name" iLIKE '%prometheus%' AND ("@timestamp"<=parseDateTime64BestEffort('2024-02-09T16:36:49.940Z') AND "@timestamp">=parseDateTime64BestEffort('2024-02-02T16:36:49.940Z')) `,
			`SELECT "severity", toInt64(toUnixTimestamp64Milli(` + "`@timestamp`" + `)/10800000), count() FROM "` + TableName + `" WHERE "host.name" iLIKE '%prometheus%' AND ("@timestamp">=parseDateTime64BestEffort('2024-02-02T16:36:49.940Z') AND "@timestamp"<=parseDateTime64BestEffort('2024-02-09T16:36:49.940Z'))  GROUP BY (severity, toInt64(toUnixTimestamp64Milli(` + "`@timestamp`)/10800000))",
			`SELECT "severity", count() FROM "` + TableName + `" WHERE "host.name" iLIKE '%prometheus%' AND ("@timestamp">=parseDateTime64BestEffort('2024-02-02T16:36:49.940Z') AND "@timestamp"<=parseDateTime64BestEffort('2024-02-09T16:36:49.940Z'))  GROUP BY (severity)`,
		},
	},
	{ // [10] doesn't work yet :(( harder than all before
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
											"key": 1707476400000,
											"key_as_string": "2024-02-09T12:00:00.000+01:00"
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
											"key_as_string": "2024-02-12T12:00:00.000+01:00"
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
											"key_as_string": "2024-02-13T00:00:00.000+01:00"
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
			{},
		},
		[]string{
			``,
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
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol("", ""), model.NewQueryResultCol("doc_count", 442)}}},
			{
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("", ""), model.NewQueryResultCol("key", "hephaestus"), model.NewQueryResultCol("doc_count", 30)}},
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("", ""), model.NewQueryResultCol("pos", "poseidon"), model.NewQueryResultCol("doc_count", 29)}},
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("", ""), model.NewQueryResultCol("pos", "jupiter"), model.NewQueryResultCol("doc_count", 28)}},
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("", ""), model.NewQueryResultCol("pos", "selen"), model.NewQueryResultCol("doc_count", 26)}},
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("", ""), model.NewQueryResultCol("pos", "demeter"), model.NewQueryResultCol("doc_count", 24)}},
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("", ""), model.NewQueryResultCol("pos", "iris"), model.NewQueryResultCol("doc_count", 24)}},
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("", ""), model.NewQueryResultCol("pos", "pan"), model.NewQueryResultCol("doc_count", 24)}},
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("", ""), model.NewQueryResultCol("pos", "hades"), model.NewQueryResultCol("doc_count", 22)}},
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("", ""), model.NewQueryResultCol("pos", "hermes"), model.NewQueryResultCol("doc_count", 22)}},
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("", ""), model.NewQueryResultCol("pos", "persephone"), model.NewQueryResultCol("doc_count", 21)}},
			},
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol("doc_count", 442)}}},
		},
		[]string{
			`SELECT count() FROM "` + TableName + `" WHERE ("@timestamp">=parseDateTime64BestEffort('2024-01-23T11:27:16.820Z') AND "@timestamp"<=parseDateTime64BestEffort('2024-01-23T11:42:16.820Z')) AND "message" iLIKE '%user%' `,
			`SELECT value_count("host.name") FROM "` + TableName + `" WHERE ("@timestamp">=parseDateTime64BestEffort('2024-01-23T11:27:16.820Z') AND "@timestamp"<=parseDateTime64BestEffort('2024-01-23T11:42:16.820Z')) AND "message" iLIKE '%user%' `,
			`SELECT '', "host.name", count() FROM "` + TableName + `" WHERE ("@timestamp">=parseDateTime64BestEffort('2024-01-23T11:27:16.820Z') AND "@timestamp"<=parseDateTime64BestEffort('2024-01-23T11:42:16.820Z')) AND "message" iLIKE '%user%'  GROUP BY (host.name)`,
			`SELECT count() FROM "` + TableName + `" WHERE ("@timestamp">=parseDateTime64BestEffort('2024-01-23T11:27:16.820Z') AND "@timestamp"<=parseDateTime64BestEffort('2024-01-23T11:42:16.820Z')) AND "message" iLIKE '%user%' `,
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
							{"doc_count": 2,  "key": 1706021670000, "key_as_string": "2024-01-23T15:54:30.000+01:00"},
							{"doc_count": 13, "key": 1706021700000, "key_as_string": "2024-01-23T15:55:00.000+01:00"},
							{"doc_count": 14, "key": 1706021730000, "key_as_string": "2024-01-23T15:55:30.000+01:00"},
							{"doc_count": 14, "key": 1706021760000, "key_as_string": "2024-01-23T15:56:00.000+01:00"},
							{"doc_count": 15, "key": 1706021790000, "key_as_string": "2024-01-23T15:56:30.000+01:00"},
							{"doc_count": 13, "key": 1706021820000, "key_as_string": "2024-01-23T15:57:00.000+01:00"},
							{"doc_count": 15, "key": 1706021850000, "key_as_string": "2024-01-23T15:57:30.000+01:00"},
							{"doc_count": 11, "key": 1706021880000, "key_as_string": "2024-01-23T15:58:00.000+01:00"}
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
			`SELECT count() FROM "` + TableName + `" WHERE "message" iLIKE '%user%' AND ("@timestamp">=parseDateTime64BestEffort('2024-01-23T14:43:19.481Z') AND "@timestamp"<=parseDateTime64BestEffort('2024-01-23T14:58:19.481Z')) `,
			`SELECT ` + timestampGroupByClause + `, count() FROM "` + TableName + `" WHERE "message" iLIKE '%user%' AND ("@timestamp">=parseDateTime64BestEffort('2024-01-23T14:43:19.481Z') AND "@timestamp"<=parseDateTime64BestEffort('2024-01-23T14:58:19.481Z'))  GROUP BY (` + timestampGroupByClause + ")"},
	},
	{ // [13], "old" test, also can be found in testdata/requests.go TestAsyncSearch[4]
		// Copied it also here to be more sure we do not create some regression
		// TODO let's copy results once we see this test it happens very often but we don't have a response.
		"histogram: regression test",
		`{
			"size":0,
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
			"track_total_hits":true
		}`,
		`TODO!!!`,
		[][]model.QueryResultRow{
			{},
		},
		[]string{`TODO`},
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
			{}, // on purpose, simulates no rows returned
			{}, // on purpose, simulates no rows returned
		},
		[]string{
			`SELECT count() FROM "` + TableName + `" WHERE "message" iLIKE '%posei%' AND ("message" iLIKE '%User%' OR "message" iLIKE '%logged%' OR "message" iLIKE '%out%') AND "host.name" iLIKE '%poseidon%' `,
			`SELECT min("@timestamp") FROM "` + TableName + `" WHERE "message" iLIKE '%posei%' AND ("message" iLIKE '%User%' OR "message" iLIKE '%logged%' OR "message" iLIKE '%out%') AND "host.name" iLIKE '%poseidon%' `,
			`SELECT max("@timestamp") FROM "` + TableName + `" WHERE "message" iLIKE '%posei%' AND ("message" iLIKE '%User%' OR "message" iLIKE '%logged%' OR "message" iLIKE '%out%') AND "host.name" iLIKE '%poseidon%' `,
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
								"key": 1708297200000,
								"key_as_string": "2024-02-19T00:00:00.000+01:00"
							},
							{
								"1": {
									"value": 11116.45703125
								},
								"doc_count": 158,
								"key": 1708383600000,
								"key_as_string": "2024-02-20T00:00:00.000+01:00"
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
			`SELECT count() FROM "` + TableName + `" WHERE "order_date">=parseDateTime64BestEffort('2024-02-19T17:40:56.351Z') AND "order_date"<=parseDateTime64BestEffort('2024-02-26T17:40:56.351Z') `,
			``,
			`SELECT ` + clickhouse.TimestampGroupBy("order_date", clickhouse.DateTime, 24*time.Hour) + `, count() FROM "` + TableName + `" WHERE "order_date">=parseDateTime64BestEffort('2024-02-19T17:40:56.351Z') AND "order_date"<=parseDateTime64BestEffort('2024-02-26T17:40:56.351Z')  GROUP BY (` + clickhouse.TimestampGroupBy("order_date", clickhouse.DateTime, 24*time.Hour) + ")",
		},
	},
}
