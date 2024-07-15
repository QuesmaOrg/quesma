// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package query_optimizers

import (
	"quesma/model"
	"quesma/testdata"
)

var MergeMetricsAggsOptimizerTests = []testdata.AggregationTestCase{
	{ // [0]
		"simple max/min aggregation as 2 siblings, all 3 queries mergeable",
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
			{{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("count()", uint64(2200)),
				model.NewQueryResultCol(`maxOrNull("AvgTicketPrice")`, 1199.72900390625),
				model.NewQueryResultCol(`minOrNull("AvgTicketPrice")`, 100.14596557617188),
			}}},
			{},
			{},
		},
		[]string{
			`SELECT count(), maxOrNull("AvgTicketPrice"), minOrNull("AvgTicketPrice") ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`WHERE ("timestamp">=parseDateTime64BestEffort('2024-02-02T13:47:16.029Z') ` +
				`AND "timestamp"<=parseDateTime64BestEffort('2024-02-09T13:47:16.029Z'))`,
			`NoDBQuery`,
			`NoDBQuery`,
		},
	},
	{ // [1]
		"simple max/min aggregation as 2 siblings, both mergeable, count - not (1/2)",
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
			"track_total_hits": 3
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
			{{Cols: []model.QueryResultCol{
				model.NewQueryResultCol(`maxOrNull("AvgTicketPrice")`, 1199.72900390625),
				model.NewQueryResultCol(`minOrNull("AvgTicketPrice")`, 100.14596557617188),
			}}},
			{},
		},
		[]string{
			`SELECT count() FROM ` +
				`(SELECT 1 ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`WHERE ("timestamp">=parseDateTime64BestEffort('2024-02-02T13:47:16.029Z') ` +
				`AND "timestamp"<=parseDateTime64BestEffort('2024-02-09T13:47:16.029Z')) ` +
				`LIMIT 3)`,
			`SELECT maxOrNull("AvgTicketPrice"), minOrNull("AvgTicketPrice") ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`WHERE ("timestamp">=parseDateTime64BestEffort('2024-02-02T13:47:16.029Z') ` +
				`AND "timestamp"<=parseDateTime64BestEffort('2024-02-09T13:47:16.029Z'))`,
			`NoDBQuery`,
		},
	},
	{ // [2]
		"simple max/min aggregation as 2 siblings, both mergeable, count - not (2/2)",
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
			"track_total_hits": false
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
			{{Cols: []model.QueryResultCol{
				model.NewQueryResultCol(`maxOrNull("AvgTicketPrice")`, 1199.72900390625),
				model.NewQueryResultCol(`minOrNull("AvgTicketPrice")`, 100.14596557617188),
			}}},
			{}, // NoDBQuery
		},
		[]string{
			`SELECT maxOrNull("AvgTicketPrice"), minOrNull("AvgTicketPrice") ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`WHERE ("timestamp">=parseDateTime64BestEffort('2024-02-02T13:47:16.029Z') ` +
				`AND "timestamp"<=parseDateTime64BestEffort('2024-02-09T13:47:16.029Z'))`,
			`NoDBQuery`,
		},
	},
}

var MergeMetricsAggsTestUpdates = []testdata.MergeMetricsAggsTestUpdate{
	{ // [0]
		TestName: "simple max/min aggregation as 2 siblings",
		ExpectedResults: [][]model.QueryResultRow{
			{{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("count()", uint64(2200)),
				model.NewQueryResultCol(`maxOrNull("AvgTicketPrice")`, 1199.72900390625),
				model.NewQueryResultCol(`minOrNull("AvgTicketPrice")`, 100.14596557617188),
			}}},
			{},
			{},
		},
		ExpectedSQLs: []string{
			`SELECT count(), maxOrNull("AvgTicketPrice"), minOrNull("AvgTicketPrice") ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`WHERE ("timestamp">=parseDateTime64BestEffort('2024-02-02T13:47:16.029Z') ` +
				`AND "timestamp"<=parseDateTime64BestEffort('2024-02-09T13:47:16.029Z'))`,
			`NoDBQuery`,
			`NoDBQuery`,
		},
	},
	{ // [1]
		TestName: "Sum",
		ExpectedResults: [][]model.QueryResultRow{
			{{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("count()", uint64(1043)),
				model.NewQueryResultCol(`sumOrNull("taxful_total_price")`, 76631.67578125),
			}}},
			{},
		},
		ExpectedSQLs: []string{
			`SELECT count(), sumOrNull("taxful_total_price") ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`WHERE ("order_date">=parseDateTime64BestEffort('2024-02-06T09:59:57.034Z') ` +
				`AND "order_date"<=parseDateTime64BestEffort('2024-02-13T09:59:57.034Z'))`,
			`NoDBQuery`,
		},
	},
	{ // [2]
		TestName: "cardinality",
		ExpectedResults: [][]model.QueryResultRow{
			{{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("count()", uint64(2200)),
				model.NewQueryResultCol(`"count(DISTINCT "OriginCityName")`, 143),
			}}},
			{
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", "Rome"), model.NewQueryResultCol("doc_count", 73)}},
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", "Bogota"), model.NewQueryResultCol("doc_count", 44)}},
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", "Milan"), model.NewQueryResultCol("doc_count", 32)}},
			},
			{},
		},
		ExpectedSQLs: []string{
			`SELECT count(), count(DISTINCT "OriginCityName") ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`WHERE ("timestamp">=parseDateTime64BestEffort('2024-02-02T13:47:16.029Z') ` +
				`AND "timestamp"<=parseDateTime64BestEffort('2024-02-09T13:47:16.029Z'))`,
			`SELECT "OriginCityName", count() ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`WHERE ("timestamp">=parseDateTime64BestEffort('2024-02-02T13:47:16.029Z') ` +
				`AND "timestamp"<=parseDateTime64BestEffort('2024-02-09T13:47:16.029Z')) ` +
				`GROUP BY "OriginCityName" ` +
				`ORDER BY count() DESC ` +
				`LIMIT 10`,
			`NoDBQuery`,
		},
	},
	{ // [3]
		TestName: "earliest/latest timestamp: regression test",
		ExpectedResults: [][]model.QueryResultRow{
			{{Cols: []model.QueryResultCol{
				model.NewQueryResultCol(`avgOrNull("@timestamp")`, nil),
				model.NewQueryResultCol(`minOrNull("@timestamp")`, nil),
				model.NewQueryResultCol(`maxOrNull("@timestamp")`, nil),
			}}},
			{},
			{},
		},
		ExpectedSQLs: []string{
			`SELECT avgOrNull("@timestamp"), minOrNull("@timestamp"), maxOrNull("@timestamp") ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`WHERE (("message" iLIKE '%posei%' AND "message" iLIKE '%User logged out%') AND "host.name" iLIKE '%poseidon%')`,
			`NoDBQuery`,
			`NoDBQuery`,
		},
	},
	{ // [4]
		TestName: "Min/max with simple script. Reproduce: Visualize -> Line -> Metrics: Count, Buckets: X-Asis Histogram",
		ExpectedResults: [][]model.QueryResultRow{
			{{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("count()", uint64(13059)),
				model.NewQueryResultCol(`maxOrNull(toHour("timestamp"))`, 23.0),
				model.NewQueryResultCol(`minOrNull(toHour("timestamp"))`, 0.0),
			}}},
			{},
			{},
		},
		ExpectedSQLs: []string{
			`SELECT count(), maxOrNull(toHour("timestamp")), minOrNull(toHour("timestamp")) ` +
				`FROM ` + testdata.QuotedTableName,
			`NoDBQuery`,
			`NoDBQuery`,
		},
	},
}
