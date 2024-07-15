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
				`ORDER BY count() DESC, "OriginCityName" ` +
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
	{ // [5]
		TestName: "Ophelia Test 2: triple terms + other aggregations + order by another aggregations",
		ExpectedResults: [][]model.QueryResultRow{
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a1"),
					model.NewQueryResultCol(`sumOrNull("total")`, 1091661.7608666667),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a2"),
					model.NewQueryResultCol(`sumOrNull("total")`, 630270.07765),
				}},
			},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a1"),
					model.NewQueryResultCol("limbName", "b11"),
					model.NewQueryResultCol(`sumOrNull("total")`, 51891.94613333333),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a1"),
					model.NewQueryResultCol("limbName", "b12"),
					model.NewQueryResultCol(`sumOrNull("total")`, 45774.291766666654),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a2"),
					model.NewQueryResultCol("limbName", "b21"),
					model.NewQueryResultCol(`sumOrNull("total")`, 399126.7496833334),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a2"),
					model.NewQueryResultCol("limbName", "b22"),
					model.NewQueryResultCol(`sumOrNull("total")`, 231143.3279666666),
				}},
			},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a1"),
					model.NewQueryResultCol("limbName", "b11"),
					model.NewQueryResultCol("organName", "c11"),
					model.NewQueryResultCol(`sumOrNull("total")`, 51891.94613333333),
					model.NewQueryResultCol(`sumOrNull("some")`, 37988.09523333333),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a1"),
					model.NewQueryResultCol("limbName", "b12"),
					model.NewQueryResultCol("organName", "c12"),
					model.NewQueryResultCol(`sumOrNull("total")`, 45774.291766666654),
					model.NewQueryResultCol(`sumOrNull("some")`, 36577.89516666666),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a2"),
					model.NewQueryResultCol("limbName", "b21"),
					model.NewQueryResultCol("organName", "c21"),
					model.NewQueryResultCol(`sumOrNull("total")`, 399126.7496833334),
					model.NewQueryResultCol(`sumOrNull("some")`, 337246.82201666664),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a2"),
					model.NewQueryResultCol("limbName", "b22"),
					model.NewQueryResultCol("organName", "c22"),
					model.NewQueryResultCol(`sumOrNull("total")`, 231143.3279666666),
					model.NewQueryResultCol(`sumOrNull("some")`, 205408.48849999998),
				}},
			},
			{}, // NoDbQuery
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a1"),
					model.NewQueryResultCol("limbName", "b11"),
					model.NewQueryResultCol("organName", "c11"),
					model.NewQueryResultCol("count()", 21),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a1"),
					model.NewQueryResultCol("limbName", "b12"),
					model.NewQueryResultCol("organName", "c12"),
					model.NewQueryResultCol("count()", 24),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a2"),
					model.NewQueryResultCol("limbName", "b21"),
					model.NewQueryResultCol("organName", "c21"),
					model.NewQueryResultCol("count()", 17),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a2"),
					model.NewQueryResultCol("limbName", "b22"),
					model.NewQueryResultCol("organName", "c22"),
					model.NewQueryResultCol("count()", 17),
				}},
			},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a1"),
					model.NewQueryResultCol("limbName", "b11"),
					model.NewQueryResultCol("count()", 21),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a1"),
					model.NewQueryResultCol("limbName", "b12"),
					model.NewQueryResultCol("count()", 24),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a2"),
					model.NewQueryResultCol("limbName", "b21"),
					model.NewQueryResultCol("count()", 17),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a2"),
					model.NewQueryResultCol("limbName", "b22"),
					model.NewQueryResultCol("count()", 17),
				}},
			},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a1"),
					model.NewQueryResultCol("count()", 1036),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a2"),
					model.NewQueryResultCol("count()", 34),
				}},
			},
		},
		ExpectedSQLs: []string{
			`WITH cte_1 AS ` +
				`(SELECT "surname" AS "cte_1_1", count() AS "cte_1_cnt" ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`GROUP BY "surname" ` +
				`ORDER BY count() DESC, "surname" ` +
				`LIMIT 200) ` +
				`SELECT "surname", sumOrNull("total") ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`INNER JOIN "cte_1" ON "surname" = "cte_1_1" ` +
				`GROUP BY "surname", cte_1_cnt ` +
				`ORDER BY cte_1_cnt DESC, "surname"`,
			`WITH cte_1 AS ` +
				`(SELECT "surname" AS "cte_1_1", count() AS "cte_1_cnt" ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`GROUP BY "surname" ` +
				`ORDER BY count() DESC, "surname" ` +
				`LIMIT 200), ` +
				`cte_2 AS ` +
				`(SELECT "surname" AS "cte_2_1", COALESCE("limbName",'__missing__') AS "cte_2_2", count() AS "cte_2_cnt" ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`GROUP BY "surname", COALESCE("limbName",'__missing__') ` +
				`ORDER BY count() DESC, COALESCE("limbName",'__missing__') ` +
				`LIMIT 20 BY "surname") ` +
				`SELECT "surname", COALESCE("limbName",'__missing__'), sumOrNull("total") ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`INNER JOIN "cte_1" ON "surname" = "cte_1_1" ` +
				`INNER JOIN "cte_2" ON "surname" = "cte_2_1" AND COALESCE("limbName",'__missing__') = "cte_2_2" ` +
				`GROUP BY "surname", COALESCE("limbName",'__missing__'), cte_1_cnt, cte_2_cnt ` +
				`ORDER BY cte_1_cnt DESC, "surname", cte_2_cnt DESC, COALESCE("limbName",'__missing__')`,
			`WITH cte_1 AS ` +
				`(SELECT "surname" AS "cte_1_1", count() AS "cte_1_cnt" ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`GROUP BY "surname" ` +
				`ORDER BY count() DESC, "surname" ` +
				`LIMIT 200), ` +
				`cte_2 AS ` +
				`(SELECT "surname" AS "cte_2_1", COALESCE("limbName",'__missing__') AS "cte_2_2", count() AS "cte_2_cnt" ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`GROUP BY "surname", COALESCE("limbName",'__missing__') ` +
				`ORDER BY count() DESC, COALESCE("limbName",'__missing__') ` +
				`LIMIT 20 BY "surname"), ` +
				`cte_3 AS ` +
				`(SELECT "surname" AS "cte_3_1", COALESCE("limbName",'__missing__') AS "cte_3_2", "organName" AS "cte_3_3", count() AS "cte_3_cnt" ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`GROUP BY "surname", COALESCE("limbName",'__missing__'), "organName" ` +
				`ORDER BY count() DESC, "organName" ` +
				`LIMIT 1 BY "surname", COALESCE("limbName",'__missing__')) ` +
				`SELECT "surname", COALESCE("limbName",'__missing__'), "organName", sumOrNull("total"), sumOrNull("some") ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`INNER JOIN "cte_1" ON "surname" = "cte_1_1" ` +
				`INNER JOIN "cte_2" ON "surname" = "cte_2_1" AND COALESCE("limbName",'__missing__') = "cte_2_2" ` +
				`INNER JOIN "cte_3" ON "surname" = "cte_3_1" AND COALESCE("limbName",'__missing__') = "cte_3_2" AND "organName" = "cte_3_3" ` +
				`GROUP BY "surname", COALESCE("limbName",'__missing__'), "organName", cte_1_cnt, cte_2_cnt, cte_3_cnt ` +
				`ORDER BY cte_1_cnt DESC, "surname", cte_2_cnt DESC, COALESCE("limbName",'__missing__'), cte_3_cnt DESC, "organName"`,
			`NoDBQuery`,
			`WITH cte_1 AS ` +
				`(SELECT "surname" AS "cte_1_1", count() AS "cte_1_cnt" ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`GROUP BY "surname" ` +
				`ORDER BY count() DESC, "surname" ` +
				`LIMIT 200), ` +
				`cte_2 AS ` +
				`(SELECT "surname" AS "cte_2_1", COALESCE("limbName",'__missing__') AS "cte_2_2", count() AS "cte_2_cnt" ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`GROUP BY "surname", COALESCE("limbName",'__missing__') ` +
				`ORDER BY count() DESC, COALESCE("limbName",'__missing__') ` +
				`LIMIT 20 BY "surname") ` +
				`SELECT "surname", COALESCE("limbName",'__missing__'), "organName", count() ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`INNER JOIN "cte_1" ON "surname" = "cte_1_1" ` +
				`INNER JOIN "cte_2" ON "surname" = "cte_2_1" AND COALESCE("limbName",'__missing__') = "cte_2_2" ` +
				`GROUP BY "surname", COALESCE("limbName",'__missing__'), "organName", cte_1_cnt, cte_2_cnt ` +
				`ORDER BY cte_1_cnt DESC, "surname", cte_2_cnt DESC, COALESCE("limbName",'__missing__'), count() DESC, "organName" ` +
				`LIMIT 1 BY "surname", COALESCE("limbName",'__missing__')`,
			`WITH cte_1 AS ` +
				`(SELECT "surname" AS "cte_1_1", count() AS "cte_1_cnt" ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`GROUP BY "surname" ` +
				`ORDER BY count() DESC, "surname" ` +
				`LIMIT 200) ` +
				`SELECT "surname", COALESCE("limbName",'__missing__'), count() ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`INNER JOIN "cte_1" ON "surname" = "cte_1_1" ` +
				`GROUP BY "surname", COALESCE("limbName",'__missing__'), cte_1_cnt ` +
				`ORDER BY cte_1_cnt DESC, "surname", count() DESC, COALESCE("limbName",'__missing__') ` +
				`LIMIT 20 BY "surname"`,
			`SELECT "surname", count() ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`GROUP BY "surname" ` +
				`ORDER BY count() DESC, "surname" ` +
				`LIMIT 200`,
		},
	},
}
