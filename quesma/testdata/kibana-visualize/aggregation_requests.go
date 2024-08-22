// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package kibana_visualize

import (
	"quesma/model"
	"quesma/testdata"
)

var AggregationTests = []testdata.AggregationTestCase{
	{ // [0]
		TestName: "Multi_terms without subaggregations. Visualize: Bar Vertical: Horizontal Axis: Date Histogram, Vertical Axis: Count of records, Breakdown: Top values (2 values)",
		QueryRequestJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"0": {
					"aggs": {
						"1": {
							"multi_terms": {
								"order": {
									"_count": "desc"
								},
								"size": 3,
								"terms": [
									{
										"field": "severity"
									},
									{
										"field": "source"
									}
								]
							}
						}
					},
					"date_histogram": {
						"extended_bounds": {
							"max": 1716812096627,
							"min": 1716811196627
						},
						"field": "@timestamp",
						"fixed_interval": "30s",
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
							"range": {
								"@timestamp": {
									"format": "strict_date_optional_time",
									"gte": "2024-05-27T11:59:56.627Z",
									"lte": "2024-05-27T12:14:56.627Z"
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
		// I erased empty date_histogram buckets, we don't support extended_bounds yet
		ExpectedResponse: `
		{
			"completion_time_in_millis": 1716834974737,
			"expiration_time_in_millis": 1716835034732,
			"id": "FnFPQm5xWDFEU2gtVlBOZnBkX3RNeFEcRVZINklxc1VTQ2lhVEtwMnpmZjNEZzoyNDM3OQ==",
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
											"doc_count": 1,
											"key": [
												"artemis",
												"error"
											],
											"key_as_string": "artemis|error"
										},
										{
											"doc_count": 1,
											"key": [
												"artemis",
												"info"
											],
											"key_as_string": "artemis|info"
										},
										{
											"doc_count": 1,
											"key": [
												"jupiter",
												"info"
											],
											"key_as_string": "jupiter|info"
										}
									],
									"doc_count_error_upper_bound": 0,
									"sum_other_doc_count": 1
								},
								"doc_count": 4,
								"key": 1716834210000,
								"key_as_string": "2024-05-27T18:23:30.000"
							},
							{
								"1": {
									"buckets": [
										{
											"doc_count": 2,
											"key": [
												"apollo",
												"info"
											],
											"key_as_string": "apollo|info"
										},
										{
											"doc_count": 1,
											"key": [
												"cassandra",
												"debug"
											],
											"key_as_string": "cassandra|debug"
										}
									],
									"doc_count_error_upper_bound": 0,
									"sum_other_doc_count": 12
								},
								"doc_count": 16,
								"key": 1716834270000,
								"key_as_string": "2024-05-27T18:24:30.000"
							}
						]
					}
				},
				"hits": {
					"hits": [],
					"max_score": null,
					"total": {
						"relation": "eq",
						"value": 378
					}
				},
				"timed_out": false,
				"took": 5
			},
			"start_time_in_millis": 1716834974732
		}`,
		ExpectedResults: [][]model.QueryResultRow{
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol("hits", uint64(378))}}},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("toInt64(toUnixTimestamp64Milli(`@timestamp`)/30000)", int64(1716834210000/30000)),
					model.NewQueryResultCol("severity", "artemis"),
					model.NewQueryResultCol("source", "error"),
					model.NewQueryResultCol("count()", 1),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("toInt64(toUnixTimestamp64Milli(`@timestamp`)/30000)", int64(1716834210000/30000)),
					model.NewQueryResultCol("severity", "artemis"),
					model.NewQueryResultCol("source", "info"),
					model.NewQueryResultCol("count()", 1),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("toInt64(toUnixTimestamp64Milli(`@timestamp`)/30000)", int64(1716834210000/30000)),
					model.NewQueryResultCol("severity", "jupiter"),
					model.NewQueryResultCol("source", "info"),
					model.NewQueryResultCol("count()", 1),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("toInt64(toUnixTimestamp64Milli(`@timestamp`)/30000)", int64(1716834270000/30000)),
					model.NewQueryResultCol("severity", "apollo"),
					model.NewQueryResultCol("source", "info"),
					model.NewQueryResultCol("count()", 2),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("toInt64(toUnixTimestamp64Milli(`@timestamp`)/30000)", int64(1716834270000/30000)),
					model.NewQueryResultCol("severity", "cassandra"),
					model.NewQueryResultCol("source", "debug"),
					model.NewQueryResultCol("count()", 1),
				}},
			},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("toInt64(toUnixTimestamp64Milli(`@timestamp`)/30000)", int64(1716834210000/30000)),
					model.NewQueryResultCol("count()", 4),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("toInt64(toUnixTimestamp64Milli(`@timestamp`)/30000)", int64(1716834270000/30000)),
					model.NewQueryResultCol("count()", 16),
				}},
			},
		},
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__key_0", int64(1716834210000/30000)),
				model.NewQueryResultCol("aggr__0__count", 4),
				model.NewQueryResultCol("aggr__0__1__parent_count", uint64(4)),
				model.NewQueryResultCol("aggr__0__1__key_0", "artemis"),
				model.NewQueryResultCol("aggr__0__1__key_1", "error"),
				model.NewQueryResultCol("aggr__0__1__count", 1),
				model.NewQueryResultCol("aggr__0__1__order_2", 1),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__key_0", int64(1716834210000/30000)),
				model.NewQueryResultCol("aggr__0__count", 4),
				model.NewQueryResultCol("aggr__0__1__parent_count", uint64(4)),
				model.NewQueryResultCol("aggr__0__1__key_0", "artemis"),
				model.NewQueryResultCol("aggr__0__1__key_1", "info"),
				model.NewQueryResultCol("aggr__0__1__count", 1),
				model.NewQueryResultCol("aggr__0__1__order_2", 1),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__key_0", int64(1716834210000/30000)),
				model.NewQueryResultCol("aggr__0__count", 4),
				model.NewQueryResultCol("aggr__0__1__parent_count", uint64(4)),
				model.NewQueryResultCol("aggr__0__1__key_0", "jupiter"),
				model.NewQueryResultCol("aggr__0__1__key_1", "info"),
				model.NewQueryResultCol("aggr__0__1__count", 1),
				model.NewQueryResultCol("aggr__0__1__order_2", 1),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__key_0", int64(1716834270000/30000)),
				model.NewQueryResultCol("aggr__0__count", 16),
				model.NewQueryResultCol("aggr__0__1__parent_count", uint64(15)),
				model.NewQueryResultCol("aggr__0__1__key_0", "apollo"),
				model.NewQueryResultCol("aggr__0__1__key_1", "info"),
				model.NewQueryResultCol("aggr__0__1__count", 2),
				model.NewQueryResultCol("aggr__0__1__order_2", 2),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__key_0", int64(1716834270000/30000)),
				model.NewQueryResultCol("aggr__0__count", 16),
				model.NewQueryResultCol("aggr__0__1__parent_count", uint64(15)),
				model.NewQueryResultCol("aggr__0__1__key_0", "cassandra"),
				model.NewQueryResultCol("aggr__0__1__key_1", "debug"),
				model.NewQueryResultCol("aggr__0__1__count", 1),
				model.NewQueryResultCol("aggr__0__1__order_2", 1),
			}},
		},

		ExpectedSQLs: []string{
			`SELECT count() ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`WHERE ("@timestamp">=parseDateTime64BestEffort('2024-05-27T11:59:56.627Z') ` +
				`AND "@timestamp"<=parseDateTime64BestEffort('2024-05-27T12:14:56.627Z'))`,
			`SELECT toInt64(toUnixTimestamp64Milli("@timestamp") / 30000), ` +
				`"severity", "source", count() ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`WHERE ("@timestamp">=parseDateTime64BestEffort('2024-05-27T11:59:56.627Z') ` +
				`AND "@timestamp"<=parseDateTime64BestEffort('2024-05-27T12:14:56.627Z')) ` +
				`GROUP BY toInt64(toUnixTimestamp64Milli("@timestamp") / 30000), ` + `"severity", "source" ` +
				`ORDER BY toInt64(toUnixTimestamp64Milli("@timestamp") / 30000), ` + `"severity", "source"`,
			`SELECT toInt64(toUnixTimestamp64Milli("@timestamp") / 30000), count() ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`WHERE ("@timestamp">=parseDateTime64BestEffort('2024-05-27T11:59:56.627Z') ` +
				`AND "@timestamp"<=parseDateTime64BestEffort('2024-05-27T12:14:56.627Z')) ` +
				`GROUP BY toInt64(toUnixTimestamp64Milli("@timestamp") / 30000) ` +
				`ORDER BY toInt64(toUnixTimestamp64Milli("@timestamp") / 30000)`,
		},
		ExpectedPancakeSQL: `
			SELECT "aggr__0__key_0", "aggr__0__count", "aggr__0__1__parent_count",
			  "aggr__0__1__key_0", "aggr__0__1__key_1", "aggr__0__1__count",
			  "aggr__0__1__order_2"
			FROM (
			  SELECT "aggr__0__key_0", "aggr__0__count", "aggr__0__1__parent_count",
				"aggr__0__1__key_0", "aggr__0__1__key_1", "aggr__0__1__count",
				"aggr__0__1__order_2",
				dense_rank() OVER (ORDER BY "aggr__0__key_0" ASC) AS "aggr__0__order_1_rank"
				,
				dense_rank() OVER (PARTITION BY "aggr__0__key_0" ORDER BY
				"aggr__0__1__order_2" DESC, "aggr__0__1__key_0" ASC, "aggr__0__1__key_1" ASC
				) AS "aggr__0__1__order_1_rank"
			  FROM (
				SELECT toInt64(toUnixTimestamp64Milli("@timestamp") / 30000) AS
				  "aggr__0__key_0",
				  sum(count(*)) OVER (PARTITION BY "aggr__0__key_0") AS "aggr__0__count",
				  sum(count(*)) OVER (PARTITION BY "aggr__0__key_0") AS
				  "aggr__0__1__parent_count", "severity" AS "aggr__0__1__key_0",
				  "source" AS "aggr__0__1__key_1", count(*) AS "aggr__0__1__count",
				  count() AS "aggr__0__1__order_2"
				FROM "logs-generic-default"
				WHERE ("@timestamp">=parseDateTime64BestEffort('2024-05-27T11:59:56.627Z')
				  AND "@timestamp"<=parseDateTime64BestEffort('2024-05-27T12:14:56.627Z'))
				GROUP BY toInt64(toUnixTimestamp64Milli("@timestamp") / 30000) AS
				  "aggr__0__key_0", "severity" AS "aggr__0__1__key_0",
				  "source" AS "aggr__0__1__key_1"))
			WHERE "aggr__0__1__order_1_rank"<=3
			ORDER BY "aggr__0__order_1_rank" ASC, "aggr__0__1__order_1_rank" ASC`,
	},
	{ // [1]
		TestName: "Multi_terms with simple count. Visualize: Bar Vertical: Horizontal Axis: Top values (2 values), Vertical: Count of records, Breakdown: @timestamp",
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
									"max": 1716812073493,
									"min": 1716811173493
								},
								"field": "@timestamp",
								"fixed_interval": "30s",
								"time_zone": "Europe/Warsaw"
							}
						}
					},
					"multi_terms": {
						"order": {
							"_count": "desc"
						},
						"size": 3,
						"terms": [
							{
								"field": "message"
							},
							{
								"field": "host.name"
							}
						]
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
		// I erased empty date_histogram buckets, we don't support extended_bounds yet
		ExpectedResponse: `
		{
			"completion_time_in_millis": 1716834668794,
			"expiration_time_in_millis": 1716834728791,
			"id": "FkpjTm9UNHhVUUNlY3Z5cVNfTk5Db3ccRVZINklxc1VTQ2lhVEtwMnpmZjNEZzoxNjMxMA==",
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
											"doc_count": 1,
											"key": 1716834420000,
											"key_as_string": "2024-05-27T18:27:00.000"
										},
										{
											"doc_count": 1,
											"key": 1716834450000,
											"key_as_string": "2024-05-27T18:27:30.000"
										},
										{
											"doc_count": 2,
											"key": 1716834510000,
											"key_as_string": "2024-05-27T18:28:30.000"
										}
									]
								},
								"doc_count": 13,
								"key": [
									"info",
									"redhat"
								],
								"key_as_string": "info|redhat"
							}
						],
						"doc_count_error_upper_bound": 0,
						"sum_other_doc_count": 188
					}
				},
				"hits": {
					"hits": [],
					"max_score": null,
					"total": {
						"relation": "eq",
						"value": 217
					}
				},
				"timed_out": false,
				"took": 3
			},
			"start_time_in_millis": 1716834668791
		}`,
		ExpectedResults: [][]model.QueryResultRow{
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol("hits", uint64(378))}}},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("message", "info"),
					model.NewQueryResultCol("host.name", "redhat"),
					model.NewQueryResultCol("toInt64(toUnixTimestamp64Milli(`@timestamp`)/30000)", int64(1716834420000/30000)),
					model.NewQueryResultCol("count()", 1),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("message", "info"),
					model.NewQueryResultCol("host.name", "redhat"),
					model.NewQueryResultCol("toInt64(toUnixTimestamp64Milli(`@timestamp`)/30000)", int64(1716834450000/30000)),
					model.NewQueryResultCol("count()", 1),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("message", "info"),
					model.NewQueryResultCol("host.name", "redhat"),
					model.NewQueryResultCol("toInt64(toUnixTimestamp64Milli(`@timestamp`)/30000)", int64(1716834510000/30000)),
					model.NewQueryResultCol("count()", 2),
				}},
			},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("message", "info"),
					model.NewQueryResultCol("host.name", "redhat"),
					model.NewQueryResultCol("count()", 13),
				}},
			},
		},
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__parent_count", uint64(201)),
				model.NewQueryResultCol("aggr__0__key_0", "info"),
				model.NewQueryResultCol("aggr__0__key_1", "redhat"),
				model.NewQueryResultCol("aggr__0__count", 13),
				model.NewQueryResultCol("aggr__0__order_2", 13),
				model.NewQueryResultCol("aggr__0__1__key_0", int64(1716834420000/30000)),
				model.NewQueryResultCol("aggr__0__1__count", 1),
				model.NewQueryResultCol("aggr__0__1__order_1", int64(1716834420000/30000)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__parent_count", uint64(201)),
				model.NewQueryResultCol("aggr__0__key_0", "info"),
				model.NewQueryResultCol("aggr__0__key_1", "redhat"),
				model.NewQueryResultCol("aggr__0__count", 13),
				model.NewQueryResultCol("aggr__0__order_2", 13),
				model.NewQueryResultCol("aggr__0__1__key_0", int64(1716834450000/30000)),
				model.NewQueryResultCol("aggr__0__1__count", 1),
				model.NewQueryResultCol("aggr__0__1__order_1", int64(1716834450000/30000)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__parent_count", uint64(201)),
				model.NewQueryResultCol("aggr__0__key_0", "info"),
				model.NewQueryResultCol("aggr__0__key_1", "redhat"),
				model.NewQueryResultCol("aggr__0__count", 13),
				model.NewQueryResultCol("aggr__0__order_2", 13),
				model.NewQueryResultCol("aggr__0__1__key_0", int64(1716834510000/30000)),
				model.NewQueryResultCol("aggr__0__1__count", 2),
				model.NewQueryResultCol("aggr__0__1__order_1", int64(1716834510000/30000)),
			}},
		},
		ExpectedSQLs: []string{
			`SELECT count() FROM ` + testdata.QuotedTableName,
			`SELECT "message", "host.name", toInt64(toUnixTimestamp64Milli("@timestamp") / 30000), count() ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`GROUP BY "message", "host.name", toInt64(toUnixTimestamp64Milli("@timestamp") / 30000) ` +
				`ORDER BY "message", "host.name", toInt64(toUnixTimestamp64Milli("@timestamp") / 30000)`,
			`SELECT "message", "host.name", count() ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`GROUP BY "message", "host.name" ` +
				`ORDER BY "message", "host.name"`,
		},
		ExpectedPancakeSQL: `
			SELECT "aggr__0__parent_count", "aggr__0__key_0", "aggr__0__key_1",
			  "aggr__0__count", "aggr__0__order_2", "aggr__0__1__key_0",
			  "aggr__0__1__count"
			FROM (
			  SELECT "aggr__0__parent_count", "aggr__0__key_0", "aggr__0__key_1",
				"aggr__0__count", "aggr__0__order_2", "aggr__0__1__key_0",
				"aggr__0__1__count",
				dense_rank() OVER (ORDER BY "aggr__0__order_2" DESC, "aggr__0__key_0" ASC,
				"aggr__0__key_1" ASC) AS "aggr__0__order_1_rank",
				dense_rank() OVER (PARTITION BY "aggr__0__key_0", "aggr__0__key_1" ORDER BY
				"aggr__0__1__key_0" ASC) AS "aggr__0__1__order_1_rank"
			  FROM (
				SELECT sum(count(*)) OVER () AS "aggr__0__parent_count",
				  "message" AS "aggr__0__key_0", "host.name" AS "aggr__0__key_1",
				  sum(count(*)) OVER (PARTITION BY "aggr__0__key_0", "aggr__0__key_1") AS
				  "aggr__0__count",
				  sum(count()) OVER (PARTITION BY "aggr__0__key_0", "aggr__0__key_1") AS
				  "aggr__0__order_2",
				  toInt64(toUnixTimestamp64Milli("@timestamp") / 30000) AS
				  "aggr__0__1__key_0", count(*) AS "aggr__0__1__count"
				FROM "logs-generic-default"
				GROUP BY "message" AS "aggr__0__key_0", "host.name" AS "aggr__0__key_1",
				  toInt64(toUnixTimestamp64Milli("@timestamp") / 30000) AS
				  "aggr__0__1__key_0"))
			WHERE "aggr__0__order_1_rank"<=3
			ORDER BY "aggr__0__order_1_rank" ASC, "aggr__0__1__order_1_rank" ASC`,
	},
	{ //[2],
		TestName: "Multi_terms with double-nested subaggregations. Visualize: Bar Vertical: Horizontal Axis: Top values (2 values), Vertical: Unique count, Breakdown: @timestamp",
		QueryRequestJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"0": {
					"aggs": {
						"1": {
							"aggs": {
								"2": {
									"cardinality": {
										"field": "severity"
									}
								}
							},
							"date_histogram": {
								"extended_bounds": {
									"max": 1716834478178,
									"min": 1716833578178
								},
								"field": "@timestamp",
								"fixed_interval": "30s",
								"time_zone": "Europe/Warsaw"
							}
						},
						"2": {
							"cardinality": {
								"field": "severity"
							}
						}
					},
					"multi_terms": {
						"order": {
							"2": "desc"
						},
						"size": 3,
						"terms": [
							{
								"field": "severity"
							},
							{
								"field": "source"
							}
						]
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
		// I erased empty date_histogram buckets, we don't support extended_bounds yet
		ExpectedResponse: `
		{
			"completion_time_in_millis": 1716834482828,
			"expiration_time_in_millis": 1716834542815,
			"id": "FlhQOUVMZDhSU1V1azdxbW9rREE2a2ccRVZINklxc1VTQ2lhVEtwMnpmZjNEZzoxMTUwNA==",
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
											"2": {
												"value": 1
											},
											"doc_count": 1,
											"key": 1716834300000,
											"key_as_string": "2024-05-27T18:25:00.000"
										},
										{
											"2": {
												"value": 1
											},
											"doc_count": 1,
											"key": 1716834390000,
											"key_as_string": "2024-05-27T18:26:30.000"
										}
									]
								},
								"2": {
									"value": 1
								},
								"doc_count": 2,
								"key": [
									"critical",
									"alpine"
								],
								"key_as_string": "critical|alpine"
							},
							{
								"1": {
									"buckets": [
										{
											"2": {
												"value": 1
											},
											"doc_count": 1,
											"key": 1716834270000,
											"key_as_string": "2024-05-27T18:24:30.000"
										}
									]
								},
								"2": {
									"value": 1
								},
								"doc_count": 1,
								"key": [
									"critical",
									"fedora"
								],
								"key_as_string": "critical|fedora"
							}
						],
						"doc_count_error_upper_bound": 0,
						"sum_other_doc_count": 121
					}
				},
				"hits": {
					"hits": [],
					"max_score": null,
					"total": {
						"relation": "eq",
						"value": 126
					}
				},
				"timed_out": false,
				"took": 13
			},
			"start_time_in_millis": 1716834482815
		}`,
		ExpectedResults: [][]model.QueryResultRow{
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol("hits", uint64(378))}}},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("severity", "critical"),
					model.NewQueryResultCol("source", "alpine"),
					model.NewQueryResultCol("toInt64(toUnixTimestamp64Milli(`@timestamp`)/30000)", int64(1716834300000/30000)),
					model.NewQueryResultCol(`count(DISTINCT "severity")`, 1),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("severity", "critical"),
					model.NewQueryResultCol("source", "alpine"),
					model.NewQueryResultCol("toInt64(toUnixTimestamp64Milli(`@timestamp`)/30000)", int64(1716834390000/30000)),
					model.NewQueryResultCol(`count(DISTINCT "severity")`, 1),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("severity", "critical"),
					model.NewQueryResultCol("source", "fedora"),
					model.NewQueryResultCol("toInt64(toUnixTimestamp64Milli(`@timestamp`)/30000)", int64(1716834270000/30000)),
					model.NewQueryResultCol(`count(DISTINCT "severity")`, 1),
				}},
			},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("severity", "critical"),
					model.NewQueryResultCol("source", "alpine"),
					model.NewQueryResultCol("toInt64(toUnixTimestamp64Milli(`@timestamp`)/30000)", int64(1716834300000/30000)),
					model.NewQueryResultCol("count()", 1),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("severity", "critical"),
					model.NewQueryResultCol("source", "alpine"),
					model.NewQueryResultCol("toInt64(toUnixTimestamp64Milli(`@timestamp`)/30000)", int64(1716834390000/30000)),
					model.NewQueryResultCol("count()", 1),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("severity", "critical"),
					model.NewQueryResultCol("source", "fedora"),
					model.NewQueryResultCol("toInt64(toUnixTimestamp64Milli(`@timestamp`)/30000)", int64(1716834270000/30000)),
					model.NewQueryResultCol("count()", 1),
				}},
			},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("severity", "critical"),
					model.NewQueryResultCol("source", "alpine"),
					model.NewQueryResultCol(`count(DISTINCT "severity")`, 1),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("severity", "critical"),
					model.NewQueryResultCol("source", "fedora"),
					model.NewQueryResultCol(`count(DISTINCT "severity")`, 1),
				}},
			},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("severity", "critical"),
					model.NewQueryResultCol("source", "alpine"),
					model.NewQueryResultCol("count()", 2),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("severity", "critical"),
					model.NewQueryResultCol("source", "fedora"),
					model.NewQueryResultCol("count()", 1),
				}},
			},
		},
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__parent_count", uint64(124)),
				model.NewQueryResultCol("aggr__0__key_0", "critical"),
				model.NewQueryResultCol("aggr__0__key_1", "alpine"),
				model.NewQueryResultCol("aggr__0__count", 2),
				model.NewQueryResultCol("aggr__0__order_2", 1),
				model.NewQueryResultCol("metric__0__2_col_0", 1),
				model.NewQueryResultCol("aggr__0__1__key_0", int64(1716834300000/30000)),
				model.NewQueryResultCol("aggr__0__1__count", 1),
				model.NewQueryResultCol("aggr__0__1__order_1", int64(1716834300000/30000)),
				model.NewQueryResultCol("metric__0__1__2_col_0", 1),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__parent_count", uint64(124)),
				model.NewQueryResultCol("aggr__0__key_0", "critical"),
				model.NewQueryResultCol("aggr__0__key_1", "alpine"),
				model.NewQueryResultCol("aggr__0__count", 2),
				model.NewQueryResultCol("aggr__0__order_2", 1),
				model.NewQueryResultCol("metric__0__2_col_0", 1),
				model.NewQueryResultCol("aggr__0__1__key_0", int64(1716834390000/30000)),
				model.NewQueryResultCol("aggr__0__1__count", 1),
				model.NewQueryResultCol("aggr__0__1__order_1", int64(1716834390000/30000)),
				model.NewQueryResultCol("metric__0__1__2_col_0", 1),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__parent_count", uint64(124)),
				model.NewQueryResultCol("aggr__0__key_0", "critical"),
				model.NewQueryResultCol("aggr__0__key_1", "fedora"),
				model.NewQueryResultCol("aggr__0__count", 1),
				model.NewQueryResultCol("aggr__0__order_2", 1),
				model.NewQueryResultCol("metric__0__2_col_0", 1),
				model.NewQueryResultCol("aggr__0__1__key_0", int64(1716834270000/30000)),
				model.NewQueryResultCol("aggr__0__1__count", 1),
				model.NewQueryResultCol("aggr__0__1__order_1", int64(1716834270000/30000)),
				model.NewQueryResultCol("metric__0__1__2_col_0", 1),
			}},
		},
		ExpectedSQLs: []string{
			`SELECT count() FROM ` + testdata.QuotedTableName,
			`SELECT "severity", "source", toInt64(toUnixTimestamp64Milli("@timestamp") / 30000), count(DISTINCT "severity") ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`GROUP BY "severity", "source", toInt64(toUnixTimestamp64Milli("@timestamp") / 30000) ` +
				`ORDER BY "severity", "source", toInt64(toUnixTimestamp64Milli("@timestamp") / 30000)`,
			`SELECT "severity", "source", toInt64(toUnixTimestamp64Milli("@timestamp") / 30000), count() ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`GROUP BY "severity", "source", toInt64(toUnixTimestamp64Milli("@timestamp") / 30000) ` +
				`ORDER BY "severity", "source", toInt64(toUnixTimestamp64Milli("@timestamp") / 30000)`,
			`SELECT "severity", "source", count(DISTINCT "severity") ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`GROUP BY "severity", "source" ` +
				`ORDER BY "severity", "source"`,
			`SELECT "severity", "source", count() ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`GROUP BY "severity", "source" ` +
				`ORDER BY "severity", "source"`,
		},
		ExpectedPancakeSQL: `
			SELECT "aggr__0__parent_count", "aggr__0__key_0", "aggr__0__key_1",
			  "aggr__0__count", "aggr__0__order_2", "metric__0__2_col_0",
			  "aggr__0__1__key_0", "aggr__0__1__count", "metric__0__1__2_col_0"
			FROM (
			  SELECT "aggr__0__parent_count", "aggr__0__key_0", "aggr__0__key_1",
				"aggr__0__count", "aggr__0__order_2", "metric__0__2_col_0",
				"aggr__0__1__key_0", "aggr__0__1__count", "metric__0__1__2_col_0",
				dense_rank() OVER (ORDER BY "aggr__0__order_2" DESC, "aggr__0__key_0" ASC,
				"aggr__0__key_1" ASC) AS "aggr__0__order_1_rank",
				dense_rank() OVER (PARTITION BY "aggr__0__key_0", "aggr__0__key_1" ORDER BY
				"aggr__0__1__key_0" ASC) AS "aggr__0__1__order_1_rank"
			  FROM (
				SELECT sum(count(*)) OVER () AS "aggr__0__parent_count",
				  "severity" AS "aggr__0__key_0", "source" AS "aggr__0__key_1",
				  sum(count(*)) OVER (PARTITION BY "aggr__0__key_0", "aggr__0__key_1") AS
				  "aggr__0__count",
				  uniqMerge(uniqState("severity")) OVER (PARTITION BY "aggr__0__key_0",
				  "aggr__0__key_1") AS "aggr__0__order_2",
				  uniqMerge(uniqState("severity")) OVER (PARTITION BY "aggr__0__key_0",
				  "aggr__0__key_1") AS "metric__0__2_col_0",
				  toInt64(toUnixTimestamp64Milli("@timestamp") / 30000) AS
				  "aggr__0__1__key_0", count(*) AS "aggr__0__1__count",
				  uniq("severity") AS "metric__0__1__2_col_0"
				FROM "logs-generic-default"
				GROUP BY "severity" AS "aggr__0__key_0", "source" AS "aggr__0__key_1",
				  toInt64(toUnixTimestamp64Milli("@timestamp") / 30000) AS
				  "aggr__0__1__key_0"))
			WHERE "aggr__0__order_1_rank"<=3
			ORDER BY "aggr__0__order_1_rank" ASC, "aggr__0__1__order_1_rank" ASC`,
	},
	{ // [3]
		TestName: "Quite simple multi_terms, but with non-string keys. Visualize: Bar Vertical: Horizontal Axis: Date Histogram, Vertical Axis: Count of records, Breakdown: Top values (2 values)",
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
								"fixed_interval": "30s",
								"min_doc_count": 1,
								"time_zone": "Europe/Warsaw"
							}
						}
					},
					"multi_terms": {
						"order": {
							"_count": "desc"
						},
						"size": 3,
						"terms": [
							{
								"field": "Cancelled"
							},
							{
								"field": "AvgTicketPrice"
							}
						]
					}
				}
			},
			"fields": [
				{
					"field": "@timestamp",
					"format": "date_time"
				}
			],
			"runtime_mappings": {
				"hour_of_day": {
					"script": {
						"source": "emit(doc['@timestamp'].value.getHour());"
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
		// I erased empty date_histogram buckets, we don't support extended_bounds yet
		ExpectedResponse: `
		{
			"completion_time_in_millis": 1716839096599,
			"expiration_time_in_millis": 1716839156591,
			"id": "FnlDTkxYWlI1VEpxQlBhS24yaW16amccRVZINklxc1VTQ2lhVEtwMnpmZjNEZzo5NjIyMA==",
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
											"doc_count": 1,
											"key": 1716839040000,
											"key_as_string": "2024-05-27T19:44:00.000"
										}
									]
								},
								"doc_count": 1,
								"key": [
									false,
									167.05126953125
								],
								"key_as_string": "false|167.05126953125"
							},
							{
								"1": {
									"buckets": [
										{
											"doc_count": 1,
											"key": 1716838530000,
											"key_as_string": "2024-05-27T19:35:30.000"
										}
									]
								},
								"doc_count": 1,
								"key": [
									false,
									331.336181640625
								],
								"key_as_string": "false|331.336181640625"
							},
							{
								"1": {
									"buckets": [
										{
											"doc_count": 1,
											"key": 1716838500000,
											"key_as_string": "2024-05-27T19:35:00.000"
										}
									]
								},
								"doc_count": 1,
								"key": [
									false,
									714.4038696289062
								],
								"key_as_string": "false|714.4038696289062"
							}
						],
						"doc_count_error_upper_bound": 0,
						"sum_other_doc_count": 1
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
				"took": 8
			},
			"start_time_in_millis": 1716839096591
		}`,
		ExpectedResults: [][]model.QueryResultRow{
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol("hits", uint64(378))}}},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("Cancelled", false),
					model.NewQueryResultCol("AvgTicketPrice", 167.05126953125),
					model.NewQueryResultCol("toInt64(toUnixTimestamp64Milli(`@timestamp`)/30000)", int64(1716839040000/30000)),
					model.NewQueryResultCol("count()", 1),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("Cancelled", false),
					model.NewQueryResultCol("AvgTicketPrice", 331.336181640625),
					model.NewQueryResultCol("toInt64(toUnixTimestamp64Milli(`@timestamp`)/30000)", int64(1716838530000/30000)),
					model.NewQueryResultCol("count()", 1),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("Cancelled", false),
					model.NewQueryResultCol("AvgTicketPrice", 714.4038696289062),
					model.NewQueryResultCol("toInt64(toUnixTimestamp64Milli(`@timestamp`)/30000)", int64(1716838500000/30000)),
					model.NewQueryResultCol("count()", 1),
				}},
			},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("Cancelled", false),
					model.NewQueryResultCol("AvgTicketPrice", 167.05126953125),
					model.NewQueryResultCol("count()", 1),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("Cancelled", false),
					model.NewQueryResultCol("AvgTicketPrice", 331.336181640625),
					model.NewQueryResultCol("count()", 1),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("Cancelled", false),
					model.NewQueryResultCol("AvgTicketPrice", 714.4038696289062),
					model.NewQueryResultCol("count()", 1),
				}},
			},
		},
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__parent_count", int64(4)),
				model.NewQueryResultCol("aggr__0__key_0", false),
				model.NewQueryResultCol("aggr__0__key_1", 167.05126953125),
				model.NewQueryResultCol("aggr__0__count", 1),
				model.NewQueryResultCol("aggr__0__order_2", 1),
				model.NewQueryResultCol("aggr__0__1__key_0", int64(1716839040000/30000)),
				model.NewQueryResultCol("aggr__0__1__count", 1),
				model.NewQueryResultCol("aggr__0__1__order_1", int64(1716839040000/30000)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__parent_count", int64(4)),
				model.NewQueryResultCol("aggr__0__key_0", false),
				model.NewQueryResultCol("aggr__0__key_1", 331.336181640625),
				model.NewQueryResultCol("aggr__0__count", 1),
				model.NewQueryResultCol("aggr__0__order_2", 1),
				model.NewQueryResultCol("aggr__0__1__key_0", int64(1716838530000/30000)),
				model.NewQueryResultCol("aggr__0__1__count", 1),
				model.NewQueryResultCol("aggr__0__1__order_1", int64(1716838530000/30000)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__parent_count", int64(4)),
				model.NewQueryResultCol("aggr__0__key_0", false),
				model.NewQueryResultCol("aggr__0__key_1", 714.4038696289062),
				model.NewQueryResultCol("aggr__0__count", 1),
				model.NewQueryResultCol("aggr__0__order_2", 1),
				model.NewQueryResultCol("aggr__0__1__key_0", int64(1716838500000/30000)),
				model.NewQueryResultCol("aggr__0__1__count", 1),
				model.NewQueryResultCol("aggr__0__1__order_1", int64(1716838500000/30000)),
			}},
		},
		ExpectedSQLs: []string{
			`SELECT count() FROM ` + testdata.QuotedTableName,
			`SELECT "Cancelled", "AvgTicketPrice", toInt64(toUnixTimestamp64Milli("@timestamp") / 30000), count() ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`GROUP BY "Cancelled", "AvgTicketPrice", toInt64(toUnixTimestamp64Milli("@timestamp") / 30000) ` +
				`ORDER BY "Cancelled", "AvgTicketPrice", toInt64(toUnixTimestamp64Milli("@timestamp") / 30000)`,
			`SELECT "Cancelled", "AvgTicketPrice", count() ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`GROUP BY "Cancelled", "AvgTicketPrice" ` +
				`ORDER BY "Cancelled", "AvgTicketPrice"`,
		},
		ExpectedPancakeSQL: `
			SELECT "aggr__0__parent_count", "aggr__0__key_0", "aggr__0__key_1",
			  "aggr__0__count", "aggr__0__order_2", "aggr__0__1__key_0",
			  "aggr__0__1__count"
			FROM (
			  SELECT "aggr__0__parent_count", "aggr__0__key_0", "aggr__0__key_1",
				"aggr__0__count", "aggr__0__order_2", "aggr__0__1__key_0",
				"aggr__0__1__count",
				dense_rank() OVER (ORDER BY "aggr__0__order_2" DESC, "aggr__0__key_0" ASC,
				"aggr__0__key_1" ASC) AS "aggr__0__order_1_rank",
				dense_rank() OVER (PARTITION BY "aggr__0__key_0", "aggr__0__key_1" ORDER BY
				"aggr__0__1__key_0" ASC) AS "aggr__0__1__order_1_rank"
			  FROM (
				SELECT sum(count(*)) OVER () AS "aggr__0__parent_count",
				  "Cancelled" AS "aggr__0__key_0", "AvgTicketPrice" AS "aggr__0__key_1",
				  sum(count(*)) OVER (PARTITION BY "aggr__0__key_0", "aggr__0__key_1") AS
				  "aggr__0__count",
				  sum(count()) OVER (PARTITION BY "aggr__0__key_0", "aggr__0__key_1") AS
				  "aggr__0__order_2",
				  toInt64(toUnixTimestamp64Milli("@timestamp") / 30000) AS
				  "aggr__0__1__key_0", count(*) AS "aggr__0__1__count"
				FROM "logs-generic-default"
				GROUP BY "Cancelled" AS "aggr__0__key_0",
				  "AvgTicketPrice" AS "aggr__0__key_1",
				  toInt64(toUnixTimestamp64Milli("@timestamp") / 30000) AS
				  "aggr__0__1__key_0"))
			WHERE "aggr__0__order_1_rank"<=3
			ORDER BY "aggr__0__order_1_rank" ASC, "aggr__0__1__order_1_rank" ASC`,
	},
}
