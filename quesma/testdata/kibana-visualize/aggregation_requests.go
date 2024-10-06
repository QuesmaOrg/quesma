// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package kibana_visualize

import (
	"quesma/model"
	"quesma/testdata"
)

const TableName = model.SingleTableNamePlaceHolder

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
								"key": 1716827010000,
								"key_as_string": "2024-05-27T16:23:30.000"
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
								"key": 1716827070000,
								"key_as_string": "2024-05-27T16:24:30.000"
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
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__key_0", int64(1716834210000/30000)),
				model.NewQueryResultCol("aggr__0__count", 4),
				model.NewQueryResultCol("aggr__0__1__parent_count", uint64(4)),
				model.NewQueryResultCol("aggr__0__1__key_0", "artemis"),
				model.NewQueryResultCol("aggr__0__1__key_1", "error"),
				model.NewQueryResultCol("aggr__0__1__count", 1),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__key_0", int64(1716834210000/30000)),
				model.NewQueryResultCol("aggr__0__count", 4),
				model.NewQueryResultCol("aggr__0__1__parent_count", uint64(4)),
				model.NewQueryResultCol("aggr__0__1__key_0", "artemis"),
				model.NewQueryResultCol("aggr__0__1__key_1", "info"),
				model.NewQueryResultCol("aggr__0__1__count", 1),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__key_0", int64(1716834210000/30000)),
				model.NewQueryResultCol("aggr__0__count", 4),
				model.NewQueryResultCol("aggr__0__1__parent_count", uint64(4)),
				model.NewQueryResultCol("aggr__0__1__key_0", "jupiter"),
				model.NewQueryResultCol("aggr__0__1__key_1", "info"),
				model.NewQueryResultCol("aggr__0__1__count", 1),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__key_0", int64(1716834270000/30000)),
				model.NewQueryResultCol("aggr__0__count", 16),
				model.NewQueryResultCol("aggr__0__1__parent_count", uint64(15)),
				model.NewQueryResultCol("aggr__0__1__key_0", "apollo"),
				model.NewQueryResultCol("aggr__0__1__key_1", "info"),
				model.NewQueryResultCol("aggr__0__1__count", 2),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__key_0", int64(1716834270000/30000)),
				model.NewQueryResultCol("aggr__0__count", 16),
				model.NewQueryResultCol("aggr__0__1__parent_count", uint64(15)),
				model.NewQueryResultCol("aggr__0__1__key_0", "cassandra"),
				model.NewQueryResultCol("aggr__0__1__key_1", "debug"),
				model.NewQueryResultCol("aggr__0__1__count", 1),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT "aggr__0__key_0", "aggr__0__count", "aggr__0__1__parent_count",
			  "aggr__0__1__key_0", "aggr__0__1__key_1", "aggr__0__1__count"
			FROM (
			  SELECT "aggr__0__key_0", "aggr__0__count", "aggr__0__1__parent_count",
				"aggr__0__1__key_0", "aggr__0__1__key_1", "aggr__0__1__count",
				dense_rank() OVER (ORDER BY "aggr__0__key_0" ASC) AS "aggr__0__order_1_rank"
				,
				dense_rank() OVER (PARTITION BY "aggr__0__key_0" ORDER BY
				"aggr__0__1__count" DESC, "aggr__0__1__key_0" ASC, "aggr__0__1__key_1" ASC)
				AS "aggr__0__1__order_1_rank"
			  FROM (
				SELECT toInt64((toUnixTimestamp64Milli("@timestamp")+timeZoneOffset(
				  toTimezone("@timestamp", 'Europe/Warsaw'))*1000) / 30000) AS
				  "aggr__0__key_0",
				  sum(count(*)) OVER (PARTITION BY "aggr__0__key_0") AS "aggr__0__count",
				  sum(count(*)) OVER (PARTITION BY "aggr__0__key_0") AS
				  "aggr__0__1__parent_count", "severity" AS "aggr__0__1__key_0",
				  "source" AS "aggr__0__1__key_1", count(*) AS "aggr__0__1__count"
				FROM __quesma_table_name
				WHERE ("@timestamp">=toDateTime64(1.716811196627e+09, 3) AND "@timestamp"<=
                  toDateTime64(1.716812096627e+09, 3))
				GROUP BY toInt64((toUnixTimestamp64Milli("@timestamp")+timeZoneOffset(
				  toTimezone("@timestamp", 'Europe/Warsaw'))*1000) / 30000) AS
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
								"fixed_interval": "30s"
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
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__parent_count", uint64(201)),
				model.NewQueryResultCol("aggr__0__key_0", "info"),
				model.NewQueryResultCol("aggr__0__key_1", "redhat"),
				model.NewQueryResultCol("aggr__0__count", 13),
				model.NewQueryResultCol("aggr__0__1__key_0", int64(1716834420000/30000)),
				model.NewQueryResultCol("aggr__0__1__count", 1),
				model.NewQueryResultCol("aggr__0__1__order_1", int64(1716834420000/30000)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__parent_count", uint64(201)),
				model.NewQueryResultCol("aggr__0__key_0", "info"),
				model.NewQueryResultCol("aggr__0__key_1", "redhat"),
				model.NewQueryResultCol("aggr__0__count", 13),
				model.NewQueryResultCol("aggr__0__1__key_0", int64(1716834450000/30000)),
				model.NewQueryResultCol("aggr__0__1__count", 1),
				model.NewQueryResultCol("aggr__0__1__order_1", int64(1716834450000/30000)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__parent_count", uint64(201)),
				model.NewQueryResultCol("aggr__0__key_0", "info"),
				model.NewQueryResultCol("aggr__0__key_1", "redhat"),
				model.NewQueryResultCol("aggr__0__count", 13),
				model.NewQueryResultCol("aggr__0__1__key_0", int64(1716834510000/30000)),
				model.NewQueryResultCol("aggr__0__1__count", 2),
				model.NewQueryResultCol("aggr__0__1__order_1", int64(1716834510000/30000)),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT "aggr__0__parent_count", "aggr__0__key_0", "aggr__0__key_1",
			  "aggr__0__count", "aggr__0__1__key_0", "aggr__0__1__count"
			FROM (
			  SELECT "aggr__0__parent_count", "aggr__0__key_0", "aggr__0__key_1",
				"aggr__0__count", "aggr__0__1__key_0",
				"aggr__0__1__count",
				dense_rank() OVER (ORDER BY "aggr__0__count" DESC, "aggr__0__key_0" ASC,
				"aggr__0__key_1" ASC) AS "aggr__0__order_1_rank",
				dense_rank() OVER (PARTITION BY "aggr__0__key_0", "aggr__0__key_1" ORDER BY
				"aggr__0__1__key_0" ASC) AS "aggr__0__1__order_1_rank"
			  FROM (
				SELECT sum(count(*)) OVER () AS "aggr__0__parent_count",
				  "message" AS "aggr__0__key_0", "host.name" AS "aggr__0__key_1",
				  sum(count(*)) OVER (PARTITION BY "aggr__0__key_0", "aggr__0__key_1") AS
				  "aggr__0__count",
				  toInt64(toUnixTimestamp64Milli("@timestamp") / 30000) AS
				  "aggr__0__1__key_0", count(*) AS "aggr__0__1__count"
				FROM ` + TableName + `
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
								"fixed_interval": "30s"
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
				FROM ` + TableName + `
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
								"min_doc_count": 1
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
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__parent_count", int64(4)),
				model.NewQueryResultCol("aggr__0__key_0", false),
				model.NewQueryResultCol("aggr__0__key_1", 167.05126953125),
				model.NewQueryResultCol("aggr__0__count", 1),
				model.NewQueryResultCol("aggr__0__1__key_0", int64(1716839040000/30000)),
				model.NewQueryResultCol("aggr__0__1__count", 1),
				model.NewQueryResultCol("aggr__0__1__order_1", int64(1716839040000/30000)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__parent_count", int64(4)),
				model.NewQueryResultCol("aggr__0__key_0", false),
				model.NewQueryResultCol("aggr__0__key_1", 331.336181640625),
				model.NewQueryResultCol("aggr__0__count", 1),
				model.NewQueryResultCol("aggr__0__1__key_0", int64(1716838530000/30000)),
				model.NewQueryResultCol("aggr__0__1__count", 1),
				model.NewQueryResultCol("aggr__0__1__order_1", int64(1716838530000/30000)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__parent_count", int64(4)),
				model.NewQueryResultCol("aggr__0__key_0", false),
				model.NewQueryResultCol("aggr__0__key_1", 714.4038696289062),
				model.NewQueryResultCol("aggr__0__count", 1),
				model.NewQueryResultCol("aggr__0__1__key_0", int64(1716838500000/30000)),
				model.NewQueryResultCol("aggr__0__1__count", 1),
				model.NewQueryResultCol("aggr__0__1__order_1", int64(1716838500000/30000)),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT "aggr__0__parent_count", "aggr__0__key_0", "aggr__0__key_1",
			  "aggr__0__count", "aggr__0__1__key_0", "aggr__0__1__count"
			FROM (
			  SELECT "aggr__0__parent_count", "aggr__0__key_0", "aggr__0__key_1",
				"aggr__0__count", "aggr__0__1__key_0",
				"aggr__0__1__count",
				dense_rank() OVER (ORDER BY "aggr__0__count" DESC, "aggr__0__key_0" ASC,
				"aggr__0__key_1" ASC) AS "aggr__0__order_1_rank",
				dense_rank() OVER (PARTITION BY "aggr__0__key_0", "aggr__0__key_1" ORDER BY
				"aggr__0__1__key_0" ASC) AS "aggr__0__1__order_1_rank"
			  FROM (
				SELECT sum(count(*)) OVER () AS "aggr__0__parent_count",
				  "Cancelled" AS "aggr__0__key_0", "AvgTicketPrice" AS "aggr__0__key_1",
				  sum(count(*)) OVER (PARTITION BY "aggr__0__key_0", "aggr__0__key_1") AS
				  "aggr__0__count",
				  toInt64(toUnixTimestamp64Milli("@timestamp") / 30000) AS
				  "aggr__0__1__key_0", count(*) AS "aggr__0__1__count"
				FROM ` + TableName + `
				GROUP BY "Cancelled" AS "aggr__0__key_0",
				  "AvgTicketPrice" AS "aggr__0__key_1",
				  toInt64(toUnixTimestamp64Milli("@timestamp") / 30000) AS
				  "aggr__0__1__key_0"))
			WHERE "aggr__0__order_1_rank"<=3
			ORDER BY "aggr__0__order_1_rank" ASC, "aggr__0__1__order_1_rank" ASC`,
	},
	{ // [4]
		TestName: "percentile with subaggregation (so, combinator). Visualize, Pie, Slice by: top5 of Cancelled, DistanceKilometers, Metric: 95th Percentile",
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
									"percentiles": {
										"field": "DistanceKilometers",
										"percents": [
											95
										]
									}
								}
							},
							"histogram": {
								"extended_bounds": {
									"max": 19538.82056368213,
									"min": 0
								},
								"field": "DistanceKilometers",
								"interval": 5000,
								"min_doc_count": 0
							}
						},
						"2": {
							"percentiles": {
								"field": "DistanceKilometers",
								"percents": [
									95
								]
							}
						}
					},
					"terms": {
						"field": "Cancelled",
						"order": {
							"2.95": "desc"
						},
						"shard_size": 25,
						"size": 5
					}
				}
			},
			"script_fields": {},
			"size": 0,
			"stored_fields": [
				"*"
			],
			"track_total_hits": true
		}`,
		// Response changed a bit from the original:
		// In bool terms, Elastic returns "key": 0, "key_as_string": "false", we return "key": false.
		// Kibana works fine with both ways.
		ExpectedResponse: `
		{
			"response": {
				"took": 19,
				"timed_out": false,
				"_shards": {
					"total": 1,
					"successful": 1,
					"skipped": 0,
					"failed": 0
				},
				"hits": {
					"total": {
						"value": 3393,
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
								"1": {
									"buckets": [
										{
											"2": {
												"values": {
													"95.0": 4476.3921875
												}
											},
											"key": 0,
											"doc_count": 908
										}
									]
								},
								"2": {
									"values": {
										"95.0": 15480.335426897316
									}
								},
								"key": false,
								"doc_count": 2974
							},
							{
								"1": {
									"buckets": [
										{
											"2": {
												"values": {
													"95.0": 4188.7347656249985
												}
											},
											"key": 0,
											"doc_count": 137
										},
										{
											"2": {
												"values": {
													"95.0": 9842.6279296875
												}
											},
											"key": 5000,
											"doc_count": 186
										}
									]
								},
								"2": {
									"values": {
										"95.0": 14463.254101562497
									}
								},
								"key": true,
								"doc_count": 419
							}
						]
					}
				}
			}
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__parent_count", 0),
				model.NewQueryResultCol("aggr__0__key_0", false),
				model.NewQueryResultCol("aggr__0__count", 2974),
				model.NewQueryResultCol("aggr__0__order_1", 15480.335426897316),
				model.NewQueryResultCol("metric__0__2_col_0", []float64{15480.335426897316}),
				model.NewQueryResultCol("aggr__0__1__key_0", 0.0),
				model.NewQueryResultCol("aggr__0__1__count", 908),
				model.NewQueryResultCol("metric__0__1__2_col_0", []float64{4476.3921875}),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__parent_count", 0),
				model.NewQueryResultCol("aggr__0__key_0", true),
				model.NewQueryResultCol("aggr__0__count", 419),
				model.NewQueryResultCol("aggr__0__order_1", 14463.254101562497),
				model.NewQueryResultCol("metric__0__2_col_0", []float64{14463.254101562497}),
				model.NewQueryResultCol("aggr__0__1__key_0", 0.0),
				model.NewQueryResultCol("aggr__0__1__count", 137),
				model.NewQueryResultCol("metric__0__1__2_col_0", []float64{4188.7347656249985}),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__parent_count", 0),
				model.NewQueryResultCol("aggr__0__key_0", true),
				model.NewQueryResultCol("aggr__0__count", 419),
				model.NewQueryResultCol("aggr__0__order_1", 14463.254101562497),
				model.NewQueryResultCol("metric__0__2_col_0", []float64{14463.254101562497}),
				model.NewQueryResultCol("aggr__0__1__key_0", 5000.0),
				model.NewQueryResultCol("aggr__0__1__count", 186),
				model.NewQueryResultCol("metric__0__1__2_col_0", []float64{9842.6279296875}),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT "aggr__0__parent_count", "aggr__0__key_0", "aggr__0__count",
			  "aggr__0__order_1", "metric__0__2_col_0", "aggr__0__1__key_0",
			  "aggr__0__1__count", "metric__0__1__2_col_0"
			FROM (
			  SELECT "aggr__0__parent_count", "aggr__0__key_0", "aggr__0__count",
				"aggr__0__order_1", "metric__0__2_col_0", "aggr__0__1__key_0",
				"aggr__0__1__count", "metric__0__1__2_col_0",
				dense_rank() OVER (ORDER BY "aggr__0__order_1" DESC, "aggr__0__key_0" ASC)
				AS "aggr__0__order_1_rank",
				dense_rank() OVER (PARTITION BY "aggr__0__key_0" ORDER BY
				"aggr__0__1__key_0" ASC) AS "aggr__0__1__order_1_rank"
			  FROM (
				SELECT sum(count(*)) OVER () AS "aggr__0__parent_count",
				  "Cancelled" AS "aggr__0__key_0",
				  sum(count(*)) OVER (PARTITION BY "aggr__0__key_0") AS "aggr__0__count",
				  quantilesMerge(0.950000)(quantilesState(0.950000)("DistanceKilometers"))
				  OVER (PARTITION BY "aggr__0__key_0") AS "aggr__0__order_1",
				  quantilesMerge(0.950000)(quantilesState(0.950000)("DistanceKilometers"))
				  OVER (PARTITION BY "aggr__0__key_0") AS "metric__0__2_col_0",
				  floor("DistanceKilometers"/5000)*5000 AS "aggr__0__1__key_0",
				  count(*) AS "aggr__0__1__count",
				  quantiles(0.950000)("DistanceKilometers") AS "metric__0__1__2_col_0"
				FROM __quesma_table_name
				GROUP BY "Cancelled" AS "aggr__0__key_0",
				  floor("DistanceKilometers"/5000)*5000 AS "aggr__0__1__key_0"))
			WHERE "aggr__0__order_1_rank"<=6
			ORDER BY "aggr__0__order_1_rank" ASC, "aggr__0__1__order_1_rank" ASC`,
	},
}
