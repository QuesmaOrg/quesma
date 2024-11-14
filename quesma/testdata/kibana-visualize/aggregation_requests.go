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
				WHERE ("@timestamp">=fromUnixTimestamp64Milli(1716811196627) AND
				  "@timestamp"<=fromUnixTimestamp64Milli(1716812096627))
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
				model.NewQueryResultCol("metric__0__2_col_0", 1),
				model.NewQueryResultCol("aggr__0__1__key_0", int64(1716834300000/30000)),
				model.NewQueryResultCol("aggr__0__1__count", 1),
				model.NewQueryResultCol("metric__0__1__2_col_0", 1),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__parent_count", uint64(124)),
				model.NewQueryResultCol("aggr__0__key_0", "critical"),
				model.NewQueryResultCol("aggr__0__key_1", "alpine"),
				model.NewQueryResultCol("aggr__0__count", 2),
				model.NewQueryResultCol("metric__0__2_col_0", 1),
				model.NewQueryResultCol("aggr__0__1__key_0", int64(1716834390000/30000)),
				model.NewQueryResultCol("aggr__0__1__count", 1),
				model.NewQueryResultCol("metric__0__1__2_col_0", 1),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__parent_count", uint64(124)),
				model.NewQueryResultCol("aggr__0__key_0", "critical"),
				model.NewQueryResultCol("aggr__0__key_1", "fedora"),
				model.NewQueryResultCol("aggr__0__count", 1),
				model.NewQueryResultCol("metric__0__2_col_0", 1),
				model.NewQueryResultCol("aggr__0__1__key_0", int64(1716834270000/30000)),
				model.NewQueryResultCol("aggr__0__1__count", 1),
				model.NewQueryResultCol("metric__0__1__2_col_0", 1),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT "aggr__0__parent_count", "aggr__0__key_0", "aggr__0__key_1",
			  "aggr__0__count", "metric__0__2_col_0", "aggr__0__1__key_0",
			  "aggr__0__1__count", "metric__0__1__2_col_0"
			FROM (
			  SELECT "aggr__0__parent_count", "aggr__0__key_0", "aggr__0__key_1",
				"aggr__0__count", "metric__0__2_col_0", "aggr__0__1__key_0",
				"aggr__0__1__count", "metric__0__1__2_col_0",
				dense_rank() OVER (ORDER BY "metric__0__2_col_0" DESC, "aggr__0__key_0" ASC,
				"aggr__0__key_1" ASC) AS "aggr__0__order_1_rank",
				dense_rank() OVER (PARTITION BY "aggr__0__key_0", "aggr__0__key_1" ORDER BY
				"aggr__0__1__key_0" ASC) AS "aggr__0__1__order_1_rank"
			  FROM (
				SELECT sum(count(*)) OVER () AS "aggr__0__parent_count",
				  "severity" AS "aggr__0__key_0", "source" AS "aggr__0__key_1",
				  sum(count(*)) OVER (PARTITION BY "aggr__0__key_0", "aggr__0__key_1") AS
				  "aggr__0__count",
				  uniqMerge(uniqState("severity")) OVER (PARTITION BY "aggr__0__key_0",
				  "aggr__0__key_1") AS "metric__0__2_col_0",
				  toInt64(toUnixTimestamp64Milli("@timestamp") / 30000) AS
				  "aggr__0__1__key_0", count(*) AS "aggr__0__1__count",
				  uniq("severity") AS "metric__0__1__2_col_0"
				FROM __quesma_table_name
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
				model.NewQueryResultCol("metric__0__2_col_0", []float64{15480.335426897316}),
				model.NewQueryResultCol("aggr__0__1__key_0", 0.0),
				model.NewQueryResultCol("aggr__0__1__count", 908),
				model.NewQueryResultCol("metric__0__1__2_col_0", []float64{4476.3921875}),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__parent_count", 0),
				model.NewQueryResultCol("aggr__0__key_0", true),
				model.NewQueryResultCol("aggr__0__count", 419),
				model.NewQueryResultCol("metric__0__2_col_0", []float64{14463.254101562497}),
				model.NewQueryResultCol("aggr__0__1__key_0", 0.0),
				model.NewQueryResultCol("aggr__0__1__count", 137),
				model.NewQueryResultCol("metric__0__1__2_col_0", []float64{4188.7347656249985}),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__parent_count", 0),
				model.NewQueryResultCol("aggr__0__key_0", true),
				model.NewQueryResultCol("aggr__0__count", 419),
				model.NewQueryResultCol("metric__0__2_col_0", []float64{14463.254101562497}),
				model.NewQueryResultCol("aggr__0__1__key_0", 5000.0),
				model.NewQueryResultCol("aggr__0__1__count", 186),
				model.NewQueryResultCol("metric__0__1__2_col_0", []float64{9842.6279296875}),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT "aggr__0__parent_count", "aggr__0__key_0", "aggr__0__count",
			  "metric__0__2_col_0", "aggr__0__1__key_0", "aggr__0__1__count",
			  "metric__0__1__2_col_0"
			FROM (
			  SELECT "aggr__0__parent_count", "aggr__0__key_0", "aggr__0__count",
				"metric__0__2_col_0", "aggr__0__1__key_0", "aggr__0__1__count",
				"metric__0__1__2_col_0",
				dense_rank() OVER (ORDER BY "metric__0__2_col_0" DESC, "aggr__0__key_0" ASC)
				AS "aggr__0__order_1_rank",
				dense_rank() OVER (PARTITION BY "aggr__0__key_0" ORDER BY
				"aggr__0__1__key_0" ASC) AS "aggr__0__1__order_1_rank"
			  FROM (
				SELECT sum(count(*)) OVER () AS "aggr__0__parent_count",
				  "Cancelled" AS "aggr__0__key_0",
				  sum(count(*)) OVER (PARTITION BY "aggr__0__key_0") AS "aggr__0__count",
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
	{ // [5]
		TestName: "terms with order by agg1>agg2 (multiple aggregations)",
		QueryRequestJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"0": {
					"aggs": {
						"2-bucket": {
							"aggs": {
								"2-metric": {
									"max": {
										"field": "DistanceKilometers"
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
															"field": "bytes_gauge"
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
						"field": "AvgTicketPrice",
						"order": {
							"2-bucket>2-metric": "desc"
						},
						"size": 2
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
							"2-bucket": {
								"2-metric": {
									"value": 19538.8203125
								},
								"doc_count": 1
							},
							"doc_count": 1,
							"key": 590.92822265625
						},
						{
							"2-bucket": {
								"2-metric": {
									"value": 19285.5078125
								},
								"doc_count": 1
							},
							"doc_count": 1,
							"key": 830.0374755859375
						}
					],
					"doc_count_error_upper_bound": -1,
					"sum_other_doc_count": 1867
				}
			},
			"hits": {
				"hits": [],
				"max_score": null,
				"total": {
					"relation": "eq",
					"value": 1879
				}
			},
			"timed_out": false,
			"took": 6
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__parent_count", int64(1869)),
				model.NewQueryResultCol("aggr__0__key_0", 590.92822265625),
				model.NewQueryResultCol("aggr__0__count", int64(1)),
				model.NewQueryResultCol("aggr__0__2-bucket__count", int64(1)),
				model.NewQueryResultCol("metric__0__2-bucket__2-metric_col_0", 19538.8203125),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__parent_count", int64(1869)),
				model.NewQueryResultCol("aggr__0__key_0", 830.0374755859375),
				model.NewQueryResultCol("aggr__0__count", int64(1)),
				model.NewQueryResultCol("aggr__0__2-bucket__count", int64(1)),
				model.NewQueryResultCol("metric__0__2-bucket__2-metric_col_0", 19285.5078125),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT sum(count(*)) OVER () AS "aggr__0__parent_count",
			  "AvgTicketPrice" AS "aggr__0__key_0", count(*) AS "aggr__0__count",
			  countIf("bytes_gauge" IS NOT NULL) AS "aggr__0__2-bucket__count",
			  maxOrNullIf("DistanceKilometers", "bytes_gauge" IS NOT NULL) AS
			  "metric__0__2-bucket__2-metric_col_0"
			FROM __quesma_table_name
			GROUP BY "AvgTicketPrice" AS "aggr__0__key_0"
			ORDER BY "metric__0__2-bucket__2-metric_col_0" DESC, "aggr__0__key_0" ASC
			LIMIT 3`,
	},
	{ // [6]
		TestName: "terms with order by stats, easily reproducible in Kibana Visualize",
		QueryRequestJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"0": {
					"aggs": {
						"1": {
							"stats": {
								"field": "FlightDelayMin"
							}
						}
					},
					"terms": {
						"field": "Carrier",
						"order": [
							{"1.min": "desc"},
							{"1.count": "desc"},
							{"1.avg": "desc"},
							{"1.max": "asc"},
							{"1.sum": "desc"}
						],
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
									"gte": "2024-09-07T15:30:24.239Z",
									"lte": "2024-09-22T15:30:24.239Z"
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
								"avg": 41.278625954198475,
								"count": 524,
								"max": 360.0,
								"min": 0.0,
								"sum": 21630.0
							},
							"doc_count": 524,
							"key": "ES-Air"
						},
						{
							"1": {
								"avg": 46.87155963302752,
								"count": 545,
								"max": 360.0,
								"min": 0.0,
								"sum": 25545.0
							},
							"doc_count": 545,
							"key": "JetBeats"
						}
					],
					"doc_count_error_upper_bound": 0,
					"sum_other_doc_count": 800
				}
			},
			"hits": {
				"hits": [],
				"max_score": null,
				"total": {
					"relation": "eq",
					"value": 2132
				}
			},
			"timed_out": false,
			"took": 0
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__parent_count", int64(1869)),
				model.NewQueryResultCol("aggr__0__key_0", "ES-Air"),
				model.NewQueryResultCol("aggr__0__count", int64(524)),
				model.NewQueryResultCol("metric__0__1_col_0", 524),
				model.NewQueryResultCol("metric__0__1_col_1", 0.0),
				model.NewQueryResultCol("metric__0__1_col_2", 360.0),
				model.NewQueryResultCol("metric__0__1_col_3", 41.278625954198475),
				model.NewQueryResultCol("metric__0__1_col_4", 21630.0),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__parent_count", int64(1869)),
				model.NewQueryResultCol("aggr__0__key_0", "JetBeats"),
				model.NewQueryResultCol("aggr__0__count", int64(545)),
				model.NewQueryResultCol("metric__0__1_col_0", 545),
				model.NewQueryResultCol("metric__0__1_col_1", 0.0),
				model.NewQueryResultCol("metric__0__1_col_2", 360.0),
				model.NewQueryResultCol("metric__0__1_col_3", 46.87155963302752),
				model.NewQueryResultCol("metric__0__1_col_4", 25545.0),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT sum(count(*)) OVER () AS "aggr__0__parent_count",
			  "Carrier" AS "aggr__0__key_0", count(*) AS "aggr__0__count",
			  count("FlightDelayMin") AS "metric__0__1_col_0",
			  minOrNull("FlightDelayMin") AS "metric__0__1_col_1",
			  maxOrNull("FlightDelayMin") AS "metric__0__1_col_2",
			  avgOrNull("FlightDelayMin") AS "metric__0__1_col_3",
			  sumOrNull("FlightDelayMin") AS "metric__0__1_col_4"
			FROM __quesma_table_name
			WHERE ("timestamp">=fromUnixTimestamp64Milli(1725723024239) AND "timestamp"<=
			  fromUnixTimestamp64Milli(1727019024239))
			GROUP BY "Carrier" AS "aggr__0__key_0"
			ORDER BY "metric__0__1_col_1" DESC, "metric__0__1_col_0" DESC,
			  "metric__0__1_col_3" DESC, "metric__0__1_col_2" ASC,
			  "metric__0__1_col_4" DESC, "aggr__0__key_0" ASC
			LIMIT 4`,
	},
	{ // [7]
		TestName: "terms with order by extended_stats (easily reproducible in Kibana Visualize)",
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
								"field": "FlightDelayMin"
							}
						}
					},
					"terms": {
						"field": "Carrier",
						"order": [
							{"1.min": "desc"},
							{"1.count": "desc"},
							{"1.avg": "desc"},
							{"1.max": "asc"},
							{"1.sum": "desc"},
							{"1.sum_of_squares": "desc"},
							{"1.variance": "desc"},
							{"1.variance_population": "desc"},
							{"1.variance_sampling": "desc"},
							{"1.std_deviation": "desc"},
							{"1.std_deviation_population": "desc"},
							{"1.std_deviation_sampling": "desc"},
						],
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
									"gte": "2024-09-07T15:30:24.239Z",
									"lte": "2024-09-22T15:30:24.239Z"
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
								"avg": 48.67620751341681,
								"count": 559,
								"max": 360.0,
								"min": 0.0,
								"std_deviation": 98.0222164000509,
								"std_deviation_bounds": {
									"lower": -147.36822528668498,
									"lower_population": -147.36822528668498,
									"lower_sampling": -147.5438137077322,
									"upper": 244.7206403135186,
									"upper_population": 244.7206403135186,
									"upper_sampling": 244.89622873456582
								},
								"std_deviation_population": 98.0222164000509,
								"std_deviation_sampling": 98.1100106105745,
								"sum": 27210.0,
								"sum_of_squares": 6695550.0,
								"variance": 9608.354907978406,
								"variance_population": 9608.354907978406,
								"variance_sampling": 9625.574182007042
							},
							"doc_count": 524,
							"key": "ES-Air"
						}
					],
					"doc_count_error_upper_bound": 0,
					"sum_other_doc_count": 1345
				}
			},
			"hits": {
				"hits": [],
				"max_score": null,
				"total": {
					"relation": "eq",
					"value": 2132
				}
			},
			"timed_out": false,
			"took": 1
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__parent_count", int64(1869)),
				model.NewQueryResultCol("aggr__0__key_0", "ES-Air"),
				model.NewQueryResultCol("aggr__0__count", int64(524)),
				model.NewQueryResultCol("metric__0__1_col_0", 559),
				model.NewQueryResultCol("metric__0__1_col_1", 0.0),
				model.NewQueryResultCol("metric__0__1_col_2", 360.0),
				model.NewQueryResultCol("metric__0__1_col_3", 48.67620751341681),
				model.NewQueryResultCol("metric__0__1_col_4", 27210.0),
				model.NewQueryResultCol("metric__0__1_col_5", 6695550.0),
				model.NewQueryResultCol("metric__0__1_col_6", 9608.354907978406),
				model.NewQueryResultCol("metric__0__1_col_7", 9625.574182007042),
				model.NewQueryResultCol("metric__0__1_col_8", 98.0222164000509),
				model.NewQueryResultCol("metric__0__1_col_9", 98.1100106105745),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT sum(count(*)) OVER () AS "aggr__0__parent_count",
			  "Carrier" AS "aggr__0__key_0", count(*) AS "aggr__0__count",
			  count("FlightDelayMin") AS "metric__0__1_col_0",
			  minOrNull("FlightDelayMin") AS "metric__0__1_col_1",
			  maxOrNull("FlightDelayMin") AS "metric__0__1_col_2",
			  avgOrNull("FlightDelayMin") AS "metric__0__1_col_3",
			  sumOrNull("FlightDelayMin") AS "metric__0__1_col_4",
			  sumOrNull("FlightDelayMin"*"FlightDelayMin") AS "metric__0__1_col_5",
			  varPop("FlightDelayMin") AS "metric__0__1_col_6",
			  varSamp("FlightDelayMin") AS "metric__0__1_col_7",
			  stddevPop("FlightDelayMin") AS "metric__0__1_col_8",
			  stddevSamp("FlightDelayMin") AS "metric__0__1_col_9"
			FROM __quesma_table_name
			WHERE ("timestamp">=fromUnixTimestamp64Milli(1725723024239) AND "timestamp"<=
			  fromUnixTimestamp64Milli(1727019024239))
			GROUP BY "Carrier" AS "aggr__0__key_0"
			ORDER BY "metric__0__1_col_1" DESC, "metric__0__1_col_0" DESC,
			  "metric__0__1_col_3" DESC, "metric__0__1_col_2" ASC,
			  "metric__0__1_col_4" DESC, "metric__0__1_col_5" DESC,
			  "metric__0__1_col_6" DESC, "metric__0__1_col_6" DESC,
			  "metric__0__1_col_7" DESC, "metric__0__1_col_8" DESC,
			  "metric__0__1_col_8" DESC, "metric__0__1_col_9" DESC, "aggr__0__key_0" ASC
			LIMIT 4`,
	},
	{ // [8]
		TestName: "Terms with order by top metrics",
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
								"2-bucket": {
									"aggs": {
										"2-metric": {
											"top_metrics": {
												"metrics": {"field": "DistanceKilometers"},
												"size": 1,
												"sort": {"timestamp": "desc"}
											}
										}
									},
									"filter": {
										"bool": {
											"filter": [{
												"bool": {
													"minimum_should_match": 1,
													"should": [{"exists": {"field": "bytes_gauge"}}]
												}}],
											"must": [], "must_not": [], "should": []
										}
									}
								}
							},
							"date_histogram": {
								"extended_bounds": {
									"max": 1726937198309,
									"min": 1725641198309
								},
								"field": "timestamp",
								"fixed_interval": "12h",
								"time_zone": "Europe/Warsaw"
							}
						},
						"2-bucket": {
							"aggs": {
								"2-metric": {
									"top_metrics": {
										"metrics": {"field": "DistanceKilometers"},
										"size": 1,
										"sort": {"timestamp": "desc"}
									}
								}
							},
							"filter": {
								"bool": {
									"filter": [{
										"bool": {
											"minimum_should_match": 1,
											"should": [{"exists": {"field": "bytes_gauge"}}]
										}}],
									"must": [], "must_not": [], "should": []
								}
							}
						}
					},
					"terms": {
						"field": "AvgTicketPrice",
						"order": {
							"2-bucket>2-metric": "desc"
						},
						"size": 12
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
			"completion_time_in_millis": 1726937127128,
			"expiration_time_in_millis": 1726937187124,
			"id": "Fm9QLU5BRXFoVEwyQk1WWC1XalJ1R2cccjdQX1ljN3hSYktWdjNya1RCY3BSdzoxMjM5Mg==",
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
								"1": {
									"buckets": [
										{
											"2-bucket": {
												"2-metric": {
													"top": [
														{
															"metrics": {
																"DistanceKilometers": 8619.34375
															},
															"sort": [
																"2024-09-09T09:56:35.000Z"
															]
														}
													]
												},
												"doc_count": 140
											},
											"doc_count": 140,
											"key": 1725832800000,
											"key_as_string": "2024-09-09T00:00:00.000+02:00"
										},
										{
											"2-bucket": {
												"2-metric": {
													"top": [
														{
															"metrics": {
																"DistanceKilometers": 11549.353515625
															},
															"sort": [
																"2024-09-09T21:58:01.000Z"
															]
														}
													]
												},
												"doc_count": 178
											},
											"doc_count": 178,
											"key": 1725876000000,
											"key_as_string": "2024-09-09T12:00:00.000+02:00"
										},
										{
											"2-bucket": {
												"2-metric": {
													"top": [
														{
															"metrics": {
																"DistanceKilometers": 10641.537109375
															},
															"sort": [
																"2024-09-10T09:59:52.000Z"
															]
														}
													]
												},
												"doc_count": 146
											},
											"doc_count": 146,
											"key": 1725919200000,
											"key_as_string": "2024-09-10T00:00:00.000+02:00"
										}
									]
								},
								"2-bucket": {
									"2-metric": {
										"top": [
											{
												"metrics": {
													"DistanceKilometers": 11077.248046875
												},
												"sort": [
													"2024-09-21T16:42:22.000Z"
												]
											}
										]
									},
									"doc_count": 4032
								},
								"doc_count": 4032
							}
						}
					}
				},
				"hits": {
					"hits": [],
					"max_score": null,
					"total": {
						"relation": "eq",
						"value": 4044
					}
				},
				"timed_out": false,
				"took": 4
			},
			"start_time_in_millis": 1726937127124
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{ // incorrect
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
			SELECT "aggr__0__parent_count", "aggr__0__key_0", "aggr__0__count",
			  "aggr__0__order_1", "aggr__0__1__key_0", "aggr__0__1__count",
			  "aggr__0__1__2-bucket__count"
			FROM (
			  SELECT "aggr__0__parent_count", "aggr__0__key_0", "aggr__0__count",
				"aggr__0__order_1", "aggr__0__1__key_0", "aggr__0__1__count",
				"aggr__0__1__2-bucket__count",
				dense_rank() OVER (ORDER BY "aggr__0__order_1" DESC, "aggr__0__key_0" ASC)
				AS "aggr__0__order_1_rank",
				dense_rank() OVER (PARTITION BY "aggr__0__key_0" ORDER BY
				"aggr__0__1__key_0" ASC) AS "aggr__0__1__order_1_rank"
			  FROM (
				SELECT sum(count(*)) OVER () AS "aggr__0__parent_count",
				  "AvgTicketPrice" AS "aggr__0__key_0",
				  sum(count(*)) OVER (PARTITION BY "aggr__0__key_0") AS "aggr__0__count",
				  "top_metrics__0__2-bucket__2-metric_col_0" AS "aggr__0__order_1",
				  toInt64((toUnixTimestamp64Milli("timestamp")+timeZoneOffset(toTimezone(
				  "timestamp", 'Europe/Warsaw'))*1000) / 43200000) AS "aggr__0__1__key_0",
				  count(*) AS "aggr__0__1__count",
				  countIf("bytes_gauge" IS NOT NULL) AS "aggr__0__1__2-bucket__count"
				FROM __quesma_table_name
				GROUP BY "AvgTicketPrice" AS "aggr__0__key_0",
				  toInt64((toUnixTimestamp64Milli("timestamp")+timeZoneOffset(toTimezone(
				  "timestamp", 'Europe/Warsaw'))*1000) / 43200000) AS "aggr__0__1__key_0"))
			WHERE "aggr__0__order_1_rank"<=13
			ORDER BY "aggr__0__order_1_rank" ASC, "aggr__0__1__order_1_rank" ASC`,
	},
	{ // [9]
		TestName: "Rate aggregation with date_histogram fixed_interval",
		QueryRequestJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"2": {
					"aggs": {
						"week": {
							"rate": {
								"field": "DistanceKilometers",
								"unit": "week"
							}
						},
						"day": {
							"rate": {
								"field": "DistanceKilometers",
								"unit": "day"
							}
						},
						"hour": {
							"rate": {
								"field": "DistanceKilometers",
								"unit": "hour"
							}
						},
						"minute": {
							"rate": {
								"field": "DistanceKilometers",
								"unit": "minute"
							}
						},
						"second": {
							"rate": {
								"field": "DistanceKilometers",
								"unit": "second"
							}
						}
					},
					"date_histogram": {
						"field": "timestamp",
						"fixed_interval": "30s",
						"min_doc_count": 1,
						"time_zone": "Europe/Warsaw"
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
									"gte": "2024-11-14T11:35:41.864Z",
									"lte": "2024-11-14T11:50:41.864Z"
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
			"completion_time_in_millis": 1731585799382,
			"expiration_time_in_millis": 1732017799373,
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
					"2": {
						"buckets": [
							{
								"day": {
									"value": 5639433.75
								},
								"doc_count": 1,
								"hour": {
									"value": 234976.40625
								},
								"key": 1731584220000,
								"key_as_string": "2024-11-14T11:37:00.000",
								"minute": {
									"value": 3916.2734375
								},
								"second": {
									"value": 65.27122395833334
								},
								"week": {
									"value": 39476036.25
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
						"value": 4
					}
				},
				"timed_out": false,
				"took": 9
			},
			"start_time_in_millis": 1731585799373
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{ // incorrect
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__key_0", int64(1731587820000/30000)),
				model.NewQueryResultCol("aggr__2__count", 1),
				model.NewQueryResultCol("metric__2__day_col_0", 1958.13671875),
				model.NewQueryResultCol("metric__2__hour_col_0", 1958.13671875),
				model.NewQueryResultCol("metric__2__minute_col_0", 1958.13671875),
				model.NewQueryResultCol("metric__2__second_col_0", 1958.13671875),
				model.NewQueryResultCol("metric__2__week_col_0", 1958.13671875),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT toInt64((toUnixTimestamp64Milli("timestamp")+timeZoneOffset(toTimezone(
			  "timestamp", 'Europe/Warsaw'))*1000) / 30000) AS "aggr__2__key_0",
			  count(*) AS "aggr__2__count", "DistanceKilometers" AS "metric__2__day_col_0",
			  "DistanceKilometers" AS "metric__2__hour_col_0",
			  "DistanceKilometers" AS "metric__2__minute_col_0",
			  "DistanceKilometers" AS "metric__2__second_col_0",
			  "DistanceKilometers" AS "metric__2__week_col_0"
			FROM __quesma_table_name
			WHERE ("timestamp">=fromUnixTimestamp64Milli(1731584141864) AND "timestamp"<=
			  fromUnixTimestamp64Milli(1731585041864))
			GROUP BY toInt64((toUnixTimestamp64Milli("timestamp")+timeZoneOffset(toTimezone(
			  "timestamp", 'Europe/Warsaw'))*1000) / 30000) AS "aggr__2__key_0"
			ORDER BY "aggr__2__key_0" ASC`,
	},
	{ // [10]
		TestName: "Rate aggregation with date_histogram calendar_interval",
		QueryRequestJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"2": {
					"aggs": {
						"month": {
							"rate": {
								"field": "DistanceKilometers",
								"unit": "month"
							}
						},
						"year": {
							"rate": {
								"field": "DistanceKilometers",
								"unit": "year"
							}
						},
						"quarter": {
							"rate": {
								"field": "DistanceKilometers",
								"unit": "quarter"
							}
						},
						"minute": {
							"rate": {
								"field": "DistanceKilometers",
								"unit": "minute"
							}
						},
						"second": {
							"rate": {
								"field": "DistanceKilometers",
								"unit": "second"
							}
						},
					},
					"date_histogram": {
						"calendar_interval": "1M",
						"field": "timestamp",
						"min_doc_count": 1,
						"time_zone": "Europe/Warsaw"
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
									"gte": "2022-11-14T12:05:53.316Z",
									"lte": "2024-11-14T12:05:53.316Z"
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
			"completion_time_in_millis": 1731586098439,
			"expiration_time_in_millis": 1732018098434,
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
					"2": {
						"buckets": [
							{
								"doc_count": 3345,
								"key": 1730415600000,
								"key_as_string": "2024-10-31T23:00:00.000",
								"minute": {
									"value": 552.2087796329569
								},
								"month": {
									"value": 23855419.280143738
								},
								"second": {
									"value": 9.20347966054928
								},
								"quarter": {
									"value": 71566257.840431
								},
								"year": {
									"value": 286265031.36172485
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
						"value": 3345
					}
				},
				"timed_out": false,
				"took": 5
			},
			"start_time_in_millis": 1731586098434
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__key_0", int64(1730419200000)),
				model.NewQueryResultCol("aggr__2__count", 3345),
				model.NewQueryResultCol("metric__2__minute_col_0", 23855419.280143738),
				model.NewQueryResultCol("metric__2__month_col_0", 23855419.280143738),
				model.NewQueryResultCol("metric__2__quarter_col_0", 23855419.280143738),
				model.NewQueryResultCol("metric__2__second_col_0", 23855419.280143738),
				model.NewQueryResultCol("metric__2__year_col_0", 23855419.280143738),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT toInt64(toUnixTimestamp(toStartOfMonth(toTimezone("timestamp",
			  'Europe/Warsaw'))))*1000 AS "aggr__2__key_0", count(*) AS "aggr__2__count",
			  "DistanceKilometers" AS "metric__2__minute_col_0",
			  "DistanceKilometers" AS "metric__2__month_col_0",
			  "DistanceKilometers" AS "metric__2__quarter_col_0",
			  "DistanceKilometers" AS "metric__2__second_col_0",
			  "DistanceKilometers" AS "metric__2__year_col_0"
			FROM __quesma_table_name
			WHERE ("timestamp">=fromUnixTimestamp64Milli(1668427553316) AND "timestamp"<=
			  fromUnixTimestamp64Milli(1731585953316))
			GROUP BY toInt64(toUnixTimestamp(toStartOfMonth(toTimezone("timestamp",
			  'Europe/Warsaw'))))*1000 AS "aggr__2__key_0"
			ORDER BY "aggr__2__key_0" ASC`,
	},
}
