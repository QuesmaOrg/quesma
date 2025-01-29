// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package kibana_visualize

import (
	"github.com/QuesmaOrg/quesma/quesma/model"
	"github.com/QuesmaOrg/quesma/quesma/testdata"
	"time"
)

var PipelineAggregationTests = []testdata.AggregationTestCase{
	{ // [0]
		TestName: "Sum bucket for dates",
		QueryRequestJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"2": {
					"aggs": {
						"1": {
							"sum_bucket": {
								"buckets_path": "1-bucket>1-metric"
							}
						},
						"1-bucket": {
							"aggs": {
								"1-metric": {
									"max": {
										"field": "timestamp"
									}
								}
							},
							"date_histogram": {
								"field": "timestamp",
								"fixed_interval": "12h",
								"min_doc_count": 1,
								"time_zone": "Europe/Warsaw"
							}
						}
					},
					"date_histogram": {
						"field": "timestamp",
						"fixed_interval": "12h",
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
									"gte": "2024-09-20T16:16:03.807Z",
									"lte": "2024-10-05T16:16:03.807Z"
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
			"completion_time_in_millis": 1728145022311,
			"expiration_time_in_millis": 1728145082303,
			"id": "FjRpRWFSZWZqVFY2ZlRESm9RanpqOGccTDFKRUl1Rl9RdTZlalJvVlhtbFZyZzoxNTk2NQ==",
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
								"1": {
									"value": 1727085395000.0
								},
								"1-bucket": {
									"buckets": [
										{
											"1-metric": {
												"value": 1727085395000.0,
												"value_as_string": "2024-09-23T09:56:35.000"
											},
											"doc_count": 140,
											"key": 1727042400000,
											"key_as_string": "2024-09-22T22:00:00.000"
										}
									]
								},
								"doc_count": 140,
								"key": 1727042400000,
								"key_as_string": "2024-09-22T22:00:00.000"
							},
							{
								"1": {
									"value": 1727128681000.0
								},
								"1-bucket": {
									"buckets": [
										{
											"1-metric": {
												"value": 1727128681000.0,
												"value_as_string": "2024-09-23T21:58:01.000"
											},
											"doc_count": 178,
											"key": 1727085600000,
											"key_as_string": "2024-09-23T10:00:00.000"
										}
									]
								},
								"doc_count": 178,
								"key": 1727085600000,
								"key_as_string": "2024-09-23T10:00:00.000"
							}
						]
					}
				},
				"hits": {
					"hits": [],
					"max_score": null,
					"total": {
						"relation": "eq",
						"value": 4037
					}
				},
				"timed_out": false,
				"took": 8
			},
			"start_time_in_millis": 1728145022303
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__key_0", int64(1727049600000/43200000)),
				model.NewQueryResultCol("aggr__2__count", int64(140)),
				model.NewQueryResultCol("aggr__2__1-bucket__key_0", int64(1727049600000/43200000)),
				model.NewQueryResultCol("aggr__2__1-bucket__count", int64(140)),
				model.NewQueryResultCol("metric__2__1-bucket__1-metric_col_0", time.UnixMilli(1727085395000)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__key_0", int64(1727092800000/43200000)),
				model.NewQueryResultCol("aggr__2__count", int64(178)),
				model.NewQueryResultCol("aggr__2__1-bucket__key_0", int64(1727092800000/43200000)),
				model.NewQueryResultCol("aggr__2__1-bucket__count", int64(178)),
				model.NewQueryResultCol("metric__2__1-bucket__1-metric_col_0", time.UnixMilli(1727128681000)),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT "aggr__2__key_0", "aggr__2__count", "aggr__2__1-bucket__key_0",
			  "aggr__2__1-bucket__count", "metric__2__1-bucket__1-metric_col_0"
			FROM (
			  SELECT "aggr__2__key_0", "aggr__2__count", "aggr__2__1-bucket__key_0",
				"aggr__2__1-bucket__count", "metric__2__1-bucket__1-metric_col_0",
				dense_rank() OVER (ORDER BY "aggr__2__key_0" ASC) AS "aggr__2__order_1_rank"
				,
				dense_rank() OVER (PARTITION BY "aggr__2__key_0" ORDER BY "aggr__2__key_0"
				ASC, "aggr__2__1-bucket__key_0" ASC) AS "aggr__2__1-bucket__order_1_rank"
			  FROM (
				SELECT toInt64((toUnixTimestamp64Milli("timestamp")+timeZoneOffset(
				  toTimezone("timestamp", 'Europe/Warsaw'))*1000) / 43200000) AS
				  "aggr__2__key_0",
				  sum(count(*)) OVER (PARTITION BY "aggr__2__key_0") AS "aggr__2__count",
				  toInt64((toUnixTimestamp64Milli("timestamp")+timeZoneOffset(toTimezone(
				  "timestamp", 'Europe/Warsaw'))*1000) / 43200000) AS
				  "aggr__2__1-bucket__key_0", count(*) AS "aggr__2__1-bucket__count",
				  maxOrNull("timestamp") AS "metric__2__1-bucket__1-metric_col_0"
				FROM __quesma_table_name
				WHERE ("timestamp">=fromUnixTimestamp64Milli(1726848963807) AND "timestamp"
				  <=fromUnixTimestamp64Milli(1728144963807))
				GROUP BY toInt64((toUnixTimestamp64Milli("timestamp")+timeZoneOffset(
				  toTimezone("timestamp", 'Europe/Warsaw'))*1000) / 43200000) AS
				  "aggr__2__key_0",
				  toInt64((toUnixTimestamp64Milli("timestamp")+timeZoneOffset(toTimezone(
				  "timestamp", 'Europe/Warsaw'))*1000) / 43200000) AS
				  "aggr__2__1-bucket__key_0"))
			ORDER BY "aggr__2__order_1_rank" ASC, "aggr__2__1-bucket__order_1_rank" ASC`,
	},
	{ // [1]
		TestName: "Reproduce: Visualize -> Vertical Bar: Metrics: Cumulative Sum (Aggregation: Avg), Buckets: Date Histogram",
		QueryRequestJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"2": {
					"aggs": {
						"1": {
							"cumulative_sum": {
								"buckets_path": "1-metric"
							}
						},
						"1-metric": {
							"avg": {
								"field": "dayOfWeek"
							}
						}
					},
					"date_histogram": {
						"calendar_interval": "1m",
						"field": "timestamp",
						"time_zone": "Europe/Warsaw"
					}
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
			"completion_time_in_millis": 1728151549059,
			"expiration_time_in_millis": 1728151609012,
			"id": "FnliVXVVa1VaUTY2eWI1cS1xM2V6RWcdTDFKRUl1Rl9RdTZlalJvVlhtbFZyZzoxMDQ0ODc=",
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
								"1": {
									"value": 5.0
								},
								"1-metric": {
									"value": 5.0
								},
								"doc_count": 1,
								"key": 1728144300000,
								"key_as_string": "2024-10-05T16:05:00.000"
							},
							{
								"1": {
									"value": 5.0
								},
								"1-metric": {
									"value": null
								},
								"doc_count": 0,
								"key": 1728144360000,
								"key_as_string": "2024-10-05T16:06:00.000"
							},
							{
								"1": {
									"value": 5.0
								},
								"1-metric": {
									"value": null
								},
								"doc_count": 0,
								"key": 1728144420000,
								"key_as_string": "2024-10-05T16:07:00.000"
							},
							{
								"1": {
									"value": 11.0
								},
								"1-metric": {
									"value": 6.0
								},
								"doc_count": 1,
								"key": 1728144480000,
								"key_as_string": "2024-10-05T16:08:00.000"
							},
							{
								"1": {
									"value": 18.0
								},
								"1-metric": {
									"value": 7.0
								},
								"doc_count": 2,
								"key": 1728144540000,
								"key_as_string": "2024-10-05T16:09:00.000"
							}
						]
					}
				},
				"hits": {
					"hits": [],
					"max_score": null,
					"total": {
						"relation": "eq",
						"value": 29
					}
				},
				"timed_out": false,
				"took": 47
			},
			"start_time_in_millis": 1728151549012
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__key_0", int64(1728151500000/60000)),
				model.NewQueryResultCol("aggr__2__count", int64(1)),
				model.NewQueryResultCol("metric__2__1-metric_col_0", 5.0),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__key_0", int64(1728151680000/60000)),
				model.NewQueryResultCol("aggr__2__count", int64(1)),
				model.NewQueryResultCol("metric__2__1-metric_col_0", 6.0),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__key_0", int64(1728151740000/60000)),
				model.NewQueryResultCol("aggr__2__count", int64(2)),
				model.NewQueryResultCol("metric__2__1-metric_col_0", 7.0),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT toInt64((toUnixTimestamp64Milli("timestamp")+timeZoneOffset(toTimezone(
			  "timestamp", 'Europe/Warsaw'))*1000) / 60000) AS "aggr__2__key_0",
			  count(*) AS "aggr__2__count",
			  avgOrNull("dayOfWeek") AS "metric__2__1-metric_col_0"
			FROM __quesma_table_name
			GROUP BY toInt64((toUnixTimestamp64Milli("timestamp")+timeZoneOffset(toTimezone(
			  "timestamp", 'Europe/Warsaw'))*1000) / 60000) AS "aggr__2__key_0"
			ORDER BY "aggr__2__key_0" ASC`,
	},
	{ // [2]
		TestName: "Reproduce: Visualize -> Vertical Bar: Metrics: Cumulative Sum (Aggregation: Cumulative Sum (Aggregation: Count)), Buckets: Date Histogram",
		QueryRequestJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"2": {
					"aggs": {
						"1": {
							"cumulative_sum": {
								"buckets_path": "1-metric"
							}
						},
						"1-metric": {
							"cumulative_sum": {
								"buckets_path": "_count"
							}
						},
						"3": {
							"max_bucket": {
								"buckets_path": "3-bucket>3-metric"
							}
						},
						"3-bucket": {
							"aggs": {
								"3-metric": {
									"cardinality": {
										"field": "timestamp"
									}
								}
							},
							"date_histogram": {
								"field": "timestamp",
								"fixed_interval": "12h",
								"min_doc_count": 1,
								"time_zone": "Europe/Warsaw"
							}
						}
					},
					"date_histogram": {
						"field": "timestamp",
						"fixed_interval": "12h",
						"time_zone": "Europe/Warsaw"
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
			"completion_time_in_millis": 1728153887695,
			"expiration_time_in_millis": 1728153947645,
			"id": "FllMUlZEaEhWU0p1LUR0b2pOYW9QekEdTDFKRUl1Rl9RdTZlalJvVlhtbFZyZzoxNDA1Mzk=",
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
								"1": {
									"value": 140.0
								},
								"1-metric": {
									"value": 140.0
								},
								"3": {
									"keys": [
										"2024-09-22T22:00:00.000"
									],
									"value": 131.0
								},
								"3-bucket": {
									"buckets": [
										{
											"3-metric": {
												"value": 131
											},
											"doc_count": 140,
											"key": 1727042400000,
											"key_as_string": "2024-09-22T22:00:00.000"
										}
									]
								},
								"doc_count": 140,
								"key": 1727042400000,
								"key_as_string": "2024-09-22T22:00:00.000"
							},
							{
								"1": {
									"value": 458.0
								},
								"1-metric": {
									"value": 318.0
								},
								"3": {
									"keys": [
										"2024-09-23T10:00:00.000"
									],
									"value": 165.0
								},
								"3-bucket": {
									"buckets": [
										{
											"3-metric": {
												"value": 165
											},
											"doc_count": 178,
											"key": 1727085600000,
											"key_as_string": "2024-09-23T10:00:00.000"
										}
									]
								},
								"doc_count": 178,
								"key": 1727085600000,
								"key_as_string": "2024-09-23T10:00:00.000"
							}
						]
					}
				},
				"hits": {
					"hits": [],
					"max_score": null,
					"total": {
						"relation": "eq",
						"value": 4063
					}
				},
				"timed_out": false,
				"took": 50
			},
			"start_time_in_millis": 1728153887645
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__key_0", int64(1727049600000/43200000)),
				model.NewQueryResultCol("aggr__2__count", int64(140)),
				model.NewQueryResultCol("aggr__2__3-bucket__key_0", int64(1727049600000/43200000)),
				model.NewQueryResultCol("aggr__2__3-bucket__count", int64(140)),
				model.NewQueryResultCol("metric__2__3-bucket__3-metric_col_0", 131),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__key_0", int64(1727092800000/43200000)),
				model.NewQueryResultCol("aggr__2__count", int64(178)),
				model.NewQueryResultCol("aggr__2__3-bucket__key_0", int64(1727092800000/43200000)),
				model.NewQueryResultCol("aggr__2__3-bucket__count", int64(178)),
				model.NewQueryResultCol("metric__2__3-bucket__3-metric_col_0", 165),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT "aggr__2__key_0", "aggr__2__count", "aggr__2__3-bucket__key_0",
			  "aggr__2__3-bucket__count", "metric__2__3-bucket__3-metric_col_0"
			FROM (
			  SELECT "aggr__2__key_0", "aggr__2__count", "aggr__2__3-bucket__key_0",
				"aggr__2__3-bucket__count", "metric__2__3-bucket__3-metric_col_0",
				dense_rank() OVER (ORDER BY "aggr__2__key_0" ASC) AS "aggr__2__order_1_rank"
				,
				dense_rank() OVER (PARTITION BY "aggr__2__key_0" ORDER BY "aggr__2__key_0"
				ASC, "aggr__2__3-bucket__key_0" ASC) AS "aggr__2__3-bucket__order_1_rank"
			  FROM (
				SELECT toInt64((toUnixTimestamp64Milli("timestamp")+timeZoneOffset(
				  toTimezone("timestamp", 'Europe/Warsaw'))*1000) / 43200000) AS
				  "aggr__2__key_0",
				  sum(count(*)) OVER (PARTITION BY "aggr__2__key_0") AS "aggr__2__count",
				  toInt64((toUnixTimestamp64Milli("timestamp")+timeZoneOffset(toTimezone(
				  "timestamp", 'Europe/Warsaw'))*1000) / 43200000) AS
				  "aggr__2__3-bucket__key_0", count(*) AS "aggr__2__3-bucket__count",
				  uniq("timestamp") AS "metric__2__3-bucket__3-metric_col_0"
				FROM __quesma_table_name
				GROUP BY toInt64((toUnixTimestamp64Milli("timestamp")+timeZoneOffset(
				  toTimezone("timestamp", 'Europe/Warsaw'))*1000) / 43200000) AS
				  "aggr__2__key_0",
				  toInt64((toUnixTimestamp64Milli("timestamp")+timeZoneOffset(toTimezone(
				  "timestamp", 'Europe/Warsaw'))*1000) / 43200000) AS
				  "aggr__2__3-bucket__key_0"))
			ORDER BY "aggr__2__order_1_rank" ASC, "aggr__2__3-bucket__order_1_rank" ASC`,
	},
	{ // [3]
		TestName: "Reproduce: Visualize -> Vertical Bar: Metrics: Cumulative Sum (Aggregation: Count), Buckets: Histogram" +
			"(need add empty rows, even though there's no min_doc_count=0)",
		QueryRequestJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"2": {
					"aggs": {
						"1": {
							"cumulative_sum": {
								"buckets_path": "_count"
							}
						}
					},
					"histogram": {
						"field": "DistanceMiles",
						"interval": 10
					}
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
			"completion_time_in_millis": 1728210862660,
			"expiration_time_in_millis": 1728642862653,
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
								"1": {
									"value": 1.0
								},
								"doc_count": 1,
								"key": 0.0
							},
							{
								"1": {
									"value": 1.0
								},
								"doc_count": 0,
								"key": 10.0
							},
							{
								"1": {
									"value": 2.0
								},
								"doc_count": 1,
								"key": 20.0
							},
							{
								"1": {
									"value": 5.0
								},
								"doc_count": 3,
								"key": 30.0
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
				"took": 7
			},
			"start_time_in_millis": 1728210862653
		}
`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__key_0", 0.0),
				model.NewQueryResultCol("aggr__2__count", int64(1)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__key_0", 20.0),
				model.NewQueryResultCol("aggr__2__count", int64(1)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__key_0", 30.0),
				model.NewQueryResultCol("aggr__2__count", int64(3)),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT floor("DistanceMiles"/10)*10 AS "aggr__2__key_0",
              count(*) AS "aggr__2__count"
            FROM __quesma_table_name
            GROUP BY floor("DistanceMiles"/10)*10 AS "aggr__2__key_0"
            ORDER BY "aggr__2__key_0" ASC`,
	},
}
