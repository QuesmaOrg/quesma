// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package testdata

import "quesma/model"

var FacetsTests = []AggregationTestCase{
	{ // [0]
		TestName: "facets, int64 as key, 3 (<10) values",
		QueryRequestJson: `
		{
			"aggs": {
				"sample": {
					"aggs": {
						"max_value": {
							"max": {
								"field": "int64-field"
							}
						},
						"min_value": {
							"min": {
								"field": "int64-field"
							}
						},
						"sample_count": {
							"value_count": {
								"field": "int64-field"
							}
						},
						"top_values": {
							"terms": {
								"field": "int64-field",
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
							"bool": {
								"filter": [],
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
			"track_total_hits": true
		}`,
		ExpectedResponse: `
		{
			"completion_status": 200,
			"completion_time_in_millis": 0,
			"expiration_time_in_millis": 0,
			"id": "quesma_async_19",
			"is_partial": false,
			"is_running": false,
			"response": {
				"_shards": {
					"failed": 0,
					"skipped": 0,
					"successful": 0,
					"total": 0
				},
				"aggregations": {
					"sample": {
						"doc_count": 2693,
						"max_value": {
							"value": 12140.860228566502
						},
						"min_value": {
							"value": 100.14596557617188
						},
						"sample_count": {
							"value": 2693
						},
						"top_values": {
							"buckets": [
								{
									"doc_count": 121,
									"key": 0
								},
								{
									"doc_count": 3,
									"key": 12.490584583112518
								},
								{
									"doc_count": 2,
									"key": 26.07052481248436
								}
							],
							"sum_other_doc_count": 2567
						}
					}
				},
				"hits": {
					"hits": [],
					"max_score": 0,
					"total": {
						"relation": "eq",
						"value": 2693
					}
				},
				"timed_out": false,
				"took": 0
			},
			"start_time_in_millis": 0
		}`,
		ExpectedResults: [][]model.QueryResultRow{
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol("count()", uint64(2200))}}},
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol(`maxOrNull("AvgTicketPrice")`, 12140.860228566502)}}},
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol(`minOrNull("AvgTicketPrice")`, 100.14596557617188)}}},
		},
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__sample__count", 2693),
				model.NewQueryResultCol("metric__sample__max_value_col_0", 12140.860228566502),
				model.NewQueryResultCol("metric__sample__min_value_col_0", 100.14596557617188),
				model.NewQueryResultCol("metric__sample__sample_count_col_0", 2693),
				model.NewQueryResultCol("aggr__sample__top_values__key_0", 0),
				model.NewQueryResultCol("aggr__sample__top_values__parent_count", 2693),
				model.NewQueryResultCol("aggr__sample__top_values__count", uint64(121)),
				model.NewQueryResultCol("aggr__sample__top_values__order_1", uint64(121)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__sample__count", 2693),
				model.NewQueryResultCol("metric__sample__max_value_col_0", 12140.860228566502),
				model.NewQueryResultCol("metric__sample__min_value_col_0", 100.14596557617188),
				model.NewQueryResultCol("metric__sample__sample_count_col_0", 2693),
				model.NewQueryResultCol("aggr__sample__top_values__key_0", 12.490584583112518),
				model.NewQueryResultCol("aggr__sample__top_values__parent_count", 2693),
				model.NewQueryResultCol("aggr__sample__top_values__count", uint64(3)),
				model.NewQueryResultCol("aggr__sample__top_values__order_1", uint64(3)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__sample__count", 2693),
				model.NewQueryResultCol("metric__sample__max_value_col_0", 12140.860228566502),
				model.NewQueryResultCol("metric__sample__min_value_col_0", 100.14596557617188),
				model.NewQueryResultCol("metric__sample__sample_count_col_0", 2693),
				model.NewQueryResultCol("aggr__sample__top_values__key_0", 26.07052481248436),
				model.NewQueryResultCol("aggr__sample__top_values__parent_count", 2693),
				model.NewQueryResultCol("aggr__sample__top_values__count", uint64(2)),
				model.NewQueryResultCol("aggr__sample__top_values__order_1", uint64(2)),
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
		ExpectedPancakeSQL: `
			SELECT "aggr__sample__count", "metric__sample__max_value_col_0",
			  "metric__sample__min_value_col_0", "metric__sample__sample_count_col_0",
			  "aggr__sample__top_values__key_0", "aggr__sample__top_values__parent_count",
			  "aggr__sample__top_values__count", "aggr__sample__top_values__order_1"
			FROM (
			  SELECT "aggr__sample__count", "metric__sample__max_value_col_0",
				"metric__sample__min_value_col_0", "metric__sample__sample_count_col_0",
				"aggr__sample__top_values__key_0", "aggr__sample__top_values__parent_count",
				 "aggr__sample__top_values__count", "aggr__sample__top_values__order_1",
				dense_rank() OVER (PARTITION BY 1
			  ORDER BY "aggr__sample__top_values__order_1" DESC,
				"aggr__sample__top_values__key_0" ASC) AS
				"aggr__sample__top_values__order_1_rank"
			  FROM (
				SELECT sum("aggr__sample__count_part") OVER (PARTITION BY 1) AS
				  "aggr__sample__count", maxOrNull("int64-field") AS
				  "metric__sample__max_value_col_0", minOrNull("int64-field") AS
				  "metric__sample__min_value_col_0", count() AS
				  "metric__sample__sample_count_col_0", "int64-field" AS
				  "aggr__sample__top_values__key_0", sum(count(*)) OVER (PARTITION BY 1) AS
				  "aggr__sample__top_values__parent_count", count(*) AS
				  "aggr__sample__top_values__count", count() AS
				  "aggr__sample__top_values__order_1", count(*) AS
				  "aggr__sample__count_part"
				FROM "logs-generic-default"
				GROUP BY "int64-field" AS "aggr__sample__top_values__key_0"))
			WHERE "aggr__sample__top_values__order_1_rank"<=10
			ORDER BY "aggr__sample__top_values__order_1_rank" ASC`,
	},
	{ // [1]
		TestName: "facets, only 1 null bucket",
		QueryRequestJson: `
		{
			"aggs": {
				"sample": {
					"aggs": {
						"max_value": {
							"max": {
								"field": "int64-field"
							}
						},
						"min_value": {
							"min": {
								"field": "int64-field"
							}
						},
						"sample_count": {
							"value_count": {
								"field": "int64-field"
							}
						},
						"top_values": {
							"terms": {
								"field": "int64-field",
								"size": 10
							}
						}
					},
					"sampler": {
						"shard_size": 5000
					}
				}
			},
			"track_total_hits": false
		}`,
		ExpectedResponse: `
		{
			"completion_status": 200,
			"completion_time_in_millis": 0,
			"expiration_time_in_millis": 0,
			"id": "quesma_async_19",
			"is_partial": false,
			"is_running": false,
			"response": {
				"_shards": {
					"failed": 0,
					"skipped": 0,
					"successful": 0,
					"total": 0
				},
				"aggregations": {
					"sample": {
						"doc_count": 100,
						"max_value": {
							"value": null
						},
						"min_value": {
							"value": null
						},
						"sample_count": {
							"value": 100
						},
						"top_values": {
							"buckets": [
								{
									"doc_count": 100,
									"key": null
								}
							],
							"sum_other_doc_count": 0
						}
					}
				},
				"hits": {
					"hits": [],
					"max_score": 0,
					"total": {
						"relation": "eq",
						"value": 2693
					}
				},
				"timed_out": false,
				"took": 0
			},
			"start_time_in_millis": 0
		}`,
		ExpectedResults: [][]model.QueryResultRow{
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol("count()", uint64(2200))}}},
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol(`maxOrNull("AvgTicketPrice")`, 12140.860228566502)}}},
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol(`minOrNull("AvgTicketPrice")`, 100.14596557617188)}}},
		},
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__sample__count", 100),
				model.NewQueryResultCol("metric__sample__max_value_col_0", nil),
				model.NewQueryResultCol("metric__sample__min_value_col_0", nil),
				model.NewQueryResultCol("metric__sample__sample_count_col_0", 100),
				model.NewQueryResultCol("aggr__sample__top_values__key_0", nil),
				model.NewQueryResultCol("aggr__sample__top_values__parent_count", 100),
				model.NewQueryResultCol("aggr__sample__top_values__count", uint64(100)),
				model.NewQueryResultCol("aggr__sample__top_values__order_1", uint64(100)),
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
		ExpectedPancakeSQL: `
			SELECT sum(count(*)) OVER () AS "aggr__sample__count",
			  maxOrNull(maxOrNull("int64-field")) OVER () AS "metric__sample__max_value_col_0",
			  minOrNull(minOrNull("int64-field")) OVER () AS "metric__sample__min_value_col_0",
			  sum(count("int64-field")) OVER () AS "metric__sample__sample_count_col_0",
			  sum(count(*)) OVER () AS "aggr__sample__top_values__parent_count",
			  "int64-field" AS "aggr__sample__top_values__key_0",
			  count(*) AS "aggr__sample__top_values__count",
			  count() AS "aggr__sample__top_values__order_1"
			FROM (
			  SELECT "int64-field"
			  FROM __quesma_table_name
			  LIMIT 20000)
			GROUP BY "int64-field" AS "aggr__sample__top_values__key_0"
			ORDER BY "aggr__sample__top_values__order_1" DESC,
			  "aggr__sample__top_values__key_0" ASC
			LIMIT 11`,
	},
}

// Tests for numeric facets (int64, float64).
// Tests for string facets are already covered in "standard" queries (see testdata/requests.go, testdata/aggregation_requests.go),
// so not repeating them here.
var TestsNumericFacets = []struct {
	Name                     string
	QueryJson                string
	ResultJson               string
	ExpectedSQL              string
	ResultRows               [][]any
	MaxExpected              float64
	MinExpected              float64
	CountExpected            float64
	SumOtherDocCountExpected float64
}{
	{
		Name: "facets, int64 as key, 3 (<10) values",
		QueryJson: `
		{
			"aggs": {
				"sample": {
					"aggs": {
						"max_value": {
							"max": {
								"field": "int64-field"
							}
						},
						"min_value": {
							"min": {
								"field": "int64-field"
							}
						},
						"sample_count": {
							"value_count": {
								"field": "int64-field"
							}
						},
						"top_values": {
							"terms": {
								"field": "int64-field",
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
							"bool": {
								"filter": [],
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
			"track_total_hits": true
		}`,
		ResultJson: `
		{
			"completion_status": 200,
			"completion_time_in_millis": 0,
			"expiration_time_in_millis": 0,
			"id": "quesma_async_19",
			"is_partial": false,
			"is_running": false,
			"response": {
				"_shards": {
					"failed": 0,
					"skipped": 0,
					"successful": 0,
					"total": 0
				},
				"aggregations": {
					"sample": {
						"doc_count": 2693,
						"max_value": {
							"value": 12140.860228566502
						},
						"min_value": {
							"value": 0
						},
						"sample_count": {
							"value": 2693
						},
						"top_values": {
							"buckets": [
								{
									"doc_count": 121,
									"key": 0
								},
								{
									"doc_count": 3,
									"key": 12.490584583112518
								},
								{
									"doc_count": 2,
									"key": 26.07052481248436
								}
							]
						}
					}
				},
				"hits": {
					"hits": [],
					"max_score": 0,
					"total": {
						"relation": "eq",
						"value": 2693
					}
				},
				"timed_out": false,
				"took": 0
			},
			"start_time_in_millis": 0
		}`,
		ExpectedSQL: `SELECT sum(count(*)) OVER () AS "metric____quesma_total_count_col_0", ` +
			`sum(count(*)) OVER () AS "aggr__sample__count", ` +
			`maxOrNull(maxOrNull("int64-field")) OVER () AS "metric__sample__max_value_col_0", ` +
			`minOrNull(minOrNull("int64-field")) OVER () AS "metric__sample__min_value_col_0", ` +
			`sum(count("int64-field")) OVER () AS "metric__sample__sample_count_col_0", ` +
			`sum(count(*)) OVER () AS "aggr__sample__top_values__parent_count", ` +
			`"int64-field" AS "aggr__sample__top_values__key_0", ` +
			`count(*) AS "aggr__sample__top_values__count", ` +
			`count() AS "aggr__sample__top_values__order_1" ` +
			`FROM (` +
			`SELECT "int64-field" ` +
			`FROM __quesma_table_name ` +
			`LIMIT 20000) ` +
			`GROUP BY "int64-field" AS "aggr__sample__top_values__key_0" ` +
			`ORDER BY "aggr__sample__top_values__order_1" DESC, "aggr__sample__top_values__key_0" ASC ` +
			`LIMIT 11`,
		ResultRows: [][]any{
			{2693, 2693, 4, 1, 2693, 2693, 0, 121, 121},
			{2693, 2693, 4, 1, 2693, 2693, 12.490584583112518, 3, 3},
			{2693, 2693, 4, 1, 2693, 2693, 26.07052481248436, 2, 2},
		},
		MaxExpected:              4,
		MinExpected:              1,
		CountExpected:            2693,
		SumOtherDocCountExpected: 2567,
	},
	{
		Name: "facets, int64 as key, 16 (>10) values - should be truncated to 10",
		QueryJson: `
		{
			"aggs": {
				"sample": {
					"aggs": {
						"max_value": {
							"max": {
								"field": "int64-field"
							}
						},
						"min_value": {
							"min": {
								"field": "int64-field"
							}
						},
						"sample_count": {
							"value_count": {
								"field": "int64-field"
							}
						},
						"top_values": {
							"terms": {
								"field": "int64-field",
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
							"bool": {
								"filter": [],
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
			"track_total_hits": true
		}`,
		ResultJson: /* Caution: right now completely incorrect, doesn't matter much for the test usefulness */ `
		{
			"completion_status": 200,
			"completion_time_in_millis": 0,
			"expiration_time_in_millis": 0,
			"id": "quesma_async_19",
			"is_partial": false,
			"is_running": false,
			"response": {
				"_shards": {
					"failed": 0,
					"skipped": 0,
					"successful": 0,
					"total": 0
				},
				"aggregations": {
					"sample": {
						"doc_count": 2693,
						"max_value": {
							"value": 12140.860228566502
						},
						"min_value": {
							"value": 0
						},
						"sample_count": {
							"value": 2693
						},
						"top_values": {
							"buckets": [
								{
									"doc_count": 121,
									"key": 0
								},
								{
									"doc_count": 3,
									"key": 12.490584583112518
								},
								{
									"doc_count": 2,
									"key": 26.07052481248436
								}
							]
						}
					}
				},
				"hits": {
					"hits": [],
					"max_score": 0,
					"total": {
						"relation": "eq",
						"value": 2693
					}
				},
				"timed_out": false,
				"took": 0
			},
			"start_time_in_millis": 0
		}`,
		ExpectedSQL: `SELECT sum(count(*)) OVER () AS "metric____quesma_total_count_col_0", ` +
			`sum(count(*)) OVER () AS "aggr__sample__count", ` +
			`maxOrNull(maxOrNull("int64-field")) OVER () AS "metric__sample__max_value_col_0", ` +
			`minOrNull(minOrNull("int64-field")) OVER () AS "metric__sample__min_value_col_0", ` +
			`sum(count("int64-field")) OVER () AS "metric__sample__sample_count_col_0", ` +
			`sum(count(*)) OVER () AS "aggr__sample__top_values__parent_count", ` +
			`"int64-field" AS "aggr__sample__top_values__key_0", ` +
			`count(*) AS "aggr__sample__top_values__count", ` +
			`count() AS "aggr__sample__top_values__order_1" ` +
			`FROM (` +
			`SELECT "int64-field" ` +
			`FROM __quesma_table_name ` +
			`LIMIT 20000) ` +
			`GROUP BY "int64-field" AS "aggr__sample__top_values__key_0" ` +
			`ORDER BY "aggr__sample__top_values__order_1" DESC, "aggr__sample__top_values__key_0" ASC ` +
			`LIMIT 11`,
		ResultRows: [][]any{
			{2693, 2693, int64(4611686018427), -100, 2693, 2693, -100, 11, 11},
			{2693, 2693, int64(4611686018427), -100, 2693, 2693, 2, 10, 10},
			{2693, 2693, int64(4611686018427), -100, 2693, 2693, 3, 9, 9},
			{2693, 2693, int64(4611686018427), -100, 2693, 2693, 4, 8, 8},
			{2693, 2693, int64(4611686018427), -100, 2693, 2693, 5, 7, 7},
			{2693, 2693, int64(4611686018427), -100, 2693, 2693, 6, 6, 6},
			{2693, 2693, int64(4611686018427), -100, 2693, 2693, 7, 5, 5},
			{2693, 2693, int64(4611686018427), -100, 2693, 2693, 8, 4, 4},
			{2693, 2693, int64(4611686018427), -100, 2693, 2693, 9, 3, 3},
			{2693, 2693, int64(4611686018427), -100, 2693, 2693, 10, 2, 2},
			{2693, 2693, int64(4611686018427), -100, 2693, 2693, int64(4611686018427), 1, 1},
		},
		MaxExpected:              4611686018427,
		MinExpected:              -100,
		CountExpected:            2693,
		SumOtherDocCountExpected: 2628,
	},
	{
		Name: "facets, float64 as key, 3 (<10) values",
		QueryJson: `
		{
			"aggs": {
				"sample": {
					"aggs": {
						"max_value": {
							"max": {
								"field": "float64-field"
							}
						},
						"min_value": {
							"min": {
								"field": "float64-field"
							}
						},
						"sample_count": {
							"value_count": {
								"field": "float64-field"
							}
						},
						"top_values": {
							"terms": {
								"field": "float64-field",
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
							"bool": {
								"filter": [],
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
			"track_total_hits": true
		}`,
		ResultJson: `
		{
			"completion_status": 200,
			"completion_time_in_millis": 0,
			"expiration_time_in_millis": 0,
			"id": "quesma_async_19",
			"is_partial": false,
			"is_running": false,
			"response": {
				"_shards": {
					"failed": 0,
					"skipped": 0,
					"successful": 0,
					"total": 0
				},
				"aggregations": {
					"sample": {
						"doc_count": 2693,
						"max_value": {
							"value": 12140.860228566502
						},
						"min_value": {
							"value": 0
						},
						"sample_count": {
							"value": 2693
						},
						"top_values": {
							"buckets": [
								{
									"doc_count": 2,
									"key": 0.5
								},
								{
									"doc_count": 1,
									"key": 2.75
								},
								{
									"doc_count": 1,
									"key": 8.33
								}
							]
						}
					}
				},
				"hits": {
					"hits": [],
					"max_score": 0,
					"total": {
						"relation": "eq",
						"value": 2693
					}
				},
				"timed_out": false,
				"took": 0
			},
			"start_time_in_millis": 0
		}`,
		ExpectedSQL: `SELECT sum(count(*)) OVER () AS "metric____quesma_total_count_col_0", ` +
			`sum(count(*)) OVER () AS "aggr__sample__count", ` +
			`maxOrNull(maxOrNull("float64-field")) OVER () AS "metric__sample__max_value_col_0", ` +
			`minOrNull(minOrNull("float64-field")) OVER () AS "metric__sample__min_value_col_0", ` +
			`sum(count("float64-field")) OVER () AS "metric__sample__sample_count_col_0", ` +
			`sum(count(*)) OVER () AS "aggr__sample__top_values__parent_count", ` +
			`"float64-field" AS "aggr__sample__top_values__key_0", ` +
			`count(*) AS "aggr__sample__top_values__count", ` +
			`count() AS "aggr__sample__top_values__order_1" ` +
			`FROM (` +
			`SELECT "float64-field" ` +
			`FROM __quesma_table_name ` +
			`LIMIT 20000) ` +
			`GROUP BY "float64-field" AS "aggr__sample__top_values__key_0" ` +
			`ORDER BY "aggr__sample__top_values__order_1" DESC, "aggr__sample__top_values__key_0" ASC ` +
			`LIMIT 11`,
		ResultRows: [][]any{
			{2693, 2693, 8.33, 0.5, 2693, 2693, 0.5, 2, 2},
			{2693, 2693, 8.33, 0.5, 2693, 2693, 2.75, 1, 1},
			{2693, 2693, 8.33, 0.5, 2693, 2693, 8.33, 1, 1},
		},
		MaxExpected:              8.33,
		MinExpected:              0.5,
		CountExpected:            2693,
		SumOtherDocCountExpected: 2689,
	},
	{
		Name: "facets, float64 as key, 16 (>10) values - should be truncated to 10",
		QueryJson: `
		{
			"aggs": {
				"sample": {
					"aggs": {
						"max_value": {
							"max": {
								"field": "float64-field"
							}
						},
						"min_value": {
							"min": {
								"field": "float64-field"
							}
						},
						"sample_count": {
							"value_count": {
								"field": "float64-field"
							}
						},
						"top_values": {
							"terms": {
								"field": "float64-field",
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
							"bool": {
								"filter": [],
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
			"track_total_hits": true
		}`,
		ResultJson: /* Caution: right now completely incorrect, doesn't matter much for the test usefulness */ `
		{
			"completion_status": 200,
			"completion_time_in_millis": 0,
			"expiration_time_in_millis": 0,
			"id": "quesma_async_19",
			"is_partial": false,
			"is_running": false,
			"response": {
				"_shards": {
					"failed": 0,
					"skipped": 0,
					"successful": 0,
					"total": 0
				},
				"aggregations": {
					"sample": {
						"doc_count": 2693,
						"max_value": {
							"value": 12140.860228566502
						},
						"min_value": {
							"value": 0
						},
						"sample_count": {
							"value": 2693
						},
						"top_values": {
							"buckets": [
								{
									"doc_count": 121,
									"key": 0
								},
								{
									"doc_count": 3,
									"key": 12.490584583112518
								},
								{
									"doc_count": 2,
									"key": 26.07052481248436
								}
							]
						}
					}
				},
				"hits": {
					"hits": [],
					"max_score": 0,
					"total": {
						"relation": "eq",
						"value": 2693
					}
				},
				"timed_out": false,
				"took": 0
			},
			"start_time_in_millis": 0
		}`,
		ExpectedSQL: `SELECT sum(count(*)) OVER () AS "metric____quesma_total_count_col_0", ` +
			`sum(count(*)) OVER () AS "aggr__sample__count", ` +
			`maxOrNull(maxOrNull("float64-field")) OVER () AS "metric__sample__max_value_col_0", ` +
			`minOrNull(minOrNull("float64-field")) OVER () AS "metric__sample__min_value_col_0", ` +
			`sum(count("float64-field")) OVER () AS "metric__sample__sample_count_col_0", ` +
			`sum(count(*)) OVER () AS "aggr__sample__top_values__parent_count", ` +
			`"float64-field" AS "aggr__sample__top_values__key_0", ` +
			`count(*) AS "aggr__sample__top_values__count", ` +
			`count() AS "aggr__sample__top_values__order_1" ` +
			`FROM (` +
			`SELECT "float64-field" ` +
			`FROM __quesma_table_name ` +
			`LIMIT 20000) ` +
			`GROUP BY "float64-field" AS "aggr__sample__top_values__key_0" ` +
			`ORDER BY "aggr__sample__top_values__order_1" DESC, "aggr__sample__top_values__key_0" ASC ` +
			`LIMIT 11`,
		ResultRows: [][]any{
			{2693, 2693, 11.08, -100.22, 2693, 2693, 11.08, 11, 11},
			{2693, 2693, 11.08, -100.22, 2693, 2693, 10, 10, 10},
			{2693, 2693, 11.08, -100.22, 2693, 2693, 9, 9, 9},
			{2693, 2693, 11.08, -100.22, 2693, 2693, 8, 8, 8},
			{2693, 2693, 11.08, -100.22, 2693, 2693, 7, 7, 7},
			{2693, 2693, 11.08, -100.22, 2693, 2693, 6, 6, 6},
			{2693, 2693, 11.08, -100.22, 2693, 2693, 5, 5, 5},
			{2693, 2693, 11.08, -100.22, 2693, 2693, 4, 4, 4},
			{2693, 2693, 11.08, -100.22, 2693, 2693, 3, 3, 3},
			{2693, 2693, 11.08, -100.22, 2693, 2693, 2, 2, 2},
			{2693, 2693, 11.08, -100.22, 2693, 2693, -100.22, 1, 1},
		},
		MaxExpected:              11.08,
		MinExpected:              -100.22,
		CountExpected:            2693,
		SumOtherDocCountExpected: 2628,
	},
}
