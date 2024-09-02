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
}

// Tests for numeric facets (int64, float64).
// Tests for string facets are already covered in "standard" queries (see testdata/requests.go, testdata/aggregation_requests.go),
// so not repeating them here.
var TestsNumericFacets = []struct {
	Name                     string
	QueryJson                string
	ResultJson               string
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
		ResultRows: [][]any{
			// value, count
			{1, uint64(2)},
			{3, uint64(1)},
			{4, uint64(1)},
		},
		MaxExpected:              4,
		MinExpected:              1,
		CountExpected:            4,
		SumOtherDocCountExpected: 0,
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
		ResultRows: [][]any{
			// value, count
			{-100, uint64(1)}, {2, uint64(2)}, {3, uint64(3)}, {4, uint64(4)},
			{5, uint64(5)}, {6, uint64(6)}, {7, uint64(7)}, {8, uint64(8)},
			{9, uint64(9)}, {10, uint64(10)}, {11, uint64(11)}, {12, uint64(12)},
			{13, uint64(13)}, {14, uint64(14)}, {15, uint64(15)}, {4611686018427, uint64(16)},
			// last one: bigger than int32, but not too big. After we fix the issue with invalid unmarshalling of int64 into float64,
			// where now we're losing precision, we can test with bigger values here.
		},
		MaxExpected:              4611686018427,
		MinExpected:              -100,
		CountExpected:            136,
		SumOtherDocCountExpected: 81,
	},
	{
		Name: "facets, int64 as key, 3 (<10) values",
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
		ResultRows: [][]any{
			// value, count
			{0.5, uint64(2)},
			{2.75, uint64(1)},
			{8.33, uint64(1)},
		},
		MaxExpected:              8.33,
		MinExpected:              0.5,
		CountExpected:            4,
		SumOtherDocCountExpected: 0,
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
		ResultRows: [][]any{
			// value, count
			{-100.22, uint64(1)}, {2, uint64(2)}, {3, uint64(3)}, {4, uint64(4)},
			{5, uint64(5)}, {6, uint64(6)}, {7, uint64(7)}, {8, uint64(8)},
			{9, uint64(9)}, {10, uint64(10)}, {11, uint64(11)}, {12.56, uint64(12)},
			{13, uint64(13)}, {14, uint64(14)}, {15, uint64(15)}, {16.08, uint64(16)},
		},
		MaxExpected:              16.08,
		MinExpected:              -100.22,
		CountExpected:            136,
		SumOtherDocCountExpected: 81,
	},
}
