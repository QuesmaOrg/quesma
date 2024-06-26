// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package testdata

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
			"id": "quesma_async_search_id_19",
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
			"id": "quesma_async_search_id_19",
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
			"id": "quesma_async_search_id_19",
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
			"id": "quesma_async_search_id_19",
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
