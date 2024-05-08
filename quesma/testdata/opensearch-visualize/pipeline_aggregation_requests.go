package opensearch_visualize

import (
	"mitmproxy/quesma/model"
	"mitmproxy/quesma/testdata"
)

var PipelineAggregationTests = []testdata.AggregationTestCase{
	{ // [0]
		TestName: "Simplest cumulative_sum (count). Reproduce: Visualize -> Vertical Bar: Metrics: Cumulative Sum (Aggregation: Count), Buckets: Histogram",
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
						"field": "day_of_week_i",
						"interval": 1,
						"min_doc_count": 0
					}
				}
			},
			"docvalue_fields": [
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
									"gte": "2024-01-24T11:23:10.802Z",
									"lte": "2024-05-08T10:23:10.802Z"
								}
							}
						}
					],
					"must": [
						{
							"match_all": {}
						}
					],
					"must_not": [],
					"should": []
				}
			},
			"script_fields": {},
			"size": 0,
			"stored_fields": [
				"*"
			]
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
				"2": {
					"buckets": [
						{
							"1": {
								"value": 282.0
							},
							"doc_count": 282,
							"key": 0.0
						},
						{
							"1": {
								"value": 582.0
							},
							"doc_count": 300,
							"key": 1.0
						}
					]
				}
			},
			"hits": {
				"hits": [],
				"max_score": null,
				"total": {
					"relation": "eq",
					"value": 1974
				}
			},
			"timed_out": false,
			"took": 8
		}`,
		ExpectedResults: [][]model.QueryResultRow{},
		ExpectedSQLs:    []string{},
	},
	{ // [1]
		TestName: "Cumulative sum with other aggregation. Reproduce: Visualize -> Vertical Bar: Metrics: Cumulative Sum (Aggregation: Average), Buckets: Histogram",
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
								"field": "day_of_week_i"
							}
						}
					},
					"histogram": {
						"field": "day_of_week_i",
						"interval": 1,
						"min_doc_count": 0
					}
				}
			},
			"docvalue_fields": [
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
					"filter": [],
					"must": [
						{
							"match_all": {}
						}
					],
					"must_not": [],
					"should": []
				}
			},
			"script_fields": {},
			"size": 0,
			"stored_fields": [
				"*"
			]
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
				"2": {
					"buckets": [
						{
							"1": {
								"value": 0.0
							},
							"1-metric": {
								"value": 0.0
							},
							"doc_count": 282,
							"key": 0.0
						},
						{
							"1": {
								"value": 1.0
							},
							"1-metric": {
								"value": 1.0
							},
							"doc_count": 300,
							"key": 1.0
						}
					]
				}
			},
			"hits": {
				"hits": [],
				"max_score": null,
				"total": {
					"relation": "eq",
					"value": 1974
				}
			},
			"timed_out": false,
			"took": 12
		}`,
		ExpectedResults: [][]model.QueryResultRow{},
		ExpectedSQLs:    []string{},
	},
	{ // [2]
		TestName: "Cumulative sum to other cumulative sum. Reproduce: Visualize -> Vertical Bar: Metrics: Cumulative Sum (Aggregation: Cumulative Sum (Aggregation: Count)), Buckets: Histogram",
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
								"field": "day_of_week_i"
							}
						}
					},
					"histogram": {
						"field": "day_of_week_i",
						"interval": 1,
						"min_doc_count": 0
					}
				}
			},
			"docvalue_fields": [
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
					"filter": [],
					"must": [
						{
							"match_all": {}
						}
					],
					"must_not": [],
					"should": []
				}
			},
			"script_fields": {},
			"size": 0,
			"stored_fields": [
				"*"
			]
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
				"2": {
					"buckets": [
						{
							"1": {
								"value": 282.0
							},
							"1-metric": {
								"value": 282.0
							},
							"doc_count": 282,
							"key": 0.0
						},
						{
							"1": {
								"value": 864.0
							},
							"1-metric": {
								"value": 582.0
							},
							"doc_count": 300,
							"key": 1.0
						}
					]
				}
			},
			"hits": {
				"hits": [],
				"max_score": null,
				"total": {
					"relation": "eq",
					"value": 1974
				}
			},
			"timed_out": false,
			"took": 10
		}`,
		ExpectedResults: [][]model.QueryResultRow{},
		ExpectedSQLs:    []string{},
	},
	{ // [3]
		TestName: "Cumulative sum - quite complex, a graph (DAG). Reproduce: Visualize -> Vertical Bar: Metrics: Cumulative Sum (Aggregation: Cumulative Sum (Aggregation: Max)), Buckets: Histogram",
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
								"buckets_path": "1-metric-metric"
							}
						},
						"1-metric-metric": {
							"max": {
								"field": "products.base_price"
							}
						}
					},
					"histogram": {
						"field": "day_of_week_i",
						"interval": 1,
						"min_doc_count": 0
					}
				}
			},
			"docvalue_fields": [
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
					"filter": [],
					"must": [
						{
							"match_all": {}
						}
					],
					"must_not": [],
					"should": []
				}
			},
			"script_fields": {},
			"size": 0,
			"stored_fields": [
				"*"
			]
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
				"2": {
					"buckets": [
						{
							"1": {
								"value": 1080.0
							},
							"1-metric": {
								"value": 1080.0
							},
							"1-metric-metric": {
								"value": 1080.0
							},
							"doc_count": 282,
							"key": 0.0
						},
						{
							"1": {
								"value": 2360.0
							},
							"1-metric": {
								"value": 1280.0
							},
							"1-metric-metric": {
								"value": 200.0
							},
							"doc_count": 300,
							"key": 1.0
						}
					]
				}
			},
			"hits": {
				"hits": [],
				"max_score": null,
				"total": {
					"relation": "eq",
					"value": 1975
				}
			},
			"timed_out": false,
			"took": 76
		}`,
		ExpectedResults: [][]model.QueryResultRow{},
		ExpectedSQLs:    []string{},
	},
}
