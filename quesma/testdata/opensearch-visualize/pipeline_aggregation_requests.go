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
		ExpectedResults: [][]model.QueryResultRow{
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol("hits", uint64(1974))}}},
			{}, // NoDBQuery
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("key", 0.0),
					model.NewQueryResultCol("doc_count", 282),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("key", 1.0),
					model.NewQueryResultCol("doc_count", 300),
				}},
			},
		},
		ExpectedSQLs: []string{
			`SELECT count() ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`WHERE "order_date">=parseDateTime64BestEffort('2024-01-24T11:23:10.802Z') AND "order_date"<=parseDateTime64BestEffort('2024-05-08T10:23:10.802Z') `,
			`NoDBQuery`,
			`SELECT "day_of_week_i", count() ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`WHERE "order_date">=parseDateTime64BestEffort('2024-01-24T11:23:10.802Z') AND "order_date"<=parseDateTime64BestEffort('2024-05-08T10:23:10.802Z')  ` +
				`GROUP BY ("day_of_week_i") ` +
				`ORDER BY ("day_of_week_i")`,
		},
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
		ExpectedResults: [][]model.QueryResultRow{
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol("hits", uint64(1974))}}},
			{}, // NoDBQuery
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("key", 0.0),
					model.NewQueryResultCol(`avgOrNull("day_of_week_i")`, 0.0),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("key", 1.0),
					model.NewQueryResultCol(`avgOrNull("day_of_week_i")`, 1.0),
				}},
			},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("key", 0.0),
					model.NewQueryResultCol("doc_count", 282),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("key", 1.0),
					model.NewQueryResultCol("doc_count", 300),
				}},
			},
		},
		ExpectedSQLs: []string{
			`SELECT count() FROM ` + testdata.QuotedTableName + ` `,
			`NoDBQuery`,
			`SELECT "day_of_week_i", avgOrNull("day_of_week_i") ` +
				`FROM ` + testdata.QuotedTableName + `  ` +
				`GROUP BY ("day_of_week_i") ` +
				`ORDER BY ("day_of_week_i")`,
			`SELECT "day_of_week_i", count() ` +
				`FROM ` + testdata.QuotedTableName + `  ` +
				`GROUP BY ("day_of_week_i") ` +
				`ORDER BY ("day_of_week_i")`,
		},
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
		ExpectedResults: [][]model.QueryResultRow{
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol("hits", uint64(1974))}}},
			{}, // NoDBQuery
			{}, // NoDBQuery
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("key", 0.0),
					model.NewQueryResultCol("doc_count", 282),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("key", 1.0),
					model.NewQueryResultCol("doc_count", 300),
				}},
			},
		},
		ExpectedSQLs: []string{
			`SELECT count() FROM ` + testdata.QuotedTableName + ` `,
			`NoDBQuery`,
			`NoDBQuery`,
			`SELECT "day_of_week_i", count() ` +
				`FROM ` + testdata.QuotedTableName + `  ` +
				`GROUP BY ("day_of_week_i") ` +
				`ORDER BY ("day_of_week_i")`,
		},
	},
	{ // [3]
		TestName: "Cumulative sum - quite complex, a graph of pipelines. Reproduce: Visualize -> Vertical Bar: Metrics: Cumulative Sum (Aggregation: Cumulative Sum (Aggregation: Max)), Buckets: Histogram",
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
		ExpectedResults: [][]model.QueryResultRow{
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol("hits", uint64(1974))}}},
			{}, // NoDBQuery
			{}, // NoDBQuery
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("key", 0.0),
					model.NewQueryResultCol(`maxOrNull("products.base_price")`, 1080.0),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("key", 1.0),
					model.NewQueryResultCol(`maxOrNull("products.base_price")`, 200.0),
				}},
			},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("key", 0.0),
					model.NewQueryResultCol("doc_count", 282),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("key", 1.0),
					model.NewQueryResultCol("doc_count", 300),
				}},
			},
		},
		ExpectedSQLs: []string{
			`SELECT count() FROM ` + testdata.QuotedTableName + ` `,
			`NoDBQuery`,
			`NoDBQuery`,
			`SELECT "day_of_week_i", maxOrNull("products.base_price") ` +
				`FROM ` + testdata.QuotedTableName + `  ` +
				`GROUP BY ("day_of_week_i") ` +
				`ORDER BY ("day_of_week_i")`,
			`SELECT "day_of_week_i", count() ` +
				`FROM ` + testdata.QuotedTableName + `  ` +
				`GROUP BY ("day_of_week_i") ` +
				`ORDER BY ("day_of_week_i")`,
		},
	},
	{ // [4]
		TestName: "Simplest Derivative (count). Reproduce: Visualize -> Vertical Bar: Metrics: Derivative (Aggregation: Count), Buckets: Histogram",
		QueryRequestJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"2": {
					"aggs": {
						"1": {
							"derivative": {
								"buckets_path": "_count"
							}
						}
					},
					"histogram": {
						"field": "bytes",
						"interval": 200,
						"min_doc_count": 0
					}
				}
			},
			"docvalue_fields": [
				{
					"field": "@timestamp",
					"format": "date_time"
				},
				{
					"field": "timestamp",
					"format": "date_time"
				},
				{
					"field": "utc_time",
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
			"script_fields": {
				"hour_of_day": {
					"script": {
						"lang": "painless",
						"source": "doc['timestamp'].value.getHour()"
					}
				}
			},
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
							"doc_count": 106,
							"key": 0.0,
							"1": {
								"value": null
							}
						},
						{
							"1": {
								"value": -67.0
							},
							"doc_count": 39,
							"key": 200.0
						},
						{
							"1": {
								"value": -18.0
							},
							"doc_count": 21,
							"key": 400.0
						}
					]
				}
			},
			"hits": {
				"hits": [],
				"max_score": null,
				"total": {
					"relation": "eq",
					"value": 2553
				}
			},
			"timed_out": false,
			"took": 40
		}`,
		ExpectedResults: [][]model.QueryResultRow{
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol("hits", uint64(2553))}}},
			{}, // NoDBQuery
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("key", 0.0),
					model.NewQueryResultCol("doc_count", 106),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("key", 200.0),
					model.NewQueryResultCol("doc_count", 39),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("key", 400.0),
					model.NewQueryResultCol("doc_count", 21),
				}},
			},
		},
		ExpectedSQLs: []string{
			`SELECT count() ` +
				`FROM ` + testdata.QuotedTableName + ` `,
			`NoDBQuery`,
			`SELECT floor("bytes" / 200.000000) * 200.000000, count() ` +
				`FROM ` + testdata.QuotedTableName + `  ` +
				`GROUP BY (floor("bytes" / 200.000000) * 200.000000) ` +
				`ORDER BY (floor("bytes" / 200.000000) * 200.000000)`,
		},
	},
	// waits for merge of #56
	/*
		{ // [5]
			TestName: "Derivative with other aggregation. Reproduce: Visualize -> Vertical Bar: Metrics: Derivative (Aggregation: Sum), Buckets: Date Histogram",
			QueryRequestJson: `
			{
				"_source": {
					"excludes": []
				},
				"aggs": {
					"2": {
						"aggs": {
							"1": {
								"derivative": {
									"buckets_path": "1-metric"
								}
							},
							"1-metric": {
								"sum": {
									"script": {
										"lang": "painless",
										"source": "doc['timestamp'].value.getHour()"
									}
								}
							}
						},
						"date_histogram": {
							"field": "timestamp",
							"fixed_interval": "10m",
							"time_zone": "Europe/Warsaw"
						}
					}
				},
				"docvalue_fields": [
					{
						"field": "@timestamp",
						"format": "date_time"
					},
					{
						"field": "timestamp",
						"format": "date_time"
					},
					{
						"field": "utc_time",
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
				"script_fields": {
					"hour_of_day": {
						"script": {
							"lang": "painless",
							"source": "doc['timestamp'].value.getHour()"
						}
					}
				},
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
								"1-metric": {
									"value": 19.0
								},
								"doc_count": 1,
								"key": 1715196000000,
								"key_as_string": "2024-05-08T21:20:00.000+02:00"
							},
							{
								"1": {
									"value": 0.0
								},
								"1-metric": {
									"value": 19.0
								},
								"doc_count": 1,
								"key": 1715196600000,
								"key_as_string": "2024-05-08T21:30:00.000+02:00"
							},
							{
								"1": {
									"value": null
								},
								"1-metric": {
									"value": 20.0
								},
								"doc_count": 1,
								"key": 1715198400000,
								"key_as_string": "2024-05-08T22:00:00.000+02:00"
							},
							{
								"1": {
									"value": 12.0
								},
								"1-metric": {
									"value": 32.0
								},
								"doc_count": 4,
								"key": 171519900000,
								"key_as_string": "2024-05-08T22:10:00.000+02:00"
							},
							{
								"1": {
									"value": -5.0
								},
								"1-metric": {
									"value": 27.0
								},
								"doc_count": 3,
								"key": 1715199600000,
								"key_as_string": "2024-05-08T22:20:00.000+02:00"
							}
						]
					}
				},
				"hits": {
					"hits": [],
					"max_score": null,
					"total": {
						"relation": "eq",
						"value": 2553
					}
				},
				"timed_out": false,
				"took": 40
			}`,
			ExpectedResults: [][]model.QueryResultRow{
				{{Cols: []model.QueryResultCol{model.NewQueryResultCol("hits", uint64(2553))}}},
				{}, // NoDBQuery
				{
					{Cols: []model.QueryResultCol{
						model.NewQueryResultCol("key", 0.0),
						model.NewQueryResultCol("doc_count", 106),
					}},
					{Cols: []model.QueryResultCol{
						model.NewQueryResultCol("key", 200.0),
						model.NewQueryResultCol("doc_count", 39),
					}},
					{Cols: []model.QueryResultCol{
						model.NewQueryResultCol("key", 400.0),
						model.NewQueryResultCol("doc_count", 21),
					}},
				},
			},
			ExpectedSQLs: []string{
				`SELECT count() ` +
					`FROM ` + testdata.QuotedTableName + ` `,
				`NoDBQuery`,
				`SELECT floor("bytes" / 200.000000) * 200.000000, count() ` +
					`FROM ` + testdata.QuotedTableName + `  ` +
					`GROUP BY (floor("bytes" / 200.000000) * 200.000000) ` +
					`ORDER BY (floor("bytes" / 200.000000) * 200.000000)`,
			},
		},
	*/
	{ // [6]
		TestName: "Derivative to cumulative sum. Reproduce: Visualize -> Vertical Bar: Metrics: Derivative (Aggregation: Cumulative Sum (Aggregation: Count)), Buckets: Date Histogram",
		QueryRequestJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"2": {
					"aggs": {
						"1": {
							"derivative": {
								"buckets_path": "1-metric"
							}
						},
						"1-metric": {
							"cumulative_sum": {
								"buckets_path": "_count"
							}
						}
					},
					"date_histogram": {
						"field": "timestamp",
						"fixed_interval": "10m",
						"time_zone": "Europe/Warsaw"
					}
				}
			},
			"docvalue_fields": [
				{
					"field": "@timestamp",
					"format": "date_time"
				},
				{
					"field": "timestamp",
					"format": "date_time"
				},
				{
					"field": "utc_time",
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
			"script_fields": {
				"hour_of_day": {
					"script": {
						"lang": "painless",
						"source": "doc['timestamp'].value.getHour()"
					}
				}
			},
			"size": 0,
			"stored_fields": [
				"*"
			]
		}`,
		ExpectedResponse: // opensearch returns "1": {null} for 2nd, 3rd and 3 last buckets. I think it's not correct... I return 0, and it seems working too.
		`{
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
							"1-metric": {
								"value": 2.0
							},
							"doc_count": 2,
							"key": 1714869000000,
							"key_as_string": "2024-05-05T02:30:00.000+02:00"
						},
						{
							"1": {
								"value": null
							},
							"1-metric": {
								"value": 2.0
							},
							"doc_count": 0,
							"key": 1714869600000,
							"key_as_string": "2024-05-05T02:40:00.000+02:00"
						},
						{
							"1": {
								"value": null
							},
							"1-metric": {
								"value": 2.0
							},
							"doc_count": 0,
							"key": 1714878600000,
							"key_as_string": "2024-05-05T05:10:00.000+02:00"
						},
						{
							"1": {
								"value": null
							},
							"1-metric": {
								"value": 4.0
							},
							"doc_count": 2,
							"key": 1714879200000,
							"key_as_string": "2024-05-05T05:20:00.000+02:00"
						},
						{
							"1": {
								"value": 6.0
							},
							"1-metric": {
								"value": 10.0
							},
							"doc_count": 6,
							"key": 1714879800000,
							"key_as_string": "2024-05-05T05:30:00.000+02:00"
						},
						{
							"1": {
								"value": 2.0
							},
							"1-metric": {
								"value": 12.0
							},
							"doc_count": 2,
							"key": 1714880400000,
							"key_as_string": "2024-05-05T05:40:00.000+02:00"
						},
						{
							"1": {
								"value": 2.0
							},
							"1-metric": {
								"value": 14.0
							},
							"doc_count": 2,
							"key": 1714881000000,
							"key_as_string": "2024-05-05T05:50:00.000+02:00"
						},
						{
							"1": {
								"value": null
							},
							"1-metric": {
								"value": 14.0
							},
							"doc_count": 0,
							"key": 1714881600000,
							"key_as_string": "2024-05-05T06:00:00.000+02:00"
						},
						{
							"1": {
								"value": null
							},
							"1-metric": {
								"value": 16.0
							},
							"doc_count": 2,
							"key": 1714882200000,
							"key_as_string": "2024-05-05T06:10:00.000+02:00"
						},
						{
							"1": {
								"value": null
							},
							"1-metric": {
								"value": 16.0
							},
							"doc_count": 0,
							"key": 1714882800000,
							"key_as_string": "2024-05-05T06:20:00.000+02:00"
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
		ExpectedResults: [][]model.QueryResultRow{
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol("hits", uint64(1974))}}},
			{}, // NoDBQuery
			{}, // NoDBQuery
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("toInt64(toUnixTimestamp64Milli(`timestamp`)/600000)", 0.0),
					model.NewQueryResultCol("count()", 282),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("toInt64(toUnixTimestamp64Milli(`timestamp`)/600000)", 1.0),
					model.NewQueryResultCol("count()", 300),
				}},
			},
		},
		ExpectedSQLs: []string{
			`SELECT count() FROM ` + testdata.QuotedTableName + ` `,
			`NoDBQuery`,
			`NoDBQuery`,
			"SELECT toInt64(toUnixTimestamp64Milli(`timestamp`)/600000), count() " +
				`FROM ` + testdata.QuotedTableName + `  ` +
				"GROUP BY (toInt64(toUnixTimestamp64Milli(`timestamp`)/600000)) " +
				"ORDER BY (toInt64(toUnixTimestamp64Milli(`timestamp`)/600000))",
		},
	},
}
