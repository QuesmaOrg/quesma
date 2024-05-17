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
				`WHERE "order_date">=parseDateTime64BestEffort('2024-01-24T11:23:10.802Z') AND "order_date"<=parseDateTime64BestEffort('2024-05-08T10:23:10.802Z')`,
			`NoDBQuery`,
			`SELECT "day_of_week_i", count() ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`WHERE "order_date">=parseDateTime64BestEffort('2024-01-24T11:23:10.802Z') AND "order_date"<=parseDateTime64BestEffort('2024-05-08T10:23:10.802Z') ` +
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
			`SELECT count() FROM ` + testdata.QuotedTableName,
			`NoDBQuery`,
			`SELECT "day_of_week_i", avgOrNull("day_of_week_i") ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`GROUP BY ("day_of_week_i") ` +
				`ORDER BY ("day_of_week_i")`,
			`SELECT "day_of_week_i", count() ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
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
			`SELECT count() FROM ` + testdata.QuotedTableName,
			`NoDBQuery`,
			`NoDBQuery`,
			`SELECT "day_of_week_i", count() ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
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
			`SELECT count() FROM ` + testdata.QuotedTableName,
			`NoDBQuery`,
			`NoDBQuery`,
			`SELECT "day_of_week_i", maxOrNull("products.base_price") ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`GROUP BY ("day_of_week_i") ` +
				`ORDER BY ("day_of_week_i")`,
			`SELECT "day_of_week_i", count() ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
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
				`FROM ` + testdata.QuotedTableName,
			`NoDBQuery`,
			`SELECT floor("bytes" / 200.000000) * 200.000000, count() ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`GROUP BY (floor("bytes" / 200.000000) * 200.000000) ` +
				`ORDER BY (floor("bytes" / 200.000000) * 200.000000)`,
		},
	},
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
								"1": {
									"value": null
								},
								"1-metric": {
									"value": 19.0
								},
								"doc_count": 1,
								"key": 1715196000000,
								"key_as_string": "2024-05-08T19:20:00.000"
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
								"key_as_string": "2024-05-08T19:30:00.000"
							},
							{
								"1": {
									"value": 1.0
								},
								"1-metric": {
									"value": 20.0
								},
								"doc_count": 1,
								"key": 1715198400000,
								"key_as_string": "2024-05-08T20:00:00.000"
							},
							{
								"1": {
									"value": 12.0
								},
								"1-metric": {
									"value": 32.0
								},
								"doc_count": 4,
								"key": 1715199000000,
								"key_as_string": "2024-05-08T20:10:00.000"
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
								"key_as_string": "2024-05-08T20:20:00.000"
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
					model.NewQueryResultCol("toInt64(toUnixTimestamp64Milli(`timestamp`)/600000)", int64(1715196000000/600000)),
					model.NewQueryResultCol("count()", 19.0),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("toInt64(toUnixTimestamp64Milli(`timestamp`)/600000)", int64(1715196600000/600000)),
					model.NewQueryResultCol("count()", 19.0),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("toInt64(toUnixTimestamp64Milli(`timestamp`)/600000)", int64(1715198400000/600000)),
					model.NewQueryResultCol("count()", 20.0),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("toInt64(toUnixTimestamp64Milli(`timestamp`)/600000)", int64(1715199000000/600000)),
					model.NewQueryResultCol("count()", 32.0),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("toInt64(toUnixTimestamp64Milli(`timestamp`)/600000)", int64(1715199600000/600000)),
					model.NewQueryResultCol("count()", 27.0),
				}},
			},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("toInt64(toUnixTimestamp64Milli(`timestamp`)/600000)", int64(1715196000000/600000)),
					model.NewQueryResultCol("count()", 1),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("toInt64(toUnixTimestamp64Milli(`timestamp`)/600000)", int64(1715196600000/600000)),
					model.NewQueryResultCol("count()", 1),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("toInt64(toUnixTimestamp64Milli(`timestamp`)/600000)", int64(1715198400000/600000)),
					model.NewQueryResultCol("count()", 1),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("toInt64(toUnixTimestamp64Milli(`timestamp`)/600000)", int64(1715199000000/600000)),
					model.NewQueryResultCol("count()", 4),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("toInt64(toUnixTimestamp64Milli(`timestamp`)/600000)", int64(1715199600000/600000)),
					model.NewQueryResultCol("count()", 3),
				}},
			},
		},
		ExpectedSQLs: []string{
			`SELECT count() ` +
				`FROM ` + testdata.QuotedTableName,
			`NoDBQuery`,
			"SELECT toInt64(toUnixTimestamp64Milli(`timestamp`)/600000), " +
				"sumOrNull(toHour(`timestamp`)) " +
				"FROM " + testdata.QuotedTableName + " " +
				"GROUP BY (toInt64(toUnixTimestamp64Milli(`timestamp`)/600000)) " +
				"ORDER BY (toInt64(toUnixTimestamp64Milli(`timestamp`)/600000))",
			"SELECT toInt64(toUnixTimestamp64Milli(`timestamp`)/600000), " +
				"count() " +
				"FROM " + testdata.QuotedTableName + " " +
				"GROUP BY (toInt64(toUnixTimestamp64Milli(`timestamp`)/600000)) " +
				"ORDER BY (toInt64(toUnixTimestamp64Milli(`timestamp`)/600000))",
		},
	},
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
		ExpectedResponse: // I changed this a bit. Opensearch returns "1": {null} for 2nd, 3rd and 3 last buckets. I think it's not correct... I return 0, and it seems working too.
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
							"1": {
								"value": null
							},
							"1-metric": {
								"value": 2.0
							},
							"doc_count": 2,
							"key": 1714869000000,
							"key_as_string": "2024-05-05T00:30:00.000"
						},
						{
							"1": {
								"value": 0.0
							},
							"1-metric": {
								"value": 2.0
							},
							"doc_count": 0,
							"key": 1714869600000,
							"key_as_string": "2024-05-05T00:40:00.000"
						},
						{
							"1": {
								"value": 0.0
							},
							"1-metric": {
								"value": 2.0
							},
							"doc_count": 0,
							"key": 1714878600000,
							"key_as_string": "2024-05-05T03:10:00.000"
						},
						{
							"1": {
								"value": 2.0
							},
							"1-metric": {
								"value": 4.0
							},
							"doc_count": 2,
							"key": 1714879200000,
							"key_as_string": "2024-05-05T03:20:00.000"
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
							"key_as_string": "2024-05-05T03:30:00.000"
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
							"key_as_string": "2024-05-05T03:40:00.000"
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
							"key_as_string": "2024-05-05T03:50:00.000"
						},
						{
							"1": {
								"value": 0.0
							},
							"1-metric": {
								"value": 14.0
							},
							"doc_count": 0,
							"key": 1714881600000,
							"key_as_string": "2024-05-05T04:00:00.000"
						},
						{
							"1": {
								"value": 2.0
							},
							"1-metric": {
								"value": 16.0
							},
							"doc_count": 2,
							"key": 1714882200000,
							"key_as_string": "2024-05-05T04:10:00.000"
						},
						{
							"1": {
								"value": 0.0
							},
							"1-metric": {
								"value": 16.0
							},
							"doc_count": 0,
							"key": 1714882800000,
							"key_as_string": "2024-05-05T04:20:00.000"
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
					model.NewQueryResultCol("toInt64(toUnixTimestamp64Milli(`timestamp`)/600000)", int64(1714869000000/600000)),
					model.NewQueryResultCol("count()", 2),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("toInt64(toUnixTimestamp64Milli(`timestamp`)/600000)", int64(1714869600000/600000)),
					model.NewQueryResultCol("count()", 0),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("toInt64(toUnixTimestamp64Milli(`timestamp`)/600000)", int64(1714878600000/600000)),
					model.NewQueryResultCol("count()", 0),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("toInt64(toUnixTimestamp64Milli(`timestamp`)/600000)", int64(1714879200000/600000)),
					model.NewQueryResultCol("count()", 2),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("toInt64(toUnixTimestamp64Milli(`timestamp`)/600000)", int64(1714879800000/600000)),
					model.NewQueryResultCol("count()", 6),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("toInt64(toUnixTimestamp64Milli(`timestamp`)/600000)", int64(1714880400000/600000)),
					model.NewQueryResultCol("count()", 2),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("toInt64(toUnixTimestamp64Milli(`timestamp`)/600000)", int64(1714881000000/600000)),
					model.NewQueryResultCol("count()", 2),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("toInt64(toUnixTimestamp64Milli(`timestamp`)/600000)", int64(1714881600000/600000)),
					model.NewQueryResultCol("count()", 0),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("toInt64(toUnixTimestamp64Milli(`timestamp`)/600000)", int64(1714882200000/600000)),
					model.NewQueryResultCol("count()", 2),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("toInt64(toUnixTimestamp64Milli(`timestamp`)/600000)", int64(1714882800000/600000)),
					model.NewQueryResultCol("count()", 0),
				}},
			},
		},
		ExpectedSQLs: []string{
			`SELECT count() FROM ` + testdata.QuotedTableName,
			`NoDBQuery`,
			`NoDBQuery`,
			"SELECT toInt64(toUnixTimestamp64Milli(`timestamp`)/600000), count() " +
				`FROM ` + testdata.QuotedTableName + ` ` +
				"GROUP BY (toInt64(toUnixTimestamp64Milli(`timestamp`)/600000)) " +
				"ORDER BY (toInt64(toUnixTimestamp64Milli(`timestamp`)/600000))",
		},
	},
	{ // [7]
		TestName: "Simplest avg_bucket. Reproduce: Visualize -> Vertical Bar: Metrics: Average Bucket (Bucket: Date Histogram, Metric: Count)",
		QueryRequestJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"1": {
					"avg_bucket": {
						"buckets_path": "1-bucket>_count"
					}
				},
				"1-bucket": {
					"date_histogram": {
						"field": "timestamp",
						"fixed_interval": "10m",
						"min_doc_count": 1,
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
					"must": [],
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
				"1": {
					"value": 1.3333333333333333
				},
				"1-bucket": {
					"buckets": [
						{
							"doc_count": 1,
							"key": 1715403000000,
							"key_as_string": "2024-05-11T04:50:00.000"
						},
						{
							"doc_count": 2,
							"key": 1715403600000,
							"key_as_string": "2024-05-11T05:00:00.000"
						},
						{
							"doc_count": 1,
							"key": 1715404200000,
							"key_as_string": "2024-05-11T05:10:00.000"
						}
					]
				}
			},
			"hits": {
				"hits": [],
				"max_score": null,
				"total": {
					"relation": "eq",
					"value": 207
				}
			},
			"timed_out": false,
			"took": 81
		}`,
		ExpectedResults: [][]model.QueryResultRow{
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol("hits", uint64(1974))}}},
			{}, // NoDBQuery
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("key", int64(1715403000000/600000)),
					model.NewQueryResultCol("doc_count", 1),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("key", int64(1715403600000/600000)),
					model.NewQueryResultCol("doc_count", 2),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("key", int64(1715404200000/600000)),
					model.NewQueryResultCol("doc_count", 1),
				}},
			},
		},
		ExpectedSQLs: []string{
			`SELECT count() ` +
				`FROM ` + testdata.QuotedTableName,
			`NoDBQuery`,
			"SELECT toInt64(toUnixTimestamp64Milli(`timestamp`)/600000), count() " +
				`FROM ` + testdata.QuotedTableName + ` ` +
				"GROUP BY (toInt64(toUnixTimestamp64Milli(`timestamp`)/600000)) " +
				"ORDER BY (toInt64(toUnixTimestamp64Milli(`timestamp`)/600000))",
		},
	},
	{ // [8]
		TestName: "avg_bucket. Reproduce: Visualize -> Vertical Bar: Metrics: Average Bucket (Bucket: Date Histogram, Metric: Max)",
		QueryRequestJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"1": {
					"avg_bucket": {
						"buckets_path": "1-bucket>1-metric"
					}
				},
				"1-bucket": {
					"aggs": {
						"1-metric": {
							"max": {
								"field": "bytes"
							}
						}
					},
					"date_histogram": {
						"field": "timestamp",
						"fixed_interval": "10m",
						"min_doc_count": 1,
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
					"must": [],
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
				"1": {
					"value": 8835.6666666666667
				},
				"1-bucket": {
					"buckets": [
						{
							"1-metric": {
								"value": 8047.0
							},
							"doc_count": 1,
							"key": 1715403000000,
							"key_as_string": "2024-05-11T04:50:00.000"
						},
						{
							"1-metric": {
								"value": 9261.0
							},
							"doc_count": 4,
							"key": 1715413800000,
							"key_as_string": "2024-05-11T07:50:00.000"
						},
						{
							"1-metric": {
								"value": 9199.0
							},
							"doc_count": 2,
							"key": 1715414400000,
							"key_as_string": "2024-05-11T08:00:00.000"
						}
					]
				}
			},
			"hits": {
				"hits": [],
				"max_score": null,
				"total": {
					"relation": "eq",
					"value": 207
				}
			},
			"timed_out": false,
			"took": 121
		}`,
		ExpectedResults: [][]model.QueryResultRow{
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol("hits", uint64(207))}}},
			{}, // NoDBQuery
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("key", int64(1715403000000/600000)),
					model.NewQueryResultCol("doc_count", 8047.0),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("key", int64(1715413800000/600000)),
					model.NewQueryResultCol("doc_count", 9261.0),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("key", int64(1715414400000/600000)),
					model.NewQueryResultCol("doc_count", 9199.0),
				}},
			},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("key", int64(1715403000000/600000)),
					model.NewQueryResultCol("doc_count", 1),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("key", int64(1715413800000/600000)),
					model.NewQueryResultCol("doc_count", 4),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("key", int64(1715414400000/600000)),
					model.NewQueryResultCol("doc_count", 2),
				}},
			},
		},
		ExpectedSQLs: []string{
			`SELECT count() ` +
				`FROM ` + testdata.QuotedTableName,
			`NoDBQuery`,
			"SELECT toInt64(toUnixTimestamp64Milli(`timestamp`)/600000), " + `maxOrNull("bytes") ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				"GROUP BY (toInt64(toUnixTimestamp64Milli(`timestamp`)/600000)) " +
				"ORDER BY (toInt64(toUnixTimestamp64Milli(`timestamp`)/600000))",
			"SELECT toInt64(toUnixTimestamp64Milli(`timestamp`)/600000), count() " +
				`FROM ` + testdata.QuotedTableName + ` ` +
				"GROUP BY (toInt64(toUnixTimestamp64Milli(`timestamp`)/600000)) " +
				"ORDER BY (toInt64(toUnixTimestamp64Milli(`timestamp`)/600000))",
		},
	},
	/* TODO need fix for date_range and subaggregations. Same one, as already merged ~1-2 weeks ago for range. It's WIP.
	{ // [9]
		TestName: "avg_bucket. Reproduce: Visualize -> Vertical Bar: Metrics: Average Bucket (Bucket: Date Range, Metric: Average), Buckets: X-Asis: Range",
		QueryRequestJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"2": {
					"aggs": {
						"1": {
							"avg_bucket": {
								"buckets_path": "1-bucket>1-metric"
							}
						},
						"1-bucket": {
							"aggs": {
								"1-metric": {
									"avg": {
										"field": "bytes"
									}
								}
							},
							"date_range": {
								"field": "timestamp",
								"ranges": [
									{
										"from": "now-1w/w",
										"to": "now"
									},
									{
										"to": "now"
									}
								],
								"time_zone": "Europe/Warsaw"
							}
						}
					},
					"range": {
						"keyed": true,
						"ranges": [
							{
								"from": 3,
								"to": 1000
							},
							{
								"from": 2,
								"to": 5
							}
						],
						"field": "dayOfWeek"
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
					"filter": [
						{
							"range": {
								"timestamp": {
									"format": "strict_date_optional_time",
									"gte": "2024-05-11T01:55:02.236Z",
									"lte": "2024-05-11T16:55:02.236Z"
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
					"buckets": {
						"2.0-5.0": {
							"1": {
								"value": 8047.0
							},
							"1-bucket": {
								"buckets": [
									{
										"1-metric": {
											"value": 8047.0
										},
										"doc_count": 1,
										"key": "*-2024-05-11T18:55:02.344+02:00",
										"to": 1715446502344.0,
										"to_as_string": "2024-05-11T18:55:02.344+02:00"
									},
									{
										"1-metric": {
											"value": 8047.0
										},
										"doc_count": 1,
										"from": 1714341600000.0,
										"from_as_string": "2024-04-29T00:00:00.000+02:00",
										"key": "2024-04-29T00:00:00.000+02:00-2024-05-11T18:55:02.344+02:00",
										"to": 1715446502344.0,
										"to_as_string": "2024-05-11T18:55:02.344+02:00"
									}
								]
							},
							"doc_count": 1,
							"from": 2.0,
							"to": 5.0
						},
						"3.0-1000.0": {
							"1": {
								"value": 5273.850241545893
							},
							"1-bucket": {
								"buckets": [
									{
										"1-metric": {
											"value": 5273.850241545893
										},
										"doc_count": 207,
										"key": "*-2024-05-11T18:55:02.344+02:00",
										"to": 1715446502344.0,
										"to_as_string": "2024-05-11T18:55:02.344+02:00"
									},
									{
										"1-metric": {
											"value": 5273.850241545893
										},
										"doc_count": 207,
										"from": 1714341600000.0,
										"from_as_string": "2024-04-29T00:00:00.000+02:00",
										"key": "2024-04-29T00:00:00.000+02:00-2024-05-11T18:55:02.344+02:00",
										"to": 1715446502344.0,
										"to_as_string": "2024-05-11T18:55:02.344+02:00"
									}
								]
							},
							"doc_count": 207,
							"from": 3.0,
							"to": 1000.0
						}
					}
				}
			},
			"hits": {
				"hits": [],
				"max_score": null,
				"total": {
					"relation": "eq",
					"value": 207
				}
			},
			"timed_out": false,
			"took": 28
		}`,
		ExpectedResults: [][]model.QueryResultRow{
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol("hits", uint64(207))}}},
			{}, // NoDBQuery
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("1", 1),
					model.NewQueryResultCol("2", int64(1714341600000)),
					model.NewQueryResultCol("3", int64(1715446502344)),
					model.NewQueryResultCol("4", 1),
					model.NewQueryResultCol("5", int64(1715446502344)),
					model.NewQueryResultCol(`avgOrNull("bytes")`, 8047.0),
				}},
			},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("1", 1),
					model.NewQueryResultCol("2", int64(1714341600000)),
					model.NewQueryResultCol("3", int64(1715446502344)),
					model.NewQueryResultCol("4", 1),
					model.NewQueryResultCol("5", int64(1715446502344)),
					model.NewQueryResultCol(`count()`, 1),
				}},
			},
			{}, // NoDBQuery
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("1", 207),
					model.NewQueryResultCol("2", int64(1714341600000)),
					model.NewQueryResultCol("3", int64(1715446502344)),
					model.NewQueryResultCol("4", 207),
					model.NewQueryResultCol("5", int64(1715446502344)),
					model.NewQueryResultCol(`avgOrNull("bytes")`, 5273.850241545893),
				}},
			},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("1", 207),
					model.NewQueryResultCol("2", int64(1714341600000)),
					model.NewQueryResultCol("3", int64(1715446502344)),
					model.NewQueryResultCol("4", 207),
					model.NewQueryResultCol("5", int64(1715446502344)),
					model.NewQueryResultCol(`count()`, 207),
				}},
			},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("1", 207),
					model.NewQueryResultCol("2", 1),
					model.NewQueryResultCol("3", 207),
				}},
			},
		},
		ExpectedSQLs: []string{
			`SELECT count() ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`WHERE "timestamp">=parseDateTime64BestEffort('2024-05-11T01:55:02.236Z') ` +
				`AND "timestamp"<=parseDateTime64BestEffort('2024-05-11T16:55:02.236Z') `,
			`NoDBQuery`,
			`SELECT count(if("timestamp" >= toStartOfWeek(subDate(now(), INTERVAL 1 week)) AND "timestamp" < now(), 1, NULL)), ` +
				`toInt64(toUnixTimestamp(toStartOfWeek(subDate(now(), INTERVAL 1 week)))), ` +
				`toInt64(toUnixTimestamp(now())), ` +
				`count(if("timestamp" < now(), 1, NULL)), ` +
				`toInt64(toUnixTimestamp(now())), ` +
				`avgOrNull("bytes") ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`WHERE ("timestamp">=parseDateTime64BestEffort('2024-05-11T01:55:02.236Z') ` +
				`AND "timestamp"<=parseDateTime64BestEffort('2024-05-11T16:55:02.236Z')) ` +
				`AND "dayOfWeek">=2 AND "dayOfWeek"<5 `,
			`SELECT count(if("timestamp" >= toStartOfWeek(subDate(now(), INTERVAL 1 week)) AND "timestamp" < now(), 1, NULL)), ` +
				`toInt64(toUnixTimestamp(toStartOfWeek(subDate(now(), INTERVAL 1 week)))), ` +
				`toInt64(toUnixTimestamp(now())), ` +
				`count(if("timestamp" < now(), 1, NULL)), ` +
				`toInt64(toUnixTimestamp(now())), ` +
				`count() ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`WHERE ("timestamp">=parseDateTime64BestEffort('2024-05-11T01:55:02.236Z') ` +
				`AND "timestamp"<=parseDateTime64BestEffort('2024-05-11T16:55:02.236Z')) ` +
				`AND "dayOfWeek">=2 AND "dayOfWeek"<5 `,
			`NoDBQuery`,
			`SELECT count(if("timestamp" >= toStartOfWeek(subDate(now(), INTERVAL 1 week)) AND "timestamp" < now(), 1, NULL)), ` +
				`toInt64(toUnixTimestamp(toStartOfWeek(subDate(now(), INTERVAL 1 week)))), ` +
				`toInt64(toUnixTimestamp(now())), ` +
				`count(if("timestamp" < now(), 1, NULL)), toInt64(toUnixTimestamp(now())), ` +
				`avgOrNull("bytes") ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`WHERE ("timestamp"<=parseDateTime64BestEffort('2024-05-11T16:55:02.236Z') ` +
				`AND "timestamp">=parseDateTime64BestEffort('2024-05-11T01:55:02.236Z')) ` +
				`AND "dayOfWeek">=3 AND "dayOfWeek"<1000 `,
			`SELECT count(if("timestamp" >= toStartOfWeek(subDate(now(), INTERVAL 1 week)) AND "timestamp" < now(), 1, NULL)), ` +
				`toInt64(toUnixTimestamp(toStartOfWeek(subDate(now(), INTERVAL 1 week)))), ` +
				`toInt64(toUnixTimestamp(now())), count(if("timestamp" < now(), 1, NULL)), ` +
				`toInt64(toUnixTimestamp(now())), ` +
				`count() ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`WHERE ("timestamp">=parseDateTime64BestEffort('2024-05-11T01:55:02.236Z') ` +
				`AND "timestamp"<=parseDateTime64BestEffort('2024-05-11T16:55:02.236Z')) ` +
				`AND "dayOfWeek">=3 AND "dayOfWeek"<1000 `,
			`SELECT count(if("dayOfWeek">=3 AND "dayOfWeek"<1000, 1, NULL)), ` +
				`count(if("dayOfWeek">=2 AND "dayOfWeek"<5, 1, NULL)), ` +
				`count() ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`WHERE "timestamp">=parseDateTime64BestEffort('2024-05-11T01:55:02.236Z') ` +
				`AND "timestamp"<=parseDateTime64BestEffort('2024-05-11T16:55:02.236Z') `,
		},
	},
	*/
	{ // [10]
		TestName: "avg_bucket. Reproduce: Visualize -> Horizontal Bar: Metrics: Average Bucket (Bucket: Histogram, Metric: Count), Buckets: X-Asis: Date Histogram",
		QueryRequestJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"2": {
					"aggs": {
						"1": {
							"avg_bucket": {
								"buckets_path": "1-bucket>_count"
							}
						},
						"1-bucket": {
							"histogram": {
								"field": "bytes",
								"interval": 1,
								"min_doc_count": 1
							}
						}
					},
					"date_histogram": {
						"field": "timestamp",
						"fixed_interval": "10m",
						"min_doc_count": 1,
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
			"query": {},
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
							"1": {
								"value": 1.0
							},
							"1-bucket": {
								"buckets": [
									{
										"doc_count": 1,
										"key": 4202.0
									}
								]
							},
							"doc_count": 1,
							"key": 1715818800000,
							"key_as_string": "2024-05-16T00:20:00.000"
						},
						{
							"1": {
								"value": 2.0
							},
							"1-bucket": {
								"buckets": [
									{
										"doc_count": 1,
										"key": 0.0
									},
									{
										"doc_count": 2,
										"key": 293.0
									},
									{
										"doc_count": 3,
										"key": 1997.0
									}
								]
							},
							"doc_count": 9,
							"key": 1715863800000,
							"key_as_string": "2024-05-16T12:50:00.000"
						}
					]
				}
			},
			"hits": {
				"hits": [],
				"max_score": null,
				"total": {
					"relation": "eq",
					"value": 141
				}
			},
			"timed_out": false,
			"took": 60
		}`,
		ExpectedResults: [][]model.QueryResultRow{
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol("hits", uint64(141))}}},
			{}, // NoDBQuery
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("key", int64(1715818800000/600000)),
					model.NewQueryResultCol("bytes", 4202.0),
					model.NewQueryResultCol("doc_count", 1),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("key", int64(1715863800000/600000)),
					model.NewQueryResultCol("bytes", 0.0),
					model.NewQueryResultCol("doc_count", 1),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("key", int64(1715863800000/600000)),
					model.NewQueryResultCol("bytes", 293.0),
					model.NewQueryResultCol("doc_count", 2),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("key", int64(1715863800000/600000)),
					model.NewQueryResultCol("bytes", 1997.0),
					model.NewQueryResultCol("doc_count", 3),
				}},
			},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("key", int64(1715818800000/600000)),
					model.NewQueryResultCol("doc_count", 1),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("key", int64(1715863800000/600000)),
					model.NewQueryResultCol("doc_count", 9),
				}},
			},
		},
		ExpectedSQLs: []string{
			`SELECT count() ` +
				`FROM ` + testdata.QuotedTableName,
			`NoDBQuery`,
			"SELECT toInt64(toUnixTimestamp64Milli(`timestamp`)/600000), " + `"bytes", count() ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				"GROUP BY (toInt64(toUnixTimestamp64Milli(`timestamp`)/600000), " + `"bytes") ` +
				"ORDER BY (toInt64(toUnixTimestamp64Milli(`timestamp`)/600000), " + `"bytes")`,
			"SELECT toInt64(toUnixTimestamp64Milli(`timestamp`)/600000), count() " +
				`FROM ` + testdata.QuotedTableName + ` ` +
				"GROUP BY (toInt64(toUnixTimestamp64Milli(`timestamp`)/600000)) " +
				"ORDER BY (toInt64(toUnixTimestamp64Milli(`timestamp`)/600000))",
		},
	},
	{ // [11]
		TestName: "Simplest min_bucket. Reproduce: Visualize -> Vertical Bar: Metrics: Min Bucket (Bucket: Terms, Metric: Count)",
		QueryRequestJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"1": {
					"min_bucket": {
						"buckets_path": "1-bucket>_count"
					}
				},
				"1-bucket": {
					"terms": {
						"field": "clientip",
						"order": {
							"_key": "desc"
						},
						"size": 5
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
					"filter": [
						{
							"range": {
								"timestamp": {
									"format": "strict_date_optional_time",
									"gte": "2024-05-11T07:40:13.606Z",
									"lte": "2024-05-11T22:40:13.606Z"
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
				"1": {
					"keys": [
						"252.102.14.111",
						"250.85.17.229",
						"249.69.222.185",
						"247.126.133.102"
					],
					"value": 1.0
				},
				"1-bucket": {
					"buckets": [
						{
							"doc_count": 1,
							"key": "252.102.14.111"
						},
						{
							"doc_count": 1,
							"key": "250.85.17.229"
						},
						{
							"doc_count": 1,
							"key": "249.69.222.185"
						},
						{
							"doc_count": 3,
							"key": "247.240.202.244"
						},
						{
							"doc_count": 1,
							"key": "247.126.133.102"
						}
					],
					"doc_count_error_upper_bound": 0,
					"sum_other_doc_count": 195
				}
			},
			"hits": {
				"hits": [],
				"max_score": null,
				"total": {
					"relation": "eq",
					"value": 202
				}
			},
			"timed_out": false,
			"took": 32
		}`,
		ExpectedResults: [][]model.QueryResultRow{
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol("hits", uint64(202))}}},
			{}, // NoDBQuery
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("key", "252.102.14.111"),
					model.NewQueryResultCol("doc_count", 1),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("key", "250.85.17.229"),
					model.NewQueryResultCol("doc_count", 1),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("key", "249.69.222.185"),
					model.NewQueryResultCol("doc_count", 1),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("key", "247.240.202.244"),
					model.NewQueryResultCol("doc_count", 3),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("key", "247.126.133.102"),
					model.NewQueryResultCol("doc_count", 1),
				}},
			},
		},
		ExpectedSQLs: []string{
			`SELECT count() ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`WHERE "timestamp">=parseDateTime64BestEffort('2024-05-11T07:40:13.606Z') AND ` +
				`"timestamp"<=parseDateTime64BestEffort('2024-05-11T22:40:13.606Z')`,
			`NoDBQuery`,
			`SELECT "clientip", count() ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`WHERE "timestamp"<=parseDateTime64BestEffort('2024-05-11T22:40:13.606Z') ` +
				`AND "timestamp">=parseDateTime64BestEffort('2024-05-11T07:40:13.606Z') ` +
				`GROUP BY ("clientip") ` +
				`ORDER BY count() DESC ` +
				`LIMIT 5`,
		},
	},
	{ // [12]
		TestName: "min_bucket. Reproduce: Visualize -> Vertical Bar: Metrics: Min Bucket (Bucket: Terms, Metric: Unique Count)",
		QueryRequestJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"1": {
					"min_bucket": {
						"buckets_path": "1-bucket>1-metric"
					}
				},
				"1-bucket": {
					"aggs": {
						"1-metric": {
							"cardinality": {
								"field": "geo.coordinates"
							}
						}
					},
					"terms": {
						"field": "clientip",
						"order": {
							"_key": "desc"
						},
						"size": 5
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
					"must": [],
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
				"1": {
					"keys": [
						"255.205.14.152",
						"255.174.89.45",
						"253.69.5.67",
						"252.177.62.191",
						"251.250.144.158"
					],
					"value": 1.0
				},
				"1-bucket": {
					"buckets": [
						{
							"1-metric": {
								"value": 1
							},
							"doc_count": 1,
							"key": "255.205.14.152"
						},
						{
							"1-metric": {
								"value": 1
							},
							"doc_count": 1,
							"key": "255.174.89.45"
						},
						{
							"1-metric": {
								"value": 1
							},
							"doc_count": 1,
							"key": "253.69.5.67"
						},
						{
							"1-metric": {
								"value": 1
							},
							"doc_count": 1,
							"key": "252.177.62.191"
						},
						{
							"1-metric": {
								"value": 1
							},
							"doc_count": 1,
							"key": "251.250.144.158"
						}
					],
					"doc_count_error_upper_bound": 0,
					"sum_other_doc_count": 194
				}
			},
			"hits": {
				"hits": [],
				"max_score": null,
				"total": {
					"relation": "eq",
					"value": 199
				}
			},
			"timed_out": false,
			"took": 17
		}`,
		ExpectedResults: [][]model.QueryResultRow{
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol("hits", uint64(199))}}},
			{}, // NoDBQuery
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("key", "255.205.14.152"),
					model.NewQueryResultCol("doc_count", 1),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("key", "255.174.89.45"),
					model.NewQueryResultCol("doc_count", 1),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("key", "253.69.5.67"),
					model.NewQueryResultCol("doc_count", 1),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("key", "252.177.62.191"),
					model.NewQueryResultCol("doc_count", 1),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("key", "251.250.144.158"),
					model.NewQueryResultCol("doc_count", 1),
				}},
			},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("key", "255.205.14.152"),
					model.NewQueryResultCol("doc_count", 1),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("key", "255.174.89.45"),
					model.NewQueryResultCol("doc_count", 1),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("key", "253.69.5.67"),
					model.NewQueryResultCol("doc_count", 1),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("key", "252.177.62.191"),
					model.NewQueryResultCol("doc_count", 1),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("key", "251.250.144.158"),
					model.NewQueryResultCol("doc_count", 1),
				}},
			},
		},
		ExpectedSQLs: []string{
			`SELECT count() ` +
				`FROM ` + testdata.QuotedTableName,
			`NoDBQuery`,
			`SELECT "clientip", COUNT(DISTINCT "geo.coordinates") ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`GROUP BY ("clientip") ` +
				`ORDER BY ("clientip")`,
			`SELECT "clientip", count() ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`GROUP BY ("clientip") ` +
				`ORDER BY ("clientip")`,
		},
	},
	{ // [13]
		TestName: "complex min_bucket. Reproduce: Visualize -> Vertical Bar: Metrics: Min Bucket (Bucket: Terms, Metric: Sum), Buckets: Split Series: Histogram",
		QueryRequestJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"2": {
					"aggs": {
						"1": {
							"min_bucket": {
								"buckets_path": "1-bucket>1-metric"
							}
						},
						"1-bucket": {
							"aggs": {
								"1-metric": {
									"sum": {
										"field": "bytes"
									}
								}
							},
							"terms": {
								"field": "clientip",
								"order": {
									"_key": "desc"
								},
								"size": 2
							}
						}
					},
					"histogram": {
						"field": "bytes",
						"interval": 200,
						"min_doc_count": 1
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
							"1": {
								"keys": [
									"252.177.62.191"
								],
								"value": 7.0
							},
							"1-bucket": {
								"buckets": [
									{
										"1-metric": {
											"value": 13.0
										},
										"doc_count": 1,
										"key": "255.205.14.152"
									},
									{
										"1-metric": {
											"value": 7.0
										},
										"doc_count": 1,
										"key": "252.177.62.191"
									}
								],
								"doc_count_error_upper_bound": 0,
								"sum_other_doc_count": 68
							},
							"doc_count": 73,
							"key": 0.0
						},
						{
							"1": {
								"keys": [
									"246.106.125.113"
								],
								"value": 7.0
							},
							"1-bucket": {
								"buckets": [
									{
										"1-metric": {
											"value": 7.0
										},
										"doc_count": 1,
										"key": "246.106.125.113"
									},
									{
										"1-metric": {
											"value": 18.0
										},
										"doc_count": 1,
										"key": "236.212.255.77"
									}
								],
								"doc_count_error_upper_bound": 0,
								"sum_other_doc_count": 20
							},
							"doc_count": 25,
							"key": 200.0
						}
					]
				}
			},
			"hits": {
				"hits": [],
				"max_score": null,
				"total": {
					"relation": "eq",
					"value": 1838
				}
			},
			"timed_out": false,
			"took": 244
		}`,
		ExpectedResults: [][]model.QueryResultRow{
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol("hits", uint64(1838))}}},
			{}, // NoDBQuery
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol(`floor("bytes" / 200.000000) * 200.000000`, 0.0),
					model.NewQueryResultCol("client_ip", "255.205.14.152"),
					model.NewQueryResultCol(`sumOrNull("bytes")`, 13.0),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol(`floor("bytes" / 200.000000) * 200.000000`, 0.0),
					model.NewQueryResultCol("client_ip", "252.177.62.191"),
					model.NewQueryResultCol(`sumOrNull("bytes")`, 7.0),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol(`floor("bytes" / 200.000000) * 200.000000`, 200.0),
					model.NewQueryResultCol("client_ip", "246.106.125.113"),
					model.NewQueryResultCol(`sumOrNull("bytes")`, 7.0),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol(`floor("bytes" / 200.000000) * 200.000000`, 200.0),
					model.NewQueryResultCol("client_ip", "236.212.255.77"),
					model.NewQueryResultCol(`sumOrNull("bytes")`, 18.0),
				}},
			},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol(`floor("bytes" / 200.000000) * 200.000000`, 0.0),
					model.NewQueryResultCol("client_ip", "255.205.14.152"),
					model.NewQueryResultCol(`count()`, 1),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol(`floor("bytes" / 200.000000) * 200.000000`, 0.0),
					model.NewQueryResultCol("client_ip", "252.177.62.191"),
					model.NewQueryResultCol(`count()`, 1),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol(`floor("bytes" / 200.000000) * 200.000000`, 200.0),
					model.NewQueryResultCol("client_ip", "246.106.125.113"),
					model.NewQueryResultCol(`count()`, 1),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol(`floor("bytes" / 200.000000) * 200.000000`, 200.0),
					model.NewQueryResultCol("client_ip", "236.212.255.77"),
					model.NewQueryResultCol(`count()`, 1),
				}},
			},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol(`floor("bytes" / 200.000000) * 200.000000`, 0.0),
					model.NewQueryResultCol(`count()`, 73),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol(`floor("bytes" / 200.000000) * 200.000000`, 200.0),
					model.NewQueryResultCol(`count()`, 25),
				}},
			},
		},
		ExpectedSQLs: []string{
			`SELECT count() ` +
				`FROM ` + testdata.QuotedTableName,
			`NoDBQuery`,
			`SELECT floor("bytes" / 200.000000) * 200.000000, "clientip", sumOrNull("bytes") ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`GROUP BY (floor("bytes" / 200.000000) * 200.000000, "clientip") ` +
				`ORDER BY (floor("bytes" / 200.000000) * 200.000000, "clientip")`,
			`SELECT floor("bytes" / 200.000000) * 200.000000, "clientip", count() ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`GROUP BY (floor("bytes" / 200.000000) * 200.000000, "clientip") ` +
				`ORDER BY (floor("bytes" / 200.000000) * 200.000000, "clientip")`,
			`SELECT floor("bytes" / 200.000000) * 200.000000, count() ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`GROUP BY (floor("bytes" / 200.000000) * 200.000000) ` +
				`ORDER BY (floor("bytes" / 200.000000) * 200.000000)`,
		},
	},
	{ // [14]
		TestName: "Simplest max_bucket. Reproduce: Visualize -> Line: Metrics: Max Bucket (Bucket: Terms, Metric: Count)",
		QueryRequestJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"1": {
					"max_bucket": {
						"buckets_path": "1-bucket>_count"
					}
				},
				"1-bucket": {
					"terms": {
						"field": "Cancelled",
						"order": {
							"_key": "desc"
						},
						"size": 5
					}
				}
			},
			"docvalue_fields": [
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
									"gte": "2024-04-27T21:56:51.264Z",
									"lte": "2024-05-12T21:56:51.264Z"
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
			"script_fields": {
				"hour_of_day": {
					"script": {
						"lang": "painless",
						"source": "doc['timestamp'].value.hourOfDay"
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
				"1": {
					"keys": [
						"false"
					],
					"value": 1923.0
				},
				"1-bucket": {
					"buckets": [
						{
							"doc_count": 260,
							"key": 1,
							"key_as_string": "true"
						},
						{
							"doc_count": 1923,
							"key": 0,
							"key_as_string": "false"
						}
					],
					"doc_count_error_upper_bound": 0,
					"sum_other_doc_count": 0
				}
			},
			"hits": {
				"hits": [],
				"max_score": null,
				"total": {
					"relation": "eq",
					"value": 2183
				}
			},
			"timed_out": false,
			"took": 98
		}`,
		ExpectedResults: [][]model.QueryResultRow{
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol("hits", uint64(202))}}},
			{}, // NoDBQuery
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("key", "252.102.14.111"),
					model.NewQueryResultCol("doc_count", 1),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("key", "250.85.17.229"),
					model.NewQueryResultCol("doc_count", 1),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("key", "249.69.222.185"),
					model.NewQueryResultCol("doc_count", 1),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("key", "247.240.202.244"),
					model.NewQueryResultCol("doc_count", 3),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("key", "247.126.133.102"),
					model.NewQueryResultCol("doc_count", 1),
				}},
			},
		},
		ExpectedSQLs: []string{
			`SELECT count() ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`WHERE "timestamp">=parseDateTime64BestEffort('2024-05-11T07:40:13.606Z') AND ` +
				`"timestamp"<=parseDateTime64BestEffort('2024-05-11T22:40:13.606Z') `,
			`NoDBQuery`,
			`SELECT "clientip", count() ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`WHERE "timestamp"<=parseDateTime64BestEffort('2024-05-11T22:40:13.606Z') ` +
				`AND "timestamp">=parseDateTime64BestEffort('2024-05-11T07:40:13.606Z')  ` +
				`GROUP BY ("clientip") ` +
				`ORDER BY ("clientip")`,
		},
	},
	{ // [15]
		TestName: "max_bucket. Reproduce: Visualize -> Line: Metrics: Max Bucket (Bucket: Filters, Metric: Sum)",
		QueryRequestJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"1": {
					"max_bucket": {
						"buckets_path": "1-bucket>1-metric"
					}
				},
				"1-bucket": {
					"aggs": {
						"1-metric": {
							"sum": {
								"field": "DistanceKilometers"
							}
						}
					},
					"filters": {
						"filters": {
							"FlightDelayMin > 100": {
								"bool": {
									"filter": [],
									"must": [
										{
											"query_string": {
												"analyze_wildcard": true,
												"query": "FlightDelayMin > 100",
												"time_zone": "Europe/Warsaw"
											}
										}
									],
									"must_not": [],
									"should": []
								}
							},
							"true": {
								"bool": {
									"filter": [
										{
											"multi_match": {
												"lenient": true,
												"query": true,
												"type": "best_fields"
											}
										}
									],
									"must": [],
									"must_not": [],
									"should": []
								}
							}
						}
					}
				}
			},
			"docvalue_fields": [
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
									"gte": "2024-04-27T21:59:22.454Z",
									"lte": "2024-05-12T21:59:22.454Z"
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
			"script_fields": {
				"hour_of_day": {
					"script": {
						"lang": "painless",
						"source": "doc['timestamp'].value.hourOfDay"
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
				"1": {
					"keys": [
						"true"
					],
					"value": 4968221.14887619
				},
				"1-bucket": {
					"buckets": {
						"FlightDelayMin > 100": {
							"1-metric": {
								"value": 0.0
							},
							"doc_count": 0
						},
						"true": {
							"1-metric": {
								"value": 4968221.14887619
							},
							"doc_count": 722
						}
					}
				}
			},
			"hits": {
				"hits": [],
				"max_score": null,
				"total": {
					"relation": "eq",
					"value": 2183
				}
			},
			"timed_out": false,
			"took": 189
		}`,
		ExpectedResults: [][]model.QueryResultRow{
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol("hits", uint64(199))}}},
			{}, // NoDBQuery
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("key", "255.205.14.152"),
					model.NewQueryResultCol("doc_count", 1),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("key", "255.174.89.45"),
					model.NewQueryResultCol("doc_count", 1),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("key", "253.69.5.67"),
					model.NewQueryResultCol("doc_count", 1),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("key", "252.177.62.191"),
					model.NewQueryResultCol("doc_count", 1),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("key", "251.250.144.158"),
					model.NewQueryResultCol("doc_count", 1),
				}},
			},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("key", "255.205.14.152"),
					model.NewQueryResultCol("doc_count", 1),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("key", "255.174.89.45"),
					model.NewQueryResultCol("doc_count", 1),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("key", "253.69.5.67"),
					model.NewQueryResultCol("doc_count", 1),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("key", "252.177.62.191"),
					model.NewQueryResultCol("doc_count", 1),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("key", "251.250.144.158"),
					model.NewQueryResultCol("doc_count", 1),
				}},
			},
		},
		ExpectedSQLs: []string{
			`SELECT count() ` +
				`FROM ` + testdata.QuotedTableName + ` `,
			`NoDBQuery`,
			`SELECT "clientip", COUNT(DISTINCT "geo.coordinates") ` +
				`FROM ` + testdata.QuotedTableName + `  ` +
				`GROUP BY ("clientip") ` +
				`ORDER BY ("clientip")`,
			`SELECT "clientip", count() ` +
				`FROM ` + testdata.QuotedTableName + `  ` +
				`GROUP BY ("clientip") ` +
				`ORDER BY ("clientip")`,
		},
	},
	{ // [16]
		TestName: "complex max_bucket. Reproduce: Visualize -> Line: Metrics: Max Bucket (Bucket: Filters, Metric: Sum), Buckets: Split chart: Rows -> Range",
		QueryRequestJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"2": {
					"aggs": {
						"1": {
							"max_bucket": {
								"buckets_path": "1-bucket>1-metric"
							}
						},
						"1-bucket": {
							"aggs": {
								"1-metric": {
									"sum": {
										"field": "DistanceKilometers"
									}
								}
							},
							"filters": {
								"filters": {
									"FlightDelayMin > 100": {
										"bool": {
											"filter": [],
											"must": [
												{
													"query_string": {
														"analyze_wildcard": true,
														"query": "FlightDelayMin > 100",
														"time_zone": "Europe/Warsaw"
													}
												}
											],
											"must_not": [],
											"should": []
										}
									},
									"true": {
										"bool": {
											"filter": [
												{
													"multi_match": {
														"lenient": true,
														"query": true,
														"type": "best_fields"
													}
												}
											],
											"must": [],
											"must_not": [],
											"should": []
										}
									}
								}
							}
						}
					},
					"range": {
						"field": "DistanceMiles",
						"keyed": true,
						"ranges": [
							{
								"from": 0,
								"to": 1000
							},
							{
								"from": 1000,
								"to": 2000
							}
						]
					}
				}
			},
			"docvalue_fields": [
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
									"gte": "2024-04-27T22:01:55.676Z",
									"lte": "2024-05-12T22:01:55.676Z"
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
			"script_fields": {
				"hour_of_day": {
					"script": {
						"lang": "painless",
						"source": "doc['timestamp'].value.hourOfDay"
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
					"buckets": {
						"0.0-1000.0": {
							"1": {
								"keys": [
									"true"
								],
								"value": 82682.96674728394
							},
							"1-bucket": {
								"buckets": {
									"FlightDelayMin > 100": {
										"1-metric": {
											"value": 0.0
										},
										"doc_count": 0
									},
									"true": {
										"1-metric": {
											"value": 82682.96674728394
										},
										"doc_count": 140
									}
								}
							},
							"doc_count": 419,
							"from": 0.0,
							"to": 1000.0
						},
						"1000.0-2000.0": {
							"1": {
								"keys": [
									"true"
								],
								"value": 140267.98315429688
							},
							"1-bucket": {
								"buckets": {
									"FlightDelayMin > 100": {
										"1-metric": {
											"value": 0.0
										},
										"doc_count": 0
									},
									"true": {
										"1-metric": {
											"value": 140267.98315429688
										},
										"doc_count": 62
									}
								}
							},
							"doc_count": 159,
							"from": 1000.0,
							"to": 2000.0
						}
					}
				}
			},
			"hits": {
				"hits": [],
				"max_score": null,
				"total": {
					"relation": "eq",
					"value": 2184
				}
			},
			"timed_out": false,
			"took": 78
		}`,
		ExpectedResults: [][]model.QueryResultRow{
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol("hits", uint64(1838))}}},
			{}, // NoDBQuery
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol(`floor("bytes" / 200.000000) * 200.000000`, 0.0),
					model.NewQueryResultCol("client_ip", "255.205.14.152"),
					model.NewQueryResultCol(`sumOrNull("bytes")`, 13.0),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol(`floor("bytes" / 200.000000) * 200.000000`, 0.0),
					model.NewQueryResultCol("client_ip", "252.177.62.191"),
					model.NewQueryResultCol(`sumOrNull("bytes")`, 7.0),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol(`floor("bytes" / 200.000000) * 200.000000`, 200.0),
					model.NewQueryResultCol("client_ip", "246.106.125.113"),
					model.NewQueryResultCol(`sumOrNull("bytes")`, 7.0),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol(`floor("bytes" / 200.000000) * 200.000000`, 200.0),
					model.NewQueryResultCol("client_ip", "236.212.255.77"),
					model.NewQueryResultCol(`sumOrNull("bytes")`, 18.0),
				}},
			},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol(`floor("bytes" / 200.000000) * 200.000000`, 0.0),
					model.NewQueryResultCol("client_ip", "255.205.14.152"),
					model.NewQueryResultCol(`count()`, 1),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol(`floor("bytes" / 200.000000) * 200.000000`, 0.0),
					model.NewQueryResultCol("client_ip", "252.177.62.191"),
					model.NewQueryResultCol(`count()`, 1),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol(`floor("bytes" / 200.000000) * 200.000000`, 200.0),
					model.NewQueryResultCol("client_ip", "246.106.125.113"),
					model.NewQueryResultCol(`count()`, 1),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol(`floor("bytes" / 200.000000) * 200.000000`, 200.0),
					model.NewQueryResultCol("client_ip", "236.212.255.77"),
					model.NewQueryResultCol(`count()`, 1),
				}},
			},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol(`floor("bytes" / 200.000000) * 200.000000`, 0.0),
					model.NewQueryResultCol(`count()`, 73),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol(`floor("bytes" / 200.000000) * 200.000000`, 200.0),
					model.NewQueryResultCol(`count()`, 25),
				}},
			},
		},
		ExpectedSQLs: []string{
			`SELECT count() ` +
				`FROM ` + testdata.QuotedTableName + ` `,
			`NoDBQuery`,
			`SELECT floor("bytes" / 200.000000) * 200.000000, "clientip", sumOrNull("bytes") ` +
				`FROM ` + testdata.QuotedTableName + `  ` +
				`GROUP BY (floor("bytes" / 200.000000) * 200.000000, "clientip") ` +
				`ORDER BY (floor("bytes" / 200.000000) * 200.000000, "clientip")`,
			`SELECT floor("bytes" / 200.000000) * 200.000000, "clientip", count() ` +
				`FROM ` + testdata.QuotedTableName + `  ` +
				`GROUP BY (floor("bytes" / 200.000000) * 200.000000, "clientip") ` +
				`ORDER BY (floor("bytes" / 200.000000) * 200.000000, "clientip")`,
			`SELECT floor("bytes" / 200.000000) * 200.000000, count() ` +
				`FROM ` + testdata.QuotedTableName + `  ` +
				`GROUP BY (floor("bytes" / 200.000000) * 200.000000) ` +
				`ORDER BY (floor("bytes" / 200.000000) * 200.000000)`,
		},
	},
}
