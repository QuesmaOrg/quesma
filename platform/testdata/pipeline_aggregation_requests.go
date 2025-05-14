// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package testdata

// FIXME I'll restore this tests very soon. I need to merge this PR + #63 first, as I need changes from both of them to do so.
var PipelineAggregationTests = []AggregationTestCase{}

/*
	{
		TestName: "pipeline simple count",
		QueryRequestJson: `
		{
			"aggs": {
				"q": {
					"aggs": {
						"time_buckets": {
							"aggs": {
								"count": {
									"bucket_script": {
										"buckets_path": "_count",
										"script": {
											"lang": "expression",
											"source": "_value"
										}
									}
								}
							},
							"date_histogram": {
								"extended_bounds": {
									"max": 1713978908651,
									"min": 1713978008651
								},
								"field": "@timestamp",
								"fixed_interval": "1h",
								"min_doc_count": 0,
								"time_zone": "Europe/Warsaw"
							},
							"meta": {
								"type": "time_buckets"
							}
						}
					},
					"filters": {
						"filters": {
							"*": {
								"query_string": {
									"query": "*"
								}
							}
						}
					},
					"meta": {
						"type": "split"
					}
				}
			},
			"query": {
				"bool": {
					"filter": {
						"bool": {
							"filter": [],
							"must": [],
							"must_not": [],
							"should": []
						}
					},
					"must": []
				}
			},
			"runtime_mappings": {},
			"size": 0,
			"timeout": "30000ms",
			"track_total_hits": true
		}`,
		ExpectedResponse: `
		{
			"is_partial": false,
			"is_running": false,
			"start_time_in_millis": 1713982993490,
			"expiration_time_in_millis": 1714414993490,
			"completion_time_in_millis": 1713982993639,
			"response": {
				"took": 149,
				"timed_out": false,
				"_shards": {
					"total": 1,
					"successful": 1,
					"skipped": 0,
					"failed": 0
				},
				"hits": {
					"total": {
						"value": 13014,
						"relation": "eq"
					},
					"max_score": null,
					"hits": []
				},
				"aggregations": {
					"q": {
						"meta": {
							"type": "split"
						},
						"buckets": {
							"*": {
								"doc_count": 1000,
								"time_buckets": {
									"meta": {
										"type": "time_buckets"
									},
									"buckets": [
										{
											"key_as_string": "2024-04-15T00:00:00.000",
											"key": 1713139200000,
											"doc_count": 10,
											"count": {
												"value": 10
											}
										},
										{
											"key_as_string": "2024-04-15T01:00:00.000",
											"key": 1713142800000,
											"doc_count": 0,
											"count": {
												"value": 0
											}
										},
										{
											"key_as_string": "2024-04-15T02:00:00.000",
											"key": 1713146400000,
											"doc_count": 0,
											"count": {
												"value": 0
											}
										},
										{
											"key_as_string": "2024-04-15T03:00:00.000",
											"key": 1713150000000,
											"doc_count": 9,
											"count": {
												"value": 9
											}
										}
									]
								}
							}
						}
					}
				}
			}
		}`,
		ExpectedResults: [][]model.QueryResultRow{
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol("hits", uint64(13014))}}},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("key", int64(1713139200000/1000/60/60)),
					model.NewQueryResultCol("doc_count", 10),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("key", int64(1713150000000/1000/60/60)),
					model.NewQueryResultCol("doc_count", 9),
				}},
			},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("key", int64(1713139200000/1000/60/60)),
					model.NewQueryResultCol("doc_count", 10),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("key", int64(1713150000000/1000/60/60)),
					model.NewQueryResultCol("doc_count", 9),
				}},
			},
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol("hits", uint64(1000))}}},
		},
		ExpectedSQLs: []string{
			`SELECT count() FROM ` + QuotedTableName + ` `,
			"SELECT toInt64(toUnixTimestamp64Milli(`@timestamp`)/3600000), count() " +
				"FROM " + QuotedTableName + ` WHERE "message" ILIKE '%'  ` +   // TODO: when uncommenting, investigate whether this shouldn't be    "message" IS NOT NULL
				"GROUP BY (toInt64(toUnixTimestamp64Milli(`@timestamp`)/3600000)) " +
				"ORDER BY (toInt64(toUnixTimestamp64Milli(`@timestamp`)/3600000))",
			"SELECT toInt64(toUnixTimestamp64Milli(`@timestamp`)/3600000), count() " +
				"FROM " + QuotedTableName + ` WHERE "message" ILIKE '%'  ` + // TODO: when uncommenting, investigate whether this shouldn't be    "message" IS NOT NULL
				"GROUP BY (toInt64(toUnixTimestamp64Milli(`@timestamp`)/3600000)) " +
				"ORDER BY (toInt64(toUnixTimestamp64Milli(`@timestamp`)/3600000))",
			`SELECT count() FROM ` + QuotedTableName + ` WHERE "message" ILIKE '%' `, // TODO: when uncommenting, investigate whether this shouldn't be    "message" IS NOT NULL
		},
	},
	/*
		{ // [1]
			TestName: "pipeline from elasticsearch docs TODO",
			QueryRequestJson: `
			{
				"size": 0,
					"aggs": {
						"sales_per_month": {
							"date_histogram": {
								"field": "date",
								"calendar_interval": "month"
							},
							"aggs": {
								"total_sales": {
									"sum": {
										"field": "price"
									}
								},
								"t-shirts": {
									"filter": {
										"term": {
											"type": "t-shirt"
										}
									},
									"aggs": {
										"sales": {
											"sum": {
												"field": "price"
											}
										}
									}
								},
								"t-shirt-percentage": {
									"bucket_script": {
										"buckets_path": {
											"tShirtSales": "t-shirts>sales",
											"totalSales": "total_sales"
										},
									"script": "params.tShirtSales / params.totalSales * 100"
								}
							}
						}
					}
				}
			}`,
			ExpectedResponse: `
			{
				"took": 11,
				"timed_out": false,
				"_shards": ...,
				"hits": ...,
				"aggregations": {
					"sales_per_month": {
						"buckets": [
							{
								"key_as_string": "2015/01/01 00:00:00",
								"key": 1420070400000,
								"doc_count": 3,
								"total_sales": {
									"value": 550.0
								},
								"t-shirts": {
									"doc_count": 1,
									"sales": {
										"value": 200.0
									}
								},
								"t-shirt-percentage": {
									"value": 36.36363636363637
								}
							},
							{
								"key_as_string": "2015/02/01 00:00:00",
								"key": 1422748800000,
								"doc_count": 2,
								"total_sales": {
									"value": 60.0
								},
								"t-shirts": {
									"doc_count": 1,
									"sales": {
										"value": 10.0
									}
								},
								"t-shirt-percentage": {
									"value": 16.666666666666664
								}
							},
							{
								"key_as_string": "2015/03/01 00:00:00",
								"key": 1425168000000,
								"doc_count": 2,
								"total_sales": {
									"value": 375.0
								},
								"t-shirts": {
									"doc_count": 1,
									"sales": {
										"value": 175.0
									}
								},
								"t-shirt-percentage": {
									"value": 46.666666666666664
								}
							}
						]
					}
				}
			}`,
			ExpectedResults: make([][]model.QueryResultRow, 0),
			ExpectedSQLs:    make([]string, 0),
		},
}
*/
