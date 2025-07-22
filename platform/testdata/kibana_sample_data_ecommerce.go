// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package testdata

import "github.com/QuesmaOrg/quesma/platform/model"

var KibanaSampleDataEcommerce = []AggregationTestCase{
	{ // [0]
		TestName: "Quantity Slider (top)",
		QueryRequestJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"maxAgg": {
					"max": {
						"field": "total_quantity"
					}
				},
				"minAgg": {
					"min": {
						"field": "total_quantity"
					}
				}
			},
			"fields": [
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
									"gte": "2025-02-19T15:48:53.594Z",
									"lte": "2025-02-26T15:48:53.594Z"
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
		ExpectedResponse: `
		{
			"completion_time_in_millis": 1740584934185,
			"expiration_time_in_millis": 1740584994180,
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
					"maxAgg": {
						"value": 4.0
					},
					"minAgg": {
						"value": 2.0
					}
				},
				"hits": {
					"hits": [],
					"max_score": null,
					"total": {
						"relation": "eq",
						"value": 1033
					}
				},
				"timed_out": false,
				"took": 5
			},
			"start_time_in_millis": 1740584934180
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("metric__maxAgg_col_0", 4.0),
				model.NewQueryResultCol("metric__minAgg_col_0", 2.0),
			}},
		},
		ExpectedPancakeSQL: "SELECT maxOrNull(`total_quantity`) AS `metric__maxAgg_col_0`,\n" +
			"  minOrNull(`total_quantity`) AS `metric__minAgg_col_0`\n" +
			"FROM `__quesma_table_name`\n" +
			"WHERE (`order_date`>=fromUnixTimestamp64Milli(1739980133594) AND `order_date`<= \n" +
			"  fromUnixTimestamp64Milli(1740584933594))",
	},
	{ // [1]
		TestName: "Promotions tracking (request 1/3)",
		QueryRequestJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"1": {
					"aggs": {
						"2": {
							"aggs": {
								"4": {
									"top_metrics": {
										"metrics": {
											"field": "order_date"
										},
										"size": 10,
										"sort": {
											"order_date": "asc"
										}
									}
								},
								"5": {
									"top_metrics": {
										"metrics": {
											"field": "taxful_total_price"
										},
										"size": 10,
										"sort": {
											"order_date": "asc"
										}
									}
								}
							},
							"date_histogram": {
								"field": "order_date",
								"fixed_interval": "12h",
								"min_doc_count": 1,
								"time_zone": "Europe/Warsaw"
							}
						}
					},
					"filters": {
						"filters": {
							"c8c30be0-b88f-11e8-a451-f37365e9f268": {
								"bool": {
									"filter": [],
									"must": [
										{
											"query_string": {
												"analyze_wildcard": true,
												"query": "taxful_total_price:>250",
												"time_zone": "Europe/Warsaw"
											}
										}
									],
									"must_not": [],
									"should": []
								}
							}
						}
					}
				}
			},
			"fields": [
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
									"gte": "2025-02-19T15:48:53.594Z",
									"lte": "2025-02-26T15:48:53.594Z"
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
		ExpectedResponse: `
		{
			"completion_time_in_millis": 1740584940851,
			"expiration_time_in_millis": 1740585000843,
			"id": "FmpIT29TeU9NU0FLTWtQWHdEMkNJc3cccTZPblY1MWNUa2lzS1RZd1lEMk9CdzoxMDc4Ng==",
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
					"1": {
						"buckets": {
							"c8c30be0-b88f-11e8-a451-f37365e9f268": {
								"2": {
									"buckets": [
										{
											"4": {
												"top": [
													{
														"metrics": {
															"order_date": "2025-02-21T17:16:48.000Z"
														},
														"sort": [
															"2025-02-21T17:16:48.000Z"
														]
													},
													{
														"metrics": {
															"order_date": "2025-02-21T21:34:34.000Z"
														},
														"sort": [
															"2025-02-21T21:34:34.000Z"
														]
													}
												]
											},
											"5": {
												"top": [
													{
														"metrics": {
															"taxful_total_price": 310.0
														},
														"sort": [
															"2025-02-21T17:16:48.000Z"
														]
													},
													{
														"metrics": {
															"taxful_total_price": 393.0
														},
														"sort": [
															"2025-02-21T21:34:34.000Z"
														]
													}
												]
											},
											"doc_count": 2,
											"key": 1740135600000,
											"key_as_string": "2025-02-21T11:00:00.000"
										},
										{
											"4": {
												"top": [
													{
														"metrics": {
															"order_date": "2025-02-24T11:38:24.000Z"
														},
														"sort": [
															"2025-02-24T11:38:24.000Z"
														]
													}
												]
											},
											"5": {
												"top": [
													{
														"metrics": {
															"taxful_total_price": 283.0
														},
														"sort": [
															"2025-02-24T11:38:24.000Z"
														]
													}
												]
											},
											"doc_count": 1,
											"key": 1740394800000,
											"key_as_string": "2025-02-24T11:00:00.000"
										},
										{
											"4": {
												"top": [
													{
														"metrics": {
															"order_date": "2025-02-25T03:50:24.000Z"
														},
														"sort": [
															"2025-02-25T03:50:24.000Z"
														]
													}
												]
											},
											"5": {
												"top": [
													{
														"metrics": {
															"taxful_total_price": 301.0
														},
														"sort": [
															"2025-02-25T03:50:24.000Z"
														]
													}
												]
											},
											"doc_count": 1,
											"key": 1740438000000,
											"key_as_string": "2025-02-24T23:00:00.000"
										}
									]
								},
								"doc_count": 4
							}
						}
					}
				},
				"hits": {
					"hits": [],
					"max_score": null,
					"total": {
						"relation": "eq",
						"value": 1033
					}
				},
				"timed_out": false,
				"took": 8
			},
			"start_time_in_millis": 1740584940843
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__1__count", int64(4)),
				model.NewQueryResultCol("aggr__1__2__key_0", int64(1740139600000/43200000)),
				model.NewQueryResultCol("aggr__1__2__count", int64(2)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__1__count", int64(4)),
				model.NewQueryResultCol("aggr__1__2__key_0", int64(1740404800000/43200000)),
				model.NewQueryResultCol("aggr__1__2__count", int64(1)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__1__count", int64(4)),
				model.NewQueryResultCol("aggr__1__2__key_0", int64(1740448000000/43200000)),
				model.NewQueryResultCol("aggr__1__2__count", int64(1)),
			}},
		},
		ExpectedPancakeSQL: "SELECT sum(count(*)) OVER () AS `aggr__1__count`,\n" +
			"  toInt64((toUnixTimestamp64Milli(`order_date`)+timeZoneOffset(toTimezone(\n" +
			"  `order_date`, 'Europe/Warsaw'))*1000) / 43200000) AS `aggr__1__2__key_0`,\n" +
			"  count(*) AS `aggr__1__2__count`\n" +
			"FROM `__quesma_table_name`\n" +
			"WHERE ((`order_date`>=fromUnixTimestamp64Milli(1739980133594) AND `order_date`<=fromUnixTimestamp64Milli(1740584933594)) AND `taxful_total_price` > '250')\n" +
			"GROUP BY toInt64((toUnixTimestamp64Milli(`order_date`)+timeZoneOffset(toTimezone\n" +
			"  (`order_date`, 'Europe/Warsaw'))*1000) / 43200000) AS `aggr__1__2__key_0`\n" +
			"ORDER BY `aggr__1__2__key_0` ASC",
		ExpectedAdditionalPancakeSQLs: []string{
			" WITH quesma_top_hits_group_table AS (\n" +
				"  SELECT sum(count(*)) OVER () AS `aggr__1__count`,\n" +
				"    toInt64((toUnixTimestamp64Milli(`order_date`)+timeZoneOffset(toTimezone(\n" +
				"    `order_date`, 'Europe/Warsaw'))*1000) / 43200000) AS `aggr__1__2__key_0`,\n" +
				"    count(*) AS `aggr__1__2__count`\n" +
				"  FROM `__quesma_table_name`\n" +
				"  WHERE ((`order_date`>=fromUnixTimestamp64Milli(1739980133594) AND `order_date`<=fromUnixTimestamp64Milli(1740584933594)) AND `taxful_total_price` > '250')\n" +
				"  GROUP BY toInt64((toUnixTimestamp64Milli(`order_date`)+timeZoneOffset(\n" +
				"    toTimezone(`order_date`, 'Europe/Warsaw'))*1000) / 43200000) AS\n" +
				"    `aggr__1__2__key_0`\n" +
				"  ORDER BY `aggr__1__2__key_0` ASC) ,\n" +
				"quesma_top_hits_join AS (\n" +
				"  SELECT `group_table`.`aggr__1__count` AS `aggr__1__count`,\n" +
				"    `group_table`.`aggr__1__2__key_0` AS `aggr__1__2__key_0`,\n" +
				"    `group_table`.`aggr__1__2__count` AS `aggr__1__2__count`,\n" +
				"    `hit_table`.`order_date` AS `top_metrics__1__2__4_col_0`,\n" +
				"    `hit_table`.`order_date` AS `top_metrics__1__2__4_col_1`,\n" +
				"    ROW_NUMBER() OVER (PARTITION BY `group_table`.`aggr__1__2__key_0` ORDER BY\n" +
				"    `order_date` ASC) AS `top_hits_rank`\n" +
				"  FROM quesma_top_hits_group_table AS `group_table` LEFT OUTER JOIN\n" +
				"    `__quesma_table_name` AS `hit_table` ON (`group_table`.`aggr__1__2__key_0`=\n" +
				"    toInt64((toUnixTimestamp64Milli(`order_date`)+timeZoneOffset(toTimezone(\n" +
				"    `order_date`, 'Europe/Warsaw'))*1000) / 43200000))\n" +
				"  WHERE ((`order_date`>=fromUnixTimestamp64Milli(1739980133594) AND `order_date`<=fromUnixTimestamp64Milli(1740584933594)) AND `taxful_total_price` > '250'))\n" +
				"SELECT `aggr__1__count`, `aggr__1__2__key_0`, `aggr__1__2__count`,\n" +
				"  `top_metrics__1__2__4_col_0`, `top_metrics__1__2__4_col_1`, `top_hits_rank`\n" +
				"FROM `quesma_top_hits_join`\n" +
				"WHERE `top_hits_rank`<=10\n" +
				"ORDER BY `aggr__1__2__key_0` ASC, `top_hits_rank` ASC",
			" WITH quesma_top_hits_group_table AS (\n" +
				"  SELECT sum(count(*)) OVER () AS `aggr__1__count`,\n" +
				"    toInt64((toUnixTimestamp64Milli(`order_date`)+timeZoneOffset(toTimezone(\n" +
				"    `order_date`, 'Europe/Warsaw'))*1000) / 43200000) AS `aggr__1__2__key_0`,\n" +
				"    count(*) AS `aggr__1__2__count`\n" +
				"  FROM `__quesma_table_name`\n" +
				"  WHERE ((`order_date`>=fromUnixTimestamp64Milli(1739980133594) AND `order_date`<=fromUnixTimestamp64Milli(1740584933594)) AND `taxful_total_price` > '250')\n" +
				"  GROUP BY toInt64((toUnixTimestamp64Milli(`order_date`)+timeZoneOffset(\n" +
				"    toTimezone(`order_date`, 'Europe/Warsaw'))*1000) / 43200000) AS\n" +
				"    `aggr__1__2__key_0`\n" +
				"  ORDER BY `aggr__1__2__key_0` ASC) ,\n" +
				"quesma_top_hits_join AS (\n" +
				"  SELECT `group_table`.`aggr__1__count` AS `aggr__1__count`,\n" +
				"    `group_table`.`aggr__1__2__key_0` AS `aggr__1__2__key_0`,\n" +
				"    `group_table`.`aggr__1__2__count` AS `aggr__1__2__count`,\n" +
				"    `hit_table`.`taxful_total_price` AS `top_metrics__1__2__5_col_0`,\n" +
				"    `hit_table`.`order_date` AS `top_metrics__1__2__5_col_1`,\n" +
				"    ROW_NUMBER() OVER (PARTITION BY `group_table`.`aggr__1__2__key_0` ORDER BY\n" +
				"    `order_date` ASC) AS `top_hits_rank`\n" +
				"  FROM quesma_top_hits_group_table AS `group_table` LEFT OUTER JOIN\n" +
				"    `__quesma_table_name` AS `hit_table` ON (`group_table`.`aggr__1__2__key_0`=\n" +
				"    toInt64((toUnixTimestamp64Milli(`order_date`)+timeZoneOffset(toTimezone(\n" +
				"    `order_date`, 'Europe/Warsaw'))*1000) / 43200000))\n" +
				"  WHERE ((`order_date`>=fromUnixTimestamp64Milli(1739980133594) AND `order_date`<=fromUnixTimestamp64Milli(1740584933594)) AND `taxful_total_price` > '250'))\n" +
				"SELECT `aggr__1__count`, `aggr__1__2__key_0`, `aggr__1__2__count`,\n" +
				"  `top_metrics__1__2__5_col_0`, `top_metrics__1__2__5_col_1`, `top_hits_rank`\n" +
				"FROM `quesma_top_hits_join`\n" +
				"WHERE `top_hits_rank`<=10\n" +
				"ORDER BY `aggr__1__2__key_0` ASC, `top_hits_rank` ASC",
		},
		ExpectedAdditionalPancakeResults: [][]model.QueryResultRow{
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("aggr__1__count", int64(4)),
					model.NewQueryResultCol("aggr__1__2__key_0", int64(1740139600000/43200000)),
					model.NewQueryResultCol("aggr__1__2__count", int64(2)),
					model.NewQueryResultCol("top_metrics__1__2__4_col_0", "2025-02-21T17:16:48.000Z"),
					model.NewQueryResultCol("top_metrics__1__2__4_col_1", "2025-02-21T17:16:48.000Z"),
					model.NewQueryResultCol("top_hits_rank", int64(1)),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("aggr__1__count", int64(4)),
					model.NewQueryResultCol("aggr__1__2__key_0", int64(1740139600000/43200000)),
					model.NewQueryResultCol("aggr__1__2__count", int64(2)),
					model.NewQueryResultCol("top_metrics__1__2__4_col_0", "2025-02-21T21:34:34.000Z"),
					model.NewQueryResultCol("top_metrics__1__2__4_col_1", "2025-02-21T21:34:34.000Z"),
					model.NewQueryResultCol("top_hits_rank", int64(2)),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("aggr__1__count", int64(4)),
					model.NewQueryResultCol("aggr__1__2__key_0", int64(1740404800000/43200000)),
					model.NewQueryResultCol("aggr__1__2__count", int64(1)),
					model.NewQueryResultCol("top_metrics__1__2__4_col_0", "2025-02-24T11:38:24.000Z"),
					model.NewQueryResultCol("top_metrics__1__2__4_col_1", "2025-02-24T11:38:24.000Z"),
					model.NewQueryResultCol("top_hits_rank", int64(1)),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("aggr__1__count", int64(4)),
					model.NewQueryResultCol("aggr__1__2__key_0", int64(1740448000000/43200000)),
					model.NewQueryResultCol("aggr__1__2__count", int64(1)),
					model.NewQueryResultCol("top_metrics__1__2__4_col_0", "2025-02-25T03:50:24.000Z"),
					model.NewQueryResultCol("top_metrics__1__2__4_col_1", "2025-02-25T03:50:24.000Z"),
					model.NewQueryResultCol("top_hits_rank", int64(1)),
				}},
			},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("aggr__1__count", int64(4)),
					model.NewQueryResultCol("aggr__1__2__key_0", int64(1740139600000/43200000)),
					model.NewQueryResultCol("aggr__1__2__count", int64(1)),
					model.NewQueryResultCol("top_metrics__1__2__5_col_0", 310.0),
					model.NewQueryResultCol("top_metrics__1__2__5_col_1", "2025-02-21T17:16:48.000Z"),
					model.NewQueryResultCol("top_hits_rank", int64(1)),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("aggr__1__count", int64(4)),
					model.NewQueryResultCol("aggr__1__2__key_0", int64(1740139600000/43200000)),
					model.NewQueryResultCol("aggr__1__2__count", int64(1)),
					model.NewQueryResultCol("top_metrics__1__2__5_col_0", 393.0),
					model.NewQueryResultCol("top_metrics__1__2__5_col_1", "2025-02-21T21:34:34.000Z"),
					model.NewQueryResultCol("top_hits_rank", int64(1)),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("aggr__1__count", int64(4)),
					model.NewQueryResultCol("aggr__1__2__key_0", int64(1740404800000/43200000)),
					model.NewQueryResultCol("aggr__1__2__count", int64(1)),
					model.NewQueryResultCol("top_metrics__1__2__5_col_0", 283.0),
					model.NewQueryResultCol("top_metrics__1__2__5_col_1", "2025-02-24T11:38:24.000Z"),
					model.NewQueryResultCol("top_hits_rank", int64(1)),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("aggr__1__count", int64(4)),
					model.NewQueryResultCol("aggr__1__2__key_0", int64(1740448000000/43200000)),
					model.NewQueryResultCol("aggr__1__2__count", int64(1)),
					model.NewQueryResultCol("top_metrics__1__2__5_col_0", 301.0),
					model.NewQueryResultCol("top_metrics__1__2__5_col_1", "2025-02-25T03:50:24.000Z"),
					model.NewQueryResultCol("top_hits_rank", int64(1)),
				}},
			},
		},
	},
	{ // [2]
		TestName: "Promotion Tracking (request 2/3)",
		QueryRequestJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"0": {
					"aggs": {
						"1-bucket": {
							"aggs": {
								"1-metric": {
									"sum": {
										"field": "taxful_total_price"
									}
								}
							},
							"filter": {
								"bool": {
									"filter": [],
									"must": [
										{
											"query_string": {
												"analyze_wildcard": true,
												"query": "products.product_name:*trouser*",
												"time_zone": "Europe/Warsaw"
											}
										}
									],
									"must_not": [],
									"should": []
								}
							}
						}
					},
					"date_histogram": {
						"extended_bounds": {
							"max": 1740794576601,
							"min": 1740819776601
						},
						"field": "order_date",
						"fixed_interval": "12h",
						"time_zone": "Europe/Warsaw"
					}
				}
			},
			"fields": [
				{
					"field": "@timestamp",
					"format": "date_time"
				},
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
									"gte": "2025-02-19T15:42:56.601Z",
									"lte": "2025-02-26T15:42:56.601Z"
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
		ExpectedResponse: `
		{
			"completion_time_in_millis": 1740838899672,
			"expiration_time_in_millis": 1740838959621,
			"id": "FjVNcnlyNHZWVHlxZk5CX2lPdWxySGcdUEQ3d19oVkxSMEthNU02NjIwRGpkZzo3MTY3Nzg=",
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
								"1-bucket": {
									"1-metric": {
										"value": 241.96875
									},
									"doc_count": 2
								},
								"doc_count": 84,
								"key": 1740783600000,
								"key_as_string": "2025-02-28T23:00:00.000"
							},
							{
								"1-bucket": {
									"1-metric": {
										"value": 0.0
									},
									"doc_count": 0
								},
								"doc_count": 18,
								"key": 1740826800000,
								"key_as_string": "2025-03-01T11:00:00.000"
							}
						]
					}
				},
				"hits": {
					"hits": [],
					"max_score": null,
					"total": {
						"relation": "eq",
						"value": 1035
					}
				},
				"timed_out": false,
				"took": 51
			},
			"start_time_in_millis": 1740838899621
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__key_0", int64(1740793600000/43200000)),
				model.NewQueryResultCol("aggr__0__count", int64(84)),
				model.NewQueryResultCol("aggr__0__1-bucket__count", int64(2)),
				model.NewQueryResultCol("metric__0__1-bucket__1-metric_col_0", 241.96875),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__key_0", int64(1740836800000/43200000)),
				model.NewQueryResultCol("aggr__0__count", int64(18)),
				model.NewQueryResultCol("aggr__0__1-bucket__count", int64(0)),
				model.NewQueryResultCol("metric__0__1-bucket__1-metric_col_0", 0.0),
			}},
		},
		ExpectedPancakeSQL: "SELECT toInt64((toUnixTimestamp64Milli(`order_date`)+timeZoneOffset(toTimezone(\n" +
			"  `order_date`, 'Europe/Warsaw'))*1000) / 43200000) AS `aggr__0__key_0`,\n" +
			"  count(*) AS `aggr__0__count`,\n" +
			"  countIf(`products.product_name` __quesma_match '%trouser%') AS\n" +
			"  `aggr__0__1-bucket__count`,\n" +
			"  sumOrNullIf(`taxful_total_price`, `products.product_name` __quesma_match '%trouser%')\n" +
			"  AS `metric__0__1-bucket__1-metric_col_0`\n" +
			"FROM `__quesma_table_name`\n" +
			"WHERE (`order_date`>=fromUnixTimestamp64Milli(1739979776601) AND `order_date`<= \n" +
			"  fromUnixTimestamp64Milli(1740584576601))\n" +
			"GROUP BY toInt64((toUnixTimestamp64Milli(`order_date`)+timeZoneOffset(toTimezone\n" +
			"  (`order_date`, 'Europe/Warsaw'))*1000) / 43200000) AS `aggr__0__key_0`\n" +
			"ORDER BY `aggr__0__key_0` ASC",
	},
	{ // [3]
		TestName: "Promotions tracking (request 3/3)",
		QueryRequestJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"0": {
					"aggs": {
						"1-bucket": {
							"aggs": {
								"1-metric": {
									"sum": {
										"field": "taxful_total_price"
									}
								}
							},
							"filter": {
								"bool": {
									"filter": [],
									"must": [
										{
											"query_string": {
												"analyze_wildcard": true,
												"query": "products.product_name:*cocktail dress*",
												"time_zone": "Europe/Warsaw"
											}
										}
									],
									"must_not": [],
									"should": []
								}
							}
						}
					},
					"date_histogram": {
						"extended_bounds": {
							"max": 1740278898238,
							"min": 1740214098238
						},
						"field": "order_date",
						"fixed_interval": "12h",
						"time_zone": "Europe/Warsaw"
					}
				}
			},
			"fields": [
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
									"gte": "2025-02-22T14:21:38.238Z",
									"lte": "2025-03-01T14:21:38.238Z"
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
		ExpectedResponse: `
		{
			"completion_time_in_millis": 1740838899788,
			"expiration_time_in_millis": 1740838959620,
			"id": "FlBvbnlxd2VMUUxTSFo5cUlKT2tFRWcdUEQ3d19oVkxSMEthNU02NjIwRGpkZzo3MTY3NzQ=",
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
								"1-bucket": {
									"1-metric": {
										"value": 191.9375
									},
									"doc_count": 3
								},
								"doc_count": 50,
								"key": 1740222000000,
								"key_as_string": "2025-02-22T11:00:00.000"
							},
							{
								"1-bucket": {
									"1-metric": {
										"value": 0.0
									},
									"doc_count": 0
								},
								"doc_count": 18,
								"key": 1740265200000,
								"key_as_string": "2025-02-22T23:00:00.000"
							}
						]
					}
				},
				"hits": {
					"hits": [],
					"max_score": null,
					"total": {
						"relation": "eq",
						"value": 1035
					}
				},
				"timed_out": false,
				"took": 168
			},
			"start_time_in_millis": 1740838899620
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__key_0", int64(1740232000000/43200000)),
				model.NewQueryResultCol("aggr__0__count", int64(50)),
				model.NewQueryResultCol("aggr__0__1-bucket__count", int64(3)),
				model.NewQueryResultCol("metric__0__1-bucket__1-metric_col_0", 191.9375),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__key_0", int64(1740275200000/43200000)),
				model.NewQueryResultCol("aggr__0__count", int64(18)),
				model.NewQueryResultCol("aggr__0__1-bucket__count", int64(0)),
				model.NewQueryResultCol("metric__0__1-bucket__1-metric_col_0", 0.0),
			}},
		},
		ExpectedPancakeSQL: "SELECT toInt64((toUnixTimestamp64Milli(`order_date`)+timeZoneOffset(toTimezone(\n" +
			"  `order_date`, 'Europe/Warsaw'))*1000) / 43200000) AS `aggr__0__key_0`,\n" +
			"  count(*) AS `aggr__0__count`,\n" +
			"  countIf((`products.product_name` __quesma_match '%cocktail' OR\n" +
			"  `__quesma_fulltext_field_name` __quesma_match 'dress%')) AS\n" +
			"  `aggr__0__1-bucket__count`,\n" +
			"  sumOrNullIf(`taxful_total_price`, (`products.product_name` __quesma_match '%cocktail'\n" +
			"  OR `__quesma_fulltext_field_name` __quesma_match 'dress%')) AS\n" +
			"  `metric__0__1-bucket__1-metric_col_0`\n" +
			"FROM `__quesma_table_name`\n" +
			"WHERE (`order_date`>=fromUnixTimestamp64Milli(1740234098238) AND `order_date`<= \n" +
			"  fromUnixTimestamp64Milli(1740838898238))\n" +
			"GROUP BY toInt64((toUnixTimestamp64Milli(`order_date`)+timeZoneOffset(toTimezone\n" +
			"  (`order_date`, 'Europe/Warsaw'))*1000) / 43200000) AS `aggr__0__key_0`\n" +
			"ORDER BY `aggr__0__key_0` ASC",
	},
	{ // [4]
		TestName: "Sum of revenue",
		QueryRequestJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"0": {
					"sum": {
						"field": "taxful_total_price"
					}
				}
			},
			"fields": [
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
									"gte": "2025-02-19T15:48:53.594Z",
									"lte": "2025-02-26T15:48:53.594Z"
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
		ExpectedResponse: `
		{
			"completion_time_in_millis": 1740584940846,
			"expiration_time_in_millis": 1740585000842,
			"id": "Fmd5U3BFaDN1UlAtdkdXb1hBVlVMVEEccTZPblY1MWNUa2lzS1RZd1lEMk9CdzoxMDc4MA==",
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
						"value": 77112.984375
					}
				},
				"hits": {
					"hits": [],
					"max_score": null,
					"total": {
						"relation": "eq",
						"value": 1033
					}
				},
				"timed_out": false,
				"took": 4
			},
			"start_time_in_millis": 1740584940842
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("metric__0_col_0", 77112.984375),
			}},
		},
		ExpectedPancakeSQL: "SELECT sumOrNull(`taxful_total_price`) AS `metric__0_col_0`\n" +
			"FROM `__quesma_table_name`\n" +
			"WHERE (`order_date`>=fromUnixTimestamp64Milli(1739980133594) AND `order_date`<= \n" +
			"  fromUnixTimestamp64Milli(1740584933594))",
	},
	{ // [5]
		TestName: "Median spending",
		QueryRequestJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"0": {
					"percentiles": {
						"field": "taxful_total_price",
						"percents": [
							50
						]
					}
				}
			},
			"fields": [
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
									"gte": "2025-02-19T15:48:53.594Z",
									"lte": "2025-02-26T15:48:53.594Z"
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
		ExpectedResponse: `
		{
			"completion_time_in_millis": 1740584940850,
			"expiration_time_in_millis": 1740585000843,
			"id": "FnlPR0VQbjdoVGpLZ3lCdmtwamRRT2cccTZPblY1MWNUa2lzS1RZd1lEMk9CdzoxMDc4Mw==",
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
						"values": {
							"50.0": 67.0
						}
					}
				},
				"hits": {
					"hits": [],
					"max_score": null,
					"total": {
						"relation": "eq",
						"value": 1033
					}
				},
				"timed_out": false,
				"took": 7
			},
			"start_time_in_millis": 1740584940843
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("metric__0_col_0", []float64{67.0}),
			}},
		},
		ExpectedPancakeSQL: "SELECT quantiles(0.500000)(`taxful_total_price`) AS `metric__0_col_0`\n" +
			"FROM `__quesma_table_name`\n" +
			"WHERE (`order_date`>=fromUnixTimestamp64Milli(1739980133594) AND `order_date`<= \n" +
			"  fromUnixTimestamp64Milli(1740584933594))",
	},
	{ // [6]
		TestName: "Avg. items sold",
		QueryRequestJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"0": {
					"avg": {
						"field": "total_quantity"
					}
				}
			},
			"fields": [
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
									"gte": "2025-02-19T15:48:53.594Z",
									"lte": "2025-02-26T15:48:53.594Z"
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
		ExpectedResponse: `
		{
			"completion_time_in_millis": 1740584940846,
			"expiration_time_in_millis": 1740585000844,
			"id": "Fmdha0ZGeGNlUkVHNl9UblpMcEFnU0EccTZPblY1MWNUa2lzS1RZd1lEMk9CdzoxMDc5Mg==",
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
						"value": 2.164569215876089
					}
				},
				"hits": {
					"hits": [],
					"max_score": null,
					"total": {
						"relation": "eq",
						"value": 1033
					}
				},
				"timed_out": false,
				"took": 2
			},
			"start_time_in_millis": 1740584940844
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("metric__0_col_0", 2.164569215876089),
			}},
		},
		ExpectedPancakeSQL: "SELECT avgOrNull(`total_quantity`) AS `metric__0_col_0`\n" +
			"FROM `__quesma_table_name`\n" +
			"WHERE (`order_date`>=fromUnixTimestamp64Milli(1739980133594) AND `order_date`<= \n" +
			"  fromUnixTimestamp64Milli(1740584933594))",
	},
	{ // [7]
		TestName: "TODO Transactions per day",
		QueryRequestJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"time_offset_split": {
					"aggs": {
						"0": {
							"aggs": {
								"1": {
									"sum": {
										"field": "products.quantity"
									}
								},
								"2": {
									"sum": {
										"field": "products.quantity"
									}
								}
							},
							"date_histogram": {
								"calendar_interval": "1d",
								"field": "order_date",
								"min_doc_count": 1,
								"time_zone": "Europe/Warsaw"
							}
						}
					},
					"filters": {
						"filters": {
							"0": {
								"range": {
									"order_date": {
										"format": "strict_date_optional_time",
										"gte": "2025-02-22T14:21:38.238Z",
										"lte": "2025-03-01T14:21:38.238Z"
									}
								}
							},
							"604800000": {
								"range": {
									"order_date": {
										"format": "strict_date_optional_time",
										"gte": "2025-02-15T14:21:38.238Z",
										"lte": "2025-02-22T14:21:38.238Z"
									}
								}
							}
						}
					}
				}
			},
			"fields": [
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
							"bool": {
								"minimum_should_match": 1,
								"should": [
									{
										"bool": {
											"filter": [
												{
													"range": {
														"order_date": {
															"format": "strict_date_optional_time",
															"gte": "2025-02-22T14:21:38.238Z",
															"lte": "2025-03-01T14:21:38.238Z"
														}
													}
												}
											]
										}
									},
									{
										"bool": {
											"filter": [
												{
													"range": {
														"order_date": {
															"format": "strict_date_optional_time",
															"gte": "2025-02-15T14:21:38.238Z",
															"lte": "2025-02-22T14:21:38.238Z"
														}
													}
												}
											]
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
			},
			"runtime_mappings": {},
			"script_fields": {},
			"size": 0,
			"stored_fields": [
				"*"
			],
			"track_total_hits": true
		}`,
		ExpectedResponse: `
		{
			"completion_time_in_millis": 1740838899669,
			"expiration_time_in_millis": 1740838959628,
			"id": "FlJ2dkFqYzdmUmRHc0thUEtFR1RrVVEdUEQ3d19oVkxSMEthNU02NjIwRGpkZzo3MTY3OTA=",
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
					"time_offset_split": {
						"buckets": {
							"0": {
								"0": {
									"buckets": [
										{
											"1": {
												"value": 104.0
											},
											"2": {
												"value": 104.0
											},
											"doc_count": 50,
											"key": 1740178800000,
											"key_as_string": "2025-02-21T23:00:00.000"
										},
										{
											"1": {
												"value": 218.0
											},
											"2": {
												"value": 218.0
											},
											"doc_count": 102,
											"key": 1740783600000,
											"key_as_string": "2025-02-28T23:00:00.000"
										}
									]
								},
								"doc_count": 1035
							},
							"604800000": {
								"0": {
									"buckets": [
										{
											"1": {
												"value": 104.0
											},
											"2": {
												"value": 104.0
											},
											"doc_count": 49,
											"key": 1739574000000,
											"key_as_string": "2025-02-14T23:00:00.000"
										}
									]
								},
								"doc_count": 49
							}
						}
					}
				},
				"hits": {
					"hits": [],
					"max_score": null,
					"total": {
						"relation": "eq",
						"value": 2075
					}
				},
				"timed_out": false,
				"took": 41
			},
			"start_time_in_millis": 1740838899628
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__time_offset_split__count", int64(1035)),
				model.NewQueryResultCol("aggr__time_offset_split__0__key_0", int64(1740188800000/86400000)),
				model.NewQueryResultCol("aggr__time_offset_split__0__count", int64(50)),
				model.NewQueryResultCol("metric__time_offset_split__0__1_col_0", 104.0),
				model.NewQueryResultCol("metric__time_offset_split__0__2_col_0", 104.0),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__time_offset_split__count", int64(1035)),
				model.NewQueryResultCol("aggr__time_offset_split__0__key_0", int64(1740793600000/86400000)),
				model.NewQueryResultCol("aggr__time_offset_split__0__count", int64(102)),
				model.NewQueryResultCol("metric__time_offset_split__0__1_col_0", 218.0),
				model.NewQueryResultCol("metric__time_offset_split__0__2_col_0", 218.0),
			}},
		},
		ExpectedPancakeSQL: "SELECT sum(count(*)) OVER () AS `aggr__time_offset_split__count`,\n" +
			"  toInt64((toUnixTimestamp64Milli(`order_date`)+timeZoneOffset(toTimezone(\n" +
			"  `order_date`, 'Europe/Warsaw'))*1000) / 86400000) AS\n" +
			"  `aggr__time_offset_split__0__key_0`,\n" +
			"  count(*) AS `aggr__time_offset_split__0__count`,\n" +
			"  sumOrNull(`products.quantity`) AS `metric__time_offset_split__0__1_col_0`,\n" +
			"  sumOrNull(`products.quantity`) AS `metric__time_offset_split__0__2_col_0`\n" +
			"FROM `__quesma_table_name`\n" +
			"WHERE (((`order_date`>=fromUnixTimestamp64Milli(1740234098238) AND `order_date`<=fromUnixTimestamp64Milli(1740838898238)) OR (`order_date`>= \n" +
			"  fromUnixTimestamp64Milli(1739629298238) AND `order_date`<= \n" +
			"  fromUnixTimestamp64Milli(1740234098238))) AND (`order_date`>= \n" +
			"  fromUnixTimestamp64Milli(1740234098238) AND `order_date`<= \n" +
			"  fromUnixTimestamp64Milli(1740838898238)))\n" +
			"GROUP BY toInt64((toUnixTimestamp64Milli(`order_date`)+timeZoneOffset(toTimezone\n" +
			"  (`order_date`, 'Europe/Warsaw'))*1000) / 86400000) AS\n" +
			"  `aggr__time_offset_split__0__key_0`\n" +
			"ORDER BY `aggr__time_offset_split__0__key_0` ASC",
		ExpectedAdditionalPancakeResults: [][]model.QueryResultRow{
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("aggr__time_offset_split__count", int64(49)),
					model.NewQueryResultCol("aggr__time_offset_split__0__key_0", int64(1739584000000/86400000)),
					model.NewQueryResultCol("aggr__time_offset_split__0__count", int64(49)),
					model.NewQueryResultCol("metric__time_offset_split__0__1_col_0", 104.0),
					model.NewQueryResultCol("metric__time_offset_split__0__2_col_0", 104.0),
				}},
			},
		},
		ExpectedAdditionalPancakeSQLs: []string{
			"SELECT sum(count(*)) OVER () AS `aggr__time_offset_split__count`,\n" +
				"  toInt64((toUnixTimestamp64Milli(`order_date`)+timeZoneOffset(toTimezone(\n" +
				"  `order_date`, 'Europe/Warsaw'))*1000) / 86400000) AS\n" +
				"  `aggr__time_offset_split__0__key_0`,\n" +
				"  count(*) AS `aggr__time_offset_split__0__count`,\n" +
				"  sumOrNull(`products.quantity`) AS `metric__time_offset_split__0__1_col_0`,\n" +
				"  sumOrNull(`products.quantity`) AS `metric__time_offset_split__0__2_col_0`\n" +
				"FROM `__quesma_table_name`\n" +
				"WHERE (((`order_date`>=fromUnixTimestamp64Milli(1740234098238) AND `order_date`<=fromUnixTimestamp64Milli(1740838898238)) OR (`order_date`>= \n" +
				"  fromUnixTimestamp64Milli(1739629298238) AND `order_date`<= \n" +
				"  fromUnixTimestamp64Milli(1740234098238))) AND (`order_date`>= \n" +
				"  fromUnixTimestamp64Milli(1739629298238) AND `order_date`<= \n" +
				"  fromUnixTimestamp64Milli(1740234098238)))\n" +
				"GROUP BY toInt64((toUnixTimestamp64Milli(`order_date`)+timeZoneOffset(toTimezone\n" +
				"  (`order_date`, 'Europe/Warsaw'))*1000) / 86400000) AS\n" +
				"  `aggr__time_offset_split__0__key_0`\n" +
				"ORDER BY `aggr__time_offset_split__0__key_0` ASC"},
	},
	{ // [8]
		TestName: "TODO Daily comparison",
		QueryRequestJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"time_offset_split": {
					"aggs": {
						"0": {
							"aggs": {
								"1": {
									"sum": {
										"field": "taxful_total_price"
									}
								},
								"2": {
									"sum": {
										"field": "taxful_total_price"
									}
								}
							},
							"date_histogram": {
								"calendar_interval": "1d",
								"field": "order_date",
								"min_doc_count": 1,
								"time_zone": "Europe/Warsaw"
							}
						}
					},
					"filters": {
						"filters": {
							"0": {
								"range": {
									"order_date": {
										"format": "strict_date_optional_time",
										"gte": "2025-02-22T14:21:38.238Z",
										"lte": "2025-03-01T14:21:38.238Z"
									}
								}
							},
							"604800000": {
								"range": {
									"order_date": {
										"format": "strict_date_optional_time",
										"gte": "2025-02-15T14:21:38.238Z",
										"lte": "2025-02-22T14:21:38.238Z"
									}
								}
							}
						}
					}
				}
			},
			"fields": [
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
							"bool": {
								"minimum_should_match": 1,
								"should": [
									{
										"bool": {
											"filter": [
												{
													"range": {
														"order_date": {
															"format": "strict_date_optional_time",
															"gte": "2025-02-22T14:21:38.238Z",
															"lte": "2025-03-01T14:21:38.238Z"
														}
													}
												}
											]
										}
									},
									{
										"bool": {
											"filter": [
												{
													"range": {
														"order_date": {
															"format": "strict_date_optional_time",
															"gte": "2025-02-15T14:21:38.238Z",
															"lte": "2025-02-22T14:21:38.238Z"
														}
													}
												}
											]
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
			},
			"runtime_mappings": {},
			"script_fields": {},
			"size": 0,
			"stored_fields": [
				"*"
			],
			"track_total_hits": true
		}`,
		ExpectedResponse: `
		{
			"completion_time_in_millis": 1740838899754,
			"expiration_time_in_millis": 1740838959658,
			"id": "FmdYSVFxMlFSUmRHUzBCcGNRSFpuZFEdUEQ3d19oVkxSMEthNU02NjIwRGpkZzo3MTY4MTU=",
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
					"time_offset_split": {
						"buckets": {
							"0": {
								"0": {
									"buckets": [
										{
											"1": {
												"value": 4033.34375
											},
											"2": {
												"value": 4033.34375
											},
											"doc_count": 50,
											"key": 1740178800000,
											"key_as_string": "2025-02-21T23:00:00.000"
										}
									]
								},
								"doc_count": 1035
							},
							"604800000": {
								"0": {
									"buckets": [
										{
											"1": {
												"value": 3579.15625
											},
											"2": {
												"value": 3579.15625
											},
											"doc_count": 49,
											"key": 1739574000000,
											"key_as_string": "2025-02-14T23:00:00.000"
										},
										{
											"1": {
												"value": 10248.6015625
											},
											"2": {
												"value": 10248.6015625
											},
											"doc_count": 140,
											"key": 1739660400000,
											"key_as_string": "2025-02-15T23:00:00.000"
										}
									]
								},
								"doc_count": 1040
							}
						}
					}
				},
				"hits": {
					"hits": [],
					"max_score": null,
					"total": {
						"relation": "eq",
						"value": 2075
					}
				},
				"timed_out": false,
				"took": 96
			},
			"start_time_in_millis": 1740838899658
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__time_offset_split__count", int64(1035)),
				model.NewQueryResultCol("aggr__time_offset_split__0__key_0", int64(1740188800000/86400000)),
				model.NewQueryResultCol("aggr__time_offset_split__0__count", int64(50)),
				model.NewQueryResultCol("metric__time_offset_split__0__1_col_0", 4033.34375),
				model.NewQueryResultCol("metric__time_offset_split__0__2_col_0", 4033.34375),
			}},
		},
		ExpectedPancakeSQL: "SELECT sum(count(*)) OVER () AS `aggr__time_offset_split__count`,\n" +
			"  toInt64((toUnixTimestamp64Milli(`order_date`)+timeZoneOffset(toTimezone(\n" +
			"  `order_date`, 'Europe/Warsaw'))*1000) / 86400000) AS\n" +
			"  `aggr__time_offset_split__0__key_0`,\n" +
			"  count(*) AS `aggr__time_offset_split__0__count`,\n" +
			"  sumOrNull(`taxful_total_price`) AS `metric__time_offset_split__0__1_col_0`,\n" +
			"  sumOrNull(`taxful_total_price`) AS `metric__time_offset_split__0__2_col_0`\n" +
			"FROM `__quesma_table_name`\n" +
			"WHERE (((`order_date`>=fromUnixTimestamp64Milli(1740234098238) AND `order_date`<=fromUnixTimestamp64Milli(1740838898238)) OR (`order_date`>= \n" +
			"  fromUnixTimestamp64Milli(1739629298238) AND `order_date`<= \n" +
			"  fromUnixTimestamp64Milli(1740234098238))) AND (`order_date`>= \n" +
			"  fromUnixTimestamp64Milli(1740234098238) AND `order_date`<= \n" +
			"  fromUnixTimestamp64Milli(1740838898238)))\n" +
			"GROUP BY toInt64((toUnixTimestamp64Milli(`order_date`)+timeZoneOffset(toTimezone\n" +
			"  (`order_date`, 'Europe/Warsaw'))*1000) / 86400000) AS\n" +
			"  `aggr__time_offset_split__0__key_0`\n" +
			"ORDER BY `aggr__time_offset_split__0__key_0` ASC",
		ExpectedAdditionalPancakeResults: [][]model.QueryResultRow{
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("aggr__time_offset_split__count", int64(1040)),
					model.NewQueryResultCol("aggr__time_offset_split__0__key_0", int64(1739584000000/86400000)),
					model.NewQueryResultCol("aggr__time_offset_split__0__count", int64(49)),
					model.NewQueryResultCol("metric__time_offset_split__0__1_col_0", 3579.15625),
					model.NewQueryResultCol("metric__time_offset_split__0__2_col_0", 3579.15625),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("aggr__time_offset_split__count", int64(1040)),
					model.NewQueryResultCol("aggr__time_offset_split__0__key_0", int64(1739674000000/86400000)),
					model.NewQueryResultCol("aggr__time_offset_split__0__count", int64(140)),
					model.NewQueryResultCol("metric__time_offset_split__0__1_col_0", 10248.6015625),
					model.NewQueryResultCol("metric__time_offset_split__0__2_col_0", 10248.6015625),
				}},
			},
		},
		ExpectedAdditionalPancakeSQLs: []string{
			"SELECT sum(count(*)) OVER () AS `aggr__time_offset_split__count`,\n" +
				"  toInt64((toUnixTimestamp64Milli(`order_date`)+timeZoneOffset(toTimezone(\n" +
				"  `order_date`, 'Europe/Warsaw'))*1000) / 86400000) AS\n" +
				"  `aggr__time_offset_split__0__key_0`,\n" +
				"  count(*) AS `aggr__time_offset_split__0__count`,\n" +
				"  sumOrNull(`taxful_total_price`) AS `metric__time_offset_split__0__1_col_0`,\n" +
				"  sumOrNull(`taxful_total_price`) AS `metric__time_offset_split__0__2_col_0`\n" +
				"FROM `__quesma_table_name`\n" +
				"WHERE (((`order_date`>=fromUnixTimestamp64Milli(1740234098238) AND `order_date`<=fromUnixTimestamp64Milli(1740838898238)) OR (`order_date`>= \n" +
				"  fromUnixTimestamp64Milli(1739629298238) AND `order_date`<= \n" +
				"  fromUnixTimestamp64Milli(1740234098238))) AND (`order_date`>= \n" +
				"  fromUnixTimestamp64Milli(1739629298238) AND `order_date`<= \n" +
				"  fromUnixTimestamp64Milli(1740234098238)))\n" +
				"GROUP BY toInt64((toUnixTimestamp64Milli(`order_date`)+timeZoneOffset(toTimezone\n" +
				"  (`order_date`, 'Europe/Warsaw'))*1000) / 86400000) AS\n" +
				"  `aggr__time_offset_split__0__key_0`\n" +
				"ORDER BY `aggr__time_offset_split__0__key_0` ASC"},
	},
	{ // [9]
		TestName: "TODO Top products this/last week",
		QueryRequestJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"0": {
					"terms": {
						"field": "products.product_name.keyword",
						"order": {
							"_count": "desc"
						},
						"shard_size": 25,
						"size": 5
					}
				}
			},
			"fields": [
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
									"gte": "2025-02-22T14:21:38.238Z",
									"lte": "2025-03-01T14:21:38.238Z"
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
		ExpectedResponse: `
		{
			"completion_time_in_millis": 1740838899666,
			"expiration_time_in_millis": 1740838959642,
			"id": "Fk5sSVYzOUExUlJDU3o2Xzg4WTAybHcdUEQ3d19oVkxSMEthNU02NjIwRGpkZzo3MTY3OTY=",
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
								"doc_count": 28,
								"key": "Lace-up boots - black"
							},
							{
								"doc_count": 26,
								"key": "Boots - black"
							},
							{
								"doc_count": 25,
								"key": "Print T-shirt - black"
							},
							{
								"doc_count": 23,
								"key": "Ankle boots - black"
							},
							{
								"doc_count": 20,
								"key": "Jumper - black"
							}
						],
						"doc_count_error_upper_bound": 0,
						"sum_other_doc_count": 2077
					}
				},
				"hits": {
					"hits": [],
					"max_score": null,
					"total": {
						"relation": "eq",
						"value": 1035
					}
				},
				"timed_out": false,
				"took": 24
			},
			"start_time_in_millis": 1740838899642
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{}},
		},
		ExpectedPancakeSQL: `
		`,
	},
	{ // [10]
		TestName: "Breakdown by category",
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
								"calendar_interval": "1d",
								"extended_bounds": {
									"max": 1740838898238,
									"min": 1740234098238
								},
								"field": "order_date",
								"time_zone": "Europe/Warsaw"
							}
						}
					},
					"terms": {
						"field": "category.keyword",
						"order": {
							"_count": "desc"
						},
						"shard_size": 25,
						"size": 10
					}
				}
			},
			"fields": [
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
									"gte": "2025-02-22T14:21:38.238Z",
									"lte": "2025-03-01T14:21:38.238Z"
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
		ExpectedResponse: `
		{
			"completion_time_in_millis": 1740838899678,
			"expiration_time_in_millis": 1740838959648,
			"id": "FlNDSk1wWk53VFIySUdRMUtLTFF6Y1EdUEQ3d19oVkxSMEthNU02NjIwRGpkZzo3MTY4MDc=",
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
											"doc_count": 21,
											"key": 1740178800000,
											"key_as_string": "2025-02-21T23:00:00.000"
										},
										{
											"doc_count": 58,
											"key": 1740265200000,
											"key_as_string": "2025-02-22T23:00:00.000"
										},
										{
											"doc_count": 62,
											"key": 1740351600000,
											"key_as_string": "2025-02-23T23:00:00.000"
										},
										{
											"doc_count": 63,
											"key": 1740438000000,
											"key_as_string": "2025-02-24T23:00:00.000"
										},
										{
											"doc_count": 69,
											"key": 1740524400000,
											"key_as_string": "2025-02-25T23:00:00.000"
										},
										{
											"doc_count": 72,
											"key": 1740610800000,
											"key_as_string": "2025-02-26T23:00:00.000"
										},
										{
											"doc_count": 56,
											"key": 1740697200000,
											"key_as_string": "2025-02-27T23:00:00.000"
										},
										{
											"doc_count": 52,
											"key": 1740783600000,
											"key_as_string": "2025-02-28T23:00:00.000"
										}
									]
								},
								"doc_count": 453,
								"key": "Men's Clothing"
							}
						],
						"doc_count_error_upper_bound": 0,
						"sum_other_doc_count": 582
					}
				},
				"hits": {
					"hits": [],
					"max_score": null,
					"total": {
						"relation": "eq",
						"value": 1035
					}
				},
				"timed_out": false,
				"took": 30
			},
			"start_time_in_millis": 1740838899648
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__parent_count", int64(1035)),
				model.NewQueryResultCol("aggr__0__key_0", "Men's Clothing"),
				model.NewQueryResultCol("aggr__0__count", int64(453)),
				model.NewQueryResultCol("aggr__0__1__key_0", int64(1740228800000/86400000)),
				model.NewQueryResultCol("aggr__0__1__count", int64(21)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__parent_count", int64(1035)),
				model.NewQueryResultCol("aggr__0__key_0", "Men's Clothing"),
				model.NewQueryResultCol("aggr__0__count", int64(453)),
				model.NewQueryResultCol("aggr__0__1__key_0", int64(1740305200000/86400000)),
				model.NewQueryResultCol("aggr__0__1__count", int64(58)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__parent_count", int64(1035)),
				model.NewQueryResultCol("aggr__0__key_0", "Men's Clothing"),
				model.NewQueryResultCol("aggr__0__count", int64(453)),
				model.NewQueryResultCol("aggr__0__1__key_0", int64(1740401600000/86400000)),
				model.NewQueryResultCol("aggr__0__1__count", int64(62)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__parent_count", int64(1035)),
				model.NewQueryResultCol("aggr__0__key_0", "Men's Clothing"),
				model.NewQueryResultCol("aggr__0__count", int64(453)),
				model.NewQueryResultCol("aggr__0__1__key_0", int64(1740478000000/86400000)),
				model.NewQueryResultCol("aggr__0__1__count", int64(63)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__parent_count", int64(1035)),
				model.NewQueryResultCol("aggr__0__key_0", "Men's Clothing"),
				model.NewQueryResultCol("aggr__0__count", int64(453)),
				model.NewQueryResultCol("aggr__0__1__key_0", int64(1740554400000/86400000)),
				model.NewQueryResultCol("aggr__0__1__count", int64(69)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__parent_count", int64(1035)),
				model.NewQueryResultCol("aggr__0__key_0", "Men's Clothing"),
				model.NewQueryResultCol("aggr__0__count", int64(453)),
				model.NewQueryResultCol("aggr__0__1__key_0", int64(1740640800000/86400000)),
				model.NewQueryResultCol("aggr__0__1__count", int64(72)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__parent_count", int64(1035)),
				model.NewQueryResultCol("aggr__0__key_0", "Men's Clothing"),
				model.NewQueryResultCol("aggr__0__count", int64(453)),
				model.NewQueryResultCol("aggr__0__1__key_0", int64(1740727200000/86400000)),
				model.NewQueryResultCol("aggr__0__1__count", int64(56)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__parent_count", int64(1035)),
				model.NewQueryResultCol("aggr__0__key_0", "Men's Clothing"),
				model.NewQueryResultCol("aggr__0__count", int64(453)),
				model.NewQueryResultCol("aggr__0__1__key_0", int64(1740803600000/86400000)),
				model.NewQueryResultCol("aggr__0__1__count", int64(52)),
			}},
		},
		ExpectedPancakeSQL: "SELECT `aggr__0__parent_count`, `aggr__0__key_0`, `aggr__0__count`,\n" +
			"  `aggr__0__1__key_0`, `aggr__0__1__count`\n" +
			"FROM (\n" +
			"  SELECT `aggr__0__parent_count`, `aggr__0__key_0`, `aggr__0__count`,\n" +
			"    `aggr__0__1__key_0`, `aggr__0__1__count`,\n" +
			"    dense_rank() OVER (ORDER BY `aggr__0__count` DESC, `aggr__0__key_0` ASC) AS\n" +
			"    `aggr__0__order_1_rank`,\n" +
			"    dense_rank() OVER (PARTITION BY `aggr__0__key_0` ORDER BY\n" +
			"    `aggr__0__1__key_0` ASC) AS `aggr__0__1__order_1_rank`\n" +
			"  FROM (\n" +
			"    SELECT sum(count(*)) OVER () AS `aggr__0__parent_count`,\n" +
			"      `category` AS `aggr__0__key_0`,\n" +
			"      sum(count(*)) OVER (PARTITION BY `aggr__0__key_0`) AS `aggr__0__count`,\n" +
			"      toInt64((toUnixTimestamp64Milli(`order_date`)+timeZoneOffset(toTimezone(\n" +
			"      `order_date`, 'Europe/Warsaw'))*1000) / 86400000) AS `aggr__0__1__key_0`,\n" +
			"      count(*) AS `aggr__0__1__count`\n" +
			"    FROM `__quesma_table_name`\n" +
			"    WHERE (`order_date`>=fromUnixTimestamp64Milli(1740234098238) AND\n" +
			"      `order_date`<=fromUnixTimestamp64Milli(1740838898238))\n" +
			"    GROUP BY `category` AS `aggr__0__key_0`,\n" +
			"      toInt64((toUnixTimestamp64Milli(`order_date`)+timeZoneOffset(toTimezone(\n" +
			"      `order_date`, 'Europe/Warsaw'))*1000) / 86400000) AS `aggr__0__1__key_0`))\n" +
			"WHERE `aggr__0__order_1_rank`<=11\n" +
			"ORDER BY `aggr__0__order_1_rank` ASC, `aggr__0__1__order_1_rank` ASC",
	},
	{ // [11]
		TestName: "% of target revenue ($10k)",
		QueryRequestJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"0": {
					"aggs": {
						"1": {
							"sum": {
								"field": "taxful_total_price"
							}
						}
					},
					"date_histogram": {
						"calendar_interval": "1d",
						"extended_bounds": {
							"max": 1740584933594,
							"min": 1739980133594
						},
						"field": "order_date",
						"time_zone": "Europe/Warsaw"
					}
				}
			},
			"fields": [
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
									"gte": "2025-02-19T15:48:53.594Z",
									"lte": "2025-02-26T15:48:53.594Z"
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
		ExpectedResponse: `
		{
			"completion_time_in_millis": 1740584940848,
			"expiration_time_in_millis": 1740585000843,
			"id": "FnBGSDhndnJvUjA2RmV5a3Iwa2ZXWEEccTZPblY1MWNUa2lzS1RZd1lEMk9CdzoxMDc4OQ==",
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
									"value": 3099.125
								},
								"doc_count": 44,
								"key": 1739919600000,
								"key_as_string": "2025-02-18T23:00:00.000"
							},
							{
								"1": {
									"value": 11132.3671875
								},
								"doc_count": 151,
								"key": 1740006000000,
								"key_as_string": "2025-02-19T23:00:00.000"
							},
							{
								"1": {
									"value": 13902.15625
								},
								"doc_count": 166,
								"key": 1740092400000,
								"key_as_string": "2025-02-20T23:00:00.000"
							},
							{
								"1": {
									"value": 9844.875
								},
								"doc_count": 139,
								"key": 1740178800000,
								"key_as_string": "2025-02-21T23:00:00.000"
							},
							{
								"1": {
									"value": 10807.5625
								},
								"doc_count": 149,
								"key": 1740265200000,
								"key_as_string": "2025-02-22T23:00:00.000"
							},
							{
								"1": {
									"value": 10270.8828125
								},
								"doc_count": 143,
								"key": 1740351600000,
								"key_as_string": "2025-02-23T23:00:00.000"
							},
							{
								"1": {
									"value": 10514.515625
								},
								"doc_count": 144,
								"key": 1740438000000,
								"key_as_string": "2025-02-24T23:00:00.000"
							},
							{
								"1": {
									"value": 7541.5
								},
								"doc_count": 97,
								"key": 1740524400000,
								"key_as_string": "2025-02-25T23:00:00.000"
							}
						]
					}
				},
				"hits": {
					"hits": [],
					"max_score": null,
					"total": {
						"relation": "eq",
						"value": 1033
					}
				},
				"timed_out": false,
				"took": 5
			},
			"start_time_in_millis": 1740584940843
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__key_0", int64(1739929600000/86400000)),
				model.NewQueryResultCol("aggr__0__count", int64(44)),
				model.NewQueryResultCol("metric__0__1_col_0", 3099.125),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__key_0", int64(1740086000000/86400000)),
				model.NewQueryResultCol("aggr__0__count", int64(151)),
				model.NewQueryResultCol("metric__0__1_col_0", 11132.3671875),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__key_0", int64(1740102400000/86400000)),
				model.NewQueryResultCol("aggr__0__count", int64(166)),
				model.NewQueryResultCol("metric__0__1_col_0", 13902.15625),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__key_0", int64(1740208800000/86400000)),
				model.NewQueryResultCol("aggr__0__count", int64(139)),
				model.NewQueryResultCol("metric__0__1_col_0", 9844.875),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__key_0", int64(1740301600000/86400000)),
				model.NewQueryResultCol("aggr__0__count", int64(149)),
				model.NewQueryResultCol("metric__0__1_col_0", 10807.5625),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__key_0", int64(1740408000000/86400000)),
				model.NewQueryResultCol("aggr__0__count", int64(143)),
				model.NewQueryResultCol("metric__0__1_col_0", 10270.8828125),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__key_0", int64(1740504400000/86400000)),
				model.NewQueryResultCol("aggr__0__count", int64(144)),
				model.NewQueryResultCol("metric__0__1_col_0", 10514.515625),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__key_0", int64(1740594400000/86400000)),
				model.NewQueryResultCol("aggr__0__count", int64(97)),
				model.NewQueryResultCol("metric__0__1_col_0", 7541.5),
			}},
		},
		ExpectedPancakeSQL: "SELECT toInt64((toUnixTimestamp64Milli(`order_date`)+timeZoneOffset(toTimezone(\n" +
			"  `order_date`, 'Europe/Warsaw'))*1000) / 86400000) AS `aggr__0__key_0`,\n" +
			"  count(*) AS `aggr__0__count`,\n" +
			"  sumOrNull(`taxful_total_price`) AS `metric__0__1_col_0`\n" +
			"FROM `__quesma_table_name`\n" +
			"WHERE (`order_date`>=fromUnixTimestamp64Milli(1739980133594) AND `order_date`<= \n" +
			"  fromUnixTimestamp64Milli(1740584933594))\n" +
			"GROUP BY toInt64((toUnixTimestamp64Milli(`order_date`)+timeZoneOffset(toTimezone\n" +
			"  (`order_date`, 'Europe/Warsaw'))*1000) / 86400000) AS `aggr__0__key_0`\n" +
			"ORDER BY `aggr__0__key_0` ASC",
	},
	{ // [12]
		TestName: "Orders by Country (request 1/3)",
		QueryRequestJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"gridSplit": {
					"aggs": {
						"gridCentroid": {
							"geo_centroid": {
								"field": "geoip.location"
							}
						},
						"sum_of_taxful_total_price": {
							"sum": {
								"field": "taxful_total_price"
							}
						}
					},
					"geotile_grid": {
						"bounds": {
							"bottom_right": [
								90,
								0
							],
							"top_left": [
								-90,
								66.51326
							]
						},
						"field": "geoip.location",
						"precision": 5,
						"shard_size": 65535,
						"size": 65535
					}
				}
			},
			"fields": [
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
							"bool": {
								"must": [
									{
										"exists": {
											"field": "geoip.location"
										}
									},
									{
										"geo_bounding_box": {
											"geoip.location": {
												"bottom_right": [
													101.25,
													-11.1784
												],
												"top_left": [
													-90,
													66.51326
												]
											}
										}
									}
								]
							}
						},
						{
							"range": {
								"order_date": {
									"format": "strict_date_optional_time",
									"gte": "2025-02-21T13:07:02.223Z",
									"lte": "2025-02-28T13:07:02.223Z"
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
			"track_total_hits": false
		}`,
		ExpectedResponse: `
		{
			"completion_time_in_millis": 1740748023284,
			"expiration_time_in_millis": 1740748083277,
			"id": "FlNrbTZUVmxSUzkyQ3RFdElNNy1MQ1EcakV2QVZUZEJSUkM3RkhHdVFaU3dtdzoyMjExMQ==",
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
					"gridSplit": {
						"buckets": [
							{
								"doc_count": 212,
								"gridCentroid": {
									"count": 212,
									"location": {
										"lat": 25.013679222331188,
										"lon": 52.11132072843611
									}
								},
								"key": "5/20/13",
								"sum_of_taxful_total_price": {
									"value": 17127.015625
								}
							},
							{
								"doc_count": 200,
								"gridCentroid": {
									"count": 200,
									"location": {
										"lat": 40.78349998171907,
										"lon": -74.00000003166497
									}
								},
								"key": "5/9/12",
								"sum_of_taxful_total_price": {
									"value": 14978.84375
								}
							}
						]
					}
				},
				"hits": {
					"hits": [],
					"max_score": null
				},
				"timed_out": false,
				"took": 7
			},
			"start_time_in_millis": 1740748023277
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__gridSplit__key_0", 20.0),
				model.NewQueryResultCol("aggr__gridSplit__key_1", 13.0),
				model.NewQueryResultCol("aggr__gridSplit__count", int64(212)),
				model.NewQueryResultCol("metric__gridSplit__gridCentroid_col_0", 25.013679222331188),
				model.NewQueryResultCol("metric__gridSplit__gridCentroid_col_1", 52.11132072843611),
				model.NewQueryResultCol("metric__gridSplit__gridCentroid_col_2", int64(212)),
				model.NewQueryResultCol("metric__gridSplit__sum_of_taxful_total_price_col_0", 17127.015625),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__gridSplit__key_0", 9.0),
				model.NewQueryResultCol("aggr__gridSplit__key_1", 12.0),
				model.NewQueryResultCol("aggr__gridSplit__count", int64(200)),
				model.NewQueryResultCol("metric__gridSplit__gridCentroid_col_0", 40.78349998171907),
				model.NewQueryResultCol("metric__gridSplit__gridCentroid_col_1", -74.00000003166497),
				model.NewQueryResultCol("metric__gridSplit__gridCentroid_col_2", int64(200)),
				model.NewQueryResultCol("metric__gridSplit__sum_of_taxful_total_price_col_0", 14978.84375),
			}},
		},
		ExpectedPancakeSQL: "SELECT FLOOR(((__quesma_geo_lon(`geoip.location`)+180)/360)*POWER(2, 5)) AS `aggr__gridSplit__key_0`,\n" +
			"  FLOOR((1-LOG(TAN(RADIANS(__quesma_geo_lat(`geoip.location`)))+(1/COS(RADIANS(\n" +
			"    __quesma_geo_lat(`geoip.location`)))))/PI())/2*POWER(2, 5)) AS `aggr__gridSplit__key_1`,\n" +
			"  count(*) AS `aggr__gridSplit__count`,\n" +
			"  avgOrNull(__quesma_geo_lat(`geoip_location`)) AS `metric__gridSplit__gridCentroid_col_0`,\n" +
			"  avgOrNull(__quesma_geo_lon(`geoip_location`)) AS `metric__gridSplit__gridCentroid_col_1`,\n" +
			"  count(*) AS `metric__gridSplit__gridCentroid_col_2`,\n" +
			"  sumOrNull(`taxful_total_price`) AS `metric__gridSplit__sum_of_taxful_total_price_col_0`\n" +
			"FROM `__quesma_table_name`\n" +
			"WHERE (`geoip.location` IS NOT NULL AND (`order_date`>=fromUnixTimestamp64Milli(\n" +
			"  1740143222223) AND `order_date`<=fromUnixTimestamp64Milli(1740748022223)))\n" +
			"GROUP BY FLOOR(((__quesma_geo_lon(`geoip.location`)+180)/360)*POWER(2, 5)) AS `aggr__gridSplit__key_0`,\n" +
			"  FLOOR((1-LOG(TAN(RADIANS(__quesma_geo_lat(`geoip.location`)))+(1/COS(RADIANS(\n" +
			"    __quesma_geo_lat(`geoip.location`)))))/PI())/2*POWER(2, 5)) AS `aggr__gridSplit__key_1`\n" +
			"ORDER BY `aggr__gridSplit__count` DESC, `aggr__gridSplit__key_0` ASC,\n" +
			"  `aggr__gridSplit__key_1` ASC\n" +
			"LIMIT 65535",
	},
	{ // [13]
		TestName: "Orders by Country (request 2/3)",
		QueryRequestJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"join": {
					"aggs": {},
					"terms": {
						"field": "geoip.region_name",
						"size": 4
					}
				}
			},
			"fields": [
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
									"gte": "2025-02-22T14:21:38.238Z",
									"lte": "2025-03-01T14:21:38.238Z"
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
			"track_total_hits": false
		}`,
		ExpectedResponse: `
		{
			"completion_time_in_millis": 1740838900680,
			"expiration_time_in_millis": 1740838960675,
			"id": "FkdfYkhhWDdHVGt1UWFhSUhrWERIVkEdUEQ3d19oVkxSMEthNU02NjIwRGpkZzo3MTY5NTY=",
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
					"join": {
						"buckets": [
							{
								"doc_count": 197,
								"key": "New York"
							},
							{
								"doc_count": 111,
								"key": "Cairo Governorate"
							},
							{
								"doc_count": 100,
								"key": "Dubai"
							},
							{
								"doc_count": 95,
								"key": "Marrakech-Tensift-Al Haouz"
							}
						],
						"doc_count_error_upper_bound": 0,
						"sum_other_doc_count": 243
					}
				},
				"hits": {
					"hits": [],
					"max_score": null
				},
				"timed_out": false,
				"took": 5
			},
			"start_time_in_millis": 1740838900675
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__join__parent_count", int64(746)),
				model.NewQueryResultCol("aggr__join__key_0", "New York"),
				model.NewQueryResultCol("aggr__join__count", int64(197)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__join__parent_count", int64(746)),
				model.NewQueryResultCol("aggr__join__key_0", "Cairo Governorate"),
				model.NewQueryResultCol("aggr__join__count", int64(111)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__join__parent_count", int64(746)),
				model.NewQueryResultCol("aggr__join__key_0", "Dubai"),
				model.NewQueryResultCol("aggr__join__count", int64(100)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__join__parent_count", int64(746)),
				model.NewQueryResultCol("aggr__join__key_0", "Marrakech-Tensift-Al Haouz"),
				model.NewQueryResultCol("aggr__join__count", int64(95)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__join__parent_count", int64(746)),
				model.NewQueryResultCol("aggr__join__key_0", "Other"),
				model.NewQueryResultCol("aggr__join__count", int64(5)),
			}},
		},
		ExpectedPancakeSQL: "SELECT sum(count(*)) OVER () AS `aggr__join__parent_count`,\n" +
			"  `geoip.region_name` AS `aggr__join__key_0`, count(*) AS `aggr__join__count`\n" +
			"FROM `__quesma_table_name`\n" +
			"WHERE (`order_date`>=fromUnixTimestamp64Milli(1740234098238) AND `order_date`<= \n" +
			"  fromUnixTimestamp64Milli(1740838898238))\n" +
			"GROUP BY `geoip.region_name` AS `aggr__join__key_0`\n" +
			"ORDER BY `aggr__join__count` DESC, `aggr__join__key_0` ASC\n" +
			"LIMIT 5",
	},
	{ // [14]
		TestName: "Orders by Country (request 3/3)",
		QueryRequestJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"join": {
					"aggs": {},
					"terms": {
						"field": "geoip.country_iso_code",
						"size": 65535
					}
				}
			},
			"fields": [
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
									"gte": "2025-02-22T14:21:38.238Z",
									"lte": "2025-03-01T14:21:38.238Z"
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
			"track_total_hits": false
		}`,
		ExpectedResponse: `
		{
			"completion_time_in_millis": 1740838900680,
			"expiration_time_in_millis": 1740838960672,
			"id": "FnBCYVZTQWtUVEgtVGNiUzFabnFqbVEdUEQ3d19oVkxSMEthNU02NjIwRGpkZzo3MTY5NTM=",
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
					"join": {
						"buckets": [
							{
								"doc_count": 276,
								"key": "US"
							},
							{
								"doc_count": 149,
								"key": "AE"
							},
							{
								"doc_count": 132,
								"key": "GB"
							},
							{
								"doc_count": 111,
								"key": "EG"
							},
							{
								"doc_count": 95,
								"key": "MA"
							},
							{
								"doc_count": 81,
								"key": "FR"
							},
							{
								"doc_count": 64,
								"key": "SA"
							},
							{
								"doc_count": 43,
								"key": "CO"
							},
							{
								"doc_count": 43,
								"key": "TR"
							},
							{
								"doc_count": 41,
								"key": "MC"
							}
						],
						"doc_count_error_upper_bound": 0,
						"sum_other_doc_count": 711
					}
				},
				"hits": {
					"hits": [],
					"max_score": null
				},
				"timed_out": false,
				"took": 8
			},
			"start_time_in_millis": 1740838900672
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__join__parent_count", int64(1746)),
				model.NewQueryResultCol("aggr__join__key_0", "US"),
				model.NewQueryResultCol("aggr__join__count", int64(276)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__join__parent_count", int64(1746)),
				model.NewQueryResultCol("aggr__join__key_0", "AE"),
				model.NewQueryResultCol("aggr__join__count", int64(149)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__join__parent_count", int64(1746)),
				model.NewQueryResultCol("aggr__join__key_0", "GB"),
				model.NewQueryResultCol("aggr__join__count", int64(132)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__join__parent_count", int64(1746)),
				model.NewQueryResultCol("aggr__join__key_0", "EG"),
				model.NewQueryResultCol("aggr__join__count", int64(111)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__join__parent_count", int64(1746)),
				model.NewQueryResultCol("aggr__join__key_0", "MA"),
				model.NewQueryResultCol("aggr__join__count", int64(95)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__join__parent_count", int64(1746)),
				model.NewQueryResultCol("aggr__join__key_0", "FR"),
				model.NewQueryResultCol("aggr__join__count", int64(81)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__join__parent_count", int64(1746)),
				model.NewQueryResultCol("aggr__join__key_0", "SA"),
				model.NewQueryResultCol("aggr__join__count", int64(64)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__join__parent_count", int64(1746)),
				model.NewQueryResultCol("aggr__join__key_0", "CO"),
				model.NewQueryResultCol("aggr__join__count", int64(43)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__join__parent_count", int64(1746)),
				model.NewQueryResultCol("aggr__join__key_0", "TR"),
				model.NewQueryResultCol("aggr__join__count", int64(43)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__join__parent_count", int64(1746)),
				model.NewQueryResultCol("aggr__join__key_0", "MC"),
				model.NewQueryResultCol("aggr__join__count", int64(41)),
			}},
		},
		ExpectedPancakeSQL: "SELECT sum(count(*)) OVER () AS `aggr__join__parent_count`,\n" +
			"  `geoip.country_iso_code` AS `aggr__join__key_0`,\n" +
			"  count(*) AS `aggr__join__count`\n" +
			"FROM `__quesma_table_name`\n" +
			"WHERE (`order_date`>=fromUnixTimestamp64Milli(1740234098238) AND `order_date`<= \n" +
			"  fromUnixTimestamp64Milli(1740838898238))\n" +
			"GROUP BY `geoip.country_iso_code` AS `aggr__join__key_0`\n" +
			"ORDER BY `aggr__join__count` DESC, `aggr__join__key_0` ASC\n" +
			"LIMIT 65536",
	},
	{ // [15]
		TestName: "weird",
		QueryRequestJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"fitToBounds": {
					"geo_bounds": {
						"field": "OriginLocation"
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
			"query": {
				"bool": {
					"filter": [
						{
							"range": {
								"timestamp": {
									"format": "strict_date_optional_time",
									"gte": "2025-03-02T14:16:32.069Z",
									"lte": "2025-03-09T14:16:32.069Z"
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
			"track_total_hits": false
		}`,
		ExpectedResponse: `
		{
			"completion_time_in_millis": 1740838900680,
			"expiration_time_in_millis": 1740838960672,
			"id": "FnBCYVZTQWtUVEgtVGNiUzFabnFqbVEdUEQ3d19oVkxSMEthNU02NjIwRGpkZzo3MTY5NTM=",
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
					"fitToBounds": {
						"bounds": {
							"top_left": {
								"lat": 68.15180202014744,
								"lon": -122.59799961000681
							},
							"bottom_right": {
								"lat": -37.67330203671008,
								"lon": 153.11700434423983
							}
						}
					}
				},
				"hits": {
					"hits": [],
					"max_score": null
				},
				"timed_out": false,
				"took": 8
			},
			"start_time_in_millis": 1740838900672
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("metric__fitToBounds_col_0", -122.59799961000681),
				model.NewQueryResultCol("metric__fitToBounds_col_1", 68.15180202014744),
				model.NewQueryResultCol("metric__fitToBounds_col_2", -37.67330203671008),
				model.NewQueryResultCol("metric__fitToBounds_col_3", 153.11700434423983),
			}},
		},
		ExpectedPancakeSQL: "SELECT minOrNull(__quesma_geo_lon(`originlocation`)) AS\n" +
			"  `metric__fitToBounds_col_0`,\n" +
			"  argMinOrNull(__quesma_geo_lat(`originlocation`), __quesma_geo_lon(\n" +
			"  `originlocation`)) AS `metric__fitToBounds_col_1`,\n" +
			"  minOrNull(__quesma_geo_lat(`originlocation`)) AS `metric__fitToBounds_col_2`,\n" +
			"  argMinOrNull(__quesma_geo_lon(`originlocation`), __quesma_geo_lat(\n" +
			"  `originlocation`)) AS `metric__fitToBounds_col_3`\n" +
			"FROM `__quesma_table_name`\n" +
			"WHERE (`timestamp`>=fromUnixTimestamp64Milli(1740924992069) AND `timestamp`<= \n" +
			"  fromUnixTimestamp64Milli(1741529792069))",
	},
}
