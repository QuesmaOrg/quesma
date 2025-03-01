package testdata

import "github.com/QuesmaOrg/quesma/quesma/model"

var KibanaSampleDataEcommerce = []AggregationTestCase{
	{ // [0]
		TestName: "Promotion Tracking 1",
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
							"max": 1740584576601,
							"min": 1739979776601
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
			"completion_time_in_millis": 1707486436398,
			"expiration_time_in_millis": 1707486496397,
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
					"sampler": {
						"doc_count": 4675,
						"eventRate": {
							"buckets": [
								{
									"doc_count": 442,
									"key": 1726358400000,
									"key_as_string": "2024-09-15T00:00:00.000"
								},
								{
									"doc_count": 0,
									"key": 1726963200000,
									"key_as_string": "2024-09-22T00:00:00.000"
								},
								{
									"doc_count": 0,
									"key": 1727568000000,
									"key_as_string": "2024-09-29T00:00:00.000"
								},
								{
									"doc_count": 0,
									"key": 1728172800000,
									"key_as_string": "2024-10-06T00:00:00.000"
								},
								{
									"doc_count": 1,
									"key": 1728777600000,
									"key_as_string": "2024-10-13T00:00:00.000"
								}
							]
						}
					}
				},
				"hits": {
					"hits": [],
					"max_score": null,
					"total": {
						"relation": "eq",
						"value": 2200
					}
				},
				"timed_out": false,
				"took": 1
			},
			"start_time_in_millis": 1707486436397
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__sampler__count", int64(4675)),
				model.NewQueryResultCol("aggr__sampler__eventRate__key_0", int64(1726358400000)),
				model.NewQueryResultCol("aggr__sampler__eventRate__count", int64(442)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__sampler__count", int64(4675)),
				model.NewQueryResultCol("aggr__sampler__eventRate__key_0", int64(1728777600000)),
				model.NewQueryResultCol("aggr__sampler__eventRate__count", int64(1)),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT sum(count(*)) OVER () AS "aggr__sampler__count",
			  toInt64(toUnixTimestamp(toStartOfWeek(toTimezone("order_date", 'UTC'))))*1000
			  AS "aggr__sampler__eventRate__key_0",
			  count(*) AS "aggr__sampler__eventRate__count"
			FROM (
			  SELECT "order_date"
			  FROM __quesma_table_name
			  LIMIT 20000)
			GROUP BY toInt64(toUnixTimestamp(toStartOfWeek(toTimezone("order_date", 'UTC')))
			  )*1000 AS "aggr__sampler__eventRate__key_0"
			ORDER BY "aggr__sampler__eventRate__key_0" ASC`,
	},
	{ // [1]
		TestName: "extended_bounds pre keys (timezone calculations most tricky to get right)",
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
		ExpectedPancakeSQL: `
			SELECT maxOrNull("total_quantity") AS "metric__maxAgg_col_0",
			  minOrNull("total_quantity") AS "metric__minAgg_col_0"
			FROM __quesma_table_name
			WHERE ("order_date">=fromUnixTimestamp64Milli(1739980133594) AND "order_date"<=
			  fromUnixTimestamp64Milli(1740584933594))`,
	},
	{ // [2] TODO
		TestName: "extended_bounds pre keys (timezone calculations most tricky to get right)",
		QueryRequestJson: `
		{
			"_source": false,
			"fields": [
				{
					"field": "*",
					"include_unmapped": "true"
				},
				{
					"field": "customer_birth_date",
					"format": "strict_date_optional_time"
				},
				{
					"field": "order_date",
					"format": "strict_date_optional_time"
				},
				{
					"field": "products.created_on",
					"format": "strict_date_optional_time"
				}
			],
			"highlight": {
				"fields": {
					"*": {}
				},
				"fragment_size": 2147483647,
				"post_tags": [
					"@/kibana-highlighted-field@"
				],
				"pre_tags": [
					"@kibana-highlighted-field@"
				]
			},
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
			"size": 500,
			"sort": [
				{
					"order_date": {
						"format": "strict_date_optional_time",
						"order": "desc",
						"unmapped_type": "boolean"
					}
				},
				{
					"_doc": {
						"order": "desc",
						"unmapped_type": "boolean"
					}
				}
			],
			"stored_fields": [
				"*"
			],
			"track_total_hits": true,
			"version": true
		}`,
		ExpectedResponse: `
		{
			"expiration_time_in_millis": 1740584994182,
			"id": "Fks1NFVMVzRuUkRPVjJDNnUtVjNhMGcccTZPblY1MWNUa2lzS1RZd1lEMk9CdzoxMDUzMg==",
			"is_partial": true,
			"is_running": true,
			"response": {
				"_shards": {
					"failed": 0,
					"skipped": 0,
					"successful": 0,
					"total": 1
				},
				"hits": {
					"hits": [],
					"max_score": null,
					"total": {
						"relation": "gte",
						"value": 0
					}
				},
				"num_reduce_phases": 0,
				"terminated_early": false,
				"timed_out": false,
				"took": 201
			},
			"start_time_in_millis": 1740584934182
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__timeseries__key_0", int64(1730374060000/10000)),
				model.NewQueryResultCol("aggr__timeseries__count", int64(1)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__timeseries__key_0", int64(1730374110000/10000)),
				model.NewQueryResultCol("aggr__timeseries__count", int64(1)),
			}},
		},
		ExpectedPancakeSQL: ``,
	},
	{ // [3]
		TestName: "extended_bounds pre keys (timezone calculations most tricky to get right)",
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
											"key_as_string": "2025-02-21T12:00:00.000+01:00"
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
											"key_as_string": "2025-02-24T12:00:00.000+01:00"
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
											"key_as_string": "2025-02-25T00:00:00.000+01:00"
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
				model.NewQueryResultCol("aggr__timeseries__key_0", int64(1730374060000/10000)),
				model.NewQueryResultCol("aggr__timeseries__count", int64(1)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__timeseries__key_0", int64(1730374110000/10000)),
				model.NewQueryResultCol("aggr__timeseries__count", int64(1)),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT toInt64((toUnixTimestamp64Milli("@timestamp")+timeZoneOffset(toTimezone(
			  "@timestamp", 'Europe/Warsaw'))*1000) / 10000) AS "aggr__timeseries__key_0",
			  count(*) AS "aggr__timeseries__count"
			FROM __quesma_table_name
			WHERE ("@timestamp">=fromUnixTimestamp64Milli(1730370296174) AND "@timestamp"<=
			  fromUnixTimestamp64Milli(1730370596174))
			GROUP BY toInt64((toUnixTimestamp64Milli("@timestamp")+timeZoneOffset(toTimezone
			  ("@timestamp", 'Europe/Warsaw'))*1000) / 10000) AS "aggr__timeseries__key_0"
			ORDER BY "aggr__timeseries__key_0" ASC`,
	},
	{ // [1]
		TestName: "extended_bounds pre keys (timezone calculations most tricky to get right)",
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
				model.NewQueryResultCol("aggr__timeseries__key_0", int64(1730374060000/10000)),
				model.NewQueryResultCol("aggr__timeseries__count", int64(1)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__timeseries__key_0", int64(1730374110000/10000)),
				model.NewQueryResultCol("aggr__timeseries__count", int64(1)),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT toInt64((toUnixTimestamp64Milli("@timestamp")+timeZoneOffset(toTimezone(
			  "@timestamp", 'Europe/Warsaw'))*1000) / 10000) AS "aggr__timeseries__key_0",
			  count(*) AS "aggr__timeseries__count"
			FROM __quesma_table_name
			WHERE ("@timestamp">=fromUnixTimestamp64Milli(1730370296174) AND "@timestamp"<=
			  fromUnixTimestamp64Milli(1730370596174))
			GROUP BY toInt64((toUnixTimestamp64Milli("@timestamp")+timeZoneOffset(toTimezone
			  ("@timestamp", 'Europe/Warsaw'))*1000) / 10000) AS "aggr__timeseries__key_0"
			ORDER BY "aggr__timeseries__key_0" ASC`,
	},
	{ // [1]
		TestName: "extended_bounds pre keys (timezone calculations most tricky to get right)",
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
				model.NewQueryResultCol("aggr__timeseries__key_0", int64(1730374060000/10000)),
				model.NewQueryResultCol("aggr__timeseries__count", int64(1)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__timeseries__key_0", int64(1730374110000/10000)),
				model.NewQueryResultCol("aggr__timeseries__count", int64(1)),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT toInt64((toUnixTimestamp64Milli("@timestamp")+timeZoneOffset(toTimezone(
			  "@timestamp", 'Europe/Warsaw'))*1000) / 10000) AS "aggr__timeseries__key_0",
			  count(*) AS "aggr__timeseries__count"
			FROM __quesma_table_name
			WHERE ("@timestamp">=fromUnixTimestamp64Milli(1730370296174) AND "@timestamp"<=
			  fromUnixTimestamp64Milli(1730370596174))
			GROUP BY toInt64((toUnixTimestamp64Milli("@timestamp")+timeZoneOffset(toTimezone
			  ("@timestamp", 'Europe/Warsaw'))*1000) / 10000) AS "aggr__timeseries__key_0"
			ORDER BY "aggr__timeseries__key_0" ASC`,
	},
	{ // [1]
		TestName: "extended_bounds pre keys (timezone calculations most tricky to get right)",
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
				model.NewQueryResultCol("aggr__0__key_0", int64(1740016000000/86400000)),
				model.NewQueryResultCol("aggr__0__count", int64(151)),
				model.NewQueryResultCol("metric__0__1_col_0", 11132.3671875),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__key_0", int64(1740192400000/86400000)),
				model.NewQueryResultCol("aggr__0__count", int64(166)),
				model.NewQueryResultCol("metric__0__1_col_0", 13902.15625),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__key_0", int64(1740278800000/86400000)),
				model.NewQueryResultCol("aggr__0__count", int64(139)),
				model.NewQueryResultCol("metric__0__1_col_0", 9844.875),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__key_0", int64(1740451600000/86400000)),
				model.NewQueryResultCol("aggr__0__count", int64(149)),
				model.NewQueryResultCol("metric__0__1_col_0", 10807.5625),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__key_0", int64(1740538000000/86400000)),
				model.NewQueryResultCol("aggr__0__count", int64(143)),
				model.NewQueryResultCol("metric__0__1_col_0", 10270.8828125),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__key_0", int64(1739929600000/86400000)),
				model.NewQueryResultCol("aggr__0__count", int64(144)),
				model.NewQueryResultCol("metric__0__1_col_0", 10514.515625),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__key_0", int64(1740534400000/86400000)),
				model.NewQueryResultCol("aggr__0__count", int64(97)),
				model.NewQueryResultCol("metric__0__1_col_0", 7541.5),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT toInt64((toUnixTimestamp64Milli("order_date")+timeZoneOffset(toTimezone(
			  "order_date", 'Europe/Warsaw'))*1000) / 86400000) AS "aggr__0__key_0",
			  count(*) AS "aggr__0__count",
			  sumOrNull("taxful_total_price") AS "metric__0__1_col_0"
			FROM __quesma_table_name
			WHERE ("order_date">=fromUnixTimestamp64Milli(1739980133594) AND "order_date"<=
			  fromUnixTimestamp64Milli(1740584933594))
			GROUP BY toInt64((toUnixTimestamp64Milli("order_date")+timeZoneOffset(toTimezone
			  ("order_date", 'Europe/Warsaw'))*1000) / 86400000) AS "aggr__0__key_0"
			ORDER BY "aggr__0__key_0" ASC`,
	},
	{ // [1]
		TestName: "extended_bounds pre keys (timezone calculations most tricky to get right)",
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
		ExpectedPancakeSQL: `
			SELECT avgOrNull("total_quantity") AS "metric__0_col_0"
			FROM __quesma_table_name
			WHERE ("order_date">=fromUnixTimestamp64Milli(1739980133594) AND "order_date"<=
			  fromUnixTimestamp64Milli(1740584933594))`,
	},
	{ // [1]
		TestName: "extended_bounds pre keys (timezone calculations most tricky to get right)",
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
							},
							{
								"doc_count": 136,
								"gridCentroid": {
									"count": 136,
									"location": {
										"lat": 52.022058804046964,
										"lon": -1.0397059056798326
									}
								},
								"key": "5/15/10",
								"sum_of_taxful_total_price": {
									"value": 9948.125
								}
							},
							{
								"doc_count": 122,
								"gridCentroid": {
									"count": 122,
									"location": {
										"lat": 43.63524586859266,
										"lon": 7.140983528014822
									}
								},
								"key": "5/16/11",
								"sum_of_taxful_total_price": {
									"value": 9626.140625
								}
							},
							{
								"doc_count": 109,
								"gridCentroid": {
									"count": 109,
									"location": {
										"lat": 30.09999997448176,
										"lon": 31.29999996162951
									}
								},
								"key": "5/18/13",
								"sum_of_taxful_total_price": {
									"value": 8335.9765625
								}
							},
							{
								"doc_count": 94,
								"gridCentroid": {
									"count": 94,
									"location": {
										"lat": 31.599999968893826,
										"lon": -8.000000026077032
									}
								},
								"key": "5/15/13",
								"sum_of_taxful_total_price": {
									"value": 5956.546875
								}
							},
							{
								"doc_count": 46,
								"gridCentroid": {
									"count": 46,
									"location": {
										"lat": 40.999999986961484,
										"lon": 28.999999947845936
									}
								},
								"key": "5/18/11",
								"sum_of_taxful_total_price": {
									"value": 3535.2578125
								}
							},
							{
								"doc_count": 43,
								"gridCentroid": {
									"count": 43,
									"location": {
										"lat": 4.599999985657632,
										"lon": -74.10000007599592
									}
								},
								"key": "5/9/15",
								"sum_of_taxful_total_price": {
									"value": 2616.25
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
				model.NewQueryResultCol("metric__0_col_0", 2.164569215876089),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT avgOrNull("total_quantity") AS "metric__0_col_0"
			FROM __quesma_table_name
			WHERE ("order_date">=fromUnixTimestamp64Milli(1739980133594) AND "order_date"<=
			  fromUnixTimestamp64Milli(1740584933594))`,
	},
}
