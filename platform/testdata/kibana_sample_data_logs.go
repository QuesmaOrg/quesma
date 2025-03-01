// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package testdata

import "github.com/QuesmaOrg/quesma/platform/model"

var KibanaSampleDataLogs = []AggregationTestCase{
	{ // [0]
		TestName: "Bytes slider",
		QueryRequestJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"maxAgg": {
					"max": {
						"field": "bytes"
					}
				},
				"minAgg": {
					"min": {
						"field": "bytes"
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
									"gte": "2025-02-21T23:00:00.000Z",
									"lte": "2025-03-01T12:14:38.103Z"
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
			"completion_time_in_millis": 1740831279251,
			"expiration_time_in_millis": 1740831339230,
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
						"value": 19837.0
					},
					"minAgg": {
						"value": 0.0
					}
				},
				"hits": {
					"hits": [],
					"max_score": null,
					"total": {
						"relation": "eq",
						"value": 1745
					}
				},
				"timed_out": false,
				"took": 21
			},
			"start_time_in_millis": 1740831279230
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("metric__maxAgg_col_0", 19837.0),
				model.NewQueryResultCol("metric__minAgg_col_0", 0.0),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT maxOrNull("bytes") AS "metric__maxAgg_col_0",
			  minOrNull("bytes") AS "metric__minAgg_col_0"
			FROM __quesma_table_name
			WHERE ("timestamp">=fromUnixTimestamp64Milli(1740178800000) AND "timestamp"<=
			  fromUnixTimestamp64Milli(1740831278103))`,
	},
	{ // [1]
		TestName: "TODO Response Codes Over Time + Annotations (1/2 request, Annotations part)",
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
											"field": "timestamp"
										},
										"size": 10,
										"sort": {
											"timestamp": "asc"
										}
									}
								},
								"5": {
									"top_metrics": {
										"metrics": {
											"field": "geo.src"
										},
										"size": 10,
										"sort": {
											"timestamp": "asc"
										}
									}
								}
							},
							"date_histogram": {
								"field": "timestamp",
								"fixed_interval": "3h",
								"min_doc_count": 1,
								"time_zone": "Europe/Warsaw"
							}
						}
					},
					"filters": {
						"filters": {
							"bd7548a0-2223-11e8-832f-d5027f3c8a47": {
								"bool": {
									"filter": [],
									"must": [
										{
											"query_string": {
												"analyze_wildcard": true,
												"query": "tags:error AND tags:security",
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
									"gte": "2025-02-21T23:00:00.000Z",
									"lte": "2025-03-01T12:14:38.103Z"
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
		}
		`,
		ExpectedResponse: `
		{
			"completion_time_in_millis": 1740831279272,
			"expiration_time_in_millis": 1740831339230,
			"id": "FkFrMDVPSXFfUVZxZldOVjN6aTdSYlEdUEQ3d19oVkxSMEthNU02NjIwRGpkZzo2MDU0MDY=",
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
							"bd7548a0-2223-11e8-832f-d5027f3c8a47": {
								"2": {
									"buckets": [
										{
											"4": {
												"top": [
													{
														"metrics": {
															"timestamp": "2025-02-22T20:20:24.125Z"
														},
														"sort": [
															"2025-02-22T20:20:24.125Z"
														]
													}
												]
											},
											"5": {
												"top": [
													{
														"metrics": {
															"geo.src": "US"
														},
														"sort": [
															"2025-02-22T20:20:24.125Z"
														]
													}
												]
											},
											"doc_count": 1,
											"key": 1740254400000,
											"key_as_string": "2025-02-22T21:00:00.000+01:00"
										},
										{
											"4": {
												"top": [
													{
														"metrics": {
															"timestamp": "2025-02-23T06:59:14.110Z"
														},
														"sort": [
															"2025-02-23T06:59:14.110Z"
														]
													}
												]
											},
											"5": {
												"top": [
													{
														"metrics": {
															"geo.src": "US"
														},
														"sort": [
															"2025-02-23T06:59:14.110Z"
														]
													}
												]
											},
											"doc_count": 1,
											"key": 1740286800000,
											"key_as_string": "2025-02-23T06:00:00.000+01:00"
										}
									]
								},
								"doc_count": 28
							}
						}
					}
				},
				"hits": {
					"hits": [],
					"max_score": null,
					"total": {
						"relation": "eq",
						"value": 1745
					}
				},
				"timed_out": false,
				"took": 42
			},
			"start_time_in_millis": 1740831279230
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("metric__maxAgg_col_0", int64(4675)),
			}},
		},
		ExpectedPancakeSQL: `
			`,
	},
	{ // [2]
		TestName: "Unique visitors",
		QueryRequestJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"0": {
					"cardinality": {
						"field": "clientip"
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
									"gte": "2025-02-21T23:00:00.000Z",
									"lte": "2025-03-01T12:14:38.103Z"
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
			"completion_time_in_millis": 1740831279271,
			"expiration_time_in_millis": 1740831339230,
			"id": "FlNwdS1OSXNmUmllRmFISnZBWVdQT0EdUEQ3d19oVkxSMEthNU02NjIwRGpkZzo2MDU0MDU=",
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
						"value": 833
					}
				},
				"hits": {
					"hits": [],
					"max_score": null,
					"total": {
						"relation": "eq",
						"value": 1745
					}
				},
				"timed_out": false,
				"took": 41
			},
			"start_time_in_millis": 1740831279230
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("metric__0_col_0", int64(833)),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT uniq("clientip") AS "metric__0_col_0"
			FROM __quesma_table_name
			WHERE ("timestamp">=fromUnixTimestamp64Milli(1740178800000) AND "timestamp"<=
			  fromUnixTimestamp64Milli(1740831278103))`,
	},
	{ // [3]
		TestName: "Response Codes Over Time + Annotations (2/2 request)",
		QueryRequestJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"0": {
					"aggs": {
						"1": {
							"filters": {
								"filters": {
									"HTTP 2xx and 3xx": {
										"bool": {
											"filter": [
												{
													"bool": {
														"filter": [
															{
																"bool": {
																	"minimum_should_match": 1,
																	"should": [
																		{
																			"range": {
																				"response.keyword": {
																					"gte": "200"
																				}
																			}
																		}
																	]
																}
															},
															{
																"bool": {
																	"minimum_should_match": 1,
																	"should": [
																		{
																			"range": {
																				"response.keyword": {
																					"lt": "400"
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
									"HTTP 4xx": {
										"bool": {
											"filter": [
												{
													"bool": {
														"filter": [
															{
																"bool": {
																	"minimum_should_match": 1,
																	"should": [
																		{
																			"range": {
																				"response.keyword": {
																					"gte": "400"
																				}
																			}
																		}
																	]
																}
															},
															{
																"bool": {
																	"minimum_should_match": 1,
																	"should": [
																		{
																			"range": {
																				"response.keyword": {
																					"lt": "500"
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
									"HTTP 5xx": {
										"bool": {
											"filter": [
												{
													"bool": {
														"minimum_should_match": 1,
														"should": [
															{
																"range": {
																	"response.keyword": {
																		"gte": "500"
																	}
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
									}
								}
							}
						}
					},
					"date_histogram": {
						"extended_bounds": {
							"max": 1740451278103,
							"min": 1740427200000
						},
						"field": "timestamp",
						"fixed_interval": "3h",
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
									"gte": "2025-02-21T23:00:00.000Z",
									"lte": "2025-03-01T12:14:38.103Z"
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
			"completion_time_in_millis": 1740831279297,
			"expiration_time_in_millis": 1740831339230,
			"id": "FjNHWWZwS3ViU1UyMkdBZWxaOXlsRHcdUEQ3d19oVkxSMEthNU02NjIwRGpkZzo2MDU0MDQ=",
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
									"buckets": {
										"HTTP 2xx and 3xx": {
											"doc_count": 1
										},
										"HTTP 4xx": {
											"doc_count": 0
										},
										"HTTP 5xx": {
											"doc_count": 0
										}
									}
								},
								"doc_count": 1,
								"key": 1740427200000,
								"key_as_string": "2025-02-24T20:00:00.000"
							},
							{
								"1": {
									"buckets": {
										"HTTP 2xx and 3xx": {
											"doc_count": 0
										},
										"HTTP 4xx": {
											"doc_count": 0
										},
										"HTTP 5xx": {
											"doc_count": 0
										}
									}
								},
								"doc_count": 0,
								"key": 1740438000000,
								"key_as_string": "2025-02-24T23:00:00.000"
							},
							{
								"1": {
									"buckets": {
										"HTTP 2xx and 3xx": {
											"doc_count": 11
										},
										"HTTP 4xx": {
											"doc_count": 0
										},
										"HTTP 5xx": {
											"doc_count": 1
										}
									}
								},
								"doc_count": 12,
								"key": 1740448800000,
								"key_as_string": "2025-02-25T02:00:00.000"
							}
						]
					}
				},
				"hits": {
					"hits": [],
					"max_score": null,
					"total": {
						"relation": "eq",
						"value": 1745
					}
				},
				"timed_out": false,
				"took": 67
			},
			"start_time_in_millis": 1740831279230
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__key_0", int64(1740437200000/10800000)),
				model.NewQueryResultCol("aggr__0__count", int64(1)),
				model.NewQueryResultCol("filter_0__aggr__0__1__count", int64(1)),
				model.NewQueryResultCol("filter_1__aggr__0__1__count", int64(0)),
				model.NewQueryResultCol("filter_2__aggr__0__1__count", int64(0)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__key_0", int64(1740458800000/10800000)),
				model.NewQueryResultCol("aggr__0__count", int64(12)),
				model.NewQueryResultCol("filter_0__aggr__0__1__count", int64(11)),
				model.NewQueryResultCol("filter_1__aggr__0__1__count", int64(0)),
				model.NewQueryResultCol("filter_2__aggr__0__1__count", int64(1)),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT toInt64((toUnixTimestamp64Milli("timestamp")+timeZoneOffset(toTimezone(
			  "timestamp", 'Europe/Warsaw'))*1000) / 10800000) AS "aggr__0__key_0",
			  count(*) AS "aggr__0__count",
			  countIf(("response">=200 AND "response"<400)) AS "filter_0__aggr__0__1__count"
			  ,
			  countIf(("response">=400 AND "response"<500)) AS "filter_1__aggr__0__1__count"
			  , countIf("response">=500) AS "filter_2__aggr__0__1__count"
			FROM __quesma_table_name
			WHERE ("timestamp">=fromUnixTimestamp64Milli(1740178800000) AND "timestamp"<=
			  fromUnixTimestamp64Milli(1740831278103))
			GROUP BY toInt64((toUnixTimestamp64Milli("timestamp")+timeZoneOffset(toTimezone(
			  "timestamp", 'Europe/Warsaw'))*1000) / 10800000) AS "aggr__0__key_0"
			ORDER BY "aggr__0__key_0" ASC`,
	},
	{ // [4]
		TestName: "HTTP 5xx",
		QueryRequestJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"0-bucket": {
					"filter": {
						"bool": {
							"filter": [
								{
									"bool": {
										"minimum_should_match": 1,
										"should": [
											{
												"range": {
													"response.keyword": {
														"gte": "500"
													}
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
									"gte": "2025-02-21T23:00:00.000Z",
									"lte": "2025-03-01T12:14:38.103Z"
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
			"completion_time_in_millis": 1740831279257,
			"expiration_time_in_millis": 1740831339230,
			"id": "FkUwMzFTR2o4U0MtZzQ5S19fNjdMUUEdUEQ3d19oVkxSMEthNU02NjIwRGpkZzo2MDU0MDM=",
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
					"0-bucket": {
						"doc_count": 63
					}
				},
				"hits": {
					"hits": [],
					"max_score": null,
					"total": {
						"relation": "eq",
						"value": 1745
					}
				},
				"timed_out": false,
				"took": 27
			},
			"start_time_in_millis": 1740831279230
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0-bucket__count", int64(63)),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT countIf("response">=500) AS "aggr__0-bucket__count"
			FROM __quesma_table_name
			WHERE ("timestamp">=fromUnixTimestamp64Milli(1740178800000) AND "timestamp"<=
			  fromUnixTimestamp64Milli(1740831278103))`,
	},
	{ // [5]
		TestName: "HTTP 4xx",
		QueryRequestJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"0-bucket": {
					"filter": {
						"bool": {
							"filter": [
								{
									"bool": {
										"filter": [
											{
												"bool": {
													"minimum_should_match": 1,
													"should": [
														{
															"range": {
																"response.keyword": {
																	"gte": "400"
																}
															}
														}
													]
												}
											},
											{
												"bool": {
													"minimum_should_match": 1,
													"should": [
														{
															"range": {
																"response.keyword": {
																	"lt": "500"
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
									"gte": "2025-02-21T23:00:00.000Z",
									"lte": "2025-03-01T12:14:38.103Z"
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
			"completion_time_in_millis": 1740831279261,
			"expiration_time_in_millis": 1740831339236,
			"id": "FjR0UVV3TDk5US11bGg1eE1oaHp5MkEdUEQ3d19oVkxSMEthNU02NjIwRGpkZzo2MDU0MTU=",
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
					"0-bucket": {
						"doc_count": 72
					}
				},
				"hits": {
					"hits": [],
					"max_score": null,
					"total": {
						"relation": "eq",
						"value": 1745
					}
				},
				"timed_out": false,
				"took": 25
			},
			"start_time_in_millis": 1740831279236
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0-bucket__count", int64(72)),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT countIf(("response">=400 AND "response"<500)) AS "aggr__0-bucket__count"
			FROM __quesma_table_name
			WHERE ("timestamp">=fromUnixTimestamp64Milli(1740178800000) AND "timestamp"<=
			  fromUnixTimestamp64Milli(1740831278103))`,
	},
	{ // [6]
		TestName: "Table gz, css, zip, etc.",
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
								"field": "bytes"
							}
						},
						"2-bucket": {
							"aggs": {
								"2-metric": {
									"sum": {
										"field": "bytes"
									}
								}
							},
							"filter": {
								"bool": {
									"filter": [
										{
											"range": {
												"timestamp": {
													"format": "strict_date_optional_time",
													"gte": "2025-02-28T13:39:32.445Z",
													"lte": "2025-02-28T14:39:32.445Z"
												}
											}
										}
									]
								}
							}
						},
						"3": {
							"cardinality": {
								"field": "clientip"
							}
						},
						"4-bucket": {
							"aggs": {
								"4-metric": {
									"cardinality": {
										"field": "clientip"
									}
								}
							},
							"filter": {
								"bool": {
									"filter": [
										{
											"range": {
												"timestamp": {
													"format": "strict_date_optional_time",
													"gte": "2025-02-28T13:39:32.445Z",
													"lte": "2025-02-28T14:39:32.445Z"
												}
											}
										}
									]
								}
							}
						}
					},
					"terms": {
						"field": "extension.keyword",
						"order": {
							"1": "desc"
						},
						"shard_size": 25,
						"size": 10
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
									"gte": "2025-02-20T23:00:00.000Z",
									"lte": "2025-02-28T14:39:32.445Z"
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
			"completion_time_in_millis": 1740753583123,
			"expiration_time_in_millis": 1740753643066,
			"id": "FlpESUxhSGhaU0ZLMnFsOUVCLXI0YlEcUEQ3d19oVkxSMEthNU02NjIwRGpkZzo1OTMxNw==",
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
									"value": 3557023.0
								},
								"2-bucket": {
									"2-metric": {
										"value": 61415.0
									},
									"doc_count": 10
								},
								"3": {
									"value": 493
								},
								"4-bucket": {
									"4-metric": {
										"value": 10
									},
									"doc_count": 10
								},
								"doc_count": 676,
								"key": ""
							},
							{
								"1": {
									"value": 525707.0
								},
								"2-bucket": {
									"2-metric": {
										"value": 0.0
									},
									"doc_count": 0
								},
								"3": {
									"value": 80
								},
								"4-bucket": {
									"4-metric": {
										"value": 0
									},
									"doc_count": 0
								},
								"doc_count": 84,
								"key": "rpm"
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
						"value": 1795
					}
				},
				"timed_out": false,
				"took": 57
			},
			"start_time_in_millis": 1740753583066
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
	{ // [7]
		TestName: "Errors by host",
		QueryRequestJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"0": {
					"aggs": {
						"2": {
							"cardinality": {
								"field": "clientip"
							}
						},
						"3-bucket": {
							"filter": {
								"bool": {
									"filter": [
										{
											"bool": {
												"minimum_should_match": 1,
												"should": [
													{
														"range": {
															"response.keyword": {
																"gte": "500"
															}
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
							}
						},
						"5-bucket": {
							"filter": {
								"bool": {
									"filter": [
										{
											"bool": {
												"filter": [
													{
														"bool": {
															"minimum_should_match": 1,
															"should": [
																{
																	"range": {
																		"response.keyword": {
																			"gte": "400"
																		}
																	}
																}
															]
														}
													},
													{
														"bool": {
															"minimum_should_match": 1,
															"should": [
																{
																	"range": {
																		"response.keyword": {
																			"lt": "500"
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
							}
						},
						"7": {
							"percentiles": {
								"field": "bytes",
								"percents": [
									95
								]
							}
						},
						"8": {
							"percentiles": {
								"field": "bytes",
								"percents": [
									50
								]
							}
						}
					},
					"terms": {
						"field": "url.keyword",
						"order": {
							"_count": "desc"
						},
						"size": 1000
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
									"gte": "2025-02-21T23:00:00.000Z",
									"lte": "2025-03-01T12:14:38.103Z"
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
			"completion_time_in_millis": 1740831279330,
			"expiration_time_in_millis": 1740831339254,
			"id": "FkE4VzVzai1UUkN1U28xMGFTNXlpaWcdUEQ3d19oVkxSMEthNU02NjIwRGpkZzo2MDU0MjQ=",
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
								"2": {
									"value": 81
								},
								"3-bucket": {
									"doc_count": 3
								},
								"5-bucket": {
									"doc_count": 2
								},
								"7": {
									"values": {
										"95.0": 13963.0
									}
								},
								"8": {
									"values": {
										"50.0": 6820.5
									}
								},
								"doc_count": 86,
								"key": "https://www.elastic.co/downloads/elasticsearch"
							},
							{
								"2": {
									"value": 83
								},
								"3-bucket": {
									"doc_count": 4
								},
								"5-bucket": {
									"doc_count": 1
								},
								"7": {
									"values": {
										"95.0": 14253.549999999997
									}
								},
								"8": {
									"values": {
										"50.0": 5929.5
									}
								},
								"doc_count": 84,
								"key": "https://www.elastic.co/downloads/beats/metricbeat"
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
						"value": 1745
					}
				},
				"timed_out": false,
				"took": 76
			},
			"start_time_in_millis": 1740831279254
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__parent_count", int64(1745)),
				model.NewQueryResultCol("aggr__0__key_0", "https://www.elastic.co/downloads/elasticsearch"),
				model.NewQueryResultCol("aggr__0__count", int64(86)),
				model.NewQueryResultCol("metric__0__2_col_0", int64(81)),
				model.NewQueryResultCol("metric__0__7_col_0", []float64{13963.0}),
				model.NewQueryResultCol("metric__0__8_col_0", []float64{6820.5}),
				model.NewQueryResultCol("aggr__0__3-bucket__count", int64(3)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__parent_count", int64(1745)),
				model.NewQueryResultCol("aggr__0__key_0", "https://www.elastic.co/downloads/beats/metricbeat"),
				model.NewQueryResultCol("aggr__0__count", int64(84)),
				model.NewQueryResultCol("metric__0__2_col_0", int64(83)),
				model.NewQueryResultCol("metric__0__7_col_0", []float64{14253.549999999997}),
				model.NewQueryResultCol("metric__0__8_col_0", []float64{5929.5}),
				model.NewQueryResultCol("aggr__0__3-bucket__count", int64(4)),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT sum(count(*)) OVER () AS "aggr__0__parent_count",
			  "url" AS "aggr__0__key_0", count(*) AS "aggr__0__count",
			  uniq("clientip") AS "metric__0__2_col_0",
			  quantiles(0.950000)("bytes") AS "metric__0__7_col_0",
			  quantiles(0.500000)("bytes") AS "metric__0__8_col_0",
			  countIf("response">=500) AS "aggr__0__3-bucket__count"
			FROM __quesma_table_name
			WHERE ("timestamp">=fromUnixTimestamp64Milli(1740178800000) AND "timestamp"<=
			  fromUnixTimestamp64Milli(1740831278103))
			GROUP BY "url" AS "aggr__0__key_0"
			ORDER BY "aggr__0__count" DESC, "aggr__0__key_0" ASC
			LIMIT 1001`,
	},
	{ // [8]
		TestName: "Total Requests and Bytes (1/2 request)",
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
								"field": "geo.coordinates"
							}
						},
						"sum_of_bytes": {
							"sum": {
								"field": "bytes"
							}
						}
					},
					"geotile_grid": {
						"bounds": {
							"bottom_right": [
								-45,
								21.94305
							],
							"top_left": [
								-135,
								55.77657
							]
						},
						"field": "geo.coordinates",
						"precision": 6,
						"shard_size": 65535,
						"size": 65535
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
							"bool": {
								"must": [
									{
										"exists": {
											"field": "geo.coordinates"
										}
									},
									{
										"geo_bounding_box": {
											"geo.coordinates": {
												"bottom_right": [
													-39.375,
													21.94305
												],
												"top_left": [
													-135,
													55.77657
												]
											}
										}
									}
								]
							}
						},
						{
							"range": {
								"timestamp": {
									"format": "strict_date_optional_time",
									"gte": "2025-02-21T23:00:00.000Z",
									"lte": "2025-03-01T12:14:38.103Z"
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
			"completion_time_in_millis": 1740831279312,
			"expiration_time_in_millis": 1740831339257,
			"id": "FnlJLUtuYy1IUXI2RHM2dHlHSjM4Q1EdUEQ3d19oVkxSMEthNU02NjIwRGpkZzo2MDU0MjY=",
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
								"doc_count": 84,
								"gridCentroid": {
									"count": 84,
									"location": {
										"lat": 34.328916899227934,
										"lon": -87.03988024233175
									}
								},
								"key": "6/16/25",
								"sum_of_bytes": {
									"value": 487212.0
								}
							},
							{
								"doc_count": 78,
								"gridCentroid": {
									"count": 78,
									"location": {
										"lat": 34.233657193835825,
										"lon": -81.91807738970965
									}
								},
								"key": "6/17/25",
								"sum_of_bytes": {
									"value": 450382.0
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
				"took": 55
			},
			"start_time_in_millis": 1740831279257
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__gridSplit__key_0", float64(6)),
				model.NewQueryResultCol("aggr__gridSplit__key_1", float64(16)),
				model.NewQueryResultCol("aggr__gridSplit__key_2", float64(25)),
				model.NewQueryResultCol("aggr__gridSplit__count", int64(84)),
				model.NewQueryResultCol("metric__gridSplit__gridCentroid_col_0", float64(34.328916899227934)),
				model.NewQueryResultCol("metric__gridSplit__gridCentroid_col_1", float64(-87.03988024233175)),
				model.NewQueryResultCol("metric__gridSplit__gridCentroid_col_2", int64(84)),
				model.NewQueryResultCol("metric__gridSplit__sum_of_bytes_col_0", float64(487212.0)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__gridSplit__key_0", float64(6)),
				model.NewQueryResultCol("aggr__gridSplit__key_1", float64(17)),
				model.NewQueryResultCol("aggr__gridSplit__key_2", float64(25)),
				model.NewQueryResultCol("aggr__gridSplit__count", int64(78)),
				model.NewQueryResultCol("metric__gridSplit__gridCentroid_col_0", float64(34.233657193835825)),
				model.NewQueryResultCol("metric__gridSplit__gridCentroid_col_1", float64(-81.91807738970965)),
				model.NewQueryResultCol("metric__gridSplit__gridCentroid_col_2", int64(78)),
				model.NewQueryResultCol("metric__gridSplit__sum_of_bytes_col_0", float64(450382.0)),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT CAST(6.000000 AS Float32) AS "aggr__gridSplit__key_0",
			  FLOOR(((toFloat64(__quesma_geo_lon("geo.coordinates"))+180)/360)*POWER(2, 6))
			  AS "aggr__gridSplit__key_1",
			  FLOOR((1-LOG(TAN(RADIANS(toFloat64(__quesma_geo_lat("geo.coordinates"))))+(1/
			  COS(RADIANS(toFloat64(__quesma_geo_lat("geo.coordinates"))))))/PI())/2*POWER(2
			  , 6)) AS "aggr__gridSplit__key_2", count(*) AS "aggr__gridSplit__count",
			  avgOrNull(CAST(__quesma_geo_lat("geo_coordinates"), 'Float')) AS
			  "metric__gridSplit__gridCentroid_col_0",
			  avgOrNull(CAST(__quesma_geo_lon("geo_coordinates"), 'Float')) AS
			  "metric__gridSplit__gridCentroid_col_1",
			  count(*) AS "metric__gridSplit__gridCentroid_col_2",
			  sumOrNull("bytes") AS "metric__gridSplit__sum_of_bytes_col_0"
			FROM __quesma_table_name
			WHERE ("geo.coordinates" IS NOT NULL AND ("timestamp">=fromUnixTimestamp64Milli(
			  1740178800000) AND "timestamp"<=fromUnixTimestamp64Milli(1740831278103)))
			GROUP BY CAST(6.000000 AS Float32) AS "aggr__gridSplit__key_0",
			  FLOOR(((toFloat64(__quesma_geo_lon("geo.coordinates"))+180)/360)*POWER(2, 6))
			  AS "aggr__gridSplit__key_1",
			  FLOOR((1-LOG(TAN(RADIANS(toFloat64(__quesma_geo_lat("geo.coordinates"))))+(1/
			  COS(RADIANS(toFloat64(__quesma_geo_lat("geo.coordinates"))))))/PI())/2*POWER(2
			  , 6)) AS "aggr__gridSplit__key_2"`,
	},
	{ // [9]
		TestName: "Total Requests and Bytes (2/2 request)",
		QueryRequestJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"join": {
					"aggs": {},
					"terms": {
						"field": "geo.dest",
						"size": 65535
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
									"gte": "2025-02-21T23:00:00.000Z",
									"lte": "2025-03-01T12:14:38.103Z"
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
			"completion_time_in_millis": 1740831279398,
			"expiration_time_in_millis": 1740831339378,
			"id": "FlB0X294TENzUThDT2FjcHVMRmw1LXcdUEQ3d19oVkxSMEthNU02NjIwRGpkZzo2MDU1MTY=",
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
								"doc_count": 349,
								"key": "CN"
							},
							{
								"doc_count": 260,
								"key": "IN"
							}
						],
						"doc_count_error_upper_bound": 0,
						"sum_other_doc_count": 0
					}
				},
				"hits": {
					"hits": [],
					"max_score": null
				},
				"timed_out": false,
				"took": 20
			},
			"start_time_in_millis": 1740831279378
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__join__parent_count", int64(609)),
				model.NewQueryResultCol("aggr__join__key_0", "CN"),
				model.NewQueryResultCol("aggr__join__count", int64(349)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__join__parent_count", int64(609)),
				model.NewQueryResultCol("aggr__join__key_0", "IN"),
				model.NewQueryResultCol("aggr__join__count", int64(260)),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT sum(count(*)) OVER () AS "aggr__join__parent_count",
			  "geo.dest" AS "aggr__join__key_0", count(*) AS "aggr__join__count"
			FROM __quesma_table_name
			WHERE ("timestamp">=fromUnixTimestamp64Milli(1740178800000) AND "timestamp"<=
			  fromUnixTimestamp64Milli(1740831278103))
			GROUP BY "geo.dest" AS "aggr__join__key_0"
			ORDER BY "aggr__join__count" DESC, "aggr__join__key_0" ASC
			LIMIT 65536`,
	},
	{ // [10]
		TestName: "Unique Destination Heatmap",
		QueryRequestJson: `
		{
			"aggs": {
				"countries": {
					"aggs": {
						"hours": {
							"aggs": {
								"unique": {
									"cardinality": {
										"field": "clientip"
									}
								}
							},
							"histogram": {
								"field": "hour_of_day",
								"interval": 1
							}
						}
					},
					"terms": {
						"field": "geo.dest",
						"size": 25
					}
				}
			},
			"query": {
				"bool": {
					"filter": [],
					"must": [
						{
							"range": {
								"@timestamp": {
									"format": "strict_date_optional_time",
									"gte": "2025-02-21T23:00:00.000Z",
									"lte": "2025-03-01T12:14:38.103Z"
								}
							}
						}
					],
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
			"size": 0,
			"track_total_hits": true
		}`,
		ExpectedResponse: `
		{
			"completion_time_in_millis": 1740831279443,
			"expiration_time_in_millis": 1740831339424,
			"id": "FjFqaU1pUExJU0pxWVdPTVducjdQQ1EdUEQ3d19oVkxSMEthNU02NjIwRGpkZzo2MDU1Mzk=",
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
					"countries": {
						"buckets": [
							{
								"doc_count": 2222,
								"hours": {
									"buckets": [
										{
											"doc_count": 2222,
											"key": 1.0,
											"unique": {
												"value": 2222
											}
										}
									]
								},
								"key": "CN"
							},
							{
								"doc_count": 260,
								"hours": {
									"buckets": [
										{
											"doc_count": 3,
											"key": 0.0,
											"unique": {
												"value": 3
											}
										},
										{
											"doc_count": 0,
											"key": 1.0,
											"unique": {
												"value": 0
											}
										},
										{
											"doc_count": 2,
											"key": 2.0,
											"unique": {
												"value": 2
											}
										}
									]
								},
								"key": "IN"
							}
						],
						"doc_count_error_upper_bound": 0,
						"sum_other_doc_count": 467
					}
				},
				"hits": {
					"hits": [],
					"max_score": null,
					"total": {
						"relation": "eq",
						"value": 1745
					}
				},
				"timed_out": false,
				"took": 19
			},
			"start_time_in_millis": 1740831279424
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__countries__parent_count", int64(1745)),
				model.NewQueryResultCol("aggr__countries__key_0", "CN"),
				model.NewQueryResultCol("aggr__countries__count", int64(2222)),
				model.NewQueryResultCol("aggr__countries__hours__key_0", float64(1.0)),
				model.NewQueryResultCol("aggr__countries__hours__count", int64(2222)),
				model.NewQueryResultCol("metric__countries__hours__unique_col_0", int64(2222)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__countries__parent_count", int64(1745)),
				model.NewQueryResultCol("aggr__countries__key_0", "IN"),
				model.NewQueryResultCol("aggr__countries__count", int64(260)),
				model.NewQueryResultCol("aggr__countries__hours__key_0", float64(0.0)),
				model.NewQueryResultCol("aggr__countries__hours__count", int64(3)),
				model.NewQueryResultCol("metric__countries__hours__unique_col_0", int64(3)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__countries__parent_count", int64(1745)),
				model.NewQueryResultCol("aggr__countries__key_0", "IN"),
				model.NewQueryResultCol("aggr__countries__count", int64(260)),
				model.NewQueryResultCol("aggr__countries__hours__key_0", float64(2.0)),
				model.NewQueryResultCol("aggr__countries__hours__count", int64(2)),
				model.NewQueryResultCol("metric__countries__hours__unique_col_0", int64(2)),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT "aggr__countries__parent_count", "aggr__countries__key_0",
			  "aggr__countries__count", "aggr__countries__hours__key_0",
			  "aggr__countries__hours__count", "metric__countries__hours__unique_col_0"
			FROM (
			  SELECT "aggr__countries__parent_count", "aggr__countries__key_0",
				"aggr__countries__count", "aggr__countries__hours__key_0",
				"aggr__countries__hours__count", "metric__countries__hours__unique_col_0",
				dense_rank() OVER (ORDER BY "aggr__countries__count" DESC,
				"aggr__countries__key_0" ASC) AS "aggr__countries__order_1_rank",
				dense_rank() OVER (PARTITION BY "aggr__countries__key_0" ORDER BY
				"aggr__countries__hours__key_0" ASC) AS
				"aggr__countries__hours__order_1_rank"
			  FROM (
				SELECT sum(count(*)) OVER () AS "aggr__countries__parent_count",
				  "geo.dest" AS "aggr__countries__key_0",
				  sum(count(*)) OVER (PARTITION BY "aggr__countries__key_0") AS
				  "aggr__countries__count",
				  "hour_of_day" AS "aggr__countries__hours__key_0",
				  count(*) AS "aggr__countries__hours__count",
				  uniq("clientip") AS "metric__countries__hours__unique_col_0"
				FROM __quesma_table_name
				WHERE ("@timestamp">=fromUnixTimestamp64Milli(1740178800000) AND
				  "@timestamp"<=fromUnixTimestamp64Milli(1740831278103))
				GROUP BY "geo.dest" AS "aggr__countries__key_0",
				  "hour_of_day" AS "aggr__countries__hours__key_0"))
			WHERE "aggr__countries__order_1_rank"<=26
			ORDER BY "aggr__countries__order_1_rank" ASC,
			  "aggr__countries__hours__order_1_rank" ASC`,
	},
	{ // [11]
		TestName: "TODO Machine OS and Destination Sankey Chart",
		QueryRequestJson: `
		{
			"aggs": {
				"table": {
					"composite": {
						"size": 10000,
						"sources": [
							{
								"stk1": {
									"terms": {
										"field": "machine.os.keyword"
									}
								}
							},
							{
								"stk2": {
									"terms": {
										"field": "geo.dest"
									}
								}
							}
						]
					}
				}
			},
			"query": {
				"bool": {
					"filter": [],
					"must": [
						{
							"range": {
								"timestamp": {
									"format": "strict_date_optional_time",
									"gte": "2025-02-21T23:00:00.000Z",
									"lte": "2025-03-01T12:14:38.103Z"
								}
							}
						}
					],
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
			"size": 0,
			"track_total_hits": true
		}`,
		ExpectedResponse: `
		{
			"completion_time_in_millis": 1740831279566,
			"expiration_time_in_millis": 1740831339525,
			"id": "FjNvZjZPZnJUUjhldzQ3VjdpemdQYXcdUEQ3d19oVkxSMEthNU02NjIwRGpkZzo2MDU1NTc=",
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
					"table": {
						"after_key": {
							"stk1": "ios",
							"stk2": "AR"
						},
						"buckets": [
							{
								"doc_count": 2,
								"key": {
									"stk1": "ios",
									"stk2": "AF"
								}
							},
							{
								"doc_count": 1,
								"key": {
									"stk1": "ios",
									"stk2": "AR"
								}
							}
						]
					}
				},
				"hits": {
					"hits": [],
					"max_score": null,
					"total": {
						"relation": "eq",
						"value": 1745
					}
				},
				"timed_out": false,
				"took": 41
			},
			"start_time_in_millis": 1740831279525
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("metric__maxAgg_col_0", int64(4675)),
			}},
		},
		ExpectedPancakeSQL: `
			`,
	},
	{ // [12]
		TestName: "Bytes distribution",
		QueryRequestJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"0": {
					"histogram": {
						"field": "bytes",
						"interval": 50,
						"min_doc_count": 1
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
									"gte": "2025-02-21T23:00:00.000Z",
									"lte": "2025-03-01T12:14:38.103Z"
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
			"completion_time_in_millis": 1740831279538,
			"expiration_time_in_millis": 1740831339534,
			"id": "FlpOX3BvT2l6UmcyRGt2amtGSFNBVUEdUEQ3d19oVkxSMEthNU02NjIwRGpkZzo2MDU1NjE=",
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
								"doc_count": 64,
								"key": 0.0
							},
							{
								"doc_count": 5,
								"key": 50.0
							},
							{
								"doc_count": 2,
								"key": 14450.0
							}
						]
					}
				},
				"hits": {
					"hits": [],
					"max_score": null,
					"total": {
						"relation": "eq",
						"value": 1745
					}
				},
				"timed_out": false,
				"took": 4
			},
			"start_time_in_millis": 1740831279534
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__key_0", 0.0),
				model.NewQueryResultCol("aggr__0__count", int64(64)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__key_0", 50.0),
				model.NewQueryResultCol("aggr__0__count", int64(5)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__key_0", 14450.0),
				model.NewQueryResultCol("aggr__0__count", int64(2)),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT floor("bytes"/50)*50 AS "aggr__0__key_0", count(*) AS "aggr__0__count"
			FROM __quesma_table_name
			WHERE ("timestamp">=fromUnixTimestamp64Milli(1740178800000) AND "timestamp"<=
			  fromUnixTimestamp64Milli(1740831278103))
			GROUP BY floor("bytes"/50)*50 AS "aggr__0__key_0"
			ORDER BY "aggr__0__key_0" ASC`,
	},
}
