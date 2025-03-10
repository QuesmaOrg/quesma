// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package testdata

import "github.com/QuesmaOrg/quesma/platform/model"

var KibanaSampleDataFlights = []AggregationTestCase{
	{ // [0]
		TestName: "Average Ticket Price Slider (top)",
		QueryRequestJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"maxAgg": {
					"max": {
						"field": "AvgTicketPrice"
					}
				},
				"minAgg": {
					"min": {
						"field": "AvgTicketPrice"
					}
				}
			},
			"fields": [
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
									"gte": "2025-02-22T13:23:28.853Z",
									"lte": "2025-03-01T13:23:28.853Z"
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
			"completion_time_in_millis": 1740835409819,
			"expiration_time_in_millis": 1740835469689,
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
						"value": 1199.72900390625
					},
					"minAgg": {
						"value": 100.14596557617188
					}
				},
				"hits": {
					"hits": [],
					"max_score": null,
					"total": {
						"relation": "eq",
						"value": 2156
					}
				},
				"timed_out": false,
				"took": 130
			},
			"start_time_in_millis": 1740835409689
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("metric__maxAgg_col_0", 1199.72900390625),
				model.NewQueryResultCol("metric__minAgg_col_0", 100.14596557617188),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT maxOrNull("AvgTicketPrice") AS "metric__maxAgg_col_0",
			  minOrNull("AvgTicketPrice") AS "metric__minAgg_col_0"
			FROM __quesma_table_name
			WHERE ("timestamp">=fromUnixTimestamp64Milli(1740230608853) AND "timestamp"<=
			  fromUnixTimestamp64Milli(1740835408853))`,
	},
	{ // [1]
		TestName: "fill out when panel starts missing - don't know which panel it is",
		QueryRequestJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"maxAgg": {
					"max": {
						"field": "FlightDelayMin"
					}
				},
				"minAgg": {
					"min": {
						"field": "FlightDelayMin"
					}
				}
			},
			"fields": [
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
									"gte": "2025-02-22T13:23:28.853Z",
									"lte": "2025-03-01T13:23:28.853Z"
								}
							}
						}
					],
					"must": [],
					"must_not": [
						{
							"match_phrase": {
								"FlightDelayMin": {
									"query": 0
								}
							}
						}
					],
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
			"completion_time_in_millis": 1740835409723,
			"expiration_time_in_millis": 1740835469689,
			"id": "Fmd4bDdrWGNnUTJ1UWt2b3ZiNVRSbUEdUEQ3d19oVkxSMEthNU02NjIwRGpkZzo2NjU5MTc=",
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
						"value": 360.0
					},
					"minAgg": {
						"value": 15.0
					}
				},
				"hits": {
					"hits": [],
					"max_score": null,
					"total": {
						"relation": "eq",
						"value": 532
					}
				},
				"timed_out": false,
				"took": 34
			},
			"start_time_in_millis": 1740835409689
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("metric__maxAgg_col_0", 360.0),
				model.NewQueryResultCol("metric__minAgg_col_0", 15.0),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT maxOrNull("FlightDelayMin") AS "metric__maxAgg_col_0",
			  minOrNull("FlightDelayMin") AS "metric__minAgg_col_0"
			FROM __quesma_table_name
			WHERE (("timestamp">=fromUnixTimestamp64Milli(1740230608853) AND "timestamp"<=
			  fromUnixTimestamp64Milli(1740835408853)) AND NOT ("FlightDelayMin" __quesma_match 0))`,
	},
	{ // [2]
		TestName: "Delays & Cancellations (request 1/2)",
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
											"field": "FlightDelay"
										},
										"size": 10,
										"sort": {
											"timestamp": "asc"
										}
									}
								},
								"6": {
									"top_metrics": {
										"metrics": {
											"field": "Cancelled"
										},
										"size": 10,
										"sort": {
											"timestamp": "asc"
										}
									}
								},
								"7": {
									"top_metrics": {
										"metrics": {
											"field": "Carrier"
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
							"53b7dff0-4c89-11e8-a66a-6989ad5a0a39": {
								"bool": {
									"filter": [],
									"must": [
										{
											"query_string": {
												"analyze_wildcard": true,
												"query": "FlightDelay:true AND Cancelled:true",
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
									"gte": "2025-02-22T13:23:28.853Z",
									"lte": "2025-03-01T13:23:28.853Z"
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
			"completion_time_in_millis": 1740835409945,
			"expiration_time_in_millis": 1740835469808,
			"id": "FmJERGFVVmhiUlotaW83bGNEX0NBRkEdUEQ3d19oVkxSMEthNU02NjIwRGpkZzo2NjU5NDA=",
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
							"53b7dff0-4c89-11e8-a66a-6989ad5a0a39": {
								"2": {
									"buckets": [
										{
											"4": {
												"top": [
													{
														"metrics": {
															"timestamp": "2025-02-27T02:05:42.000Z"
														},
														"sort": [
															"2025-02-27T02:05:42.000Z"
														]
													},
													{
														"metrics": {
															"timestamp": "2025-02-27T03:29:04.000Z"
														},
														"sort": [
															"2025-02-27T03:29:04.000Z"
														]
													},
													{
														"metrics": {
															"timestamp": "2025-02-27T04:20:24.000Z"
														},
														"sort": [
															"2025-02-27T04:20:24.000Z"
														]
													},
													{
														"metrics": {
															"timestamp": "2025-02-27T04:35:45.000Z"
														},
														"sort": [
															"2025-02-27T04:35:45.000Z"
														]
													}
												]
											},
											"5": {
												"top": [
													{
														"metrics": {
															"FlightDelay": "true"
														},
														"sort": [
															"2025-02-27T02:05:42.000Z"
														]
													},
													{
														"metrics": {
															"FlightDelay": "true"
														},
														"sort": [
															"2025-02-27T03:29:04.000Z"
														]
													},
													{
														"metrics": {
															"FlightDelay": "true"
														},
														"sort": [
															"2025-02-27T04:20:24.000Z"
														]
													},
													{
														"metrics": {
															"FlightDelay": "true"
														},
														"sort": [
															"2025-02-27T04:35:45.000Z"
														]
													}
												]
											},
											"6": {
												"top": [
													{
														"metrics": {
															"Cancelled": "true"
														},
														"sort": [
															"2025-02-27T02:05:42.000Z"
														]
													},
													{
														"metrics": {
															"Cancelled": "true"
														},
														"sort": [
															"2025-02-27T03:29:04.000Z"
														]
													},
													{
														"metrics": {
															"Cancelled": "true"
														},
														"sort": [
															"2025-02-27T04:20:24.000Z"
														]
													},
													{
														"metrics": {
															"Cancelled": "true"
														},
														"sort": [
															"2025-02-27T04:35:45.000Z"
														]
													}
												]
											},
											"7": {
												"top": [
													{
														"metrics": {
															"Carrier": "Kibana Airlines"
														},
														"sort": [
															"2025-02-27T02:05:42.000Z"
														]
													}
												]
											},
											"doc_count": 4,
											"key": 1740621600000,
											"key_as_string": "2025-02-27T02:00:00.000"
										},
										{
											"4": {
												"top": [
													{
														"metrics": {
															"timestamp": "2025-02-27T05:10:00.000Z"
														},
														"sort": [
															"2025-02-27T05:10:00.000Z"
														]
													},
													{
														"metrics": {
															"timestamp": "2025-02-27T05:24:41.000Z"
														},
														"sort": [
															"2025-02-27T05:24:41.000Z"
														]
													}
												]
											},
											"5": {
												"top": [
													{
														"metrics": {
															"FlightDelay": "true"
														},
														"sort": [
															"2025-02-27T05:10:00.000Z"
														]
													},
													{
														"metrics": {
															"FlightDelay": "true"
														},
														"sort": [
															"2025-02-27T05:24:41.000Z"
														]
													}
												]
											},
											"6": {
												"top": [
													{
														"metrics": {
															"Cancelled": "true"
														},
														"sort": [
															"2025-02-27T05:10:00.000Z"
														]
													},
													{
														"metrics": {
															"Cancelled": "true"
														},
														"sort": [
															"2025-02-27T05:24:41.000Z"
														]
													}
												]
											},
											"7": {
												"top": [
													{
														"metrics": {
															"Carrier": "Kibana Airlines"
														},
														"sort": [
															"2025-02-27T05:10:00.000Z"
														]
													},
													{
														"metrics": {
															"Carrier": "Kibana Airlines"
														},
														"sort": [
															"2025-02-27T05:24:41.000Z"
														]
													}
												]
											},
											"doc_count": 2,
											"key": 1740632400000,
											"key_as_string": "2025-02-27T05:00:00.000"
										}
									]
								},
								"doc_count": 62
							}
						}
					}
				},
				"hits": {
					"hits": [],
					"max_score": null,
					"total": {
						"relation": "eq",
						"value": 2156
					}
				},
				"timed_out": false,
				"took": 137
			},
			"start_time_in_millis": 1740835409808
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__1__count", int64(62)),
				model.NewQueryResultCol("aggr__1__2__key_0", int64(1740631600000/10800000)),
				model.NewQueryResultCol("aggr__1__2__count", int64(4)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__1__count", int64(62)),
				model.NewQueryResultCol("aggr__1__2__key_0", int64(1740642400000/10800000)),
				model.NewQueryResultCol("aggr__1__2__count", int64(2)),
			}},
		},
		ExpectedAdditionalPancakeResults: [][]model.QueryResultRow{
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("aggr__1__count", int64(62)),
					model.NewQueryResultCol("aggr__1__2__key_0", int64(1740631600000/10800000)),
					model.NewQueryResultCol("aggr__1__2__count", int64(4)),
					model.NewQueryResultCol("top_metrics__1__2__4_col_0", "2025-02-27T02:05:42.000Z"),
					model.NewQueryResultCol("top_metrics__1__2__4_col_1", "2025-02-27T02:05:42.000Z"),
					model.NewQueryResultCol("top_hits_rank", int64(1)),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("aggr__1__count", int64(62)),
					model.NewQueryResultCol("aggr__1__2__key_0", int64(1740631600000/10800000)),
					model.NewQueryResultCol("aggr__1__2__count", int64(4)),
					model.NewQueryResultCol("top_metrics__1__2__4_col_0", "2025-02-27T03:29:04.000Z"),
					model.NewQueryResultCol("top_metrics__1__2__4_col_1", "2025-02-27T03:29:04.000Z"),
					model.NewQueryResultCol("top_hits_rank", int64(2)),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("aggr__1__count", int64(62)),
					model.NewQueryResultCol("aggr__1__2__key_0", int64(1740631600000/10800000)),
					model.NewQueryResultCol("aggr__1__2__count", int64(4)),
					model.NewQueryResultCol("top_metrics__1__2__4_col_0", "2025-02-27T04:20:24.000Z"),
					model.NewQueryResultCol("top_metrics__1__2__4_col_1", "2025-02-27T04:20:24.000Z"),
					model.NewQueryResultCol("top_hits_rank", int64(3)),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("aggr__1__count", int64(62)),
					model.NewQueryResultCol("aggr__1__2__key_0", int64(1740631600000/10800000)),
					model.NewQueryResultCol("aggr__1__2__count", int64(4)),
					model.NewQueryResultCol("top_metrics__1__2__4_col_0", "2025-02-27T04:35:45.000Z"),
					model.NewQueryResultCol("top_metrics__1__2__4_col_1", "2025-02-27T04:35:45.000Z"),
					model.NewQueryResultCol("top_hits_rank", int64(4)),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("aggr__1__count", int64(62)),
					model.NewQueryResultCol("aggr__1__2__key_0", int64(1740642400000/10800000)),
					model.NewQueryResultCol("aggr__1__2__count", int64(2)),
					model.NewQueryResultCol("top_metrics__1__2__4_col_0", "2025-02-27T05:10:00.000Z"),
					model.NewQueryResultCol("top_metrics__1__2__4_col_1", "2025-02-27T05:10:00.000Z"),
					model.NewQueryResultCol("top_hits_rank", int64(1)),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("aggr__1__count", int64(62)),
					model.NewQueryResultCol("aggr__1__2__key_0", int64(1740642400000/10800000)),
					model.NewQueryResultCol("aggr__1__2__count", int64(2)),
					model.NewQueryResultCol("top_metrics__1__2__4_col_0", "2025-02-27T05:24:41.000Z"),
					model.NewQueryResultCol("top_metrics__1__2__4_col_1", "2025-02-27T05:24:41.000Z"),
					model.NewQueryResultCol("top_hits_rank", int64(2)),
				}},
			},

			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("aggr__1__count", int64(62)),
					model.NewQueryResultCol("aggr__1__2__key_0", int64(1740631600000/10800000)),
					model.NewQueryResultCol("aggr__1__2__count", int64(4)),
					model.NewQueryResultCol("top_metrics__1__2__5_col_0", "true"),
					model.NewQueryResultCol("top_metrics__1__2__5_col_1", "2025-02-27T02:05:42.000Z"),
					model.NewQueryResultCol("top_hits_rank", int64(1)),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("aggr__1__count", int64(62)),
					model.NewQueryResultCol("aggr__1__2__key_0", int64(1740631600000/10800000)),
					model.NewQueryResultCol("aggr__1__2__count", int64(4)),
					model.NewQueryResultCol("top_metrics__1__2__5_col_0", "true"),
					model.NewQueryResultCol("top_metrics__1__2__5_col_1", "2025-02-27T03:29:04.000Z"),
					model.NewQueryResultCol("top_hits_rank", int64(2)),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("aggr__1__count", int64(62)),
					model.NewQueryResultCol("aggr__1__2__key_0", int64(1740631600000/10800000)),
					model.NewQueryResultCol("aggr__1__2__count", int64(4)),
					model.NewQueryResultCol("top_metrics__1__2__5_col_0", "true"),
					model.NewQueryResultCol("top_metrics__1__2__5_col_1", "2025-02-27T04:20:24.000Z"),
					model.NewQueryResultCol("top_hits_rank", int64(3)),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("aggr__1__count", int64(62)),
					model.NewQueryResultCol("aggr__1__2__key_0", int64(1740631600000/10800000)),
					model.NewQueryResultCol("aggr__1__2__count", int64(4)),
					model.NewQueryResultCol("top_metrics__1__2__5_col_0", "true"),
					model.NewQueryResultCol("top_metrics__1__2__5_col_1", "2025-02-27T04:35:45.000Z"),
					model.NewQueryResultCol("top_hits_rank", int64(4)),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("aggr__1__count", int64(62)),
					model.NewQueryResultCol("aggr__1__2__key_0", int64(1740642400000/10800000)),
					model.NewQueryResultCol("aggr__1__2__count", int64(2)),
					model.NewQueryResultCol("top_metrics__1__2__5_col_0", "true"),
					model.NewQueryResultCol("top_metrics__1__2__5_col_1", "2025-02-27T05:10:00.000Z"),
					model.NewQueryResultCol("top_hits_rank", int64(1)),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("aggr__1__count", int64(62)),
					model.NewQueryResultCol("aggr__1__2__key_0", int64(1740642400000/10800000)),
					model.NewQueryResultCol("aggr__1__2__count", int64(2)),
					model.NewQueryResultCol("top_metrics__1__2__5_col_0", "true"),
					model.NewQueryResultCol("top_metrics__1__2__5_col_1", "2025-02-27T05:24:41.000Z"),
					model.NewQueryResultCol("top_hits_rank", int64(2)),
				}},
			},

			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("aggr__1__count", int64(62)),
					model.NewQueryResultCol("aggr__1__2__key_0", int64(1740631600000/10800000)),
					model.NewQueryResultCol("aggr__1__2__count", int64(4)),
					model.NewQueryResultCol("top_metrics__1__2__6_col_0", "true"),
					model.NewQueryResultCol("top_metrics__1__2__6_col_1", "2025-02-27T02:05:42.000Z"),
					model.NewQueryResultCol("top_hits_rank", int64(1)),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("aggr__1__count", int64(62)),
					model.NewQueryResultCol("aggr__1__2__key_0", int64(1740631600000/10800000)),
					model.NewQueryResultCol("aggr__1__2__count", int64(4)),
					model.NewQueryResultCol("top_metrics__1__2__6_col_0", "true"),
					model.NewQueryResultCol("top_metrics__1__2__6_col_1", "2025-02-27T03:29:04.000Z"),
					model.NewQueryResultCol("top_hits_rank", int64(2)),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("aggr__1__count", int64(62)),
					model.NewQueryResultCol("aggr__1__2__key_0", int64(1740631600000/10800000)),
					model.NewQueryResultCol("aggr__1__2__count", int64(4)),
					model.NewQueryResultCol("top_metrics__1__2__6_col_0", "true"),
					model.NewQueryResultCol("top_metrics__1__2__6_col_1", "2025-02-27T04:20:24.000Z"),
					model.NewQueryResultCol("top_hits_rank", int64(3)),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("aggr__1__count", int64(62)),
					model.NewQueryResultCol("aggr__1__2__key_0", int64(1740631600000/10800000)),
					model.NewQueryResultCol("aggr__1__2__count", int64(4)),
					model.NewQueryResultCol("top_metrics__1__2__6_col_0", "true"),
					model.NewQueryResultCol("top_metrics__1__2__6_col_1", "2025-02-27T04:35:45.000Z"),
					model.NewQueryResultCol("top_hits_rank", int64(4)),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("aggr__1__count", int64(62)),
					model.NewQueryResultCol("aggr__1__2__key_0", int64(1740642400000/10800000)),
					model.NewQueryResultCol("aggr__1__2__count", int64(2)),
					model.NewQueryResultCol("top_metrics__1__2__6_col_0", "true"),
					model.NewQueryResultCol("top_metrics__1__2__6_col_1", "2025-02-27T05:10:00.000Z"),
					model.NewQueryResultCol("top_hits_rank", int64(1)),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("aggr__1__count", int64(62)),
					model.NewQueryResultCol("aggr__1__2__key_0", int64(1740642400000/10800000)),
					model.NewQueryResultCol("aggr__1__2__count", int64(2)),
					model.NewQueryResultCol("top_metrics__1__2__6_col_0", "true"),
					model.NewQueryResultCol("top_metrics__1__2__6_col_1", "2025-02-27T05:24:41.000Z"),
					model.NewQueryResultCol("top_hits_rank", int64(2)),
				}},
			},

			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("aggr__1__count", int64(62)),
					model.NewQueryResultCol("aggr__1__2__key_0", int64(1740631600000/10800000)),
					model.NewQueryResultCol("aggr__1__2__count", int64(4)),
					model.NewQueryResultCol("top_metrics__1__2__7_col_0", "Kibana Airlines"),
					model.NewQueryResultCol("top_metrics__1__2__7_col_1", "2025-02-27T02:05:42.000Z"),
					model.NewQueryResultCol("top_hits_rank", int64(1)),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("aggr__1__count", int64(62)),
					model.NewQueryResultCol("aggr__1__2__key_0", int64(1740642400000/10800000)),
					model.NewQueryResultCol("aggr__1__2__count", int64(2)),
					model.NewQueryResultCol("top_metrics__1__2__7_col_0", "Kibana Airlines"),
					model.NewQueryResultCol("top_metrics__1__2__7_col_1", "2025-02-27T05:10:00.000Z"),
					model.NewQueryResultCol("top_hits_rank", int64(1)),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("aggr__1__count", int64(62)),
					model.NewQueryResultCol("aggr__1__2__key_0", int64(1740642400000/10800000)),
					model.NewQueryResultCol("aggr__1__2__count", int64(2)),
					model.NewQueryResultCol("top_metrics__1__2__7_col_0", "Kibana Airlines"),
					model.NewQueryResultCol("top_metrics__1__2__7_col_1", "2025-02-27T05:24:41.000Z"),
					model.NewQueryResultCol("top_hits_rank", int64(2)),
				}},
			},
		},
		ExpectedPancakeSQL: `
			SELECT sum(count(*)) OVER () AS "aggr__1__count",
			  toInt64((toUnixTimestamp64Milli("timestamp")+timeZoneOffset(toTimezone(
			  "timestamp", 'Europe/Warsaw'))*1000) / 10800000) AS "aggr__1__2__key_0",
			  count(*) AS "aggr__1__2__count"
			FROM __quesma_table_name
			WHERE (("timestamp">=fromUnixTimestamp64Milli(1740230608853) AND "timestamp"<=
			  fromUnixTimestamp64Milli(1740835408853)) AND ("FlightDelay" __quesma_match '%true%' AND
			  "Cancelled" __quesma_match '%true%'))
			GROUP BY toInt64((toUnixTimestamp64Milli("timestamp")+timeZoneOffset(toTimezone(
			  "timestamp", 'Europe/Warsaw'))*1000) / 10800000) AS "aggr__1__2__key_0"
			ORDER BY "aggr__1__2__key_0" ASC`,
		ExpectedAdditionalPancakeSQLs: []string{`
				WITH quesma_top_hits_group_table AS (
				  SELECT sum(count(*)) OVER () AS "aggr__1__count",
					toInt64((toUnixTimestamp64Milli("timestamp")+timeZoneOffset(toTimezone(
					"timestamp", 'Europe/Warsaw'))*1000) / 10800000) AS "aggr__1__2__key_0",
					count(*) AS "aggr__1__2__count"
				  FROM __quesma_table_name
				  WHERE (("timestamp">=fromUnixTimestamp64Milli(1740230608853) AND "timestamp"<=
					fromUnixTimestamp64Milli(1740835408853)) AND ("FlightDelay" __quesma_match '%true%'
					AND "Cancelled" __quesma_match '%true%'))
				  GROUP BY toInt64((toUnixTimestamp64Milli("timestamp")+timeZoneOffset(
					toTimezone("timestamp", 'Europe/Warsaw'))*1000) / 10800000) AS
					"aggr__1__2__key_0"
				  ORDER BY "aggr__1__2__key_0" ASC) ,
				quesma_top_hits_join AS (
				  SELECT "group_table"."aggr__1__count" AS "aggr__1__count",
					"group_table"."aggr__1__2__key_0" AS "aggr__1__2__key_0",
					"group_table"."aggr__1__2__count" AS "aggr__1__2__count",
					"hit_table"."timestamp" AS "top_metrics__1__2__4_col_0",
					"hit_table"."timestamp" AS "top_metrics__1__2__4_col_1",
					ROW_NUMBER() OVER (PARTITION BY "group_table"."aggr__1__2__key_0" ORDER BY
					"timestamp" ASC) AS "top_hits_rank"
				  FROM quesma_top_hits_group_table AS "group_table" LEFT OUTER JOIN
					__quesma_table_name AS "hit_table" ON ("group_table"."aggr__1__2__key_0"=
					toInt64((toUnixTimestamp64Milli("timestamp")+timeZoneOffset(toTimezone(
					"timestamp", 'Europe/Warsaw'))*1000) / 10800000))
				  WHERE (("timestamp">=fromUnixTimestamp64Milli(1740230608853) AND "timestamp"<=
					fromUnixTimestamp64Milli(1740835408853)) AND ("FlightDelay" __quesma_match '%true%'
					AND "Cancelled" __quesma_match '%true%')))
				SELECT "aggr__1__count", "aggr__1__2__key_0", "aggr__1__2__count",
				  "top_metrics__1__2__4_col_0", "top_metrics__1__2__4_col_1", "top_hits_rank"
				FROM "quesma_top_hits_join"
				WHERE "top_hits_rank"<=10
				ORDER BY "aggr__1__2__key_0" ASC, "top_hits_rank" ASC`,
			`
				WITH quesma_top_hits_group_table AS (
				  SELECT sum(count(*)) OVER () AS "aggr__1__count",
					toInt64((toUnixTimestamp64Milli("timestamp")+timeZoneOffset(toTimezone(
					"timestamp", 'Europe/Warsaw'))*1000) / 10800000) AS "aggr__1__2__key_0",
					count(*) AS "aggr__1__2__count"
				  FROM __quesma_table_name
				  WHERE (("timestamp">=fromUnixTimestamp64Milli(1740230608853) AND "timestamp"<=
					fromUnixTimestamp64Milli(1740835408853)) AND ("FlightDelay" __quesma_match '%true%'
					AND "Cancelled" __quesma_match '%true%'))
				  GROUP BY toInt64((toUnixTimestamp64Milli("timestamp")+timeZoneOffset(
					toTimezone("timestamp", 'Europe/Warsaw'))*1000) / 10800000) AS
					"aggr__1__2__key_0"
				  ORDER BY "aggr__1__2__key_0" ASC) ,
				quesma_top_hits_join AS (
				  SELECT "group_table"."aggr__1__count" AS "aggr__1__count",
					"group_table"."aggr__1__2__key_0" AS "aggr__1__2__key_0",
					"group_table"."aggr__1__2__count" AS "aggr__1__2__count",
					"hit_table"."FlightDelay" AS "top_metrics__1__2__5_col_0",
					"hit_table"."timestamp" AS "top_metrics__1__2__5_col_1",
					ROW_NUMBER() OVER (PARTITION BY "group_table"."aggr__1__2__key_0" ORDER BY
					"timestamp" ASC) AS "top_hits_rank"
				  FROM quesma_top_hits_group_table AS "group_table" LEFT OUTER JOIN
					__quesma_table_name AS "hit_table" ON ("group_table"."aggr__1__2__key_0"=
					toInt64((toUnixTimestamp64Milli("timestamp")+timeZoneOffset(toTimezone(
					"timestamp", 'Europe/Warsaw'))*1000) / 10800000))
				  WHERE (("timestamp">=fromUnixTimestamp64Milli(1740230608853) AND "timestamp"<=
					fromUnixTimestamp64Milli(1740835408853)) AND ("FlightDelay" __quesma_match '%true%'
					AND "Cancelled" __quesma_match '%true%')))
				SELECT "aggr__1__count", "aggr__1__2__key_0", "aggr__1__2__count",
				  "top_metrics__1__2__5_col_0", "top_metrics__1__2__5_col_1", "top_hits_rank"
				FROM "quesma_top_hits_join"
				WHERE "top_hits_rank"<=10
				ORDER BY "aggr__1__2__key_0" ASC, "top_hits_rank" ASC`,
			`
				WITH quesma_top_hits_group_table AS (
				  SELECT sum(count(*)) OVER () AS "aggr__1__count",
					toInt64((toUnixTimestamp64Milli("timestamp")+timeZoneOffset(toTimezone(
					"timestamp", 'Europe/Warsaw'))*1000) / 10800000) AS "aggr__1__2__key_0",
					count(*) AS "aggr__1__2__count"
				  FROM __quesma_table_name
				  WHERE (("timestamp">=fromUnixTimestamp64Milli(1740230608853) AND "timestamp"<=
					fromUnixTimestamp64Milli(1740835408853)) AND ("FlightDelay" __quesma_match '%true%'
					AND "Cancelled" __quesma_match '%true%'))
				  GROUP BY toInt64((toUnixTimestamp64Milli("timestamp")+timeZoneOffset(
					toTimezone("timestamp", 'Europe/Warsaw'))*1000) / 10800000) AS
					"aggr__1__2__key_0"
				  ORDER BY "aggr__1__2__key_0" ASC) ,
				quesma_top_hits_join AS (
				  SELECT "group_table"."aggr__1__count" AS "aggr__1__count",
					"group_table"."aggr__1__2__key_0" AS "aggr__1__2__key_0",
					"group_table"."aggr__1__2__count" AS "aggr__1__2__count",
					"hit_table"."Cancelled" AS "top_metrics__1__2__6_col_0",
					"hit_table"."timestamp" AS "top_metrics__1__2__6_col_1",
					ROW_NUMBER() OVER (PARTITION BY "group_table"."aggr__1__2__key_0" ORDER BY
					"timestamp" ASC) AS "top_hits_rank"
				  FROM quesma_top_hits_group_table AS "group_table" LEFT OUTER JOIN
					__quesma_table_name AS "hit_table" ON ("group_table"."aggr__1__2__key_0"=
					toInt64((toUnixTimestamp64Milli("timestamp")+timeZoneOffset(toTimezone(
					"timestamp", 'Europe/Warsaw'))*1000) / 10800000))
				  WHERE (("timestamp">=fromUnixTimestamp64Milli(1740230608853) AND "timestamp"<=
					fromUnixTimestamp64Milli(1740835408853)) AND ("FlightDelay" __quesma_match '%true%'
					AND "Cancelled" __quesma_match '%true%')))
				SELECT "aggr__1__count", "aggr__1__2__key_0", "aggr__1__2__count",
				  "top_metrics__1__2__6_col_0", "top_metrics__1__2__6_col_1", "top_hits_rank"
				FROM "quesma_top_hits_join"
				WHERE "top_hits_rank"<=10
				ORDER BY "aggr__1__2__key_0" ASC, "top_hits_rank" ASC`,
			`
				WITH quesma_top_hits_group_table AS (
				  SELECT sum(count(*)) OVER () AS "aggr__1__count",
					toInt64((toUnixTimestamp64Milli("timestamp")+timeZoneOffset(toTimezone(
					"timestamp", 'Europe/Warsaw'))*1000) / 10800000) AS "aggr__1__2__key_0",
					count(*) AS "aggr__1__2__count"
				  FROM __quesma_table_name
				  WHERE (("timestamp">=fromUnixTimestamp64Milli(1740230608853) AND "timestamp"<=
					fromUnixTimestamp64Milli(1740835408853)) AND ("FlightDelay" __quesma_match '%true%'
					AND "Cancelled" __quesma_match '%true%'))
				  GROUP BY toInt64((toUnixTimestamp64Milli("timestamp")+timeZoneOffset(
					toTimezone("timestamp", 'Europe/Warsaw'))*1000) / 10800000) AS
					"aggr__1__2__key_0"
				  ORDER BY "aggr__1__2__key_0" ASC) ,
				quesma_top_hits_join AS (
				  SELECT "group_table"."aggr__1__count" AS "aggr__1__count",
					"group_table"."aggr__1__2__key_0" AS "aggr__1__2__key_0",
					"group_table"."aggr__1__2__count" AS "aggr__1__2__count",
					"hit_table"."Carrier" AS "top_metrics__1__2__7_col_0",
					"hit_table"."timestamp" AS "top_metrics__1__2__7_col_1",
					ROW_NUMBER() OVER (PARTITION BY "group_table"."aggr__1__2__key_0" ORDER BY
					"timestamp" ASC) AS "top_hits_rank"
				  FROM quesma_top_hits_group_table AS "group_table" LEFT OUTER JOIN
					__quesma_table_name AS "hit_table" ON ("group_table"."aggr__1__2__key_0"=
					toInt64((toUnixTimestamp64Milli("timestamp")+timeZoneOffset(toTimezone(
					"timestamp", 'Europe/Warsaw'))*1000) / 10800000))
				  WHERE (("timestamp">=fromUnixTimestamp64Milli(1740230608853) AND "timestamp"<=
					fromUnixTimestamp64Milli(1740835408853)) AND ("FlightDelay" __quesma_match '%true%'
					AND "Cancelled" __quesma_match '%true%')))
				SELECT "aggr__1__count", "aggr__1__2__key_0", "aggr__1__2__count",
				  "top_metrics__1__2__7_col_0", "top_metrics__1__2__7_col_1", "top_hits_rank"
				FROM "quesma_top_hits_join"
				WHERE "top_hits_rank"<=10
				ORDER BY "aggr__1__2__key_0" ASC, "top_hits_rank" ASC`,
		},
	},
	{ // [3]
		TestName: "Delayed",
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
												"match": {
													"FlightDelay": true
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
									"gte": "2025-02-22T13:23:28.853Z",
									"lte": "2025-03-01T13:23:28.853Z"
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
			"completion_time_in_millis": 1740835409829,
			"expiration_time_in_millis": 1740835469806,
			"id": "FloyNUVsdHI5UktXcnRYWmptRkx2eVEdUEQ3d19oVkxSMEthNU02NjIwRGpkZzo2NjU5Mzc=",
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
						"doc_count": 532
					}
				},
				"hits": {
					"hits": [],
					"max_score": null,
					"total": {
						"relation": "eq",
						"value": 2156
					}
				},
				"timed_out": false,
				"took": 23
			},
			"start_time_in_millis": 1740835409806
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("metric__0-bucket_col_0", int64(532)),
			}},
		},
		// TODO Sprawdz boola
		ExpectedPancakeSQL: `
			SELECT countIf("FlightDelay" __quesma_match true) AS "metric__0-bucket_col_0"
			FROM __quesma_table_name
			WHERE ("timestamp">=fromUnixTimestamp64Milli(1740230608853) AND "timestamp"<=
			  fromUnixTimestamp64Milli(1740835408853))`,
	},
	{ // [4]
		TestName: "Cancelled",
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
												"match": {
													"Cancelled": true
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
									"gte": "2025-02-22T13:23:28.853Z",
									"lte": "2025-03-01T13:23:28.853Z"
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
			"completion_time_in_millis": 1740835409848,
			"expiration_time_in_millis": 1740835469839,
			"id": "FmlHOGM5bEpvU2NlQjNnS09hRmZIdlEdUEQ3d19oVkxSMEthNU02NjIwRGpkZzo2NjU5NDY=",
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
						"doc_count": 278
					}
				},
				"hits": {
					"hits": [],
					"max_score": null,
					"total": {
						"relation": "eq",
						"value": 2156
					}
				},
				"timed_out": false,
				"took": 9
			},
			"start_time_in_millis": 1740835409839
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("metric__0-bucket_col_0", int64(278)),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT countIf("Cancelled" __quesma_match true) AS "metric__0-bucket_col_0"
			FROM __quesma_table_name
			WHERE ("timestamp">=fromUnixTimestamp64Milli(1740230608853) AND "timestamp"<=
			  fromUnixTimestamp64Milli(1740835408853))`,
	},
	{ // [5]
		TestName: "Delayed/Cancelled vs 1 week earlier",
		QueryRequestJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"time_offset_split": {
					"aggs": {},
					"filters": {
						"filters": {
							"0": {
								"range": {
									"timestamp": {
										"format": "strict_date_optional_time",
										"gte": "2025-02-22T13:23:28.853Z",
										"lte": "2025-03-01T13:23:28.853Z"
									}
								}
							},
							"604800000": {
								"range": {
									"timestamp": {
										"format": "strict_date_optional_time",
										"gte": "2025-02-15T13:23:28.853Z",
										"lte": "2025-02-22T13:23:28.853Z"
									}
								}
							}
						}
					}
				}
			},
			"fields": [
				{
					"field": "timestamp",
					"format": "date_time"
				}
			],
			"query": {
				"bool": {
					"filter": [
						{
							"match_phrase": {
								"Cancelled": true
							}
						},
						{
							"bool": {
								"minimum_should_match": 1,
								"should": [
									{
										"bool": {
											"filter": [
												{
													"range": {
														"timestamp": {
															"format": "strict_date_optional_time",
															"gte": "2025-02-22T13:23:28.853Z",
															"lte": "2025-03-01T13:23:28.853Z"
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
														"timestamp": {
															"format": "strict_date_optional_time",
															"gte": "2025-02-15T13:23:28.853Z",
															"lte": "2025-02-22T13:23:28.853Z"
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
			"completion_time_in_millis": 1740835409959,
			"expiration_time_in_millis": 1740835469864,
			"id": "FjhmdTVaN2UtU1lTMEstNW5feExscGcdUEQ3d19oVkxSMEthNU02NjIwRGpkZzo2NjU5NzI=",
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
								"doc_count": 278
							},
							"604800000": {
								"doc_count": 222
							}
						}
					}
				},
				"hits": {
					"hits": [],
					"max_score": null,
					"total": {
						"relation": "eq",
						"value": 500
					}
				},
				"timed_out": false,
				"took": 95
			},
			"start_time_in_millis": 1740835409864
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("filter_0__aggr__time_offset_split__count", int64(278)),
				model.NewQueryResultCol("filter_1__aggr__time_offset_split__count", int64(222)),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT countIf(("timestamp">=fromUnixTimestamp64Milli(1740230608853) AND
			  "timestamp"<=fromUnixTimestamp64Milli(1740835408853))) AS
			  "filter_0__aggr__time_offset_split__count",
			  countIf(("timestamp">=fromUnixTimestamp64Milli(1739625808853) AND "timestamp"
			  <=fromUnixTimestamp64Milli(1740230608853))) AS
			  "filter_1__aggr__time_offset_split__count"
			FROM __quesma_table_name
			WHERE ("Cancelled" __quesma_match true AND (("timestamp">=fromUnixTimestamp64Milli(
			  1740230608853) AND "timestamp"<=fromUnixTimestamp64Milli(1740835408853)) OR (
			  "timestamp">=fromUnixTimestamp64Milli(1739625808853) AND "timestamp"<=
			  fromUnixTimestamp64Milli(1740230608853))))`,
	},
	{ // [6]
		TestName: "Flight count",
		QueryRequestJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"0": {
					"date_histogram": {
						"extended_bounds": {
							"max": 1740815408853,
							"min": 1740790608853
						},
						"field": "timestamp",
						"fixed_interval": "3h",
						"time_zone": "Europe/Warsaw"
					}
				}
			},
			"fields": [
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
									"gte": "2025-02-22T13:23:28.853Z",
									"lte": "2025-03-01T13:23:28.853Z"
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
			"completion_time_in_millis": 1740835409934,
			"expiration_time_in_millis": 1740835469854,
			"id": "FmREMTNHa0lMUTVXVC16VDJkcm5wRXcdUEQ3d19oVkxSMEthNU02NjIwRGpkZzo2NjU5NzA=",
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
								"doc_count": 33,
								"key": 1740794400000,
								"key_as_string": "2025-03-01T02:00:00.000"
							},
							{
								"doc_count": 31,
								"key": 1740805200000,
								"key_as_string": "2025-03-01T05:00:00.000"
							}
						]
					}
				},
				"hits": {
					"hits": [],
					"max_score": null,
					"total": {
						"relation": "eq",
						"value": 2156
					}
				},
				"timed_out": false,
				"took": 80
			},
			"start_time_in_millis": 1740835409854
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__key_0", int64(1740805400000/10800000)),
				model.NewQueryResultCol("aggr__0__count", int64(33)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__key_0", int64(1740819200000/10800000)),
				model.NewQueryResultCol("aggr__0__count", int64(31)),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT toInt64((toUnixTimestamp64Milli("timestamp")+timeZoneOffset(toTimezone(
			  "timestamp", 'Europe/Warsaw'))*1000) / 10800000) AS "aggr__0__key_0",
			  count(*) AS "aggr__0__count"
			FROM __quesma_table_name
			WHERE ("timestamp">=fromUnixTimestamp64Milli(1740230608853) AND "timestamp"<=
			  fromUnixTimestamp64Milli(1740835408853))
			GROUP BY toInt64((toUnixTimestamp64Milli("timestamp")+timeZoneOffset(toTimezone(
			  "timestamp", 'Europe/Warsaw'))*1000) / 10800000) AS "aggr__0__key_0"
			ORDER BY "aggr__0__key_0" ASC`,
	},
	{ // [7]
		TestName: "Delays & Cancellations (request 2/2)",
		QueryRequestJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"0": {
					"aggs": {
						"1-bucket": {
							"filter": {
								"bool": {
									"filter": [],
									"must": [
										{
											"query_string": {
												"analyze_wildcard": true,
												"query": "FlightDelay:true",
												"time_zone": "Europe/Warsaw"
											}
										}
									],
									"must_not": [],
									"should": []
								}
							}
						},
						"2-bucket": {
							"filter": {
								"bool": {
									"filter": [
										{
											"query_string": {
												"query": "*"
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
					"date_histogram": {
						"extended_bounds": {
							"max": 1740245408853,
							"min": 1740220608853
						},
						"field": "timestamp",
						"fixed_interval": "3h",
						"time_zone": "Europe/Warsaw"
					}
				}
			},
			"fields": [
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
									"gte": "2025-02-22T13:23:28.853Z",
									"lte": "2025-03-01T13:23:28.853Z"
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
			"completion_time_in_millis": 1740835409938,
			"expiration_time_in_millis": 1740835469853,
			"id": "Fm0xWUhXbHVJUlBTTkpoeEI0ZlJJWFEdUEQ3d19oVkxSMEthNU02NjIwRGpkZzo2NjU5NjM=",
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
									"doc_count": 1
								},
								"2-bucket": {
									"doc_count": 6
								},
								"doc_count": 6,
								"key": 1740222000000,
								"key_as_string": "2025-02-22T11:00:00.000"
							},
							{
								"1-bucket": {
									"doc_count": 7
								},
								"2-bucket": {
									"doc_count": 41
								},
								"doc_count": 41,
								"key": 1740232800000,
								"key_as_string": "2025-02-22T14:00:00.000"
							},
							{
								"1-bucket": {
									"doc_count": 0
								},
								"2-bucket": {
									"doc_count": 0
								},
								"doc_count": 0,
								"key": 1740243600000,
								"key_as_string": "2025-02-22T17:00:00.000"
							}
						]
					}
				},
				"hits": {
					"hits": [],
					"max_score": null,
					"total": {
						"relation": "eq",
						"value": 2156
					}
				},
				"timed_out": false,
				"took": 85
			},
			"start_time_in_millis": 1740835409853
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__key_0", int64(1740232000000/10800000)),
				model.NewQueryResultCol("aggr__0__count", int64(6)),
				model.NewQueryResultCol("metric__0__2-bucket_col_0", int64(6)),
				model.NewQueryResultCol("metric__0__1-bucket_col_0", int64(1)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__key_0", int64(1740242800000/10800000)),
				model.NewQueryResultCol("aggr__0__count", int64(41)),
				model.NewQueryResultCol("metric__0__2-bucket_col_0", int64(41)),
				model.NewQueryResultCol("metric__0__1-bucket_col_0", int64(7)),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT toInt64((toUnixTimestamp64Milli("timestamp")+timeZoneOffset(toTimezone(
			  "timestamp", 'Europe/Warsaw'))*1000) / 10800000) AS "aggr__0__key_0",
			  count(*) AS "aggr__0__count",
			  countIf("FlightDelay" __quesma_match '%true%') AS "metric__0__1-bucket_col_0",
			  countIf("__quesma_fulltext_field_name" __quesma_match '%') AS "metric__0__2-bucket_col_0"
			FROM __quesma_table_name
			WHERE ("timestamp">=fromUnixTimestamp64Milli(1740230608853) AND "timestamp"<=
			  fromUnixTimestamp64Milli(1740835408853))
			GROUP BY toInt64((toUnixTimestamp64Milli("timestamp")+timeZoneOffset(toTimezone(
			  "timestamp", 'Europe/Warsaw'))*1000) / 10800000) AS "aggr__0__key_0"
			ORDER BY "aggr__0__key_0" ASC`,
	},
	{ // [8]
		TestName: "Most delayed cities",
		QueryRequestJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"0": {
					"aggs": {
						"1-bucket": {
							"filter": {
								"bool": {
									"filter": [
										{
											"bool": {
												"minimum_should_match": 1,
												"should": [
													{
														"match": {
															"FlightDelay": true
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
						"3-bucket": {
							"filter": {
								"bool": {
									"filter": [
										{
											"bool": {
												"minimum_should_match": 1,
												"should": [
													{
														"match": {
															"Cancelled": true
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
					"terms": {
						"field": "OriginCityName",
						"order": {
							"_key": "asc"
						},
						"size": 1000
					}
				}
			},
			"fields": [
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
									"gte": "2025-02-22T13:23:28.853Z",
									"lte": "2025-03-01T13:23:28.853Z"
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
			"completion_time_in_millis": 1740835410003,
			"expiration_time_in_millis": 1740835469872,
			"id": "FkdHVHBPbjc2UVRHX0M5eDB0bUJocEEdUEQ3d19oVkxSMEthNU02NjIwRGpkZzo2NjU5ODA=",
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
									"doc_count": 5
								},
								"3-bucket": {
									"doc_count": 2
								},
								"doc_count": 18,
								"key": "Abu Dhabi"
							},
							{
								"1-bucket": {
									"doc_count": 0
								},
								"3-bucket": {
									"doc_count": 0
								},
								"doc_count": 4,
								"key": "Atlanta"
							},
							{
								"1-bucket": {
									"doc_count": 2
								},
								"3-bucket": {
									"doc_count": 0
								},
								"doc_count": 5,
								"key": "Baltimore"
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
						"value": 2156
					}
				},
				"timed_out": false,
				"took": 131
			},
			"start_time_in_millis": 1740835409872
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__parent_count", int64(27)),
				model.NewQueryResultCol("aggr__0__key_0", "Abu Dhabi"),
				model.NewQueryResultCol("aggr__0__count", int64(18)),
				model.NewQueryResultCol("metric__0__3-bucket_col_0", int64(2)),
				model.NewQueryResultCol("metric__0__1-bucket_col_0", int64(5)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__parent_count", int64(27)),
				model.NewQueryResultCol("aggr__0__key_0", "Atlanta"),
				model.NewQueryResultCol("aggr__0__count", int64(4)),
				model.NewQueryResultCol("metric__0__3-bucket_col_0", int64(0)),
				model.NewQueryResultCol("metric__0__1-bucket_col_0", int64(0)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__parent_count", int64(27)),
				model.NewQueryResultCol("aggr__0__key_0", "Baltimore"),
				model.NewQueryResultCol("aggr__0__count", int64(5)),
				model.NewQueryResultCol("metric__0__3-bucket_col_0", int64(0)),
				model.NewQueryResultCol("metric__0__1-bucket_col_0", int64(2)),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT sum(count(*)) OVER () AS "aggr__0__parent_count",
			  "OriginCityName" AS "aggr__0__key_0", count(*) AS "aggr__0__count",
			  countIf("FlightDelay" __quesma_match true) AS "metric__0__1-bucket_col_0",
			  countIf("Cancelled" __quesma_match true) AS "metric__0__3-bucket_col_0"
			FROM __quesma_table_name
			WHERE ("timestamp">=fromUnixTimestamp64Milli(1740230608853) AND "timestamp"<=
			  fromUnixTimestamp64Milli(1740835408853))
			GROUP BY "OriginCityName" AS "aggr__0__key_0"
			ORDER BY "aggr__0__key_0" ASC
			LIMIT 1001`,
	},
	{ // [9]
		TestName: "Delay Type",
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
								"extended_bounds": {
									"max": 1740245408853,
									"min": 1740222000000
								},
								"field": "timestamp",
								"fixed_interval": "3h",
								"time_zone": "Europe/Warsaw"
							}
						}
					},
					"terms": {
						"field": "FlightDelayType",
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
									"gte": "2025-02-22T13:23:28.853Z",
									"lte": "2025-03-01T13:23:28.853Z"
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
			"completion_time_in_millis": 1740835409984,
			"expiration_time_in_millis": 1740835469868,
			"id": "FjBEMTh1WDNGU3ktdzUwRnNBZzh1QVEdUEQ3d19oVkxSMEthNU02NjIwRGpkZzo2NjU5NzU=",
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
											"doc_count": 0,
											"key": 1740222000000,
											"key_as_string": "2025-02-22T11:00:00.000"
										},
										{
											"doc_count": 0,
											"key": 1740232800000,
											"key_as_string": "2025-02-22T14:00:00.000"
										},
										{
											"doc_count": 1,
											"key": 1740243600000,
											"key_as_string": "2025-02-22T17:00:00.000"
										}
									]
								},
								"doc_count": 151,
								"key": "Late Aircraft Delay"
							},
							{
								"1": {
									"buckets": [
										{
											"doc_count": 1,
											"key": 1740222000000,
											"key_as_string": "2025-02-22T11:00:00.000"
										},
										{
											"doc_count": 0,
											"key": 1740232800000,
											"key_as_string": "2025-02-22T14:00:00.000"
										},
										{
											"doc_count": 1,
											"key": 1740243600000,
											"key_as_string": "2025-02-22T17:00:00.000"
										}
									]
								},
								"doc_count": 141,
								"key": "NAS Delay"
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
						"value": 2156
					}
				},
				"timed_out": false,
				"took": 116
			},
			"start_time_in_millis": 1740835409868
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__parent_count", int64(292)),
				model.NewQueryResultCol("aggr__0__key_0", "Late Aircraft Delay"),
				model.NewQueryResultCol("aggr__0__count", int64(151)),
				model.NewQueryResultCol("aggr__0__1__key_0", int64(1740252000000/10800000)),
				model.NewQueryResultCol("aggr__0__1__count", int64(1)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__parent_count", int64(292)),
				model.NewQueryResultCol("aggr__0__key_0", "NAS Delay"),
				model.NewQueryResultCol("aggr__0__count", int64(141)),
				model.NewQueryResultCol("aggr__0__1__key_0", int64(1740232000000/10800000)),
				model.NewQueryResultCol("aggr__0__1__count", int64(1)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__parent_count", int64(292)),
				model.NewQueryResultCol("aggr__0__key_0", "NAS Delay"),
				model.NewQueryResultCol("aggr__0__count", int64(141)),
				model.NewQueryResultCol("aggr__0__1__key_0", int64(1740253600000/10800000)),
				model.NewQueryResultCol("aggr__0__1__count", int64(1)),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT "aggr__0__parent_count", "aggr__0__key_0", "aggr__0__count",
			  "aggr__0__1__key_0", "aggr__0__1__count"
			FROM (
			  SELECT "aggr__0__parent_count", "aggr__0__key_0", "aggr__0__count",
				"aggr__0__1__key_0", "aggr__0__1__count",
				dense_rank() OVER (ORDER BY "aggr__0__count" DESC, "aggr__0__key_0" ASC) AS
				"aggr__0__order_1_rank",
				dense_rank() OVER (PARTITION BY "aggr__0__key_0" ORDER BY
				"aggr__0__1__key_0" ASC) AS "aggr__0__1__order_1_rank"
			  FROM (
				SELECT sum(count(*)) OVER () AS "aggr__0__parent_count",
				  "FlightDelayType" AS "aggr__0__key_0",
				  sum(count(*)) OVER (PARTITION BY "aggr__0__key_0") AS "aggr__0__count",
				  toInt64((toUnixTimestamp64Milli("timestamp")+timeZoneOffset(toTimezone(
				  "timestamp", 'Europe/Warsaw'))*1000) / 10800000) AS "aggr__0__1__key_0",
				  count(*) AS "aggr__0__1__count"
				FROM __quesma_table_name
				WHERE ("timestamp">=fromUnixTimestamp64Milli(1740230608853) AND "timestamp"
				  <=fromUnixTimestamp64Milli(1740835408853))
				GROUP BY "FlightDelayType" AS "aggr__0__key_0",
				  toInt64((toUnixTimestamp64Milli("timestamp")+timeZoneOffset(toTimezone(
				  "timestamp", 'Europe/Warsaw'))*1000) / 10800000) AS "aggr__0__1__key_0"))
			WHERE "aggr__0__order_1_rank"<=11
			ORDER BY "aggr__0__order_1_rank" ASC, "aggr__0__1__order_1_rank" ASC`,
	},
	{ // [10]
		TestName: "Count of records by DestWeather (bottom right)",
		QueryRequestJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"0": {
					"terms": {
						"field": "DestWeather",
						"order": {
							"_count": "desc"
						},
						"shard_size": 25,
						"size": 2
					}
				}
			},
			"fields": [
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
									"gte": "2025-02-22T13:23:28.853Z",
									"lte": "2025-03-01T13:23:28.853Z"
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
			"completion_time_in_millis": 1740835409934,
			"expiration_time_in_millis": 1740835469872,
			"id": "FlBvd21tTmJWUWhlQWp2ZllkQW5JalEdUEQ3d19oVkxSMEthNU02NjIwRGpkZzo2NjU5Nzg=",
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
								"doc_count": 420,
								"key": "Rain"
							},
							{
								"doc_count": 402,
								"key": "Clear"
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
						"value": 2156
					}
				},
				"timed_out": false,
				"took": 62
			},
			"start_time_in_millis": 1740835409872
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__parent_count", int64(822)),
				model.NewQueryResultCol("aggr__0__key_0", "Rain"),
				model.NewQueryResultCol("aggr__0__count", int64(420)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__parent_count", int64(822)),
				model.NewQueryResultCol("aggr__0__key_0", "Clear"),
				model.NewQueryResultCol("aggr__0__count", int64(402)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__parent_count", int64(822)),
				model.NewQueryResultCol("aggr__0__key_0", "Cloudy"),
				model.NewQueryResultCol("aggr__0__count", int64(373)),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT sum(count(*)) OVER () AS "aggr__0__parent_count",
			  "DestWeather" AS "aggr__0__key_0", count(*) AS "aggr__0__count"
			FROM __quesma_table_name
			WHERE ("timestamp">=fromUnixTimestamp64Milli(1740230608853) AND "timestamp"<=
			  fromUnixTimestamp64Milli(1740835408853))
			GROUP BY "DestWeather" AS "aggr__0__key_0"
			ORDER BY "aggr__0__count" DESC, "aggr__0__key_0" ASC
			LIMIT 3`,
	},
	{ // [11]
		TestName: "Delay Type",
		QueryRequestJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"0": {
					"terms": {
						"field": "FlightDelayType",
						"order": {
							"_count": "desc"
						},
						"shard_size": 25,
						"size": 2
					}
				}
			},
			"fields": [
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
									"gte": "2025-02-22T13:23:28.853Z",
									"lte": "2025-03-01T13:23:28.853Z"
								}
							}
						}
					],
					"must": [],
					"must_not": [
						{
							"match_phrase": {
								"FlightDelayType": "No Delay"
							}
						}
					],
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
			"completion_time_in_millis": 1740835409946,
			"expiration_time_in_millis": 1740835469890,
			"id": "FkpIMlFDS0hpU0ZtN0dFaS1ocXNQUEEdUEQ3d19oVkxSMEthNU02NjIwRGpkZzo2NjU5ODQ=",
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
								"doc_count": 151,
								"key": "Late Aircraft Delay"
							},
							{
								"doc_count": 141,
								"key": "NAS Delay"
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
						"value": 532
					}
				},
				"timed_out": false,
				"took": 56
			},
			"start_time_in_millis": 1740835409890
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__parent_count", int64(292)),
				model.NewQueryResultCol("aggr__0__key_0", "Late Aircraft Delay"),
				model.NewQueryResultCol("aggr__0__count", int64(151)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__parent_count", int64(292)),
				model.NewQueryResultCol("aggr__0__key_0", "NAS Delay"),
				model.NewQueryResultCol("aggr__0__count", int64(141)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__parent_count", int64(292)),
				model.NewQueryResultCol("aggr__0__key_0", "Carrier Delay"),
				model.NewQueryResultCol("aggr__0__count", int64(1)),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT sum(count(*)) OVER () AS "aggr__0__parent_count",
			  "FlightDelayType" AS "aggr__0__key_0", count(*) AS "aggr__0__count"
			FROM __quesma_table_name
			WHERE (("timestamp">=fromUnixTimestamp64Milli(1740230608853) AND "timestamp"<=
			  fromUnixTimestamp64Milli(1740835408853)) AND NOT ("FlightDelayType"
			  __quesma_match '%No Delay%'))
			GROUP BY "FlightDelayType" AS "aggr__0__key_0"
			ORDER BY "aggr__0__count" DESC, "aggr__0__key_0" ASC
			LIMIT 3`,
	},
	{ // [12]
		TestName: "Origin Time Delayed",
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
								"field": "OriginLocation"
							}
						},
						"sum_of_FlightDelayMin": {
							"sum": {
								"field": "FlightDelayMin"
							}
						}
					},
					"geotile_grid": {
						"bounds": {
							"bottom_right": [
								-90,
								40.9799
							],
							"top_left": [
								-135,
								55.77657
							]
						},
						"field": "OriginLocation",
						"precision": 7,
						"shard_size": 65535,
						"size": 65535
					}
				}
			},
			"fields": [
				{
					"field": "timestamp",
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
											"field": "OriginLocation"
										}
									},
									{
										"geo_bounding_box": {
											"OriginLocation": {
												"bottom_right": [
													-87.1875,
													40.9799
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
									"gte": "2025-02-22T13:23:28.853Z",
									"lte": "2025-03-01T13:23:28.853Z"
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
			"completion_time_in_millis": 1740835410343,
			"expiration_time_in_millis": 1740835470329,
			"id": "Fl85b0RzWEwxUklhODdmc1FNSldJekEdUEQ3d19oVkxSMEthNU02NjIwRGpkZzo2NjYxMDg=",
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
								"doc_count": 25,
								"gridCentroid": {
									"count": 25,
									"location": {
										"lat": 53.30969999078661,
										"lon": -113.58000185340643
									}
								},
								"key": "7/23/41",
								"sum_of_FlightDelayMin": {
									"value": 870.0
								}
							},
							{
								"doc_count": 21,
								"gridCentroid": {
									"count": 21,
									"location": {
										"lat": 49.909999812953174,
										"lon": -97.23989870399237
									}
								},
								"key": "7/29/43",
								"sum_of_FlightDelayMin": {
									"value": 1185.0
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
				"took": 14
			},
			"start_time_in_millis": 1740835410329
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__gridSplit__key_0", 23.0),
				model.NewQueryResultCol("aggr__gridSplit__key_1", 41.0),
				model.NewQueryResultCol("aggr__gridSplit__count", int64(25)),
				model.NewQueryResultCol("metric__gridSplit__gridCentroid_col_0", 53.30969999078661),
				model.NewQueryResultCol("metric__gridSplit__gridCentroid_col_1", -113.58000185340643),
				model.NewQueryResultCol("metric__gridSplit__gridCentroid_col_2", int64(25)),
				model.NewQueryResultCol("metric__gridSplit__sum_of_FlightDelayMin_col_0", 870.0),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__gridSplit__key_0", 29.0),
				model.NewQueryResultCol("aggr__gridSplit__key_1", 43.0),
				model.NewQueryResultCol("aggr__gridSplit__count", int64(21)),
				model.NewQueryResultCol("metric__gridSplit__gridCentroid_col_0", 49.909999812953174),
				model.NewQueryResultCol("metric__gridSplit__gridCentroid_col_1", -97.23989870399237),
				model.NewQueryResultCol("metric__gridSplit__gridCentroid_col_2", int64(21)),
				model.NewQueryResultCol("metric__gridSplit__sum_of_FlightDelayMin_col_0", 1185.0),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT FLOOR(((__quesma_geo_lon("OriginLocation")+180)/360)*POWER(2, 7)) AS "aggr__gridSplit__key_0",
			  FLOOR((1-LOG(TAN(RADIANS(__quesma_geo_lat("OriginLocation")))+(1/COS(RADIANS(
			  __quesma_geo_lat("OriginLocation")))))/PI())/2*POWER(2, 7))
			  AS "aggr__gridSplit__key_1",
			  count(*) AS "aggr__gridSplit__count",
			  avgOrNull(__quesma_geo_lat("originlocation")) AS "metric__gridSplit__gridCentroid_col_0",
			  avgOrNull(__quesma_geo_lon("originlocation")) AS "metric__gridSplit__gridCentroid_col_1",
			  count(*) AS "metric__gridSplit__gridCentroid_col_2",
			  sumOrNull("FlightDelayMin") AS "metric__gridSplit__sum_of_FlightDelayMin_col_0"
			FROM __quesma_table_name
			WHERE ("OriginLocation" IS NOT NULL AND ("timestamp">=fromUnixTimestamp64Milli(
			  1740230608853) AND "timestamp"<=fromUnixTimestamp64Milli(1740835408853)))
			GROUP BY FLOOR(((__quesma_geo_lon("OriginLocation")+180)/360)*POWER(2, 7)) AS "aggr__gridSplit__key_0",
			  FLOOR((1-LOG(TAN(RADIANS(__quesma_geo_lat("OriginLocation")))+(1/COS(RADIANS(
			  __quesma_geo_lat("OriginLocation")))))/PI())/2*POWER(2, 7)) AS "aggr__gridSplit__key_1"
			ORDER BY "aggr__gridSplit__count" DESC, "aggr__gridSplit__key_0" ASC,
              "aggr__gridSplit__key_1" ASC
            LIMIT 65535`,
	},
	{ // [13]
		TestName: "Delay Buckets",
		QueryRequestJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"0": {
					"histogram": {
						"field": "FlightDelayMin",
						"interval": 1,
						"min_doc_count": 1
					}
				}
			},
			"fields": [
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
									"gte": "2025-02-22T13:23:28.853Z",
									"lte": "2025-03-01T13:23:28.853Z"
								}
							}
						}
					],
					"must": [],
					"must_not": [
						{
							"match_phrase": {
								"FlightDelayMin": {
									"query": 0
								}
							}
						}
					],
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
			"completion_time_in_millis": 1740835410335,
			"expiration_time_in_millis": 1740835470325,
			"id": "FlVpc2s0cTkwU3pLZU5WVmxQSTNxLWcdUEQ3d19oVkxSMEthNU02NjIwRGpkZzo2NjYxMDM=",
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
								"doc_count": 18,
								"key": 15.0
							},
							{
								"doc_count": 32,
								"key": 45.0
							}
						]
					}
				},
				"hits": {
					"hits": [],
					"max_score": null,
					"total": {
						"relation": "eq",
						"value": 532
					}
				},
				"timed_out": false,
				"took": 10
			},
			"start_time_in_millis": 1740835410325
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__key_0", 15.0),
				model.NewQueryResultCol("aggr__0__count", int64(18)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__key_0", 45.0),
				model.NewQueryResultCol("aggr__0__count", int64(32)),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT "FlightDelayMin" AS "aggr__0__key_0", count(*) AS "aggr__0__count"
			FROM __quesma_table_name
			WHERE (("timestamp">=fromUnixTimestamp64Milli(1740230608853) AND "timestamp"<=
			  fromUnixTimestamp64Milli(1740835408853)) AND NOT ("FlightDelayMin" __quesma_match 0))
			GROUP BY "FlightDelayMin" AS "aggr__0__key_0"
			ORDER BY "aggr__0__key_0" ASC`,
	},
	{ // [14]
		TestName: "TODO Airport Connections (Hover Over Airport)",
		QueryRequestJson: `
		{
			"aggs": {
				"origins": {
					"aggs": {
						"distinations": {
							"aggs": {
								"destLocation": {
									"top_hits": {
										"_source": {
											"includes": [
												"DestLocation"
											]
										},
										"size": 1
									}
								}
							},
							"terms": {
								"field": "DestAirportID",
								"size": 10000
							}
						},
						"originLocation": {
							"top_hits": {
								"_source": {
									"includes": [
										"OriginLocation",
										"Origin"
									]
								},
								"size": 1
							}
						}
					},
					"terms": {
						"field": "OriginAirportID",
						"size": 10000
					}
				}
			},
			"query": {
				"bool": {
					"filter": [],
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
			"size": 0,
			"track_total_hits": true
		}`,
		ExpectedResponse: `
		{
			"completion_time_in_millis": 1740837138628,
			"expiration_time_in_millis": 1741269138528,
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
					"origins": {
						"buckets": [
							{
								"distinations": {
									"buckets": [
										{
											"destLocation": {
												"hits": {
													"hits": [
														{
															"_id": "Tw79TJUB2-926WOCGLoQ",
															"_index": "__quesma_table_name",
															"_score": 1.0,
															"_source": {
																"DestLocation": {
																	"lat": -34.8222,
																	"lon": -58.5358
																}
															}
														}
													],
													"max_score": 1.0,
													"total": {
														"relation": "eq",
														"value": 21
													}
												}
											},
											"doc_count": 21,
											"key": "EZE"
										},
										{
											"destLocation": {
												"hits": {
													"hits": [
														{
															"_id": "Nw79TJUB2-926WOCGr8B",
															"_index": "__quesma_table_name",
															"_score": 1.0,
															"_source": {
																"DestLocation": {
																	"lat": -0.129166667,
																	"lon": -78.3575
																}
															}
														}
													],
													"max_score": 1.0,
													"total": {
														"relation": "eq",
														"value": 12
													}
												}
											},
											"doc_count": 12,
											"key": "UIO"
										}
									],
									"doc_count_error_upper_bound": 0,
									"sum_other_doc_count": 250
								},
								"doc_count": 283,
								"key": "UIO",
								"originLocation": {
									"hits": {
										"hits": [
											{
												"_id": "Tw79TJUB2-926WOCGLoQ",
												"_index": "__quesma_table_name",
												"_score": 1.0,
												"_source": {
													"Origin": "Mariscal Sucre International Airport",
													"OriginLocation": {
														"lat": -0.129166667,
														"lon": -78.3575
													}
												}
											}
										],
										"max_score": 1.0,
										"total": {
											"relation": "eq",
											"value": 283
										}
									}
								}
							},
							{
								"distinations": {
									"buckets": [
										{
											"destLocation": {
												"hits": {
													"hits": [
														{
															"_id": "9Q79TJUB2-926WOCGr8B",
															"_index": "__quesma_table_name",
															"_score": 1.0,
															"_source": {
																"DestLocation": {
																	"lat": 45.47060013,
																	"lon": -73.74079895
																}
															}
														}
													],
													"max_score": 1.0,
													"total": {
														"relation": "eq",
														"value": 11
													}
												}
											},
											"doc_count": 11,
											"key": "YUL"
										},
										{
											"destLocation": {
												"hits": {
													"hits": [
														{
															"_id": "AQ79TJUB2-926WOCGLsQ",
															"_index": "__quesma_table_name",
															"_score": 1.0,
															"_source": {
																"DestLocation": {
																	"lat": -34.8222,
																	"lon": -58.5358
																}
															}
														}
													],
													"max_score": 1.0,
													"total": {
														"relation": "eq",
														"value": 10
													}
												}
											},
											"doc_count": 10,
											"key": "EZE"
										}
									],
									"doc_count_error_upper_bound": 0,
									"sum_other_doc_count": 236
								},
								"doc_count": 257,
								"key": "EZE",
								"originLocation": {
									"hits": {
										"hits": [
											{
												"_id": "cg79TJUB2-926WOCGLoQ",
												"_index": "__quesma_table_name",
												"_score": 1.0,
												"_source": {
													"Origin": "Ministro Pistarini International Airport",
													"OriginLocation": {
														"lat": -34.8222,
														"lon": -58.5358
													}
												}
											}
										],
										"max_score": 1.0,
										"total": {
											"relation": "eq",
											"value": 257
										}
									}
								}
							}
						],
						"doc_count_error_upper_bound": 0,
						"sum_other_doc_count": 1460
					}
				},
				"hits": {
					"hits": [],
					"max_score": null,
					"total": {
						"relation": "eq",
						"value": 2000
					}
				},
				"timed_out": false,
				"took": 100
			},
			"start_time_in_millis": 1740837138528
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__origins__parent_count", int64(2000)),
				model.NewQueryResultCol("aggr__origins__key_0", "UIO"),
				model.NewQueryResultCol("aggr__origins__count", int64(283)),
				model.NewQueryResultCol("aggr__origins__distinations__parent_count", int64(283)),
				model.NewQueryResultCol("aggr__origins__distinations__key_0", "EZE"),
				model.NewQueryResultCol("aggr__origins__distinations__count", int64(21)),
				model.NewQueryResultCol("top_hits__origins__distinations__destLocation_col_0", model.JsonMap{"lat": -34.8222, "lon": -58.5358}),
				model.NewQueryResultCol("top_hits_rank", int64(1)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__origins__parent_count", int64(2000)),
				model.NewQueryResultCol("aggr__origins__key_0", "UIO"),
				model.NewQueryResultCol("aggr__origins__count", int64(283)),
				model.NewQueryResultCol("aggr__origins__distinations__parent_count", int64(283)),
				model.NewQueryResultCol("aggr__origins__distinations__key_0", "UIO"),
				model.NewQueryResultCol("aggr__origins__distinations__count", int64(12)),
				model.NewQueryResultCol("top_hits__origins__distinations__destLocation_col_0", model.JsonMap{"lat": -0.129167, "lon": -78.3575}),
				model.NewQueryResultCol("top_hits_rank", int64(1)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__origins__parent_count", int64(2000)),
				model.NewQueryResultCol("aggr__origins__key_0", "EZE"),
				model.NewQueryResultCol("aggr__origins__count", int64(257)),
				model.NewQueryResultCol("aggr__origins__distinations__parent_count", int64(257)),
				model.NewQueryResultCol("aggr__origins__distinations__key_0", "YUL"),
				model.NewQueryResultCol("aggr__origins__distinations__count", int64(11)),
				model.NewQueryResultCol("top_hits__origins__distinations__destLocation_col_0", model.JsonMap{"lat": 45.470600, "lon": -73.740799}),
				model.NewQueryResultCol("top_hits_rank", int64(1)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__origins__parent_count", int64(2000)),
				model.NewQueryResultCol("aggr__origins__key_0", "EZE"),
				model.NewQueryResultCol("aggr__origins__count", int64(257)),
				model.NewQueryResultCol("aggr__origins__distinations__parent_count", int64(257)),
				model.NewQueryResultCol("aggr__origins__distinations__key_0", "EZE"),
				model.NewQueryResultCol("aggr__origins__distinations__count", int64(10)),
				model.NewQueryResultCol("top_hits__origins__distinations__destLocation_col_0", model.JsonMap{"lat": -34.822200, "lon": -58.535800}),
				model.NewQueryResultCol("top_hits_rank", int64(1)),
			}},
		},
		ExpectedAdditionalPancakeResults: [][]model.QueryResultRow{
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("aggr__origins__parent_count", int64(2000)),
					model.NewQueryResultCol("aggr__origins__key_0", "UIO"),
					model.NewQueryResultCol("aggr__origins__count", int64(283)),
					model.NewQueryResultCol("aggr__origins__distinations__parent_count", int64(283)),
					model.NewQueryResultCol("aggr__origins__distinations__key_0", "EZE"),
					model.NewQueryResultCol("aggr__origins__distinations__count", int64(21)),
					model.NewQueryResultCol("top_hits__origins__originLocation_col_0", model.JsonMap{"lat": -0.129167, "lon": -78.3575}),
					model.NewQueryResultCol("top_hits__origins__originLocation_col_1", "Mariscal Sucre International Airport"),
					model.NewQueryResultCol("top_hits_rank", int64(1)),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("aggr__origins__parent_count", int64(2000)),
					model.NewQueryResultCol("aggr__origins__key_0", "EZE"),
					model.NewQueryResultCol("aggr__origins__count", int64(257)),
					model.NewQueryResultCol("aggr__origins__distinations__parent_count", int64(257)),
					model.NewQueryResultCol("aggr__origins__distinations__key_0", "YUL"),
					model.NewQueryResultCol("aggr__origins__distinations__count", int64(11)),
					model.NewQueryResultCol("top_hits__origins__originLocation_col_0", model.JsonMap{"lat": -34.822200, "lon": -58.535800}),
					model.NewQueryResultCol("top_hits__origins__originLocation_col_1", "Ministro Pistarini International Airport"),
					model.NewQueryResultCol("top_hits_rank", int64(1)),
				}},
			},
		},
		ExpectedPancakeSQL: `
			WITH quesma_top_hits_group_table AS (
			  SELECT "aggr__origins__parent_count", "aggr__origins__key_0",
				"aggr__origins__count", "aggr__origins__distinations__parent_count",
				"aggr__origins__distinations__key_0", "aggr__origins__distinations__count",
				"aggr__origins__order_1_rank", "aggr__origins__distinations__order_1_rank"
			  FROM (
				SELECT "aggr__origins__parent_count", "aggr__origins__key_0",
				  "aggr__origins__count", "aggr__origins__distinations__parent_count",
				  "aggr__origins__distinations__key_0",
				  "aggr__origins__distinations__count",
				  dense_rank() OVER (ORDER BY "aggr__origins__count" DESC,
				  "aggr__origins__key_0" ASC) AS "aggr__origins__order_1_rank",
				  dense_rank() OVER (PARTITION BY "aggr__origins__key_0" ORDER BY
				  "aggr__origins__distinations__count" DESC,
				  "aggr__origins__distinations__key_0" ASC) AS
				  "aggr__origins__distinations__order_1_rank"
				FROM (
				  SELECT sum(count(*)) OVER () AS "aggr__origins__parent_count",
					"OriginAirportID" AS "aggr__origins__key_0",
					sum(count(*)) OVER (PARTITION BY "aggr__origins__key_0") AS
					"aggr__origins__count",
					sum(count(*)) OVER (PARTITION BY "aggr__origins__key_0") AS
					"aggr__origins__distinations__parent_count",
					"DestAirportID" AS "aggr__origins__distinations__key_0",
					count(*) AS "aggr__origins__distinations__count"
				  FROM __quesma_table_name
				  GROUP BY "OriginAirportID" AS "aggr__origins__key_0",
					"DestAirportID" AS "aggr__origins__distinations__key_0"))
			  WHERE ("aggr__origins__order_1_rank"<=10001 AND
				"aggr__origins__distinations__order_1_rank"<=10001)
			  ORDER BY "aggr__origins__order_1_rank" ASC,
				"aggr__origins__distinations__order_1_rank" ASC) ,
			quesma_top_hits_join AS (
			  SELECT "group_table"."aggr__origins__parent_count" AS
				"aggr__origins__parent_count",
				"group_table"."aggr__origins__key_0" AS "aggr__origins__key_0",
				"group_table"."aggr__origins__count" AS "aggr__origins__count",
				"group_table"."aggr__origins__distinations__parent_count" AS
				"aggr__origins__distinations__parent_count",
				"group_table"."aggr__origins__distinations__key_0" AS
				"aggr__origins__distinations__key_0",
				"group_table"."aggr__origins__distinations__count" AS
				"aggr__origins__distinations__count",
				"hit_table"."DestLocation" AS
				"top_hits__origins__distinations__destLocation_col_0",
				ROW_NUMBER() OVER (PARTITION BY "group_table"."aggr__origins__key_0",
				"group_table"."aggr__origins__distinations__key_0") AS "top_hits_rank",
				"group_table"."aggr__origins__order_1_rank" AS "aggr__origins__order_1_rank"
				,
				"group_table"."aggr__origins__distinations__order_1_rank" AS
				"aggr__origins__distinations__order_1_rank"
			  FROM quesma_top_hits_group_table AS "group_table" LEFT OUTER JOIN
				__quesma_table_name AS "hit_table" ON (("group_table"."aggr__origins__key_0"
				="hit_table"."OriginAirportID" AND
				"group_table"."aggr__origins__distinations__key_0"=
				"hit_table"."DestAirportID")))
			SELECT "aggr__origins__parent_count", "aggr__origins__key_0",
			  "aggr__origins__count", "aggr__origins__distinations__parent_count",
			  "aggr__origins__distinations__key_0", "aggr__origins__distinations__count",
			  "top_hits__origins__distinations__destLocation_col_0", "top_hits_rank"
			FROM "quesma_top_hits_join"
			WHERE "top_hits_rank"<=1
			ORDER BY "aggr__origins__order_1_rank" ASC,
			  "aggr__origins__distinations__order_1_rank" ASC, "top_hits_rank" ASC`,
		ExpectedAdditionalPancakeSQLs: []string{`
			WITH quesma_top_hits_group_table AS (
			  SELECT sum(count(*)) OVER () AS "aggr__origins__parent_count",
				"OriginAirportID" AS "aggr__origins__key_0",
				count(*) AS "aggr__origins__count"
			  FROM __quesma_table_name
			  GROUP BY "OriginAirportID" AS "aggr__origins__key_0"
			  ORDER BY "aggr__origins__count" DESC, "aggr__origins__key_0" ASC
			  LIMIT 10001) ,
			quesma_top_hits_join AS (
			  SELECT "group_table"."aggr__origins__parent_count" AS
				"aggr__origins__parent_count",
				"group_table"."aggr__origins__key_0" AS "aggr__origins__key_0",
				"group_table"."aggr__origins__count" AS "aggr__origins__count",
				"hit_table"."OriginLocation" AS "top_hits__origins__originLocation_col_0",
				"hit_table"."Origin" AS "top_hits__origins__originLocation_col_1",
				ROW_NUMBER() OVER (PARTITION BY "group_table"."aggr__origins__key_0") AS
				"top_hits_rank"
			  FROM quesma_top_hits_group_table AS "group_table" LEFT OUTER JOIN
				__quesma_table_name AS "hit_table" ON ("group_table"."aggr__origins__key_0"=
				"hit_table"."OriginAirportID"))
			SELECT "aggr__origins__parent_count", "aggr__origins__key_0",
			  "aggr__origins__count", "top_hits__origins__originLocation_col_0",
			  "top_hits__origins__originLocation_col_1", "top_hits_rank"
			FROM "quesma_top_hits_join"
			WHERE "top_hits_rank"<=1
			ORDER BY "aggr__origins__count" DESC, "aggr__origins__key_0" ASC,
			  "top_hits_rank" ASC`,
		},
	},
}
