package opensearch_visualize

import (
	"mitmproxy/quesma/model"
	"mitmproxy/quesma/testdata"
)

var AggregationTests = []testdata.AggregationTestCase{
	{ // [0]
		TestName: "Range with subaggregations. Reproduce: Visualize -> Pie chart -> Aggregation: Unique Count, Buckets: Aggregation: Range",
		QueryRequestJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"2": {
					"aggs": {
						"1": {
							"cardinality": {
								"field": "ftd_session_time"
							}
						}
					},
					"range": {
						"field": "ftd_session_time",
						"keyed": true,
						"ranges": [
							{
								"to": 1000
							},
							{
								"from": -100
							}
						]
					}
				}
			},
			"docvalue_fields": [
				{
					"field": "epoch_time",
					"format": "date_time"
				},
				{
					"field": "ts_time_druid",
					"format": "date_time"
				}
			],
			"query": {
				"bool": {
					"filter": [
						{
							"range": {
								"epoch_time": {
									"format": "strict_date_optional_time",
									"gte": "2024-04-27T14:25:59.383Z",
									"lte": "2024-04-27T14:40:59.383Z"
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
					"buckets": {
						"*-1000.0": {
							"1": {
								"value": 0
							},
							"doc_count": 0,
							"to": 1000.0
						},
						"-100.0-*": {
							"1": {
								"value": 1
							},
							"doc_count": 1260,
							"from": -100.0
						}
					}
				}
			},
			"hits": {
				"hits": [],
				"max_score": null,
				"total": {
					"relation": "eq",
					"value": 1260
				}
			},
			"timed_out": false,
			"took": 131
		}`,
		ExpectedResults: [][]model.QueryResultRow{
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol("hits", uint64(1260))}}},
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol("doc_count", uint64(0))}}},
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol("doc_count", uint64(1))}}},
			{{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("value", 0),
				model.NewQueryResultCol("value", 1260),
				model.NewQueryResultCol("value", 1260),
			}}},
		},
		ExpectedSQLs: []string{
			// TODO after merge of some PR, change logs-generic-default to testdata.QuotedTableName
			`SELECT count() FROM "logs-generic-default" WHERE "epoch_time">='2024-04-27T14:25:59.383Z' AND "epoch_time"<='2024-04-27T14:40:59.383Z' `,
			`SELECT COUNT(DISTINCT "ftd_session_time") FROM "logs-generic-default" ` +
				`WHERE ("epoch_time">='2024-04-27T14:25:59.383Z' AND "epoch_time"<='2024-04-27T14:40:59.383Z') AND "ftd_session_time"<1000 `,
			`SELECT COUNT(DISTINCT "ftd_session_time") FROM "logs-generic-default" ` +
				`WHERE ("epoch_time">='2024-04-27T14:25:59.383Z' AND "epoch_time"<='2024-04-27T14:40:59.383Z') AND "ftd_session_time">=-100 `,
			`SELECT count(if("ftd_session_time"<1000, 1, NULL)), count(if("ftd_session_time">=-100, 1, NULL)), count() ` +
				`FROM "logs-generic-default" WHERE "epoch_time">='2024-04-27T14:25:59.383Z' AND "epoch_time"<='2024-04-27T14:40:59.383Z' `,
		},
	},
	// Need to improve 'top_hits' aggregation. Seems really easy, but done in next PR.
	{ // [1]
		TestName: "Range with subaggregations. Reproduce: Visualize -> Pie chart -> Aggregation: Top Hit, Buckets: Aggregation: Range",
		QueryRequestJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"2": {
					"aggs": {
						"1": {
							"top_hits": {
								"_source": "properties.entry_time",
								"docvalue_fields": [
									{
										"field": "properties.entry_time"
									}
								],
								"size": 2,
								"sort": [
									{
										"epoch_time": {
											"order": "desc"
										}
									}
								]
							}
						}
					},
					"range": {
						"field": "properties.entry_time",
						"keyed": true,
						"ranges": [
							{
								"to": 1000
							},
							{
								"from": -100
							}
						]
					}
				}
			},
			"docvalue_fields": [
				{
					"field": "epoch_time",
					"format": "date_time"
				},
				{
					"field": "properties.session_start_time",
					"format": "date_time"
				},
				{
					"field": "timestamps.topology_entry_time",
					"format": "date_time"
				},
				{
					"field": "ts_day",
					"format": "date_time"
				},
				{
					"field": "ts_time_druid",
					"format": "date_time"
				}
			],
			"query": {
				"bool": {
					"filter": [
						{
							"range": {
								"epoch_time": {
									"format": "strict_date_optional_time",
									"gte": "2024-04-27T14:38:33.527Z",
									"lte": "2024-04-27T14:53:33.527Z"
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
					"buckets": {
						"*-1000.0": {
							"1": {
								"hits": {
									"hits": [],
									"max_score": null,
									"total": {
										"relation": "eq",
										"value": 0
									}
								}
							},
							"doc_count": 0,
							"to": 1000.0
						},
						"-100.0-*": {
							"1": {
								"hits": {
									"hits": [
										{
											"_id": "YcwMII8BiWIsMAbUDSt-",
											"_index": "device_logs",
											"_score": null,
											"_source": {
												"properties": {
													"entry_time": 1704129696028
												}
											},
											"fields": {
												"properties.entry_time": [
													1704129696028
												]
											},
											"sort": [
												1714229611000
											]
										},
										{
											"_id": "YswMII8BiWIsMAbUDSt-",
											"_index": "device_logs",
											"_score": null,
											"_source": {
												"properties": {
													"entry_time": 1704129696028
												}
											},
											"fields": {
												"properties.entry_time": [
													1704129696028
												]
											},
											"sort": [
												1714229611000
											]
										}
									],
									"max_score": null,
									"total": {
										"relation": "eq",
										"value": 1880
									}
								}
							},
							"doc_count": 1880,
							"from": -100.0
						}
					}
				}
			},
			"hits": {
				"hits": [],
				"max_score": null,
				"total": {
					"relation": "eq",
					"value": 1880
				}
			},
			"timed_out": false,
			"took": 3
		}`,
		ExpectedResults: [][]model.QueryResultRow{
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol("hits", uint64(1880))}}},
			{},
		},
		ExpectedSQLs: []string{
			// TODO after merge of some PR, change logs-generic-default to testdata.QuotedTableName
			``,
		},
	},
	{ // [2]
		TestName: "Range with subaggregations. Reproduce: Visualize -> Pie chart -> Aggregation: Sum, Buckets: Aggregation: Range",
		QueryRequestJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"2": {
					"aggs": {
						"1": {
							"sum": {
								"field": "properties.entry_time"
							}
						}
					},
					"range": {
						"field": "epoch_time_original",
						"keyed": true,
						"ranges": [
							{
								"from": 0,
								"to": 1000
							},
							{
								"from": 1000
							}
						]
					}
				}
			},
			"docvalue_fields": [
				{
					"field": "epoch_time",
					"format": "date_time"
				},
				{
					"field": "properties.session_start_time",
					"format": "date_time"
				},
				{
					"field": "timestamps.topology_entry_time",
					"format": "date_time"
				},
				{
					"field": "ts_day",
					"format": "date_time"
				},
				{
					"field": "ts_time_druid",
					"format": "date_time"
				}
			],
			"query": {
				"bool": {
					"filter": [
						{
							"range": {
								"epoch_time": {
									"format": "strict_date_optional_time",
									"gte": "2024-04-28T14:34:22.674Z",
									"lte": "2024-04-28T14:49:22.674Z"
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
					"buckets": {
						"0.0-1000.0": {
							"1": {
								"value": 0.0
							},
							"doc_count": 0,
							"from": 0.0,
							"to": 1000.0
						},
						"1000.0-*": {
							"1": {
								"value": 7460679809210584.0
							},
							"doc_count": 4378,
							"from": 1000.0
						}
					}
				}
			},
			"hits": {
				"hits": [],
				"max_score": null,
				"total": {
					"relation": "eq",
					"value": 4378
				}
			},
			"timed_out": false,
			"took": 3
		}`,
		ExpectedResults: [][]model.QueryResultRow{
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol("hits", uint64(4378))}}},
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol("sum", 0.0)}}},
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol("sum", 7460679809210584.0)}}},
			{{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("value", 0),
				model.NewQueryResultCol("value", 4378),
				model.NewQueryResultCol("value", 4378),
			}}},
		},
		ExpectedSQLs: []string{
			// TODO after merge of some PR, change logs-generic-default to testdata.QuotedTableName
			`SELECT count() FROM "logs-generic-default" ` +
				`WHERE "epoch_time">='2024-04-28T14:34:22.674Z' AND "epoch_time"<='2024-04-28T14:49:22.674Z' `,
			`SELECT sum("properties.entry_time") FROM "logs-generic-default" ` +
				`WHERE ("epoch_time">='2024-04-28T14:34:22.674Z' AND "epoch_time"<='2024-04-28T14:49:22.674Z') ` +
				`AND "epoch_time_original">=0 AND "epoch_time_original"<1000 `,
			`SELECT sum("properties.entry_time") FROM "logs-generic-default" ` +
				`WHERE ("epoch_time">='2024-04-28T14:34:22.674Z' AND "epoch_time"<='2024-04-28T14:49:22.674Z') ` +
				`AND "epoch_time_original">=1000 `,
			`SELECT count(if("epoch_time_original">=0 AND "epoch_time_original"<1000, 1, NULL)), ` +
				`count(if("epoch_time_original">=1000, 1, NULL)), count() FROM "logs-generic-default" ` +
				`WHERE "epoch_time">='2024-04-28T14:34:22.674Z' AND "epoch_time"<='2024-04-28T14:49:22.674Z' `,
		},
	},
	{ // [3]
		TestName: `Range with subaggregations. Reproduce: Visualize -> Heat Map -> Metrics: Median, Buckets: X-Asis Range`,
		QueryRequestJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"2": {
					"aggs": {
						"1": {
							"percentiles": {
								"field": "properties::entry_time",
								"percents": [
									50
								]
							}
						}
					},
					"range": {
						"field": "properties::exoestimation_connection_speedinkbps",
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
					"field": "@timestamp",
					"format": "date_time"
				},
				{
					"field": "ts_time_druid",
					"format": "date_time"
				}
			],
			"query": {
				"bool": {
					"filter": [
						{
							"range": {
								"epoch_time": {
									"format": "strict_date_optional_time",
									"gte": "2024-04-18T04:40:12.252Z",
									"lte": "2024-05-03T04:40:12.252Z"
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
					"buckets": {
						"0.0-1000.0": {
							"1": {
								"values": {
									"50.0": 46.9921875
								}
							},
							"doc_count": 1,
							"from": 0.0,
							"to": 1000.0
						},
						"1000.0-2000.0": {
							"1": {
								"values": {
									"50.0": 45.5
								}
							},
							"doc_count": 2,
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
					"value": 4
				}
			},
			"timed_out": false,
			"took": 95
		}`,
		ExpectedResults: [][]model.QueryResultRow{
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol("hits", uint64(4))}}},
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol("quantile_50", []float64{46.9921875})}}},
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol("quantile_50", []float64{45.5})}}},
			{{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("doc_count", 1),
				model.NewQueryResultCol("doc_count", 2),
				model.NewQueryResultCol("doc_count", 4),
			}}},
		},
		ExpectedSQLs: []string{
			`SELECT count() FROM ` + testdata.QuotedTableName + ` ` +
				`WHERE "epoch_time">='2024-04-18T04:40:12.252Z' AND "epoch_time"<='2024-05-03T04:40:12.252Z' `,
			"SELECT quantiles(0.500000)(`properties::entry_time`) AS `quantile_50` " +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`WHERE ("epoch_time">='2024-04-18T04:40:12.252Z' AND "epoch_time"<='2024-05-03T04:40:12.252Z') ` +
				`AND "properties::exoestimation_connection_speedinkbps">=0 AND "properties::exoestimation_connection_speedinkbps"<1000 `,
			"SELECT quantiles(0.500000)(`properties::entry_time`) AS `quantile_50` " +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`WHERE ("epoch_time">='2024-04-18T04:40:12.252Z' AND "epoch_time"<='2024-05-03T04:40:12.252Z') ` +
				`AND "properties::exoestimation_connection_speedinkbps">=1000 AND "properties::exoestimation_connection_speedinkbps"<2000 `,
			`SELECT count(if("properties::exoestimation_connection_speedinkbps">=0 AND "properties::exoestimation_connection_speedinkbps"<1000, 1, NULL)), ` +
				`count(if("properties::exoestimation_connection_speedinkbps">=1000 AND "properties::exoestimation_connection_speedinkbps"<2000, 1, NULL)), ` +
				`count() ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`WHERE "epoch_time">='2024-04-18T04:40:12.252Z' AND "epoch_time"<='2024-05-03T04:40:12.252Z' `,
		},
	},
}
