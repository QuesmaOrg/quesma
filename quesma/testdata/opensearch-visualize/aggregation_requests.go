// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package opensearch_visualize

import (
	"github.com/QuesmaOrg/quesma/quesma/model"
	"github.com/QuesmaOrg/quesma/quesma/testdata"
	"github.com/QuesmaOrg/quesma/quesma/util"
	"math"
	"time"
)

const TableName = model.SingleTableNamePlaceHolder

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
			],
			"track_total_hits": true
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
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("range_0__aggr__2__count", uint64(0)),
				model.NewQueryResultCol("range_0__metric__2__1_col_0", 0),
				model.NewQueryResultCol("range_1__aggr__2__count", uint64(1260)),
				model.NewQueryResultCol("range_1__metric__2__1_col_0", 1),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT countIf("ftd_session_time"<1000) AS "range_0__aggr__2__count",
			  uniqIf("ftd_session_time", "ftd_session_time"<1000) AS
			  "range_0__metric__2__1_col_0",
			  countIf("ftd_session_time">=-100) AS "range_1__aggr__2__count",
			  uniqIf("ftd_session_time", "ftd_session_time">=-100) AS
			  "range_1__metric__2__1_col_0"
			FROM ` + TableName + `
			WHERE ("epoch_time">='2024-04-27T14:25:59.383Z' AND "epoch_time"<=
			  '2024-04-27T14:40:59.383Z')`,
	},
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
			],
			"track_total_hits": true
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
											"_score": 1.0,
											"_source": {
												"properties.entry_time": 1704129696028
											},
											"sort": [
												1714229611000
											]
										},
										{
											"_id": "YswMII8BiWIsMAbUDSt-",
											"_index": "device_logs",
											"_score": 1.0,
											"_source": {
												"properties.entry_time": 1704129696028
											},
											"sort": [
												1714229611000
											]
										}
									],
									"max_score": 1.0,
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
		// TODO: Remove value as it is used for total hits
		// TODO: Remove sort, it should be implemented
		AdditionalAcceptableDifference: []string{"_index", "_id", "value", "sort"},
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("range_0__aggr__2__count", uint64(0)),
				model.NewQueryResultCol("range_1__aggr__2__count", uint64(1880)),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT countIf("properties.entry_time"<1000) AS "range_0__aggr__2__count",
			  countIf("properties.entry_time">=-100) AS "range_1__aggr__2__count"
			FROM __quesma_table_name
			WHERE ("epoch_time">='2024-04-27T14:38:33.527Z' AND "epoch_time"<=
			  '2024-04-27T14:53:33.527Z')`,
		ExpectedAdditionalPancakeResults: [][]model.QueryResultRow{
			{{}}, // 0 results
			{
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("top_hits__2__1_col_0", uint64(1704129696028))}},
				{Cols: []model.QueryResultCol{model.NewQueryResultCol("top_hits__2__1_col_0", uint64(1704129696028))}},
			},
		},
		ExpectedAdditionalPancakeSQLs: []string{`
			SELECT "properties.entry_time" AS "top_hits__2__1_col_0"
			FROM __quesma_table_name
			WHERE ("properties.entry_time"<1000 AND ("epoch_time">=
			  '2024-04-27T14:38:33.527Z' AND "epoch_time"<='2024-04-27T14:53:33.527Z'))
			LIMIT 2`, `
			SELECT "properties.entry_time" AS "top_hits__2__1_col_0"
			FROM __quesma_table_name
			WHERE ("properties.entry_time">=-100 AND ("epoch_time">=
			  '2024-04-27T14:38:33.527Z' AND "epoch_time"<='2024-04-27T14:53:33.527Z'))
			LIMIT 2`,
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
			],
			"track_total_hits": true
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
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("range_0__aggr__2__count", uint64(0)),
				model.NewQueryResultCol("range_0__metric__2__1_col_0", 0.0),
				model.NewQueryResultCol("range_1__aggr__2__count", uint64(4378)),
				model.NewQueryResultCol("range_1__metric__2__1_col_0", 7460679809210584.0),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT countIf(("epoch_time_original">=0 AND "epoch_time_original"<1000)) AS
			  "range_0__aggr__2__count",
			  sumOrNullIf("properties.entry_time", ("epoch_time_original">=0 AND
			  "epoch_time_original"<1000)) AS "range_0__metric__2__1_col_0",
			  countIf("epoch_time_original">=1000) AS "range_1__aggr__2__count",
			  sumOrNullIf("properties.entry_time", "epoch_time_original">=1000) AS
			  "range_1__metric__2__1_col_0"
			FROM ` + TableName + `
			WHERE ("epoch_time">='2024-04-28T14:34:22.674Z' AND "epoch_time"<=
			  '2024-04-28T14:49:22.674Z')`,
	},
	{ // [3]
		TestName: "Range with subaggregations. Reproduce: Visualize -> Heat Map -> Metrics: Median, Buckets: X-Asis Range",
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
			],
			"track_total_hits": true
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
									"50.0": null
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
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("range_0__aggr__2__count", uint64(1)),
				model.NewQueryResultCol("range_0__metric__2__1_col_0", []float64{46.9921875}),
				model.NewQueryResultCol("range_1__aggr__2__count", uint64(2)),
				model.NewQueryResultCol("range_1__metric__2__1_col_0", []float64{math.NaN()}),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT countIf(("properties::exoestimation_connection_speedinkbps">=0 AND
			  "properties::exoestimation_connection_speedinkbps"<1000)) AS
			  "range_0__aggr__2__count",
			  quantilesIf(0.500000)("properties::entry_time", (
			  "properties::exoestimation_connection_speedinkbps">=0 AND
			  "properties::exoestimation_connection_speedinkbps"<1000)) AS
			  "range_0__metric__2__1_col_0",
			  countIf(("properties::exoestimation_connection_speedinkbps">=1000 AND
			  "properties::exoestimation_connection_speedinkbps"<2000)) AS
			  "range_1__aggr__2__count",
			  quantilesIf(0.500000)("properties::entry_time", (
			  "properties::exoestimation_connection_speedinkbps">=1000 AND
			  "properties::exoestimation_connection_speedinkbps"<2000)) AS
			  "range_1__metric__2__1_col_0"
			FROM ` + TableName + `
			WHERE ("epoch_time">='2024-04-18T04:40:12.252Z' AND "epoch_time"<=
			  '2024-05-03T04:40:12.252Z')`,
	},
	{ // [4]
		TestName: "Max on DateTime field. Reproduce: Visualize -> Line: Metrics -> Max @timestamp, Buckets: Add X-Asis, Aggregation: Significant Terms",
		QueryRequestJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"2": {
					"aggs": {
						"1": {
							"max": {
								"field": "timestamp"
							}
						}
					},
					"significant_terms": {
						"field": "response.keyword",
						"size": 3
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
									"gte": "2024-04-18T00:49:59.517Z",
									"lte": "2024-05-03T00:49:59.517Z"
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
			],
			"track_total_hits": true
		}`,
		ExpectedResponse: // erased "2": "bg_count": 14074, "doc_count": 2786 from the real response. It should be there in 'significant_terms' (not in 'terms'), but it seems to work without it.
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
								"value": 1714687096297.0,
								"value_as_string": "2024-05-02T21:58:16.297"
							},
							"bg_count": 2570,
							"doc_count": 2570,
							"key": "200",
							"score": 2570
						},
						{
							"1": {
								"value": 1714665552949.0,
								"value_as_string": "2024-05-02T15:59:12.949"
							},
							"bg_count": 94,
							"doc_count": 94,
							"key": "503",
							"score": 94
						}
					],
					"doc_count": 5000,
					"bg_count": 5000
				}
			},
			"hits": {
				"hits": [],
				"max_score": null,
				"total": {
					"relation": "eq",
					"value": 2786
				}
			},
			"timed_out": false,
			"took": 91
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", 5000),
				model.NewQueryResultCol("aggr__2__key_0", "200"),
				model.NewQueryResultCol("aggr__2__count", int64(2570)),
				model.NewQueryResultCol("metric__2__1_col_0", util.ParseTime("2024-05-02T21:58:16.297Z")),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", 5000),
				model.NewQueryResultCol("aggr__2__key_0", "503"),
				model.NewQueryResultCol("aggr__2__count", int64(94)),
				model.NewQueryResultCol("metric__2__1_col_0", util.ParseTime("2024-05-02T15:59:12.949Z")),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT sum(count(*)) OVER () AS "aggr__2__parent_count",
			  "response" AS "aggr__2__key_0", count(*) AS "aggr__2__count",
			  maxOrNull("timestamp") AS "metric__2__1_col_0"
			FROM __quesma_table_name
			WHERE ("timestamp">=fromUnixTimestamp64Milli(1713401399517) AND "timestamp"<=
			  fromUnixTimestamp64Milli(1714697399517))
			GROUP BY "response" AS "aggr__2__key_0"
			ORDER BY "aggr__2__count" DESC, "aggr__2__key_0" ASC
			LIMIT 4`,
	},
	{ // [5]
		TestName: "Min on DateTime field. Reproduce: Visualize -> Line: Metrics -> Min @timestamp, Buckets: Add X-Asis, Aggregation: Significant Terms",
		QueryRequestJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"2": {
					"aggs": {
						"1": {
							"min": {
								"field": "timestamp"
							}
						}
					},
					"significant_terms": {
						"field": "response.keyword",
						"size": 3
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
									"gte": "2024-04-18T00:51:00.471Z",
									"lte": "2024-05-03T00:51:00.471Z"
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
			],
			"track_total_hits": true
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
					"bg_count": 5300,
					"buckets": [
						{
							"1": {
								"value": 1713659942912.0,
								"value_as_string": "2024-04-21T00:39:02.912"
							},
							"bg_count": 2570,
							"doc_count": 2570,
							"key": "200",
							"score": 2570
						},
						{
							"1": {
								"value": 1713670225131.0,
								"value_as_string": "2024-04-21T03:30:25.131"
							},
							"bg_count": 94,
							"doc_count": 94,
							"key": "503",
							"score": 94
						}
					],
					"doc_count": 5300
				}
			},
			"hits": {
				"hits": [],
				"max_score": null,
				"total": {
					"relation": "eq",
					"value": 2786
				}
			},
			"timed_out": false,
			"took": 15
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", 5300),
				model.NewQueryResultCol("aggr__2__key_0", "200"),
				model.NewQueryResultCol("aggr__2__count", uint64(2570)),
				model.NewQueryResultCol("metric__2__1_col_0", util.ParseTime("2024-04-21T00:39:02.912Z")),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", 5300),
				model.NewQueryResultCol("aggr__2__key_0", "503"),
				model.NewQueryResultCol("aggr__2__count", uint64(94)),
				model.NewQueryResultCol("metric__2__1_col_0", util.ParseTime("2024-04-21T03:30:25.131Z")),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT sum(count(*)) OVER () AS "aggr__2__parent_count",
			  "response" AS "aggr__2__key_0", count(*) AS "aggr__2__count",
			  minOrNull("timestamp") AS "metric__2__1_col_0"
			FROM __quesma_table_name
			WHERE ("timestamp">=fromUnixTimestamp64Milli(1713401460471) AND "timestamp"<=
			  fromUnixTimestamp64Milli(1714697460471))
			GROUP BY "response" AS "aggr__2__key_0"
			ORDER BY "aggr__2__count" DESC, "aggr__2__key_0" ASC
			LIMIT 4`,
	},
	{ // [6]
		TestName: "Percentiles on DateTime field. Reproduce: Visualize -> Line: Metrics -> Percentiles (or Median, it's the same aggregation) @timestamp, Buckets: Add X-Asis, Aggregation: Significant Terms",
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
								"field": "timestamp",
								"keyed": false,
								"percents": [1, 2, 25, 50, 75, 95, 99]
							}
						}
					},
					"significant_terms": {
						"field": "response.keyword",
						"size": 3
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
									"gte": "2024-04-18T00:51:15.845Z",
									"lte": "2024-05-03T00:51:15.845Z"
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
			],
			"track_total_hits": true
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
					"bg_count": 2786,
					"doc_count": 2786,
					"buckets": [
						{
							"1": {
								"values": [
									{
										"key": 1.0,
										"value": 1713679873619.0,
										"value_as_string": "2024-04-21T06:11:13.619Z"
									},
									{
										"key": 2,
										"value": 1713702073414.0,
										"value_as_string": "2024-04-21T12:21:13.414Z"
									},
									{
										"key": 25.0,
										"value": 1713898065613.0,
										"value_as_string": "2024-04-23T18:47:45.613Z"
									},
									{
										"key": 50.0,
										"value": 1714163505522.0,
										"value_as_string": "2024-04-26T20:31:45.522Z"
									},
									{
										"key": 75.0,
										"value": 1714419555029.0,
										"value_as_string": "2024-04-29T19:39:15.029Z"
									},
									{
										"key": 95.0,
										"value": 1714649082507.0,
										"value_as_string": "2024-05-02T11:24:42.507Z"
									},
									{
										"key": 99.0,
										"value": 1714666168003.0,
										"value_as_string": "2024-05-02T16:09:28.003Z"
									}
								]
							},
							"bg_count": 2570,
							"doc_count": 2570,
							"key": "200",
							"score": 2570
						}
					]
				}
			},
			"hits": {
				"hits": [],
				"max_score": null,
				"total": {
					"relation": "eq",
					"value": 2786
				}
			},
			"timed_out": false,
			"took": 9
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", int64(2786)),
				model.NewQueryResultCol("aggr__2__key_0", "200"),
				model.NewQueryResultCol("aggr__2__count", int64(2570)),
				model.NewQueryResultCol("metric__2__1_col_0", []time.Time{util.ParseTime("2024-04-21T06:11:13.619Z")}),
				model.NewQueryResultCol("metric__2__1_col_1", []time.Time{util.ParseTime("2024-04-21T12:21:13.414Z")}),
				model.NewQueryResultCol("metric__2__1_col_2", []time.Time{util.ParseTime("2024-04-23T18:47:45.613Z")}),
				model.NewQueryResultCol("metric__2__1_col_3", []time.Time{util.ParseTime("2024-04-26T20:31:45.522Z")}),
				model.NewQueryResultCol("metric__2__1_col_4", []time.Time{util.ParseTime("2024-04-29T19:39:15.029Z")}),
				model.NewQueryResultCol("metric__2__1_col_5", []time.Time{util.ParseTime("2024-05-02T11:24:42.507Z")}),
				model.NewQueryResultCol("metric__2__1_col_6", []time.Time{util.ParseTime("2024-05-02T16:09:28.003Z")}),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT sum(count(*)) OVER () AS "aggr__2__parent_count",
			  "response" AS "aggr__2__key_0", count(*) AS "aggr__2__count",
			  quantiles(0.010000)("timestamp") AS "metric__2__1_col_0",
			  quantiles(0.020000)("timestamp") AS "metric__2__1_col_1",
			  quantiles(0.250000)("timestamp") AS "metric__2__1_col_2",
			  quantiles(0.500000)("timestamp") AS "metric__2__1_col_3",
			  quantiles(0.750000)("timestamp") AS "metric__2__1_col_4",
			  quantiles(0.950000)("timestamp") AS "metric__2__1_col_5",
			  quantiles(0.990000)("timestamp") AS "metric__2__1_col_6"
			FROM __quesma_table_name
			WHERE ("timestamp">=fromUnixTimestamp64Milli(1713401475845) AND "timestamp"<=
			  fromUnixTimestamp64Milli(1714697475845))
			GROUP BY "response" AS "aggr__2__key_0"
			ORDER BY "aggr__2__count" DESC, "aggr__2__key_0" ASC
			LIMIT 4`,
	},
	{ // [7]
		TestName: "Percentile_ranks keyed=false. Reproduce: Visualize -> Line -> Metrics: Percentile Ranks, Buckets: X-Asis Date Histogram",
		QueryRequestJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"2": {
					"aggs": {
						"1": {
							"percentile_ranks": {
								"field": "AvgTicketPrice",
								"keyed": false,
								"values": [
									0,
									50000
								]
							}
						}
					},
					"date_histogram": {
						"calendar_interval": "1h",
						"field": "timestamp",
						"min_doc_count": 1,
						"time_zone": "Europe/Warsaw"
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
						"source": "doc['timestamp'].value.hourOfDay"
					}
				}
			},
			"size": 0,
			"stored_fields": [
				"*"
			],
			"track_total_hits": true
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
								"values": [
									{
										"key": 0.0,
										"value": 0.0
									},
									{
										"key": 50000.0,
										"value": 100.0
									}
								]
							},
							"doc_count": 9,
							"key": 1714852800000,
							"key_as_string": "2024-05-04T20:00:00.000"
						},
						{
							"1": {
								"values": [
									{
										"key": 0.0,
										"value": 0.0
									},
									{
										"key": 50000.0,
										"value": 50.0
									}
								]
							},
							"doc_count": 12,
							"key": 1714856400000,
							"key_as_string": "2024-05-04T21:00:00.000"
						}
					]
				}
			},
			"hits": {
				"hits": [],
				"max_score": null,
				"total": {
					"relation": "eq",
					"value": 884
				}
			},
			"timed_out": false,
			"took": 0
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__key_0", int64(1714860000000/3600000)),
				model.NewQueryResultCol("aggr__2__count", 9),
				model.NewQueryResultCol("metric__2__1_col_0", 0.0),
				model.NewQueryResultCol("metric__2__1_col_1", 100.0),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__key_0", int64(1714863600000/3600000)),
				model.NewQueryResultCol("aggr__2__count", 12),
				model.NewQueryResultCol("metric__2__1_col_0", 0.0),
				model.NewQueryResultCol("metric__2__1_col_1", 50.0),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT toInt64((toUnixTimestamp64Milli("timestamp")+timeZoneOffset(toTimezone(
  			  "timestamp", 'Europe/Warsaw'))*1000) / 3600000) AS "aggr__2__key_0",
			  count(*) AS "aggr__2__count",
			  countIf("AvgTicketPrice"<=0)/count(*)*100 AS "metric__2__1_col_0",
			  countIf("AvgTicketPrice"<=50000)/count(*)*100 AS "metric__2__1_col_1"
			FROM ` + TableName + `
			GROUP BY toInt64((toUnixTimestamp64Milli("timestamp")+timeZoneOffset(toTimezone(
              "timestamp", 'Europe/Warsaw'))*1000) / 3600000) AS "aggr__2__key_0"
			ORDER BY "aggr__2__key_0" ASC`,
	},
	{ // [8]
		TestName: "Min/max with simple script. Reproduce: Visualize -> Line -> Metrics: Count, Buckets: X-Asis Histogram",
		QueryRequestJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"maxAgg": {
					"max": {
						"script": {
							"lang": "painless",
							"source": "doc['timestamp'].value.getHour()"
						}
					}
				},
				"minAgg": {
					"min": {
						"script": {
							"lang": "painless",
							"source": "doc['timestamp'].value.getHour()"
						}
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
			],
			"track_total_hits": true
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
				"maxAgg": {
					"value": 23.0
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
					"value": 13059
				}
			},
			"timed_out": false,
			"took": 17
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("metric__maxAgg_col_0", 23.0),
				model.NewQueryResultCol("metric__minAgg_col_0", 0.0),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT maxOrNull(toHour("timestamp")) AS "metric__maxAgg_col_0",
			  minOrNull(toHour("timestamp")) AS "metric__minAgg_col_0"
			FROM ` + TableName + ``,
	},
	{ // [9]
		TestName: "Histogram with simple script. Reproduce: Visualize -> Line -> Metrics: Count, Buckets: X-Asis Histogram",
		QueryRequestJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"2": {
					"histogram": {
						"interval": 1,
						"min_doc_count": 1,
						"script": {
							"lang": "painless",
							"source": "doc['timestamp'].value.getHour()"
						}
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
			],
			"track_total_hits": true
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
							"doc_count": 44,
							"key": 0.0
						},
						{
							"doc_count": 43,
							"key": 1.0
						},
						{
							"doc_count": 34,
							"key": 2.0
						}
					]
				}
			},
			"hits": {
				"hits": [],
				"max_score": null,
				"total": {
					"relation": "eq",
					"value": 886
				}
			},
			"timed_out": false,
			"took": 41
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__key_0", 0.0),
				model.NewQueryResultCol("aggr__2__count", 44),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__key_0", 1.0),
				model.NewQueryResultCol("aggr__2__count", 43),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__key_0", 2.0),
				model.NewQueryResultCol("aggr__2__count", 34),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT toHour("timestamp") AS "aggr__2__key_0", count(*) AS "aggr__2__count"
			FROM ` + TableName + `
			GROUP BY toHour("timestamp") AS "aggr__2__key_0"
			ORDER BY "aggr__2__key_0" ASC`,
	},
}
