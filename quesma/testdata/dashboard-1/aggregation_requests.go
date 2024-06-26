// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package dashboard_1

import (
	"quesma/model"
	"quesma/testdata"
)

/*
test below looked like this:
TODO restore it, and add extended_bounds support (other PR)
"histogram": {
								"extended_bounds": {
									"max": 6054099,
									"min": 0
								},
								"field": "rspContentLen",
								"interval": 2000000,
								"min_doc_count": 0
							}
						}
					},
					"histogram": {
						"extended_bounds": {
							"max": 6054099,
							"min": 0
						},
*/

var AggregationTests = []testdata.AggregationTestCase{
	{ // [0]
		TestName: "dashboard-1: latency by region",
		QueryRequestJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"0": {
					"aggs": {
						"1": {
							"aggs": {
								"2": {
									"avg": {
										"field": "rspContentLen"
									}
								}
							},
							"histogram": {
								"field": "rspContentLen",
								"interval": 2000000,
								"min_doc_count": 0
							}
						}
					},
					"histogram": {
						"field": "rspContentLen",
						"interval": 2000000,
						"min_doc_count": 0
					}
				}
			},
			"fields": [
				{
					"field": "@timestamp",
					"format": "date_time"
				},
				{
					"field": "reqTimeSec",
					"format": "date_time"
				}
			],
			"query": {
				"bool": {
					"filter": [
						{
							"range": {
								"reqTimeSec": {
									"format": "strict_date_optional_time",
									"gte": "2024-04-24T10:55:23.606Z",
									"lte": "2024-04-24T11:10:23.606Z"
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
			"took": 0,
			"timed_out": false,
			"_shards": {
				"total": 1,
				"successful": 1,
				"failed": 0,
				"skipped": 0
			},
			"hits": {
				"total": {
					"value": 4636,
					"relation": "eq"
				},
				"max_score": null,
				"hits": []
			},
			"aggregations": {
				"0": {
					"buckets": [
						{
							"1": {
								"buckets": [
									{
										"2": {
											"value": 42516.52153947081
										},
										"doc_count": 4573,
										"key": 0
									}
								]
							},
							"doc_count": 4573,
							"key": 0
						},
						{
							"1": {
								"buckets": [
									{
										"2": {
											"value": 658654099
										},
										"doc_count": 1,
										"key": 6000000
									}
								]
							},
							"doc_count": 1,
							"key": 658000000
						}
					]
				}
			}
		}`,
		ExpectedResults: [][]model.QueryResultRow{
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol("hits", uint64(4636))}}},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("floor(rspContentLen / 2000000.000000) * 2000000.000000", 0.0),
					model.NewQueryResultCol("floor(rspContentLen / 2000000.000000) * 2000000.000000", 0.0),
					model.NewQueryResultCol("avgOrNull(rspContentLen)", 42516.52153947081),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("floor(rspContentLen / 2000000.000000) * 2000000.000000", 6000000.0),
					model.NewQueryResultCol("floor(rspContentLen / 2000000.000000) * 2000000.000000", 6000000.0),
					model.NewQueryResultCol("avgOrNull(rspContentLen)", 658654099),
				}},
			},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("floor(rspContentLen / 2000000.000000) * 2000000.000000", 0.0),
					model.NewQueryResultCol("floor(rspContentLen / 2000000.000000) * 2000000.000000", 0.0),
					model.NewQueryResultCol("doc_count", 4573),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("floor(rspContentLen / 2000000.000000) * 2000000.000000", 6000000.0),
					model.NewQueryResultCol("floor(rspContentLen / 2000000.000000) * 2000000.000000", 6000000.0),
					model.NewQueryResultCol("doc_count", 1),
				}},
			},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("floor(rspContentLen / 2000000.000000) * 2000000.000000", 0.0),
					model.NewQueryResultCol("doc_count", 4573),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("floor(rspContentLen / 2000000.000000) * 2000000.000000", 6000000.0),
					model.NewQueryResultCol("doc_count", 1),
				}},
			},
		},
		ExpectedSQLs: []string{
			`SELECT count() FROM ` + testdata.QuotedTableName + ` WHERE "reqTimeSec">='2024-04-24T10:55:23.606Z' AND "reqTimeSec"<='2024-04-24T11:10:23.606Z' `,
			`SELECT floor("rspContentLen" / 2000000.000000) * 2000000.000000, floor("rspContentLen" / 2000000.000000) * 2000000.000000, avgOrNull("rspContentLen") ` +
				`FROM ` + testdata.QuotedTableName + ` WHERE "reqTimeSec">='2024-04-24T10:55:23.606Z' AND "reqTimeSec"<='2024-04-24T11:10:23.606Z'  ` +
				`GROUP BY floor("rspContentLen" / 2000000.000000) * 2000000.000000, floor("rspContentLen" / 2000000.000000) * 2000000.000000 ` +
				`ORDER BY floor("rspContentLen" / 2000000.000000) * 2000000.000000, floor("rspContentLen" / 2000000.000000) * 2000000.000000`,
			`SELECT floor("rspContentLen" / 2000000.000000) * 2000000.000000, floor("rspContentLen" / 2000000.000000) * 2000000.000000, count() ` +
				`FROM ` + testdata.QuotedTableName + ` WHERE "reqTimeSec">='2024-04-24T10:55:23.606Z' AND "reqTimeSec"<='2024-04-24T11:10:23.606Z'  ` +
				`GROUP BY floor("rspContentLen" / 2000000.000000) * 2000000.000000, floor("rspContentLen" / 2000000.000000) * 2000000.000000 ` +
				`ORDER BY floor("rspContentLen" / 2000000.000000) * 2000000.000000, floor("rspContentLen" / 2000000.000000) * 2000000.000000`,
			`SELECT floor("rspContentLen" / 2000000.000000) * 2000000.000000, count() FROM ` + testdata.QuotedTableName + ` ` +
				`WHERE "reqTimeSec">='2024-04-24T10:55:23.606Z' AND "reqTimeSec"<='2024-04-24T11:10:23.606Z'  ` +
				`GROUP BY floor("rspContentLen" / 2000000.000000) * 2000000.000000 ` +
				`ORDER BY floor("rspContentLen" / 2000000.000000) * 2000000.000000`,
			`SELECT floor("rspContentLen" / 2000000.000000) * 2000000.000000, count() FROM ` + testdata.QuotedTableName + ` ` +
				`WHERE "reqTimeSec">='2024-04-24T10:55:23.606Z' AND "reqTimeSec"<='2024-04-24T11:10:23.606Z'  ` +
				`GROUP BY floor("rspContentLen" / 2000000.000000) * 2000000.000000 ` +
				`ORDER BY floor("rspContentLen" / 2000000.000000) * 2000000.000000`,
		},
	},
	{ // [1]
		TestName: "dashboard-1: bug, used to be infinite loop",
		QueryRequestJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"0": {
					"aggs": {
						"1": {
							"aggs": {
								"2": {
									"percentiles": {
										"field": "latency",
										"percents": [
											95
										]
									}
								}
							},
							"histogram": {
								"extended_bounds": {
									"max": 8,
									"min": 1
								},
								"field": "billingRegion",
								"interval": 0.02,
								"min_doc_count": 0
							}
						}
					},
					"date_histogram": {
						"field": "reqTimeSec",
						"fixed_interval": "30s",
						"min_doc_count": 1,
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
					"field": "reqTimeSec",
					"format": "date_time"
				}
			],
			"query": {
				"bool": {
					"filter": [
						{
							"range": {
								"reqTimeSec": {
									"format": "strict_date_optional_time",
									"gte": "2024-04-24T11:15:46.279Z",
									"lte": "2024-04-24T11:30:46.279Z"
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
			"took": 0,
			"timed_out": false,
			"_shards": {
				"total": 1,
				"successful": 1,
				"failed": 0,
				"skipped": 0
			},
			"hits": {
				"total": {
					"value": 4800,
					"relation": "eq"
				},
				"max_score": null,
				"hits": []
			},
			"aggregations": {
				"0": {
					"buckets": [
						{
							"1": {
								"buckets": [
									{
										"2": {
											"values": {
												"95.0": 77
											}
										},
										"doc_count": 159,
										"key": 1
									},
									{
										"2": {
											"values": {
												"95.0": 71
											}
										},
										"doc_count": 8,
										"key": 3
									}
								]
							},
							"doc_count": 167,
							"key": 1713957330000,
							"key_as_string": "2024-04-24T11:15:30.000"
						},
						{
							"1": {
								"buckets": [
									{
										"2": {
											"values": {
												"95.0": 80.44999999999999
											}
										},
										"doc_count": 52,
										"key": 1
									},
									{
										"2": {
											"values": {
												"95.0": 63
											}
										},
										"doc_count": 21,
										"key": 3
									},
									{
										"2": {
											"values": {
												"95.0": 83.8
											}
										},
										"doc_count": 5,
										"key": 5
									}
								]
							},
							"doc_count": 78,
							"key": 1713957360000,
							"key_as_string": "2024-04-24T11:16:00.000"
						}
					]
				}
			}
		}`,
		ExpectedResults: [][]model.QueryResultRow{
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol("hits", uint64(4800))}}},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("key", int64(1713957330000/30000)),
					model.NewQueryResultCol(`floor("billingRegion\" / 0.020000) * 0.020000`, 1.0),
					model.NewQueryResultCol("quantile_95", []float64{77}),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("key", int64(1713957330000/30000)),
					model.NewQueryResultCol(`floor("billingRegion\" / 0.020000) * 0.020000`, 3.0),
					model.NewQueryResultCol("quantile_95", []float64{71}),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("key", int64(1713957360000/30000)),
					model.NewQueryResultCol(`floor("billingRegion\" / 0.020000) * 0.020000`, 1.0),
					model.NewQueryResultCol("quantile_95", []float64{80.44999999999999}),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("key", int64(1713957360000/30000)),
					model.NewQueryResultCol(`floor("billingRegion\" / 0.020000) * 0.020000`, 3.0),
					model.NewQueryResultCol("quantile_95", []float64{63}),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("key", int64(1713957360000/30000)),
					model.NewQueryResultCol(`floor("billingRegion\" / 0.020000) * 0.020000`, 5.0),
					model.NewQueryResultCol("quantile_95", []float64{83.8}),
				}},
			},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("key", int64(1713957330000/30000)),
					model.NewQueryResultCol(`floor("billingRegion\" / 0.020000) * 0.020000`, 1.0),
					model.NewQueryResultCol("doc_count", 159),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("key", int64(1713957330000/30000)),
					model.NewQueryResultCol(`floor("billingRegion\" / 0.020000) * 0.020000`, 3.0),
					model.NewQueryResultCol("doc_count", 8),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("key", int64(1713957360000/30000)),
					model.NewQueryResultCol(`floor("billingRegion\" / 0.020000) * 0.020000`, 1.0),
					model.NewQueryResultCol("doc_count", 52),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("key", int64(1713957360000/30000)),
					model.NewQueryResultCol(`floor("billingRegion\" / 0.020000) * 0.020000`, 3.0),
					model.NewQueryResultCol("doc_count", 21),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("key", int64(1713957360000/30000)),
					model.NewQueryResultCol(`floor("billingRegion\" / 0.020000) * 0.020000`, 5.0),
					model.NewQueryResultCol("doc_count", 5),
				}},
			},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("key", int64(1713957330000/30000)),
					model.NewQueryResultCol("doc_count", 167),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("key", int64(1713957360000/30000)),
					model.NewQueryResultCol("doc_count", 78),
				}},
			},
		},
		ExpectedSQLs: []string{
			`SELECT count() FROM ` + testdata.QuotedTableName + ` WHERE "reqTimeSec">='2024-04-24T11:15:46.279Z' AND "reqTimeSec"<='2024-04-24T11:30:46.279Z' `,
			"SELECT toInt64(toUnixTimestamp64Milli(`reqTimeSec`)/30000), floor(" + `"billingRegion"` + " / 0.020000) * 0.020000, quantiles(0.950000)(\"latency\") AS \"quantile_95\" " +
				`FROM ` + testdata.QuotedTableName + ` WHERE "reqTimeSec">='2024-04-24T11:15:46.279Z' AND "reqTimeSec"<='2024-04-24T11:30:46.279Z'  ` +
				"GROUP BY toInt64(toUnixTimestamp64Milli(`reqTimeSec`)/30000), " + `floor("billingRegion" / 0.020000) * 0.020000 ` +
				"ORDER BY toInt64(toUnixTimestamp64Milli(`reqTimeSec`)/30000), " + `floor("billingRegion" / 0.020000) * 0.020000`,
			"SELECT toInt64(toUnixTimestamp64Milli(`reqTimeSec`)/30000), floor(" + `"billingRegion" / 0.020000) * 0.020000, count() ` +
				`FROM ` + testdata.QuotedTableName + ` WHERE "reqTimeSec">='2024-04-24T11:15:46.279Z' AND "reqTimeSec"<='2024-04-24T11:30:46.279Z'  ` +
				"GROUP BY toInt64(toUnixTimestamp64Milli(`reqTimeSec`)/30000), " + `floor("billingRegion" / 0.020000) * 0.020000 ` +
				"ORDER BY toInt64(toUnixTimestamp64Milli(`reqTimeSec`)/30000), " + `floor("billingRegion" / 0.020000) * 0.020000`,
			"SELECT toInt64(toUnixTimestamp64Milli(`reqTimeSec`)/30000), count() " +
				`FROM ` + testdata.QuotedTableName + ` WHERE "reqTimeSec">='2024-04-24T11:15:46.279Z' AND "reqTimeSec"<='2024-04-24T11:30:46.279Z'  ` +
				"GROUP BY toInt64(toUnixTimestamp64Milli(`reqTimeSec`)/30000) " +
				"ORDER BY toInt64(toUnixTimestamp64Milli(`reqTimeSec`)/30000)",
		},
	},
}
