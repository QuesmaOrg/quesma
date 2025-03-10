// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package dashboard_1

import (
	"github.com/QuesmaOrg/quesma/platform/model"
	"github.com/QuesmaOrg/quesma/platform/testdata"
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
								"buckets": []
							},
							"doc_count": 0,
							"key": 2000000
						},
						{
							"1": {
								"buckets": []
							},
							"doc_count": 0,
							"key": 4000000
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
							"key": 6000000
						}
					]
				}
			}
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__key_0", 0.0),
				model.NewQueryResultCol("aggr__0__count", 4573),
				model.NewQueryResultCol("aggr__0__1__key_0", 0.0),
				model.NewQueryResultCol("aggr__0__1__count", 4573),
				model.NewQueryResultCol("metric__0__1__2_col_0", 42516.52153947081),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__key_0", 6000000.0),
				model.NewQueryResultCol("aggr__0__count", 1),
				model.NewQueryResultCol("aggr__0__1__key_0", 6000000.0),
				model.NewQueryResultCol("aggr__0__1__count", 1),
				model.NewQueryResultCol("metric__0__1__2_col_0", 658654099),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT "aggr__0__key_0", "aggr__0__count", "aggr__0__1__key_0",
			  "aggr__0__1__count", "metric__0__1__2_col_0"
			FROM (
			  SELECT "aggr__0__key_0", "aggr__0__count", "aggr__0__1__key_0",
				"aggr__0__1__count", "metric__0__1__2_col_0",
				dense_rank() OVER (ORDER BY "aggr__0__key_0" ASC) AS "aggr__0__order_1_rank"
				,
				dense_rank() OVER (PARTITION BY "aggr__0__key_0" ORDER BY "aggr__0__key_0"
				ASC, "aggr__0__1__key_0" ASC) AS "aggr__0__1__order_1_rank"
			  FROM (
				SELECT floor("rspContentLen"/2e+06)*2e+06 AS "aggr__0__key_0",
				  sum(count(*)) OVER (PARTITION BY "aggr__0__key_0") AS "aggr__0__count",
				  floor("rspContentLen"/2e+06)*2e+06 AS "aggr__0__1__key_0",
				  count(*) AS "aggr__0__1__count",
				  avgOrNull("rspContentLen") AS "metric__0__1__2_col_0"
				FROM __quesma_table_name
				WHERE ("reqTimeSec">=fromUnixTimestamp64Milli(1713956123606) AND
                  "reqTimeSec"<=fromUnixTimestamp64Milli(1713957023606))
				GROUP BY floor("rspContentLen"/2e+06)*2e+06 AS "aggr__0__key_0",
				  floor("rspContentLen"/2e+06)*2e+06 AS "aggr__0__1__key_0"))
			ORDER BY "aggr__0__order_1_rank" ASC, "aggr__0__1__order_1_rank" ASC`,
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
								"interval": 0.5,
								"min_doc_count": 0
							}
						}
					},
					"date_histogram": {
						"field": "reqTimeSec",
						"fixed_interval": "30s",
						"min_doc_count": 0,
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
										"key": 0.5
									},
									{
										"doc_count": 0,
										"key": 1,
										"2": {
											"values": {
												"95.0": null
											}
										}
									},
									{
										"2": {
											"values": {
												"95.0": 71
											}
										},
										"doc_count": 8,
										"key": 1.5
									}
								]
							},
							"doc_count": 167,
							"key": 1713957330000,
							"key_as_string": "2024-04-24T11:15:30.000"
						},
						{
							"1": {
								"buckets": []
							},
							"doc_count": 0,
							"key": 1713957360000,
							"key_as_string": "2024-04-24T11:16:00.000"
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
												"95.0": null
											}
										},
										"doc_count": 0,
										"key": 1.5
									},
									{
										"2": {
											"values": {
												"95.0": 63
											}
										},
										"doc_count": 21,
										"key": 2
									},
									{
										"2": {
											"values": {
												"95.0": null
											}
										},
										"doc_count": 0,
										"key": 2.5
									},
									{
										"2": {
											"values": {
												"95.0": 83.8
											}
										},
										"doc_count": 5,
										"key": 3
									}
								]
							},
							"doc_count": 78,
							"key": 1713957390000,
							"key_as_string": "2024-04-24T11:16:30.000"
						}
					]
				}
			}
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__key_0", int64(1713964530000/30000)),
				model.NewQueryResultCol("aggr__0__count", 167),
				model.NewQueryResultCol("aggr__0__1__key_0", 0.5),
				model.NewQueryResultCol("aggr__0__1__count", 159),
				model.NewQueryResultCol("metric__0__1__2_col_0", []float64{77}),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__key_0", int64(1713964530000/30000)),
				model.NewQueryResultCol("aggr__0__count", 167),
				model.NewQueryResultCol("aggr__0__1__key_0", 1.5),
				model.NewQueryResultCol("aggr__0__1__count", 8),
				model.NewQueryResultCol("metric__0__1__2_col_0", []float64{71}),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__key_0", int64(1713964590000/30000)),
				model.NewQueryResultCol("aggr__0__count", 78),
				model.NewQueryResultCol("aggr__0__1__key_0", 1.0),
				model.NewQueryResultCol("aggr__0__1__count", 52),
				model.NewQueryResultCol("metric__0__1__2_col_0", []float64{80.44999999999999}),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__key_0", int64(1713964590000/30000)),
				model.NewQueryResultCol("aggr__0__count", 78),
				model.NewQueryResultCol("aggr__0__1__key_0", 2.0),
				model.NewQueryResultCol("aggr__0__1__count", 21),
				model.NewQueryResultCol("metric__0__1__2_col_0", []float64{63}),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__key_0", int64(1713964590000/30000)),
				model.NewQueryResultCol("aggr__0__count", 78),
				model.NewQueryResultCol("aggr__0__1__key_0", 3.0),
				model.NewQueryResultCol("aggr__0__1__count", 5),
				model.NewQueryResultCol("metric__0__1__2_col_0", []float64{83.8}),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT "aggr__0__key_0", "aggr__0__count", "aggr__0__1__key_0",
			  "aggr__0__1__count", "metric__0__1__2_col_0"
			FROM (
			  SELECT "aggr__0__key_0", "aggr__0__count", "aggr__0__1__key_0",
				"aggr__0__1__count", "metric__0__1__2_col_0",
				dense_rank() OVER (ORDER BY "aggr__0__key_0" ASC) AS "aggr__0__order_1_rank"
				,
				dense_rank() OVER (PARTITION BY "aggr__0__key_0" ORDER BY
				"aggr__0__1__key_0" ASC) AS "aggr__0__1__order_1_rank"
			  FROM (
				SELECT toInt64((toUnixTimestamp64Milli("reqTimeSec")+timeZoneOffset(
				  toTimezone("reqTimeSec", 'Europe/Warsaw'))*1000) / 30000) AS
				  "aggr__0__key_0",
				  sum(count(*)) OVER (PARTITION BY "aggr__0__key_0") AS "aggr__0__count",
				  floor("billingRegion"/0.5)*0.5 AS "aggr__0__1__key_0",
				  count(*) AS "aggr__0__1__count",
				  quantiles(0.950000)("latency") AS "metric__0__1__2_col_0"
				FROM __quesma_table_name
				WHERE ("reqTimeSec">=fromUnixTimestamp64Milli(1713957346279) AND
      				"reqTimeSec"<=fromUnixTimestamp64Milli(1713958246279))
				GROUP BY toInt64((toUnixTimestamp64Milli("reqTimeSec")+timeZoneOffset(
				  toTimezone("reqTimeSec", 'Europe/Warsaw'))*1000) / 30000) AS
				  "aggr__0__key_0", floor("billingRegion"/0.5)*0.5 AS "aggr__0__1__key_0"))
			ORDER BY "aggr__0__order_1_rank" ASC, "aggr__0__1__order_1_rank" ASC`,
	},
}
