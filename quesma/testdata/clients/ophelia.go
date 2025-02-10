// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package clients

import (
	"github.com/QuesmaOrg/quesma/quesma/model"
	"github.com/QuesmaOrg/quesma/quesma/testdata"
)

var OpheliaTests = []testdata.AggregationTestCase{
	{ // [0]
		TestName: "Ophelia Test 1: triple terms + default order",
		QueryRequestJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"2": {
					"aggs": {
						"8": {
							"aggs": {
								"4": {
									"terms": {
										"field": "organName",
										"shard_size": 25,
										"size": 1
									}
								}
							},
							"terms": {
								"field": "limbName",
								"missing": "__missing__",
								"size": 20
							}
						}
					},
					"terms": {
						"field": "surname",
						"shard_size": 1000,
						"size": 200
					}
				}
			},
			"fields": [
				{
					"field": "@timestamp",
					"format": "date_time"
				},
				{
					"field": "createdAt",
					"format": "date_time"
				},
				{
					"field": "date",
					"format": "date_time"
				},
				{
					"field": "endTime",
					"format": "date_time"
				},
				{
					"field": "startTime",
					"format": "date_time"
				}
			],
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
			"completion_time_in_millis": 1720352002293,
			"expiration_time_in_millis": 1720352062445,
			"id": "FnpTUXdfTTZLUlBtQVo1YzBTVFBseEEcM19IaHdFWG5RN1d1eV9VaUcxenYwdzo0MTc0MA==",
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
					"2": {
						"buckets": [
							{
								"8": {
									"buckets": [
										{
											"4": {
												"buckets": [
													{
														"doc_count": 21,
														"key": "c11"
													}
												],
												"doc_count_error_upper_bound": 0,
												"sum_other_doc_count": 0
											},
											"doc_count": 21,
											"key": "b11"
										},
										{
											"4": {
												"buckets": [
													{
														"doc_count": 24,
														"key": "c12"
													}
												],
												"doc_count_error_upper_bound": 0,
												"sum_other_doc_count": 0
											},
											"doc_count": 24,
											"key": "b12"
										}
									],
									"doc_count_error_upper_bound": -1,
									"sum_other_doc_count": 991
								},
								"doc_count": 1036,
								"key": "a1"
							},
							{
								"8": {
									"buckets": [
										{
											"4": {
												"buckets": [
													{
														"doc_count": 17,
														"key": "c21"
													}
												],
												"doc_count_error_upper_bound": 0,
												"sum_other_doc_count": 0
											},
											"doc_count": 17,
											"key": "b21"
										},
										{
											"4": {
												"buckets": [
													{
														"doc_count": 17,
														"key": "c22"
													}
												],
												"doc_count_error_upper_bound": 0,
												"sum_other_doc_count": 0
											},
											"doc_count": 17,
											"key": "b22"
										}
									],
									"doc_count_error_upper_bound": 0,
									"sum_other_doc_count": 0
								},
								"doc_count": 34,
								"key": "a2"
							}
						],
						"doc_count_error_upper_bound": -1,
						"sum_other_doc_count": 33220
					}
				},
				"hits": {
					"hits": [],
					"max_score": null,
					"total": {
						"relation": "eq",
						"value": 50427
					}
				},
				"timed_out": false,
				"took": 554
			},
			"start_time_in_millis": 1720352001739
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{ // mixing different int types (int, int64, uint64) on purpose, at least both (u)int64 can be returned from ClickHouse
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", 34290),
				model.NewQueryResultCol("aggr__2__key_0", "a1"),
				model.NewQueryResultCol("aggr__2__count", uint64(1036)),
				model.NewQueryResultCol("aggr__2__8__parent_count", 1036),
				model.NewQueryResultCol("aggr__2__8__key_0", "b11"),
				model.NewQueryResultCol("aggr__2__8__count", int64(21)),
				model.NewQueryResultCol("aggr__2__8__4__parent_count", 21),
				model.NewQueryResultCol("aggr__2__8__4__key_0", "c11"),
				model.NewQueryResultCol("aggr__2__8__4__count", int64(21)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", 34290),
				model.NewQueryResultCol("aggr__2__key_0", "a1"),
				model.NewQueryResultCol("aggr__2__count", uint64(1036)),
				model.NewQueryResultCol("aggr__2__8__parent_count", 1036),
				model.NewQueryResultCol("aggr__2__8__key_0", "b12"),
				model.NewQueryResultCol("aggr__2__8__count", int64(24)),
				model.NewQueryResultCol("aggr__2__8__4__parent_count", 24),
				model.NewQueryResultCol("aggr__2__8__4__key_0", "c12"),
				model.NewQueryResultCol("aggr__2__8__4__count", int64(24)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", 34290),
				model.NewQueryResultCol("aggr__2__key_0", "a2"),
				model.NewQueryResultCol("aggr__2__count", uint64(34)),
				model.NewQueryResultCol("aggr__2__8__parent_count", 34),
				model.NewQueryResultCol("aggr__2__8__key_0", "b21"),
				model.NewQueryResultCol("aggr__2__8__count", int64(17)),
				model.NewQueryResultCol("aggr__2__8__4__parent_count", 17),
				model.NewQueryResultCol("aggr__2__8__4__key_0", "c21"),
				model.NewQueryResultCol("aggr__2__8__4__count", int64(17)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", 34290),
				model.NewQueryResultCol("aggr__2__key_0", "a2"),
				model.NewQueryResultCol("aggr__2__count", uint64(34)),
				model.NewQueryResultCol("aggr__2__parent_count", 34290),
				model.NewQueryResultCol("aggr__2__8__key_0", "b22"),
				model.NewQueryResultCol("aggr__2__8__count", int64(17)),
				model.NewQueryResultCol("aggr__2__8__4__parent_count", 17),
				model.NewQueryResultCol("aggr__2__8__4__key_0", "c22"),
				model.NewQueryResultCol("aggr__2__8__4__count", int64(17)),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT "aggr__2__parent_count", "aggr__2__key_0", "aggr__2__count",
			  "aggr__2__8__parent_count", "aggr__2__8__key_0", "aggr__2__8__count",
			  "aggr__2__8__4__parent_count", "aggr__2__8__4__key_0", "aggr__2__8__4__count"
			FROM (
			  SELECT "aggr__2__parent_count", "aggr__2__key_0", "aggr__2__count",
				"aggr__2__8__parent_count", "aggr__2__8__key_0", "aggr__2__8__count",
				"aggr__2__8__4__parent_count", "aggr__2__8__4__key_0",
				"aggr__2__8__4__count",
				dense_rank() OVER (ORDER BY "aggr__2__count" DESC, "aggr__2__key_0" ASC) AS
				"aggr__2__order_1_rank",
				dense_rank() OVER (PARTITION BY "aggr__2__key_0" ORDER BY
				"aggr__2__8__count" DESC, "aggr__2__8__key_0" ASC) AS
				"aggr__2__8__order_1_rank",
				dense_rank() OVER (PARTITION BY "aggr__2__key_0", "aggr__2__8__key_0" ORDER
				BY "aggr__2__8__4__count" DESC, "aggr__2__8__4__key_0" ASC) AS
				"aggr__2__8__4__order_1_rank"
			  FROM (
				SELECT sum(count(*)) OVER () AS "aggr__2__parent_count",
				  "surname" AS "aggr__2__key_0",
				  sum(count(*)) OVER (PARTITION BY "aggr__2__key_0") AS "aggr__2__count",
				  sum(count(*)) OVER (PARTITION BY "aggr__2__key_0") AS
				  "aggr__2__8__parent_count",
				  COALESCE("limbName", '__missing__') AS "aggr__2__8__key_0",
				  sum(count(*)) OVER (PARTITION BY "aggr__2__key_0", "aggr__2__8__key_0") AS
				  "aggr__2__8__count",
				  sum(count(*)) OVER (PARTITION BY "aggr__2__key_0", "aggr__2__8__key_0") AS
				  "aggr__2__8__4__parent_count", "organName" AS "aggr__2__8__4__key_0",
				  count(*) AS "aggr__2__8__4__count"
				FROM __quesma_table_name
				GROUP BY "surname" AS "aggr__2__key_0",
				  COALESCE("limbName", '__missing__') AS "aggr__2__8__key_0",
				  "organName" AS "aggr__2__8__4__key_0"))
			WHERE (("aggr__2__order_1_rank"<=201 AND "aggr__2__8__order_1_rank"<=20) AND
			  "aggr__2__8__4__order_1_rank"<=2)
			ORDER BY "aggr__2__order_1_rank" ASC, "aggr__2__8__order_1_rank" ASC,
			  "aggr__2__8__4__order_1_rank" ASC`,
	},
	{ // [1]
		TestName: "Ophelia Test 2: triple terms + other aggregations + default order",
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
								"field": "total"
							}
						},
						"8": {
							"aggs": {
								"1": {
									"sum": {
										"field": "total"
									}
								},
								"4": {
									"aggs": {
										"1": {
											"sum": {
												"field": "total"
											}
										},
										"5": {
											"sum": {
												"field": "some"
											}
										}
									},
									"terms": {
										"field": "organName",
										"shard_size": 25,
										"size": 1
									}
								}
							},
							"terms": {
								"field": "limbName",
								"missing": "__missing__",
								"size": 20
							}
						}
					},
					"terms": {
						"field": "surname",
						"shard_size": 1000,
						"size": 200
					}
				}
			},
			"fields": [
				{
					"field": "@timestamp",
					"format": "date_time"
				},
				{
					"field": "createdAt",
					"format": "date_time"
				},
				{
					"field": "date",
					"format": "date_time"
				},
				{
					"field": "endTime",
					"format": "date_time"
				},
				{
					"field": "startTime",
					"format": "date_time"
				}
			],
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
			"completion_time_in_millis": 1720352002293,
			"expiration_time_in_millis": 1720352062445,
			"id": "FnpTUXdfTTZLUlBtQVo1YzBTVFBseEEcM19IaHdFWG5RN1d1eV9VaUcxenYwdzo0MTc0MA==",
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
					"2": {
						"buckets": [
							{
								"1": {
									"value": 1091661.7608666667
								},
								"8": {
									"buckets": [
										{
											"1": {
												"value": 45774.291766666654
											},
											"4": {
												"buckets": [
													{
														"1": {
															"value": 45774.291766666654
														},
														"5": {
															"value": 36577.89516666666
														},
														"doc_count": 24,
														"key": "c12"
													}
												],
												"doc_count_error_upper_bound": 0,
												"sum_other_doc_count": 0
											},
											"doc_count": 24,
											"key": "b12"
										},
										{
											"1": {
												"value": 51891.94613333333
											},
											"4": {
												"buckets": [
													{
														"1": {
															"value": 51891.94613333333
														},
														"5": {
															"value": 37988.09523333333
														},
														"doc_count": 21,
														"key": "c11"
													}
												],
												"doc_count_error_upper_bound": 0,
												"sum_other_doc_count": 0
											},
											"doc_count": 21,
											"key": "b11"
										}
									],
									"doc_count_error_upper_bound": -1,
									"sum_other_doc_count": 991
								},
								"doc_count": 1036,
								"key": "a1"
							},
							{
								"1": {
									"value": 630270.07765
								},
								"8": {
									"buckets": [
										{
											"1": {
												"value": 399126.7496833334
											},
											"4": {
												"buckets": [
													{
														"1": {
															"value": 399126.7496833334
														},
														"5": {
															"value": 337246.82201666664
														},
														"doc_count": 17,
														"key": "c21"
													}
												],
												"doc_count_error_upper_bound": 0,
												"sum_other_doc_count": 0
											},
											"doc_count": 17,
											"key": "b21"
										},
										{
											"1": {
												"value": 231143.3279666666
											},
											"4": {
												"buckets": [
													{
														"1": {
															"value": 231143.3279666666
														},
														"5": {
															"value": 205408.48849999998
														},
														"doc_count": 17,
														"key": "c22"
													}
												],
												"doc_count_error_upper_bound": 0,
												"sum_other_doc_count": 0
											},
											"doc_count": 17,
											"key": "b22"
										}
									],
									"doc_count_error_upper_bound": 0,
									"sum_other_doc_count": 0
								},
								"doc_count": 34,
								"key": "a2"
							}
						],
						"doc_count_error_upper_bound": -1,
						"sum_other_doc_count": 33220
					}
				},
				"hits": {
					"hits": [],
					"max_score": null,
					"total": {
						"relation": "eq",
						"value": 50427
					}
				},
				"timed_out": false,
				"took": 554
			},
			"start_time_in_millis": 1720352001739
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", 34290),
				model.NewQueryResultCol("aggr__2__key_0", "a1"),
				model.NewQueryResultCol("aggr__2__count", uint64(1036)),
				model.NewQueryResultCol("metric__2__1_col_0", 1091661.760867),
				model.NewQueryResultCol("aggr__2__8__parent_count", 1036),
				model.NewQueryResultCol("aggr__2__8__key_0", "b12"),
				model.NewQueryResultCol("aggr__2__8__count", int64(24)),
				model.NewQueryResultCol("metric__2__8__1_col_0", 45774.291766666654),
				model.NewQueryResultCol("aggr__2__8__4__parent_count", 24),
				model.NewQueryResultCol("aggr__2__8__4__key_0", "c12"),
				model.NewQueryResultCol("aggr__2__8__4__count", int64(24)),
				model.NewQueryResultCol("metric__2__8__4__1_col_0", 45774.291766666654),
				model.NewQueryResultCol("metric__2__8__4__5_col_0", 36577.89516666666),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", 34290),
				model.NewQueryResultCol("aggr__2__key_0", "a1"),
				model.NewQueryResultCol("aggr__2__count", uint64(1036)),
				model.NewQueryResultCol("metric__2__1_col_0", 1091661.760867),
				model.NewQueryResultCol("aggr__2__8__parent_count", 1036),
				model.NewQueryResultCol("aggr__2__8__key_0", "b11"),
				model.NewQueryResultCol("aggr__2__8__count", int64(21)),
				model.NewQueryResultCol("metric__2__8__1_col_0", 51891.94613333333),
				model.NewQueryResultCol("aggr__2__8__4__parent_count", 21),
				model.NewQueryResultCol("aggr__2__8__4__key_0", "c11"),
				model.NewQueryResultCol("aggr__2__8__4__count", int64(21)),
				model.NewQueryResultCol("metric__2__8__4__1_col_0", 51891.94613333333),
				model.NewQueryResultCol("metric__2__8__4__5_col_0", 37988.09523333333),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", 34290),
				model.NewQueryResultCol("aggr__2__key_0", "a2"),
				model.NewQueryResultCol("aggr__2__count", uint64(34)),
				model.NewQueryResultCol("metric__2__1_col_0", 630270.07765),
				model.NewQueryResultCol("aggr__2__8__parent_count", 34),
				model.NewQueryResultCol("aggr__2__8__key_0", "b21"),
				model.NewQueryResultCol("aggr__2__8__count", int64(17)),
				model.NewQueryResultCol("metric__2__8__1_col_0", 399126.7496833334),
				model.NewQueryResultCol("aggr__2__8__4__parent_count", 17),
				model.NewQueryResultCol("aggr__2__8__4__key_0", "c21"),
				model.NewQueryResultCol("aggr__2__8__4__count", int64(17)),
				model.NewQueryResultCol("metric__2__8__4__1_col_0", 399126.7496833334),
				model.NewQueryResultCol("metric__2__8__4__5_col_0", 337246.82201666664),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", 34290),
				model.NewQueryResultCol("aggr__2__key_0", "a2"),
				model.NewQueryResultCol("aggr__2__count", uint64(34)),
				model.NewQueryResultCol("metric__2__1_col_0", 231143.3279666666),
				model.NewQueryResultCol("aggr__2__8__parent_count", 34),
				model.NewQueryResultCol("aggr__2__8__key_0", "b22"),
				model.NewQueryResultCol("aggr__2__8__count", int64(17)),
				model.NewQueryResultCol("metric__2__8__1_col_0", 231143.3279666666),
				model.NewQueryResultCol("aggr__2__8__4__parent_count", 17),
				model.NewQueryResultCol("aggr__2__8__4__key_0", "c22"),
				model.NewQueryResultCol("aggr__2__8__4__count", int64(17)),
				model.NewQueryResultCol("metric__2__8__4__1_col_0", 231143.3279666666),
				model.NewQueryResultCol("metric__2__8__4__5_col_0", 205408.48849999998),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT "aggr__2__parent_count", "aggr__2__key_0", "aggr__2__count",
			  "metric__2__1_col_0", "aggr__2__8__parent_count", "aggr__2__8__key_0",
			  "aggr__2__8__count", "metric__2__8__1_col_0", "aggr__2__8__4__parent_count",
			  "aggr__2__8__4__key_0", "aggr__2__8__4__count", "metric__2__8__4__1_col_0",
			  "metric__2__8__4__5_col_0"
			FROM (
			  SELECT "aggr__2__parent_count", "aggr__2__key_0", "aggr__2__count",
				"metric__2__1_col_0", "aggr__2__8__parent_count", "aggr__2__8__key_0",
				"aggr__2__8__count", "metric__2__8__1_col_0", "aggr__2__8__4__parent_count",
				"aggr__2__8__4__key_0", "aggr__2__8__4__count", "metric__2__8__4__1_col_0",
				"metric__2__8__4__5_col_0",
				dense_rank() OVER (ORDER BY "aggr__2__count" DESC, "aggr__2__key_0" ASC) AS
				"aggr__2__order_1_rank",
				dense_rank() OVER (PARTITION BY "aggr__2__key_0" ORDER BY
				"aggr__2__8__count" DESC, "aggr__2__8__key_0" ASC) AS
				"aggr__2__8__order_1_rank",
				dense_rank() OVER (PARTITION BY "aggr__2__key_0", "aggr__2__8__key_0" ORDER
				BY "aggr__2__8__4__count" DESC, "aggr__2__8__4__key_0" ASC) AS
				"aggr__2__8__4__order_1_rank"
			  FROM (
				SELECT sum(count(*)) OVER () AS "aggr__2__parent_count",
				  "surname" AS "aggr__2__key_0",
				  sum(count(*)) OVER (PARTITION BY "aggr__2__key_0") AS "aggr__2__count",
				  sumOrNull(sumOrNull("total")) OVER (PARTITION BY "aggr__2__key_0") AS
				  "metric__2__1_col_0",
				  sum(count(*)) OVER (PARTITION BY "aggr__2__key_0") AS
				  "aggr__2__8__parent_count",
				  COALESCE("limbName", '__missing__') AS "aggr__2__8__key_0",
				  sum(count(*)) OVER (PARTITION BY "aggr__2__key_0", "aggr__2__8__key_0") AS
				  "aggr__2__8__count",
				  sumOrNull(sumOrNull("total")) OVER (PARTITION BY "aggr__2__key_0",
				  "aggr__2__8__key_0") AS "metric__2__8__1_col_0",
				  sum(count(*)) OVER (PARTITION BY "aggr__2__key_0", "aggr__2__8__key_0") AS
				  "aggr__2__8__4__parent_count", "organName" AS "aggr__2__8__4__key_0",
				  count(*) AS "aggr__2__8__4__count",
				  sumOrNull("total") AS "metric__2__8__4__1_col_0",
				  sumOrNull("some") AS "metric__2__8__4__5_col_0"
				FROM __quesma_table_name
				GROUP BY "surname" AS "aggr__2__key_0",
				  COALESCE("limbName", '__missing__') AS "aggr__2__8__key_0",
				  "organName" AS "aggr__2__8__4__key_0"))
			WHERE (("aggr__2__order_1_rank"<=201 AND "aggr__2__8__order_1_rank"<=20) AND
			  "aggr__2__8__4__order_1_rank"<=2)
			ORDER BY "aggr__2__order_1_rank" ASC, "aggr__2__8__order_1_rank" ASC,
			  "aggr__2__8__4__order_1_rank" ASC`,
	},
	{ // [2]
		TestName: "Ophelia Test 3: 5x terms + a lot of other aggregations",
		QueryRequestJson: `
		{
			"aggs": {
				"2": {
					"terms": {
						"field": "surname",
						"order": {
							"1": "desc"
						},
						"size": 100,
						"shard_size": 2000
					},
					"aggs": {
						"1": {
							"sum": {
								"field": "total"
							}
						},
						"7": {
							"terms": {
								"field": "limbName",
								"order": {
									"1": "desc"
								},
								"missing": "__missing__",
								"size": 10,
								"shard_size": 25
							},
							"aggs": {
								"1": {
									"sum": {
										"field": "total"
									}
								},
								"8": {
									"terms": {
										"field": "organName",
										"order": {
											"1": "desc"
										},
										"missing": "__missing__",
										"size": 10,
										"shard_size": 25
									},
									"aggs": {
										"1": {
											"sum": {
												"field": "total"
											}
										},
										"4": {
											"terms": {
												"field": "doctorName",
												"order": {	
													"1": "desc"
												},
												"size": 6,
												"shard_size": 25
											},
											"aggs": {
												"1": {
													"sum": {
														"field": "total"
													}
												},
												"3": {
													"terms": {
														"field": "height",
														"order": {
															"1": "desc"
														},
														"size": 1,
														"shard_size": 25
													},
													"aggs": {
														"1": {
															"sum": {
																"field": "total"
															}
														},
														"5": {
															"sum": {
																"field": "some"
															}
														},
														"6": {
															"sum": {
																"field": "cost"
															}
														}
													}
												}
											}
										}
									}
								}
							}
						}
					}
				}
			},
			"size": 0,
			"fields": [
				{
					"field": "@timestamp",
					"format": "date_time"
				},
				{
					"field": "date",
					"format": "date_time"
				}
			],
			"script_fields": {},
			"stored_fields": [
				"*"
			],
			"runtime_mappings": {},
			"_source": {
				"excludes": []
			},
		}`,
		ExpectedResponse: `
		{
			"completion_time_in_millis": 1720352002293,
			"expiration_time_in_millis": 1720352062445,
			"id": "FnpTUXdfTTZLUlBtQVo1YzBTVFBseEEcM19IaHdFWG5RN1d1eV9VaUcxenYwdzo0MTc0MA==",
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
					"2": {
						"buckets": [
							{
								"1": {
									"value": 1091661.7608666667
								},
								"7": {
									"buckets": [
										{
											"1": {
												"value": 51891.94613333333
											},
											"8": {
												"buckets": [
													{
														"1": {
															"value": 51891.94613333333
														},
														"4": {
															"buckets": [
																{
																	"doc_count": 10,
																	"key": "d11",
																	"1": {
																		"value": 1.1
																	},
																	"3": {
																		"buckets": [
																			{
																				"doc_count": 3,
																				"key": "e11",
																				"1": {
																					"value": -1
																				},
																				"5": {
																					"value": -2
																				},
																				"6": {
																					"value": -3	
																				}
																			}
																		],
																		"doc_count_error_upper_bound": 0,
																		"sum_other_doc_count": 7
																	}
																},
																{
																	"doc_count": 5,
																	"key": "d12",
																	"1": {
																		"value": 2.2
																	},
																	"3": {
																		"buckets": [
																			{
																				"doc_count": 1,
																				"key": "e12",
																				"1": {
																					"value": null
																				},
																				"5": {
																					"value": -22
																				},
																				"6": {
																					"value": -33
																				}
																			}
																		],
																		"doc_count_error_upper_bound": 0,
																		"sum_other_doc_count": 4
																	}
																}
															],
															"doc_count_error_upper_bound": 0,
															"sum_other_doc_count": 6
														},
														"doc_count": 21,
														"key": "c1"
													}
												],
												"doc_count_error_upper_bound": 0,
												"sum_other_doc_count": 0
											},
											"doc_count": 21,
											"key": "b1"
										}
									],
									"doc_count_error_upper_bound": -1,
									"sum_other_doc_count": 1015
								},
								"doc_count": 1036,
								"key": "a1"
							},
							{
								"1": {
									"value": 0
								},
								"7": {
									"buckets": [
										{
											"1": {
												"value": 0.1
											},
											"8": {
												"buckets": [
													{
														"1": {
															"value": 0.2
														},
														"4": {
															"buckets": [
																{
																	"doc_count": 2,
																	"key": "d2",
																	"1": {
																		"value": 0.3
																	},
																	"3": {
																		"buckets": [
																			{
																				"doc_count": 1,
																				"key": "e2",
																				"1": {
																					"value": -0.4
																				},
																				"5": {
																					"value": -0.5
																				},
																				"6": {
																					"value": -0.6
																				}
																			}
																		],
																		"doc_count_error_upper_bound": 0,
																		"sum_other_doc_count": 1
																	}
																}
															],
															"doc_count_error_upper_bound": 0,
															"sum_other_doc_count": 1
														},
														"doc_count": 3,
														"key": "c2"
													}
												],
												"doc_count_error_upper_bound": 0,
												"sum_other_doc_count": 1
											},
											"doc_count": 4,
											"key": "b2"
										}
									],
									"doc_count_error_upper_bound": -1,
									"sum_other_doc_count": 1
								},
								"doc_count": 5,
								"key": "a2"
							}
						],
						"doc_count_error_upper_bound": -1,
						"sum_other_doc_count": 49386
					}
				},
				"hits": {
					"hits": [],
					"max_score": null,
					"total": {
						"relation": "eq",
						"value": 50427
					}
				},
				"timed_out": false,
				"took": 554
			},
			"start_time_in_millis": 1720352001739
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", 50427),
				model.NewQueryResultCol("aggr__2__key_0", "a1"),
				model.NewQueryResultCol("aggr__2__count", uint64(1036)),
				model.NewQueryResultCol("metric__2__1_col_0", 1091661.7608666667),
				model.NewQueryResultCol("aggr__2__7__parent_count", 1036),
				model.NewQueryResultCol("aggr__2__7__key_0", "b1"),
				model.NewQueryResultCol("aggr__2__7__count", int64(21)),
				model.NewQueryResultCol("metric__2__7__1_col_0", 51891.94613333333),
				model.NewQueryResultCol("aggr__2__7__8__parent_count", 21),
				model.NewQueryResultCol("aggr__2__7__8__key_0", "c1"),
				model.NewQueryResultCol("aggr__2__7__8__count", int64(21)),
				model.NewQueryResultCol("metric__2__7__8__1_col_0", 51891.94613333333),
				model.NewQueryResultCol("aggr__2__7__8__4__parent_count", 21),
				model.NewQueryResultCol("aggr__2__7__8__4__key_0", "d11"),
				model.NewQueryResultCol("aggr__2__7__8__4__count", int64(10)),
				model.NewQueryResultCol("metric__2__7__8__4__1_col_0", 1.1),
				model.NewQueryResultCol("aggr__2__7__8__4__3__parent_count", 10),
				model.NewQueryResultCol("aggr__2__7__8__4__3__key_0", "e11"),
				model.NewQueryResultCol("aggr__2__7__8__4__3__count", int64(3)),
				model.NewQueryResultCol("metric__2__7__8__4__3__1_col_0", -1),
				model.NewQueryResultCol("metric__2__7__8__4__3__5_col_0", -2),
				model.NewQueryResultCol("metric__2__7__8__4__3__6_col_0", -3),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", 50427),
				model.NewQueryResultCol("aggr__2__key_0", "a1"),
				model.NewQueryResultCol("aggr__2__count", uint64(1036)),
				model.NewQueryResultCol("metric__2__1_col_0", 1091661.7608666667),
				model.NewQueryResultCol("aggr__2__7__parent_count", 1036),
				model.NewQueryResultCol("aggr__2__7__key_0", "b1"),
				model.NewQueryResultCol("aggr__2__7__count", int64(21)),
				model.NewQueryResultCol("metric__2__7__1_col_0", 51891.94613333333),
				model.NewQueryResultCol("aggr__2__7__8__parent_count", 21),
				model.NewQueryResultCol("aggr__2__7__8__key_0", "c1"),
				model.NewQueryResultCol("aggr__2__7__8__count", int64(21)),
				model.NewQueryResultCol("metric__2__7__8__1_col_0", 51891.94613333333),
				model.NewQueryResultCol("aggr__2__7__8__4__parent_count", 21),
				model.NewQueryResultCol("aggr__2__7__8__4__key_0", "d12"),
				model.NewQueryResultCol("aggr__2__7__8__4__count", int64(5)),
				model.NewQueryResultCol("metric__2__7__8__4__1_col_0", 2.2),
				model.NewQueryResultCol("aggr__2__7__8__4__3__parent_count", 5),
				model.NewQueryResultCol("aggr__2__7__8__4__3__key_0", "e12"),
				model.NewQueryResultCol("aggr__2__7__8__4__3__count", int64(1)),
				model.NewQueryResultCol("metric__2__7__8__4__3__1_col_0", nil),
				model.NewQueryResultCol("metric__2__7__8__4__3__5_col_0", -22),
				model.NewQueryResultCol("metric__2__7__8__4__3__6_col_0", -33),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", 50427),
				model.NewQueryResultCol("aggr__2__key_0", "a2"),
				model.NewQueryResultCol("aggr__2__count", uint64(5)),
				model.NewQueryResultCol("metric__2__1_col_0", 0),
				model.NewQueryResultCol("aggr__2__7__parent_count", 5),
				model.NewQueryResultCol("aggr__2__7__key_0", "b2"),
				model.NewQueryResultCol("aggr__2__7__count", int64(4)),
				model.NewQueryResultCol("metric__2__7__1_col_0", 0.1),
				model.NewQueryResultCol("aggr__2__7__8__parent_count", 4),
				model.NewQueryResultCol("aggr__2__7__8__key_0", "c2"),
				model.NewQueryResultCol("aggr__2__7__8__count", int64(3)),
				model.NewQueryResultCol("metric__2__7__8__1_col_0", 0.2),
				model.NewQueryResultCol("aggr__2__7__8__4__parent_count", 3),
				model.NewQueryResultCol("aggr__2__7__8__4__key_0", "d2"),
				model.NewQueryResultCol("aggr__2__7__8__4__count", int64(2)),
				model.NewQueryResultCol("metric__2__7__8__4__1_col_0", 0.3),
				model.NewQueryResultCol("aggr__2__7__8__4__3__parent_count", 2),
				model.NewQueryResultCol("aggr__2__7__8__4__3__key_0", "e2"),
				model.NewQueryResultCol("aggr__2__7__8__4__3__count", int64(1)),
				model.NewQueryResultCol("metric__2__7__8__4__3__1_col_0", -0.4),
				model.NewQueryResultCol("metric__2__7__8__4__3__5_col_0", -0.5),
				model.NewQueryResultCol("metric__2__7__8__4__3__6_col_0", -0.6),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT "aggr__2__parent_count", "aggr__2__key_0", "aggr__2__count",
			  "metric__2__1_col_0", "aggr__2__7__parent_count", "aggr__2__7__key_0",
			  "aggr__2__7__count", "metric__2__7__1_col_0", "aggr__2__7__8__parent_count",
			  "aggr__2__7__8__key_0", "aggr__2__7__8__count", "metric__2__7__8__1_col_0",
			  "aggr__2__7__8__4__parent_count", "aggr__2__7__8__4__key_0",
			  "aggr__2__7__8__4__count", "metric__2__7__8__4__1_col_0",
			  "aggr__2__7__8__4__3__parent_count", "aggr__2__7__8__4__3__key_0",
			  "aggr__2__7__8__4__3__count", "metric__2__7__8__4__3__1_col_0",
			  "metric__2__7__8__4__3__5_col_0", "metric__2__7__8__4__3__6_col_0"
			FROM (
			  SELECT "aggr__2__parent_count", "aggr__2__key_0", "aggr__2__count",
				"metric__2__1_col_0", "aggr__2__7__parent_count", "aggr__2__7__key_0",
				"aggr__2__7__count", "metric__2__7__1_col_0", "aggr__2__7__8__parent_count",
				"aggr__2__7__8__key_0", "aggr__2__7__8__count", "metric__2__7__8__1_col_0",
				"aggr__2__7__8__4__parent_count", "aggr__2__7__8__4__key_0",
				"aggr__2__7__8__4__count", "metric__2__7__8__4__1_col_0",
				"aggr__2__7__8__4__3__parent_count", "aggr__2__7__8__4__3__key_0",
				"aggr__2__7__8__4__3__count", "metric__2__7__8__4__3__1_col_0",
				"metric__2__7__8__4__3__5_col_0", "metric__2__7__8__4__3__6_col_0",
				dense_rank() OVER (ORDER BY "metric__2__1_col_0" DESC, "aggr__2__key_0" ASC)
				AS "aggr__2__order_1_rank",
				dense_rank() OVER (PARTITION BY "aggr__2__key_0" ORDER BY
				"metric__2__7__1_col_0" DESC, "aggr__2__7__key_0" ASC) AS
				"aggr__2__7__order_1_rank",
				dense_rank() OVER (PARTITION BY "aggr__2__key_0", "aggr__2__7__key_0" ORDER
				BY "metric__2__7__8__1_col_0" DESC, "aggr__2__7__8__key_0" ASC) AS
				"aggr__2__7__8__order_1_rank",
				dense_rank() OVER (PARTITION BY "aggr__2__key_0", "aggr__2__7__key_0",
				"aggr__2__7__8__key_0" ORDER BY "metric__2__7__8__4__1_col_0" DESC,
				"aggr__2__7__8__4__key_0" ASC) AS "aggr__2__7__8__4__order_1_rank",
				dense_rank() OVER (PARTITION BY "aggr__2__key_0", "aggr__2__7__key_0",
				"aggr__2__7__8__key_0", "aggr__2__7__8__4__key_0" ORDER BY
				"metric__2__7__8__4__3__1_col_0" DESC, "aggr__2__7__8__4__3__key_0" ASC) AS
				"aggr__2__7__8__4__3__order_1_rank"
			  FROM (
				SELECT sum(count(*)) OVER () AS "aggr__2__parent_count",
				  "surname" AS "aggr__2__key_0",
				  sum(count(*)) OVER (PARTITION BY "aggr__2__key_0") AS "aggr__2__count",
				  sumOrNull(sumOrNull("total")) OVER (PARTITION BY "aggr__2__key_0") AS
				  "metric__2__1_col_0",
				  sum(count(*)) OVER (PARTITION BY "aggr__2__key_0") AS
				  "aggr__2__7__parent_count",
				  COALESCE("limbName", '__missing__') AS "aggr__2__7__key_0",
				  sum(count(*)) OVER (PARTITION BY "aggr__2__key_0", "aggr__2__7__key_0") AS
				  "aggr__2__7__count",
				  sumOrNull(sumOrNull("total")) OVER (PARTITION BY "aggr__2__key_0",
				  "aggr__2__7__key_0") AS "metric__2__7__1_col_0",
				  sum(count(*)) OVER (PARTITION BY "aggr__2__key_0", "aggr__2__7__key_0") AS
				  "aggr__2__7__8__parent_count",
				  COALESCE("organName", '__missing__') AS "aggr__2__7__8__key_0",
				  sum(count(*)) OVER (PARTITION BY "aggr__2__key_0", "aggr__2__7__key_0",
				  "aggr__2__7__8__key_0") AS "aggr__2__7__8__count",
				  sumOrNull(sumOrNull("total")) OVER (PARTITION BY "aggr__2__key_0",
				  "aggr__2__7__key_0", "aggr__2__7__8__key_0") AS "metric__2__7__8__1_col_0"
				  ,
				  sum(count(*)) OVER (PARTITION BY "aggr__2__key_0", "aggr__2__7__key_0",
				  "aggr__2__7__8__key_0") AS "aggr__2__7__8__4__parent_count",
				  "doctorName" AS "aggr__2__7__8__4__key_0",
				  sum(count(*)) OVER (PARTITION BY "aggr__2__key_0", "aggr__2__7__key_0",
				  "aggr__2__7__8__key_0", "aggr__2__7__8__4__key_0") AS
				  "aggr__2__7__8__4__count",
				  sumOrNull(sumOrNull("total")) OVER (PARTITION BY "aggr__2__key_0",
				  "aggr__2__7__key_0", "aggr__2__7__8__key_0", "aggr__2__7__8__4__key_0") AS
				  "metric__2__7__8__4__1_col_0",
				  sum(count(*)) OVER (PARTITION BY "aggr__2__key_0", "aggr__2__7__key_0",
				  "aggr__2__7__8__key_0", "aggr__2__7__8__4__key_0") AS
				  "aggr__2__7__8__4__3__parent_count",
				  "height" AS "aggr__2__7__8__4__3__key_0",
				  count(*) AS "aggr__2__7__8__4__3__count",
				  sumOrNull("total") AS "metric__2__7__8__4__3__1_col_0",
				  sumOrNull("some") AS "metric__2__7__8__4__3__5_col_0",
				  sumOrNull("cost") AS "metric__2__7__8__4__3__6_col_0"
				FROM __quesma_table_name
				GROUP BY "surname" AS "aggr__2__key_0",
				  COALESCE("limbName", '__missing__') AS "aggr__2__7__key_0",
				  COALESCE("organName", '__missing__') AS "aggr__2__7__8__key_0",
				  "doctorName" AS "aggr__2__7__8__4__key_0",
				  "height" AS "aggr__2__7__8__4__3__key_0"))
			WHERE (((("aggr__2__order_1_rank"<=101 AND "aggr__2__7__order_1_rank"<=10) AND
			  "aggr__2__7__8__order_1_rank"<=10) AND "aggr__2__7__8__4__order_1_rank"<=7)
			  AND "aggr__2__7__8__4__3__order_1_rank"<=2)
			ORDER BY "aggr__2__order_1_rank" ASC, "aggr__2__7__order_1_rank" ASC,
			  "aggr__2__7__8__order_1_rank" ASC, "aggr__2__7__8__4__order_1_rank" ASC,
			  "aggr__2__7__8__4__3__order_1_rank" ASC`,
	},
	{ // [3]
		TestName: "Ophelia Test 4: triple terms + order by another aggregations",
		QueryRequestJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"2": {
					"aggs": {
						"8": {
							"aggs": {
								"4": {
									"terms": {
										"field": "organName",
										"order": {
											"_key": "desc"
										},
										"shard_size": 25,
										"size": 1
									},
								},
								"1": {
									"sum": {
										"field": "total"
									}
								}
							},
							"terms": {
								"field": "limbName",
								"missing": "__missing__",
								"order": {
									"1": "asc"
								},
								"size": 20
							}
						},
						"1": {
							"avg": {
								"field": "total"
							}
						}
					},
					"terms": {
						"field": "surname",
						"order": {
							"1": "desc"
						},
						"shard_size": 1000,
						"size": 200
					}
				}
			},
			"fields": [
				{
					"field": "@timestamp",
					"format": "date_time"
				},
				{
					"field": "createdAt",
					"format": "date_time"
				},
				{
					"field": "date",
					"format": "date_time"
				},
				{
					"field": "endTime",
					"format": "date_time"
				},
				{
					"field": "startTime",
					"format": "date_time"
				}
			],
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
			"completion_time_in_millis": 1720352002293,
			"expiration_time_in_millis": 1720352062445,
			"id": "FnpTUXdfTTZLUlBtQVo1YzBTVFBseEEcM19IaHdFWG5RN1d1eV9VaUcxenYwdzo0MTc0MA==",
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
					"2": {
						"buckets": [
							{
								"1": {
									"value": 1091661.7608666667
								},
								"8": {
									"buckets": [
										{
											"1": {
												"value": 45774.291766666654
											},
											"4": {
												"buckets": [
													{
														"doc_count": 24,
														"key": "c12"
													}
												],
												"doc_count_error_upper_bound": 0,
												"sum_other_doc_count": 0
											},
											"doc_count": 24,
											"key": "b12"
										},
										{
											"1": {
												"value": 51891.94613333333
											},
											"4": {
												"buckets": [
													{
														"doc_count": 21,
														"key": "c11"
													}
												],
												"doc_count_error_upper_bound": 0,
												"sum_other_doc_count": 0
											},
											"doc_count": 21,
											"key": "b11"
										}
									],
									"doc_count_error_upper_bound": -1,
									"sum_other_doc_count": 991
								},
								"doc_count": 1036,
								"key": "a1"
							},
							{
								"1": {
									"value": 630270.07765
								},
								"8": {
									"buckets": [
										{
											"1": {
												"value": 231143.3279666666
											},
											"4": {
												"buckets": [
													{
														"doc_count": 17,
														"key": "c22"
													}
												],
												"doc_count_error_upper_bound": 0,
												"sum_other_doc_count": 0
											},
											"doc_count": 17,
											"key": "b22"
										},
										{
											"1": {
												"value": 399126.7496833334
											},
											"4": {
												"buckets": [
													{
														"doc_count": 17,
														"key": "c21"
													}
												],
												"doc_count_error_upper_bound": 0,
												"sum_other_doc_count": 0
											},
											"doc_count": 17,
											"key": "b21"
										}
									],
									"doc_count_error_upper_bound": 0,
									"sum_other_doc_count": 0
								},
								"doc_count": 34,
								"key": "a2"
							}
						],
						"doc_count_error_upper_bound": -1,
						"sum_other_doc_count": 33220
					}
				},
				"hits": {
					"hits": [],
					"max_score": null,
					"total": {
						"relation": "eq",
						"value": 50427
					}
				},
				"timed_out": false,
				"took": 554
			},
			"start_time_in_millis": 1720352001739
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", 34290),
				model.NewQueryResultCol("aggr__2__key_0", "a1"),
				model.NewQueryResultCol("aggr__2__count", uint64(1036)),
				model.NewQueryResultCol("metric__2__1_col_0", 1091661.7608666667),
				model.NewQueryResultCol("aggr__2__8__parent_count", 1036),
				model.NewQueryResultCol("aggr__2__8__key_0", "b12"),
				model.NewQueryResultCol("aggr__2__8__count", int64(24)),
				model.NewQueryResultCol("metric__2__8__1_col_0", 45774.291766666654),
				model.NewQueryResultCol("aggr__2__8__4__parent_count", 24),
				model.NewQueryResultCol("aggr__2__8__4__key_0", "c12"),
				model.NewQueryResultCol("aggr__2__8__4__count", int64(24)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", 34290),
				model.NewQueryResultCol("aggr__2__key_0", "a1"),
				model.NewQueryResultCol("aggr__2__count", uint64(1036)),
				model.NewQueryResultCol("metric__2__1_col_0", 1091661.7608666667),
				model.NewQueryResultCol("aggr__2__8__parent_count", 1036),
				model.NewQueryResultCol("aggr__2__8__key_0", "b11"),
				model.NewQueryResultCol("aggr__2__8__count", int64(21)),
				model.NewQueryResultCol("metric__2__8__1_col_0", 51891.94613333333),
				model.NewQueryResultCol("aggr__2__8__4__parent_count", 21),
				model.NewQueryResultCol("aggr__2__8__4__key_0", "c11"),
				model.NewQueryResultCol("aggr__2__8__4__count", int64(21)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", 34290),
				model.NewQueryResultCol("aggr__2__key_0", "a2"),
				model.NewQueryResultCol("aggr__2__count", uint64(34)),
				model.NewQueryResultCol("metric__2__1_col_0", 630270.07765),
				model.NewQueryResultCol("aggr__2__8__parent_count", 34),
				model.NewQueryResultCol("aggr__2__8__key_0", "b22"),
				model.NewQueryResultCol("aggr__2__8__count", int64(17)),
				model.NewQueryResultCol("metric__2__8__1_col_0", 231143.3279666666),
				model.NewQueryResultCol("aggr__2__8__4__parent_count", 17),
				model.NewQueryResultCol("aggr__2__8__4__key_0", "c22"),
				model.NewQueryResultCol("aggr__2__8__4__count", int64(17)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", 34290),
				model.NewQueryResultCol("aggr__2__key_0", "a2"),
				model.NewQueryResultCol("aggr__2__count", uint64(34)),
				model.NewQueryResultCol("metric__2__1_col_0", 630270.07765),
				model.NewQueryResultCol("aggr__2__8__parent_count", 34),
				model.NewQueryResultCol("aggr__2__8__key_0", "b21"),
				model.NewQueryResultCol("aggr__2__8__count", int64(17)),
				model.NewQueryResultCol("metric__2__8__1_col_0", 399126.7496833334),
				model.NewQueryResultCol("aggr__2__8__4__parent_count", 17),
				model.NewQueryResultCol("aggr__2__8__4__key_0", "c21"),
				model.NewQueryResultCol("aggr__2__8__4__count", int64(17)),
			}},
		},
		ExpectedPancakeSQL: `
			 SELECT "aggr__2__parent_count", "aggr__2__key_0", "aggr__2__count",
			  "metric__2__1_col_0", "aggr__2__8__parent_count", "aggr__2__8__key_0",
			  "aggr__2__8__count", "metric__2__8__1_col_0", "aggr__2__8__4__parent_count",
			  "aggr__2__8__4__key_0", "aggr__2__8__4__count"
			FROM (
			  SELECT "aggr__2__parent_count", "aggr__2__key_0", "aggr__2__count",
				"metric__2__1_col_0", "aggr__2__8__parent_count", "aggr__2__8__key_0",
				"aggr__2__8__count", "metric__2__8__1_col_0", "aggr__2__8__4__parent_count",
				"aggr__2__8__4__key_0", "aggr__2__8__4__count",
				dense_rank() OVER (ORDER BY "metric__2__1_col_0" DESC, "aggr__2__key_0" ASC)
				AS "aggr__2__order_1_rank",
				dense_rank() OVER (PARTITION BY "aggr__2__key_0" ORDER BY
				"metric__2__8__1_col_0" ASC, "aggr__2__8__key_0" ASC) AS
				"aggr__2__8__order_1_rank",
				dense_rank() OVER (PARTITION BY "aggr__2__key_0", "aggr__2__8__key_0" ORDER
				BY "aggr__2__8__4__key_0" DESC) AS "aggr__2__8__4__order_1_rank"
			  FROM (
				SELECT sum(count(*)) OVER () AS "aggr__2__parent_count",
				  "surname" AS "aggr__2__key_0",
				  sum(count(*)) OVER (PARTITION BY "aggr__2__key_0") AS "aggr__2__count",
				  avgOrNullMerge(avgOrNullState("total")) OVER (PARTITION BY
				  "aggr__2__key_0") AS "metric__2__1_col_0",
				  sum(count(*)) OVER (PARTITION BY "aggr__2__key_0") AS
				  "aggr__2__8__parent_count",
				  COALESCE("limbName", '__missing__') AS "aggr__2__8__key_0",
				  sum(count(*)) OVER (PARTITION BY "aggr__2__key_0", "aggr__2__8__key_0") AS
				  "aggr__2__8__count",
				  sumOrNull(sumOrNull("total")) OVER (PARTITION BY "aggr__2__key_0",
				  "aggr__2__8__key_0") AS "metric__2__8__1_col_0",
				  sum(count(*)) OVER (PARTITION BY "aggr__2__key_0", "aggr__2__8__key_0") AS
				  "aggr__2__8__4__parent_count", "organName" AS "aggr__2__8__4__key_0",
				  count(*) AS "aggr__2__8__4__count"
				FROM __quesma_table_name
				GROUP BY "surname" AS "aggr__2__key_0",
				  COALESCE("limbName", '__missing__') AS "aggr__2__8__key_0",
				  "organName" AS "aggr__2__8__4__key_0"))
			WHERE (("aggr__2__order_1_rank"<=201 AND "aggr__2__8__order_1_rank"<=20) AND
			  "aggr__2__8__4__order_1_rank"<=2)
			ORDER BY "aggr__2__order_1_rank" ASC, "aggr__2__8__order_1_rank" ASC,
			  "aggr__2__8__4__order_1_rank" ASC`,
	},
	{ // [4]
		TestName: "Ophelia Test 5: 4x terms + order by another aggregations",
		QueryRequestJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"2": {
					"aggs": {
						"8": {
							"aggs": {
								"4": {
									"terms": {
										"field": "organName",
										"order": {
											"_key": "desc"
										},
										"shard_size": 25,
										"size": 1
									},
									"aggs": {
										"5": {
											"terms": {
												"field": "organName",
												"size": 2
											}
										}
									}
								},
								"1": {
									"sum": {
										"field": "total"
									}
								}
							},
							"terms": {
								"field": "limbName",
								"missing": "__missing__",
								"order": {
									"1": "asc"
								},
								"size": 20
							}
						}
					},
					"terms": {
						"field": "surname",
						"order": {
							"_key": "desc"
						},
						"shard_size": 1000,
						"size": 200
					}
				}
			},
			"fields": [
				{
					"field": "@timestamp",
					"format": "date_time"
				},
				{
					"field": "createdAt",
					"format": "date_time"
				},
				{
					"field": "date",
					"format": "date_time"
				},
				{
					"field": "endTime",
					"format": "date_time"
				},
				{
					"field": "startTime",
					"format": "date_time"
				}
			],
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
			"completion_time_in_millis": 1720352002293,
			"expiration_time_in_millis": 1720352062445,
			"id": "FnpTUXdfTTZLUlBtQVo1YzBTVFBseEEcM19IaHdFWG5RN1d1eV9VaUcxenYwdzo0MTc0MA==",
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
					"2": {
						"buckets": [
							{
								"8": {
									"buckets": [
										{
											"1": {
												"value": 231143.3279666666
											},
											"4": {
												"buckets": [
													{
														"doc_count": 17,
														"key": "c22",
														"5": {
															"buckets": [
																{
																	"doc_count": 17,
																	"key": "d22"
																}
															],
															"sum_other_doc_count": 0
														}
													}
												],
												"doc_count_error_upper_bound": 0,
												"sum_other_doc_count": 0
											},
											"doc_count": 17,
											"key": "b22"
										},
										{
											"1": {
												"value": 399126.7496833334
											},
											"4": {
												"buckets": [
													{
														"doc_count": 17,
														"key": "c21",
														"5": {
															"buckets": [
																{
																	"doc_count": 17,
																	"key": "d21"
																}
															],
															"sum_other_doc_count": 0
														}
													}
												],
												"doc_count_error_upper_bound": 0,
												"sum_other_doc_count": 0
											},
											"doc_count": 17,
											"key": "b21"
										}
									],
									"doc_count_error_upper_bound": 0,
									"sum_other_doc_count": 0
								},
								"doc_count": 34,
								"key": "a2"
							},
							{
								"8": {
									"buckets": [
										{
											"1": {
												"value": 45774.291766666654
											},
											"4": {
												"buckets": [
													{
														"doc_count": 24,
														"key": "c12",
														"5": {
															"buckets": [
																{
																	"doc_count": 24,
																	"key": "d12"
																}
															],
															"sum_other_doc_count": 0
														}
													}
												],
												"doc_count_error_upper_bound": 0,
												"sum_other_doc_count": 0
											},
											"doc_count": 24,
											"key": "b12"
										},
										{
											"1": {
												"value": 51891.94613333333
											},
											"4": {
												"buckets": [
													{
														"doc_count": 21,
														"key": "c11",
														"5": {
															"buckets": [
																{
																	"doc_count": 21,
																	"key": "d11"
																}
															],
															"sum_other_doc_count": 0
														}
													}
												],
												"doc_count_error_upper_bound": 0,
												"sum_other_doc_count": 0
											},
											"doc_count": 21,
											"key": "b11"
										}
									],
									"doc_count_error_upper_bound": -1,
									"sum_other_doc_count": 991
								},
								"doc_count": 1036,
								"key": "a1"
							}
						],
						"doc_count_error_upper_bound": -1,
						"sum_other_doc_count": 33220
					}
				},
				"hits": {
					"hits": [],
					"max_score": null,
					"total": {
						"relation": "eq",
						"value": 50427
					}
				},
				"timed_out": false,
				"took": 554
			},
			"start_time_in_millis": 1720352001739
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", 34290),
				model.NewQueryResultCol("aggr__2__key_0", "a2"),
				model.NewQueryResultCol("aggr__2__count", uint64(34)),
				model.NewQueryResultCol("aggr__2__8__parent_count", 34),
				model.NewQueryResultCol("aggr__2__8__key_0", "b22"),
				model.NewQueryResultCol("aggr__2__8__count", int64(17)),
				model.NewQueryResultCol("metric__2__8__1_col_0", 231143.3279666666),
				model.NewQueryResultCol("aggr__2__8__4__parent_count", 17),
				model.NewQueryResultCol("aggr__2__8__4__key_0", "c22"),
				model.NewQueryResultCol("aggr__2__8__4__count", int64(17)),
				model.NewQueryResultCol("aggr__2__8__4__5__parent_count", 17),
				model.NewQueryResultCol("aggr__2__8__4__5__key_0", "d22"),
				model.NewQueryResultCol("aggr__2__8__4__5__count", int64(17)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", 34290),
				model.NewQueryResultCol("aggr__2__key_0", "a2"),
				model.NewQueryResultCol("aggr__2__count", uint64(34)),
				model.NewQueryResultCol("aggr__2__8__parent_count", 34),
				model.NewQueryResultCol("aggr__2__8__key_0", "b21"),
				model.NewQueryResultCol("aggr__2__8__count", int64(17)),
				model.NewQueryResultCol("metric__2__8__1_col_0", 399126.7496833334),
				model.NewQueryResultCol("aggr__2__8__4__parent_count", 17),
				model.NewQueryResultCol("aggr__2__8__4__key_0", "c21"),
				model.NewQueryResultCol("aggr__2__8__4__count", int64(17)),
				model.NewQueryResultCol("aggr__2__8__4__5__parent_count", 17),
				model.NewQueryResultCol("aggr__2__8__4__5__key_0", "d21"),
				model.NewQueryResultCol("aggr__2__8__4__5__count", int64(17)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", 34290),
				model.NewQueryResultCol("aggr__2__key_0", "a1"),
				model.NewQueryResultCol("aggr__2__count", uint64(1036)),
				model.NewQueryResultCol("aggr__2__8__parent_count", 1036),
				model.NewQueryResultCol("aggr__2__8__key_0", "b12"),
				model.NewQueryResultCol("aggr__2__8__count", int64(24)),
				model.NewQueryResultCol("metric__2__8__1_col_0", 45774.291766666654),
				model.NewQueryResultCol("aggr__2__8__4__parent_count", 24),
				model.NewQueryResultCol("aggr__2__8__4__key_0", "c12"),
				model.NewQueryResultCol("aggr__2__8__4__count", int64(24)),
				model.NewQueryResultCol("aggr__2__8__4__5__parent_count", 24),
				model.NewQueryResultCol("aggr__2__8__4__5__key_0", "d12"),
				model.NewQueryResultCol("aggr__2__8__4__5__count", int64(24)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", 34290),
				model.NewQueryResultCol("aggr__2__key_0", "a1"),
				model.NewQueryResultCol("aggr__2__count", uint64(1036)),
				model.NewQueryResultCol("aggr__2__8__parent_count", 1036),
				model.NewQueryResultCol("aggr__2__8__key_0", "b11"),
				model.NewQueryResultCol("aggr__2__8__count", int64(21)),
				model.NewQueryResultCol("metric__2__8__1_col_0", 51891.94613333333),
				model.NewQueryResultCol("aggr__2__8__4__parent_count", 21),
				model.NewQueryResultCol("aggr__2__8__4__key_0", "c11"),
				model.NewQueryResultCol("aggr__2__8__4__count", int64(21)),
				model.NewQueryResultCol("aggr__2__8__4__5__parent_count", 21),
				model.NewQueryResultCol("aggr__2__8__4__5__key_0", "d11"),
				model.NewQueryResultCol("aggr__2__8__4__5__count", int64(21)),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT "aggr__2__parent_count", "aggr__2__key_0", "aggr__2__count",
			  "aggr__2__8__parent_count", "aggr__2__8__key_0", "aggr__2__8__count",
			  "metric__2__8__1_col_0", "aggr__2__8__4__parent_count",
			  "aggr__2__8__4__key_0", "aggr__2__8__4__count",
			  "aggr__2__8__4__5__parent_count", "aggr__2__8__4__5__key_0",
			  "aggr__2__8__4__5__count"
			FROM (
			  SELECT "aggr__2__parent_count", "aggr__2__key_0", "aggr__2__count",
				"aggr__2__8__parent_count", "aggr__2__8__key_0", "aggr__2__8__count",
				"metric__2__8__1_col_0", "aggr__2__8__4__parent_count",
				"aggr__2__8__4__key_0", "aggr__2__8__4__count",
				"aggr__2__8__4__5__parent_count", "aggr__2__8__4__5__key_0",
				"aggr__2__8__4__5__count",
				dense_rank() OVER (ORDER BY "aggr__2__key_0" DESC) AS
				"aggr__2__order_1_rank",
				dense_rank() OVER (PARTITION BY "aggr__2__key_0" ORDER BY
				"metric__2__8__1_col_0" ASC, "aggr__2__8__key_0" ASC) AS
				"aggr__2__8__order_1_rank",
				dense_rank() OVER (PARTITION BY "aggr__2__key_0", "aggr__2__8__key_0" ORDER
				BY "aggr__2__8__4__key_0" DESC) AS "aggr__2__8__4__order_1_rank",
				dense_rank() OVER (PARTITION BY "aggr__2__key_0", "aggr__2__8__key_0",
				"aggr__2__8__4__key_0" ORDER BY "aggr__2__8__4__5__count" DESC,
				"aggr__2__8__4__5__key_0" ASC) AS "aggr__2__8__4__5__order_1_rank"
			  FROM (
				SELECT sum(count(*)) OVER () AS "aggr__2__parent_count",
				  "surname" AS "aggr__2__key_0",
				  sum(count(*)) OVER (PARTITION BY "aggr__2__key_0") AS "aggr__2__count",
				  sum(count(*)) OVER (PARTITION BY "aggr__2__key_0") AS
				  "aggr__2__8__parent_count",
				  COALESCE("limbName", '__missing__') AS "aggr__2__8__key_0",
				  sum(count(*)) OVER (PARTITION BY "aggr__2__key_0", "aggr__2__8__key_0") AS
				  "aggr__2__8__count",
				  sumOrNull(sumOrNull("total")) OVER (PARTITION BY "aggr__2__key_0",
				  "aggr__2__8__key_0") AS "metric__2__8__1_col_0",
				  sum(count(*)) OVER (PARTITION BY "aggr__2__key_0", "aggr__2__8__key_0") AS
				  "aggr__2__8__4__parent_count", "organName" AS "aggr__2__8__4__key_0",
				  sum(count(*)) OVER (PARTITION BY "aggr__2__key_0", "aggr__2__8__key_0",
				  "aggr__2__8__4__key_0") AS "aggr__2__8__4__count",
				  sum(count(*)) OVER (PARTITION BY "aggr__2__key_0", "aggr__2__8__key_0",
				  "aggr__2__8__4__key_0") AS "aggr__2__8__4__5__parent_count",
				  "organName" AS "aggr__2__8__4__5__key_0",
				  count(*) AS "aggr__2__8__4__5__count"
				FROM __quesma_table_name
				GROUP BY "surname" AS "aggr__2__key_0",
				  COALESCE("limbName", '__missing__') AS "aggr__2__8__key_0",
				  "organName" AS "aggr__2__8__4__key_0",
				  "organName" AS "aggr__2__8__4__5__key_0"))
			WHERE ((("aggr__2__order_1_rank"<=201 AND "aggr__2__8__order_1_rank"<=20) AND
			  "aggr__2__8__4__order_1_rank"<=2) AND "aggr__2__8__4__5__order_1_rank"<=3)
			ORDER BY "aggr__2__order_1_rank" ASC, "aggr__2__8__order_1_rank" ASC,
			  "aggr__2__8__4__order_1_rank" ASC, "aggr__2__8__4__5__order_1_rank" ASC`,
	},
	{ // [5]
		TestName: "Ophelia Test 6: triple terms + other aggregations + order by another aggregations",
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
								"field": "total"
							}
						},
						"8": {
							"aggs": {
								"1": {
									"sum": {
										"field": "total"
									}
								},
								"4": {
									"aggs": {
										"1": {
											"sum": {
												"field": "total"
											}
										},
										"5": {
											"sum": {
												"field": "some"
											}
										}
									},
									"terms": {
										"field": "organName",
										"order": {
											"1": "desc"
										},
										"shard_size": 25,
										"size": 1
									}
								}
							},
							"terms": {
								"field": "limbName",
								"missing": "__missing__",
								"order": {
									"1": "desc"
								},
								"size": 20
							}
						}
					},
					"terms": {
						"field": "surname",
						"order": {
							"1": "desc"
						},
						"shard_size": 1000,
						"size": 200
					}
				}
			},
			"fields": [
				{
					"field": "@timestamp",
					"format": "date_time"
				},
				{
					"field": "createdAt",
					"format": "date_time"
				},
				{
					"field": "date",
					"format": "date_time"
				},
				{
					"field": "endTime",
					"format": "date_time"
				},
				{
					"field": "startTime",
					"format": "date_time"
				}
			],
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
			"completion_time_in_millis": 1720352002293,
			"expiration_time_in_millis": 1720352062445,
			"id": "FnpTUXdfTTZLUlBtQVo1YzBTVFBseEEcM19IaHdFWG5RN1d1eV9VaUcxenYwdzo0MTc0MA==",
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
					"2": {
						"buckets": [
							{
								"1": {
									"value": 1091661.7608666667
								},
								"8": {
									"buckets": [
										{
											"1": {
												"value": 51891.94613333333
											},
											"4": {
												"buckets": [
													{
														"1": {
															"value": 51891.94613333333
														},
														"5": {
															"value": 37988.09523333333
														},
														"doc_count": 21,
														"key": "c11"
													}
												],
												"doc_count_error_upper_bound": 0,
												"sum_other_doc_count": 0
											},
											"doc_count": 21,
											"key": "b11"
										},
										{
											"1": {
												"value": 45774.291766666654
											},
											"4": {
												"buckets": [
													{
														"1": {
															"value": 45774.291766666654
														},
														"5": {
															"value": 36577.89516666666
														},
														"doc_count": 24,
														"key": "c12"
													}
												],
												"doc_count_error_upper_bound": 0,
												"sum_other_doc_count": 0
											},
											"doc_count": 24,
											"key": "b12"
										}
									],
									"doc_count_error_upper_bound": -1,
									"sum_other_doc_count": 991
								},
								"doc_count": 1036,
								"key": "a1"
							},
							{
								"1": {
									"value": 630270.07765
								},
								"8": {
									"buckets": [
										{
											"1": {
												"value": 399126.7496833334
											},
											"4": {
												"buckets": [
													{
														"1": {
															"value": 399126.7496833334
														},
														"5": {
															"value": 337246.82201666664
														},
														"doc_count": 17,
														"key": "c21"
													}
												],
												"doc_count_error_upper_bound": 0,
												"sum_other_doc_count": 0
											},
											"doc_count": 17,
											"key": "b21"
										},
										{
											"1": {
												"value": 231143.3279666666
											},
											"4": {
												"buckets": [
													{
														"1": {
															"value": 231143.3279666666
														},
														"5": {
															"value": 205408.48849999998
														},
														"doc_count": 17,
														"key": "c22"
													}
												],
												"doc_count_error_upper_bound": 0,
												"sum_other_doc_count": 0
											},
											"doc_count": 17,
											"key": "b22"
										}
									],
									"doc_count_error_upper_bound": 0,
									"sum_other_doc_count": 0
								},
								"doc_count": 34,
								"key": "a2"
							}
						],
						"doc_count_error_upper_bound": -1,
						"sum_other_doc_count": 33220
					}
				},
				"hits": {
					"hits": [],
					"max_score": null,
					"total": {
						"relation": "eq",
						"value": 50427
					}
				},
				"timed_out": false,
				"took": 554
			},
			"start_time_in_millis": 1720352001739
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", 34290),
				model.NewQueryResultCol("aggr__2__key_0", "a1"),
				model.NewQueryResultCol("aggr__2__count", uint64(1036)),
				model.NewQueryResultCol("metric__2__1_col_0", 1091661.7608666667),
				model.NewQueryResultCol("aggr__2__8__parent_count", 1036),
				model.NewQueryResultCol("aggr__2__8__key_0", "b11"),
				model.NewQueryResultCol("aggr__2__8__count", int64(21)),
				model.NewQueryResultCol("metric__2__8__1_col_0", 51891.94613333333),
				model.NewQueryResultCol("aggr__2__8__4__parent_count", 21),
				model.NewQueryResultCol("aggr__2__8__4__key_0", "c11"),
				model.NewQueryResultCol("aggr__2__8__4__count", int64(21)),
				model.NewQueryResultCol("metric__2__8__4__1_col_0", 51891.94613333333),
				model.NewQueryResultCol("metric__2__8__4__5_col_0", 37988.09523333333),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", 34290),
				model.NewQueryResultCol("aggr__2__key_0", "a1"),
				model.NewQueryResultCol("aggr__2__count", uint64(1036)),
				model.NewQueryResultCol("metric__2__1_col_0", 1091661.7608666667),
				model.NewQueryResultCol("aggr__2__8__parent_count", 1036),
				model.NewQueryResultCol("aggr__2__8__key_0", "b12"),
				model.NewQueryResultCol("aggr__2__8__count", int64(24)),
				model.NewQueryResultCol("metric__2__8__1_col_0", 45774.291766666654),
				model.NewQueryResultCol("aggr__2__8__4__parent_count", 24),
				model.NewQueryResultCol("aggr__2__8__4__key_0", "c12"),
				model.NewQueryResultCol("aggr__2__8__4__count", int64(24)),
				model.NewQueryResultCol("metric__2__8__4__1_col_0", 45774.291766666654),
				model.NewQueryResultCol("metric__2__8__4__5_col_0", 36577.89516666666),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", 34290),
				model.NewQueryResultCol("aggr__2__key_0", "a2"),
				model.NewQueryResultCol("aggr__2__count", uint64(34)),
				model.NewQueryResultCol("metric__2__1_col_0", 630270.07765),
				model.NewQueryResultCol("aggr__2__8__parent_count", 34),
				model.NewQueryResultCol("aggr__2__8__key_0", "b21"),
				model.NewQueryResultCol("aggr__2__8__count", int64(17)),
				model.NewQueryResultCol("metric__2__8__1_col_0", 399126.7496833334),
				model.NewQueryResultCol("aggr__2__8__4__parent_count", 17),
				model.NewQueryResultCol("aggr__2__8__4__key_0", "c21"),
				model.NewQueryResultCol("aggr__2__8__4__count", int64(17)),
				model.NewQueryResultCol("metric__2__8__4__1_col_0", 399126.7496833334),
				model.NewQueryResultCol("metric__2__8__4__5_col_0", 337246.82201666664),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", 34290),
				model.NewQueryResultCol("aggr__2__key_0", "a2"),
				model.NewQueryResultCol("aggr__2__count", uint64(34)),
				model.NewQueryResultCol("metric__2__1_col_0", 630270.07765),
				model.NewQueryResultCol("aggr__2__8__parent_count", 34),
				model.NewQueryResultCol("aggr__2__8__key_0", "b22"),
				model.NewQueryResultCol("aggr__2__8__count", int64(17)),
				model.NewQueryResultCol("metric__2__8__1_col_0", 231143.3279666666),
				model.NewQueryResultCol("aggr__2__8__4__parent_count", 17),
				model.NewQueryResultCol("aggr__2__8__4__key_0", "c22"),
				model.NewQueryResultCol("aggr__2__8__4__count", int64(17)),
				model.NewQueryResultCol("metric__2__8__4__1_col_0", 231143.3279666666),
				model.NewQueryResultCol("metric__2__8__4__5_col_0", 205408.48849999998),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT "aggr__2__parent_count", "aggr__2__key_0", "aggr__2__count",
			  "metric__2__1_col_0", "aggr__2__8__parent_count", "aggr__2__8__key_0",
			  "aggr__2__8__count", "metric__2__8__1_col_0", "aggr__2__8__4__parent_count",
			  "aggr__2__8__4__key_0", "aggr__2__8__4__count", "metric__2__8__4__1_col_0",
			  "metric__2__8__4__5_col_0"
			FROM (
			  SELECT "aggr__2__parent_count", "aggr__2__key_0", "aggr__2__count",
				"metric__2__1_col_0", "aggr__2__8__parent_count", "aggr__2__8__key_0",
				"aggr__2__8__count", "metric__2__8__1_col_0", "aggr__2__8__4__parent_count",
				"aggr__2__8__4__key_0", "aggr__2__8__4__count", "metric__2__8__4__1_col_0",
				"metric__2__8__4__5_col_0",
				dense_rank() OVER (ORDER BY "metric__2__1_col_0" DESC, "aggr__2__key_0" ASC)
				AS "aggr__2__order_1_rank",
				dense_rank() OVER (PARTITION BY "aggr__2__key_0" ORDER BY
				"metric__2__8__1_col_0" DESC, "aggr__2__8__key_0" ASC) AS
				"aggr__2__8__order_1_rank",
				dense_rank() OVER (PARTITION BY "aggr__2__key_0", "aggr__2__8__key_0" ORDER
				BY "metric__2__8__4__1_col_0" DESC, "aggr__2__8__4__key_0" ASC) AS
				"aggr__2__8__4__order_1_rank"
			  FROM (
				SELECT sum(count(*)) OVER () AS "aggr__2__parent_count",
				  "surname" AS "aggr__2__key_0",
				  sum(count(*)) OVER (PARTITION BY "aggr__2__key_0") AS "aggr__2__count",
				  sumOrNull(sumOrNull("total")) OVER (PARTITION BY "aggr__2__key_0") AS
				  "metric__2__1_col_0",
				  sum(count(*)) OVER (PARTITION BY "aggr__2__key_0") AS
				  "aggr__2__8__parent_count",
				  COALESCE("limbName", '__missing__') AS "aggr__2__8__key_0",
				  sum(count(*)) OVER (PARTITION BY "aggr__2__key_0", "aggr__2__8__key_0") AS
				  "aggr__2__8__count",
				  sumOrNull(sumOrNull("total")) OVER (PARTITION BY "aggr__2__key_0",
				  "aggr__2__8__key_0") AS "metric__2__8__1_col_0",
				  sum(count(*)) OVER (PARTITION BY "aggr__2__key_0", "aggr__2__8__key_0") AS
				  "aggr__2__8__4__parent_count", "organName" AS "aggr__2__8__4__key_0",
				  count(*) AS "aggr__2__8__4__count",
				  sumOrNull("total") AS "metric__2__8__4__1_col_0",
				  sumOrNull("some") AS "metric__2__8__4__5_col_0"
				FROM __quesma_table_name
				GROUP BY "surname" AS "aggr__2__key_0",
				  COALESCE("limbName", '__missing__') AS "aggr__2__8__key_0",
				  "organName" AS "aggr__2__8__4__key_0"))
			WHERE (("aggr__2__order_1_rank"<=201 AND "aggr__2__8__order_1_rank"<=20) AND
			  "aggr__2__8__4__order_1_rank"<=2)
			ORDER BY "aggr__2__order_1_rank" ASC, "aggr__2__8__order_1_rank" ASC,
			  "aggr__2__8__4__order_1_rank" ASC`,
	},
	{ // [6]
		TestName: "Ophelia Test 7: 5x terms + a lot of other aggregations + different order bys",
		QueryRequestJson: `
		{
			"aggs": {
				"2": {
					"terms": {
						"field": "surname",
						"order": {
							"1": "desc"
						},
						"size": 100,
						"shard_size": 2000
					},
					"aggs": {
						"1": {
							"sum": {
								"field": "total"
							}
						},
						"7": {
							"terms": {
								"field": "limbName",
								"order": {
									"_key": "asc"
								},
								"missing": "__missing__",
								"size": 10,
								"shard_size": 25
							},
							"aggs": {
								"1": {
									"sum": {
										"field": "total"
									}
								},
								"8": {
									"terms": {
										"field": "organName",
										"order": {
											"_count": "desc"
										},
										"missing": "__missing__",
										"size": 10,
										"shard_size": 25
									},
									"aggs": {
										"1": {
											"sum": {
												"field": "total"
											}
										},
										"4": {
											"terms": {
												"field": "doctorName",
												"order": {	
													"1": "desc"
												},
												"size": 6,
												"shard_size": 25
											},
											"aggs": {
												"1": {
													"sum": {
														"field": "total"
													}
												},
												"3": {
													"terms": {
														"field": "height",
														"order": {
															"6": "asc"
														},
														"size": 1,
														"shard_size": 25
													},
													"aggs": {
														"1": {
															"sum": {
																"field": "total"
															}
														},
														"5": {
															"sum": {
																"field": "some"
															}
														},
														"6": {
															"sum": {
																"field": "cost"
															}
														}
													}
												}
											}
										}
									}
								}
							}
						}
					}
				}
			},
			"size": 0,
			"fields": [
				{
					"field": "@timestamp",
					"format": "date_time"
				},
				{
					"field": "date",
					"format": "date_time"
				}
			],
			"script_fields": {},
			"stored_fields": [
				"*"
			],
			"runtime_mappings": {},
			"_source": {
				"excludes": []
			},
		}`,
		ExpectedResponse: `
		{
			"completion_time_in_millis": 1720352002293,
			"expiration_time_in_millis": 1720352062445,
			"id": "FnpTUXdfTTZLUlBtQVo1YzBTVFBseEEcM19IaHdFWG5RN1d1eV9VaUcxenYwdzo0MTc0MA==",
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
					"2": {
						"buckets": [
							{
								"1": {
									"value": 1091661.7608666667
								},
								"7": {
									"buckets": [
										{
											"1": {
												"value": 51891.94613333333
											},
											"8": {
												"buckets": [
													{
														"1": {
															"value": 51891.94613333333
														},
														"4": {
															"buckets": [
																{
																	"doc_count": 10,
																	"key": "d11",
																	"1": {
																		"value": 1.1
																	},
																	"3": {
																		"buckets": [
																			{
																				"doc_count": 3,
																				"key": "e11",
																				"1": {
																					"value": -1
																				},
																				"5": {
																					"value": -2
																				},
																				"6": {
																					"value": -3	
																				}
																			}
																		],
																		"doc_count_error_upper_bound": 0,
																		"sum_other_doc_count": 7
																	}
																},
																{
																	"doc_count": 5,
																	"key": "d12",
																	"1": {
																		"value": 2.2
																	},
																	"3": {
																		"buckets": [
																			{
																				"doc_count": 1,
																				"key": "e12",
																				"1": {
																					"value": null
																				},
																				"5": {
																					"value": -22
																				},
																				"6": {
																					"value": -33
																				}
																			}
																		],
																		"doc_count_error_upper_bound": 0,
																		"sum_other_doc_count": 4
																	}
																}
															],
															"doc_count_error_upper_bound": 0,
															"sum_other_doc_count": 6
														},
														"doc_count": 21,
														"key": "c1"
													}
												],
												"doc_count_error_upper_bound": 0,
												"sum_other_doc_count": 0
											},
											"doc_count": 21,
											"key": "b1"
										}
									],
									"doc_count_error_upper_bound": -1,
									"sum_other_doc_count": 1015
								},
								"doc_count": 1036,
								"key": "a1"
							},
							{
								"1": {
									"value": 0
								},
								"7": {
									"buckets": [
										{
											"1": {
												"value": 0.1
											},
											"8": {
												"buckets": [
													{
														"1": {
															"value": 0.2
														},
														"4": {
															"buckets": [
																{
																	"doc_count": 2,
																	"key": "d2",
																	"1": {
																		"value": 0.3
																	},
																	"3": {
																		"buckets": [
																			{
																				"doc_count": 1,
																				"key": "e2",
																				"1": {
																					"value": -0.4
																				},
																				"5": {
																					"value": -0.5
																				},
																				"6": {
																					"value": -0.6
																				}
																			}
																		],
																		"doc_count_error_upper_bound": 0,
																		"sum_other_doc_count": 1
																	}
																}
															],
															"doc_count_error_upper_bound": 0,
															"sum_other_doc_count": 1
														},
														"doc_count": 3,
														"key": "c2"
													}
												],
												"doc_count_error_upper_bound": 0,
												"sum_other_doc_count": 1
											},
											"doc_count": 4,
											"key": "b2"
										}
									],
									"doc_count_error_upper_bound": -1,
									"sum_other_doc_count": 1
								},
								"doc_count": 5,
								"key": "a2"
							}
						],
						"doc_count_error_upper_bound": -1,
						"sum_other_doc_count": 49386
					}
				},
				"hits": {
					"hits": [],
					"max_score": null,
					"total": {
						"relation": "eq",
						"value": 50427
					}
				},
				"timed_out": false,
				"took": 554
			},
			"start_time_in_millis": 1720352001739
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", 50427),
				model.NewQueryResultCol("aggr__2__key_0", "a1"),
				model.NewQueryResultCol("aggr__2__count", uint64(1036)),
				model.NewQueryResultCol("metric__2__1_col_0", 1091661.7608666667),
				model.NewQueryResultCol("aggr__2__7__parent_count", 1036),
				model.NewQueryResultCol("aggr__2__7__key_0", "b1"),
				model.NewQueryResultCol("aggr__2__7__count", int64(21)),
				model.NewQueryResultCol("metric__2__7__1_col_0", 51891.94613333333),
				model.NewQueryResultCol("aggr__2__7__8__parent_count", 21),
				model.NewQueryResultCol("aggr__2__7__8__key_0", "c1"),
				model.NewQueryResultCol("aggr__2__7__8__count", int64(21)),
				model.NewQueryResultCol("metric__2__7__8__1_col_0", 51891.94613333333),
				model.NewQueryResultCol("aggr__2__7__8__4__parent_count", 21),
				model.NewQueryResultCol("aggr__2__7__8__4__key_0", "d11"),
				model.NewQueryResultCol("aggr__2__7__8__4__count", int64(10)),
				model.NewQueryResultCol("metric__2__7__8__4__1_col_0", 1.1),
				model.NewQueryResultCol("aggr__2__7__8__4__3__parent_count", 10),
				model.NewQueryResultCol("aggr__2__7__8__4__3__key_0", "e11"),
				model.NewQueryResultCol("aggr__2__7__8__4__3__count", int64(3)),
				model.NewQueryResultCol("metric__2__7__8__4__3__1_col_0", -1),
				model.NewQueryResultCol("metric__2__7__8__4__3__5_col_0", -2),
				model.NewQueryResultCol("metric__2__7__8__4__3__6_col_0", -3),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", 50427),
				model.NewQueryResultCol("aggr__2__key_0", "a1"),
				model.NewQueryResultCol("aggr__2__count", uint64(1036)),
				model.NewQueryResultCol("metric__2__1_col_0", 1091661.7608666667),
				model.NewQueryResultCol("aggr__2__7__parent_count", 1036),
				model.NewQueryResultCol("aggr__2__7__key_0", "b1"),
				model.NewQueryResultCol("aggr__2__7__count", int64(21)),
				model.NewQueryResultCol("metric__2__7__1_col_0", 51891.94613333333),
				model.NewQueryResultCol("aggr__2__7__8__parent_count", 21),
				model.NewQueryResultCol("aggr__2__7__8__key_0", "c1"),
				model.NewQueryResultCol("aggr__2__7__8__count", int64(21)),
				model.NewQueryResultCol("metric__2__7__8__1_col_0", 51891.94613333333),
				model.NewQueryResultCol("aggr__2__7__8__4__parent_count", 21),
				model.NewQueryResultCol("aggr__2__7__8__4__key_0", "d12"),
				model.NewQueryResultCol("aggr__2__7__8__4__count", int64(5)),
				model.NewQueryResultCol("metric__2__7__8__4__1_col_0", 2.2),
				model.NewQueryResultCol("aggr__2__7__8__4__3__parent_count", 5),
				model.NewQueryResultCol("aggr__2__7__8__4__3__key_0", "e12"),
				model.NewQueryResultCol("aggr__2__7__8__4__3__count", int64(1)),
				model.NewQueryResultCol("metric__2__7__8__4__3__1_col_0", nil),
				model.NewQueryResultCol("metric__2__7__8__4__3__5_col_0", -22),
				model.NewQueryResultCol("metric__2__7__8__4__3__6_col_0", -33),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", 50427),
				model.NewQueryResultCol("aggr__2__key_0", "a2"),
				model.NewQueryResultCol("aggr__2__count", uint64(5)),
				model.NewQueryResultCol("metric__2__1_col_0", 0),
				model.NewQueryResultCol("aggr__2__7__parent_count", 5),
				model.NewQueryResultCol("aggr__2__7__key_0", "b2"),
				model.NewQueryResultCol("aggr__2__7__count", int64(4)),
				model.NewQueryResultCol("metric__2__7__1_col_0", 0.1),
				model.NewQueryResultCol("aggr__2__7__8__parent_count", 4),
				model.NewQueryResultCol("aggr__2__7__8__key_0", "c2"),
				model.NewQueryResultCol("aggr__2__7__8__count", int64(3)),
				model.NewQueryResultCol("metric__2__7__8__1_col_0", 0.2),
				model.NewQueryResultCol("aggr__2__7__8__4__parent_count", 3),
				model.NewQueryResultCol("aggr__2__7__8__4__key_0", "d2"),
				model.NewQueryResultCol("aggr__2__7__8__4__count", int64(2)),
				model.NewQueryResultCol("metric__2__7__8__4__1_col_0", 0.3),
				model.NewQueryResultCol("aggr__2__7__8__4__3__parent_count", 2),
				model.NewQueryResultCol("aggr__2__7__8__4__3__key_0", "e2"),
				model.NewQueryResultCol("aggr__2__7__8__4__3__count", int64(1)),
				model.NewQueryResultCol("metric__2__7__8__4__3__1_col_0", -0.4),
				model.NewQueryResultCol("metric__2__7__8__4__3__5_col_0", -0.5),
				model.NewQueryResultCol("metric__2__7__8__4__3__6_col_0", -0.6),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT "aggr__2__parent_count", "aggr__2__key_0", "aggr__2__count",
			  "metric__2__1_col_0", "aggr__2__7__parent_count", "aggr__2__7__key_0",
			  "aggr__2__7__count", "metric__2__7__1_col_0", "aggr__2__7__8__parent_count",
			  "aggr__2__7__8__key_0", "aggr__2__7__8__count", "metric__2__7__8__1_col_0",
			  "aggr__2__7__8__4__parent_count", "aggr__2__7__8__4__key_0",
			  "aggr__2__7__8__4__count", "metric__2__7__8__4__1_col_0",
			  "aggr__2__7__8__4__3__parent_count", "aggr__2__7__8__4__3__key_0",
			  "aggr__2__7__8__4__3__count", "metric__2__7__8__4__3__1_col_0",
			  "metric__2__7__8__4__3__5_col_0", "metric__2__7__8__4__3__6_col_0"
			FROM (
			  SELECT "aggr__2__parent_count", "aggr__2__key_0", "aggr__2__count",
				"metric__2__1_col_0", "aggr__2__7__parent_count", "aggr__2__7__key_0",
				"aggr__2__7__count", "metric__2__7__1_col_0", "aggr__2__7__8__parent_count",
				"aggr__2__7__8__key_0", "aggr__2__7__8__count", "metric__2__7__8__1_col_0",
				"aggr__2__7__8__4__parent_count", "aggr__2__7__8__4__key_0",
				"aggr__2__7__8__4__count", "metric__2__7__8__4__1_col_0",
				"aggr__2__7__8__4__3__parent_count", "aggr__2__7__8__4__3__key_0",
				"aggr__2__7__8__4__3__count", "metric__2__7__8__4__3__1_col_0",
				"metric__2__7__8__4__3__5_col_0", "metric__2__7__8__4__3__6_col_0",
				dense_rank() OVER (ORDER BY "metric__2__1_col_0" DESC, "aggr__2__key_0" ASC)
				AS "aggr__2__order_1_rank",
				dense_rank() OVER (PARTITION BY "aggr__2__key_0" ORDER BY
				"aggr__2__7__key_0" ASC) AS "aggr__2__7__order_1_rank",
				dense_rank() OVER (PARTITION BY "aggr__2__key_0", "aggr__2__7__key_0" ORDER
				BY "aggr__2__7__8__count" DESC, "aggr__2__7__8__key_0" ASC) AS
				"aggr__2__7__8__order_1_rank",
				dense_rank() OVER (PARTITION BY "aggr__2__key_0", "aggr__2__7__key_0",
				"aggr__2__7__8__key_0" ORDER BY "metric__2__7__8__4__1_col_0" DESC,
				"aggr__2__7__8__4__key_0" ASC) AS "aggr__2__7__8__4__order_1_rank",
				dense_rank() OVER (PARTITION BY "aggr__2__key_0", "aggr__2__7__key_0",
				"aggr__2__7__8__key_0", "aggr__2__7__8__4__key_0" ORDER BY
				"metric__2__7__8__4__3__6_col_0" ASC, "aggr__2__7__8__4__3__key_0" ASC) AS
				"aggr__2__7__8__4__3__order_1_rank"
			  FROM (
				SELECT sum(count(*)) OVER () AS "aggr__2__parent_count",
				  "surname" AS "aggr__2__key_0",
				  sum(count(*)) OVER (PARTITION BY "aggr__2__key_0") AS "aggr__2__count",
				  sumOrNull(sumOrNull("total")) OVER (PARTITION BY "aggr__2__key_0") AS
				  "metric__2__1_col_0",
				  sum(count(*)) OVER (PARTITION BY "aggr__2__key_0") AS
				  "aggr__2__7__parent_count",
				  COALESCE("limbName", '__missing__') AS "aggr__2__7__key_0",
				  sum(count(*)) OVER (PARTITION BY "aggr__2__key_0", "aggr__2__7__key_0") AS
				  "aggr__2__7__count",
				  sumOrNull(sumOrNull("total")) OVER (PARTITION BY "aggr__2__key_0",
				  "aggr__2__7__key_0") AS "metric__2__7__1_col_0",
				  sum(count(*)) OVER (PARTITION BY "aggr__2__key_0", "aggr__2__7__key_0") AS
				  "aggr__2__7__8__parent_count",
				  COALESCE("organName", '__missing__') AS "aggr__2__7__8__key_0",
				  sum(count(*)) OVER (PARTITION BY "aggr__2__key_0", "aggr__2__7__key_0",
				  "aggr__2__7__8__key_0") AS "aggr__2__7__8__count",
				  sumOrNull(sumOrNull("total")) OVER (PARTITION BY "aggr__2__key_0",
				  "aggr__2__7__key_0", "aggr__2__7__8__key_0") AS "metric__2__7__8__1_col_0"
				  ,
				  sum(count(*)) OVER (PARTITION BY "aggr__2__key_0", "aggr__2__7__key_0",
				  "aggr__2__7__8__key_0") AS "aggr__2__7__8__4__parent_count",
				  "doctorName" AS "aggr__2__7__8__4__key_0",
				  sum(count(*)) OVER (PARTITION BY "aggr__2__key_0", "aggr__2__7__key_0",
				  "aggr__2__7__8__key_0", "aggr__2__7__8__4__key_0") AS
				  "aggr__2__7__8__4__count",
				  sumOrNull(sumOrNull("total")) OVER (PARTITION BY "aggr__2__key_0",
				  "aggr__2__7__key_0", "aggr__2__7__8__key_0", "aggr__2__7__8__4__key_0") AS
				  "metric__2__7__8__4__1_col_0",
				  sum(count(*)) OVER (PARTITION BY "aggr__2__key_0", "aggr__2__7__key_0",
				  "aggr__2__7__8__key_0", "aggr__2__7__8__4__key_0") AS
				  "aggr__2__7__8__4__3__parent_count",
				  "height" AS "aggr__2__7__8__4__3__key_0",
				  count(*) AS "aggr__2__7__8__4__3__count",
				  sumOrNull("total") AS "metric__2__7__8__4__3__1_col_0",
				  sumOrNull("some") AS "metric__2__7__8__4__3__5_col_0",
				  sumOrNull("cost") AS "metric__2__7__8__4__3__6_col_0"
				FROM __quesma_table_name
				GROUP BY "surname" AS "aggr__2__key_0",
				  COALESCE("limbName", '__missing__') AS "aggr__2__7__key_0",
				  COALESCE("organName", '__missing__') AS "aggr__2__7__8__key_0",
				  "doctorName" AS "aggr__2__7__8__4__key_0",
				  "height" AS "aggr__2__7__8__4__3__key_0"))
			WHERE (((("aggr__2__order_1_rank"<=101 AND "aggr__2__7__order_1_rank"<=10) AND
			  "aggr__2__7__8__order_1_rank"<=10) AND "aggr__2__7__8__4__order_1_rank"<=7)
			  AND "aggr__2__7__8__4__3__order_1_rank"<=2)
			ORDER BY "aggr__2__order_1_rank" ASC, "aggr__2__7__order_1_rank" ASC,
			  "aggr__2__7__8__order_1_rank" ASC, "aggr__2__7__8__4__order_1_rank" ASC,
			  "aggr__2__7__8__4__3__order_1_rank" ASC`,
	},
}
