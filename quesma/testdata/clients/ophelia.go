// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package clients

import (
	"quesma/model"
	"quesma/testdata"
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
		ExpectedResults: [][]model.QueryResultRow{
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a1"),
					model.NewQueryResultCol("limbName", "b11"),
					model.NewQueryResultCol("organName", "c11"),
					model.NewQueryResultCol("count()", 21),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a1"),
					model.NewQueryResultCol("limbName", "b12"),
					model.NewQueryResultCol("organName", "c12"),
					model.NewQueryResultCol("count()", 24),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a2"),
					model.NewQueryResultCol("limbName", "b21"),
					model.NewQueryResultCol("organName", "c21"),
					model.NewQueryResultCol("count()", 17),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a2"),
					model.NewQueryResultCol("limbName", "b22"),
					model.NewQueryResultCol("organName", "c22"),
					model.NewQueryResultCol("count()", 17),
				}},
			},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a1"),
					model.NewQueryResultCol("limbName", "b11"),
					model.NewQueryResultCol("count()", 21),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a1"),
					model.NewQueryResultCol("limbName", "b12"),
					model.NewQueryResultCol("count()", 24),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a2"),
					model.NewQueryResultCol("limbName", "b21"),
					model.NewQueryResultCol("count()", 17),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a2"),
					model.NewQueryResultCol("limbName", "b22"),
					model.NewQueryResultCol("count()", 17),
				}},
			},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a1"),
					model.NewQueryResultCol("count()", 1036),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a2"),
					model.NewQueryResultCol("count()", 34),
				}},
			},
		},
		ExpectedPancakeResults: []model.QueryResultRow{ // mixing different int types (int, int64, uint64) on purpose, at least both (u)int64 can be returned from ClickHouse
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", 34290),
				model.NewQueryResultCol("aggr__2__key_0", "a1"),
				model.NewQueryResultCol("aggr__2__count", uint64(1036)),
				model.NewQueryResultCol("aggr__2__order_1", uint64(1036)),
				model.NewQueryResultCol("aggr__2__8__parent_count", 1036),
				model.NewQueryResultCol("aggr__2__8__key_0", "b11"),
				model.NewQueryResultCol("aggr__2__8__count", int64(21)),
				model.NewQueryResultCol("aggr__2__8__order_1", int64(21)),
				model.NewQueryResultCol("aggr__2__8__4__parent_count", 21),
				model.NewQueryResultCol("aggr__2__8__4__key_0", "c11"),
				model.NewQueryResultCol("aggr__2__8__4__count", int64(21)),
				model.NewQueryResultCol("aggr__2__8__4__order_1", int64(21)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", 34290),
				model.NewQueryResultCol("aggr__2__key_0", "a1"),
				model.NewQueryResultCol("aggr__2__count", uint64(1036)),
				model.NewQueryResultCol("aggr__2__order_1", 1036),
				model.NewQueryResultCol("aggr__2__8__parent_count", 1036),
				model.NewQueryResultCol("aggr__2__8__key_0", "b12"),
				model.NewQueryResultCol("aggr__2__8__count", int64(24)),
				model.NewQueryResultCol("aggr__2__8__order_1", 24),
				model.NewQueryResultCol("aggr__2__8__4__parent_count", 24),
				model.NewQueryResultCol("aggr__2__8__4__key_0", "c12"),
				model.NewQueryResultCol("aggr__2__8__4__count", int64(24)),
				model.NewQueryResultCol("aggr__2__8__4__order_1", 24),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", 34290),
				model.NewQueryResultCol("aggr__2__key_0", "a2"),
				model.NewQueryResultCol("aggr__2__count", uint64(34)),
				model.NewQueryResultCol("aggr__2__order_1", 34),
				model.NewQueryResultCol("aggr__2__8__parent_count", 34),
				model.NewQueryResultCol("aggr__2__8__key_0", "b21"),
				model.NewQueryResultCol("aggr__2__8__count", int64(17)),
				model.NewQueryResultCol("aggr__2__8__order_1", 17),
				model.NewQueryResultCol("aggr__2__8__4__parent_count", 17),
				model.NewQueryResultCol("aggr__2__8__4__key_0", "c21"),
				model.NewQueryResultCol("aggr__2__8__4__count", int64(17)),
				model.NewQueryResultCol("aggr__2__8__4__order_1", 17),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", 34290),
				model.NewQueryResultCol("aggr__2__key_0", "a2"),
				model.NewQueryResultCol("aggr__2__count", uint64(34)),
				model.NewQueryResultCol("aggr__2__order_1", 34),
				model.NewQueryResultCol("aggr__2__parent_count", 34290),
				model.NewQueryResultCol("aggr__2__8__key_0", "b22"),
				model.NewQueryResultCol("aggr__2__8__count", int64(17)),
				model.NewQueryResultCol("aggr__2__8__order_1", 17),
				model.NewQueryResultCol("aggr__2__8__4__parent_count", 17),
				model.NewQueryResultCol("aggr__2__8__4__key_0", "c22"),
				model.NewQueryResultCol("aggr__2__8__4__count", int64(17)),
				model.NewQueryResultCol("aggr__2__8__4__order_1", 17),
			}},
		},
		ExpectedSQLs: []string{
			`WITH cte_1 AS ` +
				`(SELECT "surname" AS "cte_1_1", count() AS "cte_1_cnt" ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`WHERE "surname" IS NOT NULL ` +
				`GROUP BY "surname" ` +
				`ORDER BY count() DESC, "surname" ` +
				`LIMIT 200), cte_2 AS ` +
				`(SELECT "surname" AS "cte_2_1", COALESCE("limbName",'__missing__') AS "cte_2_2", count() AS "cte_2_cnt" ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`WHERE "surname" IS NOT NULL ` +
				`GROUP BY "surname", COALESCE("limbName",'__missing__') ` +
				`ORDER BY count() DESC, COALESCE("limbName",'__missing__') ` +
				`LIMIT 20 BY "surname") ` +
				`SELECT "surname", COALESCE("limbName",'__missing__'), "organName", count() ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`INNER JOIN "cte_1" ON "surname" = "cte_1_1" ` +
				`INNER JOIN "cte_2" ON "surname" = "cte_2_1" AND COALESCE("limbName",'__missing__') = "cte_2_2" ` +
				`WHERE ("surname" IS NOT NULL AND "organName" IS NOT NULL) ` +
				`GROUP BY "surname", COALESCE("limbName",'__missing__'), "organName", cte_1_cnt, cte_2_cnt ` +
				`ORDER BY cte_1_cnt DESC, "surname", cte_2_cnt DESC, COALESCE("limbName",'__missing__'), count() DESC, "organName" ` +
				`LIMIT 1 BY "surname", COALESCE("limbName",'__missing__')`,
			`WITH cte_1 AS ` +
				`(SELECT "surname" AS "cte_1_1", count() AS "cte_1_cnt" ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`WHERE "surname" IS NOT NULL ` +
				`GROUP BY "surname" ` +
				`ORDER BY count() DESC, "surname" ` +
				`LIMIT 200) ` +
				`SELECT "surname", COALESCE("limbName",'__missing__'), count() ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`INNER JOIN "cte_1" ON "surname" = "cte_1_1" ` +
				`WHERE "surname" IS NOT NULL ` +
				`GROUP BY "surname", COALESCE("limbName",'__missing__'), cte_1_cnt ` +
				`ORDER BY cte_1_cnt DESC, "surname", count() DESC, COALESCE("limbName",'__missing__') ` +
				`LIMIT 20 BY "surname"`,
			`SELECT "surname", count() ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`WHERE "surname" IS NOT NULL ` +
				`GROUP BY "surname" ` +
				`ORDER BY count() DESC, "surname" ` +
				`LIMIT 200`,
		},
		ExpectedPancakeSQL: `
			SELECT "aggr__2__parent_count", "aggr__2__key_0", "aggr__2__count",
			  "aggr__2__order_1", "aggr__2__8__parent_count", "aggr__2__8__key_0",
			  "aggr__2__8__count", "aggr__2__8__order_1", "aggr__2__8__4__parent_count",
			  "aggr__2__8__4__key_0", "aggr__2__8__4__count", "aggr__2__8__4__order_1"
			FROM (
			  SELECT "aggr__2__parent_count", "aggr__2__key_0", "aggr__2__count",
				"aggr__2__order_1", "aggr__2__8__parent_count", "aggr__2__8__key_0",
				"aggr__2__8__count", "aggr__2__8__order_1", "aggr__2__8__4__parent_count",
				"aggr__2__8__4__key_0", "aggr__2__8__4__count", "aggr__2__8__4__order_1",
				dense_rank() OVER (ORDER BY "aggr__2__order_1" DESC, "aggr__2__key_0" ASC)
				AS "aggr__2__order_1_rank",
				dense_rank() OVER (PARTITION BY "aggr__2__key_0" ORDER BY
				"aggr__2__8__order_1" DESC, "aggr__2__8__key_0" ASC) AS
				"aggr__2__8__order_1_rank",
				dense_rank() OVER (PARTITION BY "aggr__2__key_0", "aggr__2__8__key_0" ORDER
				BY "aggr__2__8__4__order_1" DESC, "aggr__2__8__4__key_0" ASC) AS
				"aggr__2__8__4__order_1_rank"
			  FROM (
				SELECT sum(count(*)) OVER () AS "aggr__2__parent_count",
				  "surname" AS "aggr__2__key_0",
				  sum(count(*)) OVER (PARTITION BY "aggr__2__key_0") AS "aggr__2__count",
				  sum(count()) OVER (PARTITION BY "aggr__2__key_0") AS "aggr__2__order_1",
				  sum(count(*)) OVER (PARTITION BY "aggr__2__key_0") AS
				  "aggr__2__8__parent_count",
				  COALESCE("limbName", '__missing__') AS "aggr__2__8__key_0",
				  sum(count(*)) OVER (PARTITION BY "aggr__2__key_0", "aggr__2__8__key_0") AS
				  "aggr__2__8__count",
				  sum(count()) OVER (PARTITION BY "aggr__2__key_0", "aggr__2__8__key_0") AS
				  "aggr__2__8__order_1",
				  sum(count(*)) OVER (PARTITION BY "aggr__2__key_0", "aggr__2__8__key_0") AS
				  "aggr__2__8__4__parent_count", "organName" AS "aggr__2__8__4__key_0",
				  count(*) AS "aggr__2__8__4__count", count() AS "aggr__2__8__4__order_1"
				FROM "logs-generic-default"
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
		ExpectedResults: [][]model.QueryResultRow{
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a1"),
					model.NewQueryResultCol(`sumOrNull("total")`, 1091661.7608666667),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a2"),
					model.NewQueryResultCol(`sumOrNull("total")`, 630270.07765),
				}},
			},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a1"),
					model.NewQueryResultCol("limbName", "b12"),
					model.NewQueryResultCol(`sumOrNull("total")`, 45774.291766666654),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a1"),
					model.NewQueryResultCol("limbName", "b11"),
					model.NewQueryResultCol(`sumOrNull("total")`, 51891.94613333333),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a2"),
					model.NewQueryResultCol("limbName", "b21"),
					model.NewQueryResultCol(`sumOrNull("total")`, 399126.7496833334),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a2"),
					model.NewQueryResultCol("limbName", "b22"),
					model.NewQueryResultCol(`sumOrNull("total")`, 231143.3279666666),
				}},
			},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a1"),
					model.NewQueryResultCol("limbName", "b12"),
					model.NewQueryResultCol("organName", "c12"),
					model.NewQueryResultCol(`sumOrNull("total")`, 45774.291766666654),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a1"),
					model.NewQueryResultCol("limbName", "b11"),
					model.NewQueryResultCol("organName", "c11"),
					model.NewQueryResultCol(`sumOrNull("total")`, 51891.94613333333),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a2"),
					model.NewQueryResultCol("limbName", "b21"),
					model.NewQueryResultCol("organName", "c21"),
					model.NewQueryResultCol(`sumOrNull("total")`, 399126.7496833334),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a2"),
					model.NewQueryResultCol("limbName", "b22"),
					model.NewQueryResultCol("organName", "c22"),
					model.NewQueryResultCol(`sumOrNull("total")`, 231143.3279666666),
				}},
			},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a1"),
					model.NewQueryResultCol("limbName", "b12"),
					model.NewQueryResultCol("organName", "c12"),
					model.NewQueryResultCol(`sumOrNull("some")`, 36577.89516666666),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a1"),
					model.NewQueryResultCol("limbName", "b11"),
					model.NewQueryResultCol("organName", "c11"),
					model.NewQueryResultCol(`sumOrNull("some")`, 37988.09523333333),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a2"),
					model.NewQueryResultCol("limbName", "b21"),
					model.NewQueryResultCol("organName", "c21"),
					model.NewQueryResultCol(`sumOrNull("some")`, 337246.82201666664),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a2"),
					model.NewQueryResultCol("limbName", "b22"),
					model.NewQueryResultCol("organName", "c22"),
					model.NewQueryResultCol(`sumOrNull("some")`, 205408.48849999998),
				}},
			},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a1"),
					model.NewQueryResultCol("limbName", "b12"),
					model.NewQueryResultCol("organName", "c12"),
					model.NewQueryResultCol("count()", 24),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a1"),
					model.NewQueryResultCol("limbName", "b11"),
					model.NewQueryResultCol("organName", "c11"),
					model.NewQueryResultCol("count()", 21),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a2"),
					model.NewQueryResultCol("limbName", "b21"),
					model.NewQueryResultCol("organName", "c21"),
					model.NewQueryResultCol("count()", 17),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a2"),
					model.NewQueryResultCol("limbName", "b22"),
					model.NewQueryResultCol("organName", "c22"),
					model.NewQueryResultCol("count()", 17),
				}},
			},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a1"),
					model.NewQueryResultCol("limbName", "b12"),
					model.NewQueryResultCol("count()", 24),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a1"),
					model.NewQueryResultCol("limbName", "b11"),
					model.NewQueryResultCol("count()", 21),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a2"),
					model.NewQueryResultCol("limbName", "b21"),
					model.NewQueryResultCol("count()", 17),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a2"),
					model.NewQueryResultCol("limbName", "b22"),
					model.NewQueryResultCol("count()", 17),
				}},
			},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a1"),
					model.NewQueryResultCol("count()", 1036),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a2"),
					model.NewQueryResultCol("count()", 34),
				}},
			},
		},
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", 34290),
				model.NewQueryResultCol("aggr__2__key_0", "a1"),
				model.NewQueryResultCol("aggr__2__count", uint64(1036)),
				model.NewQueryResultCol("aggr__2__order_1", 1036),
				model.NewQueryResultCol("metric__2__1_col_0", 1091661.760867),
				model.NewQueryResultCol("aggr__2__8__parent_count", 1036),
				model.NewQueryResultCol("aggr__2__8__key_0", "b12"),
				model.NewQueryResultCol("aggr__2__8__count", int64(24)),
				model.NewQueryResultCol("aggr__2__8__order_1", 24),
				model.NewQueryResultCol("metric__2__8__1_col_0", 45774.291766666654),
				model.NewQueryResultCol("aggr__2__8__4__parent_count", 24),
				model.NewQueryResultCol("aggr__2__8__4__key_0", "c12"),
				model.NewQueryResultCol("aggr__2__8__4__count", int64(24)),
				model.NewQueryResultCol("aggr__2__8__4__order_1", 24),
				model.NewQueryResultCol("metric__2__8__4__1_col_0", 45774.291766666654),
				model.NewQueryResultCol("metric__2__8__4__5_col_0", 36577.89516666666),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", 34290),
				model.NewQueryResultCol("aggr__2__key_0", "a1"),
				model.NewQueryResultCol("aggr__2__count", uint64(1036)),
				model.NewQueryResultCol("aggr__2__order_1", 1036),
				model.NewQueryResultCol("metric__2__1_col_0", 1091661.760867),
				model.NewQueryResultCol("aggr__2__8__parent_count", 1036),
				model.NewQueryResultCol("aggr__2__8__key_0", "b11"),
				model.NewQueryResultCol("aggr__2__8__count", int64(21)),
				model.NewQueryResultCol("aggr__2__8__order_1", 21),
				model.NewQueryResultCol("metric__2__8__1_col_0", 51891.94613333333),
				model.NewQueryResultCol("aggr__2__8__4__parent_count", 21),
				model.NewQueryResultCol("aggr__2__8__4__key_0", "c11"),
				model.NewQueryResultCol("aggr__2__8__4__count", int64(21)),
				model.NewQueryResultCol("aggr__2__8__4__order_1", 21),
				model.NewQueryResultCol("metric__2__8__4__1_col_0", 51891.94613333333),
				model.NewQueryResultCol("metric__2__8__4__5_col_0", 37988.09523333333),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", 34290),
				model.NewQueryResultCol("aggr__2__key_0", "a2"),
				model.NewQueryResultCol("aggr__2__count", uint64(34)),
				model.NewQueryResultCol("aggr__2__order_1", 34),
				model.NewQueryResultCol("metric__2__1_col_0", 630270.07765),
				model.NewQueryResultCol("aggr__2__8__parent_count", 34),
				model.NewQueryResultCol("aggr__2__8__key_0", "b21"),
				model.NewQueryResultCol("aggr__2__8__count", int64(17)),
				model.NewQueryResultCol("aggr__2__8__order_1", 17),
				model.NewQueryResultCol("metric__2__8__1_col_0", 399126.7496833334),
				model.NewQueryResultCol("aggr__2__8__4__parent_count", 17),
				model.NewQueryResultCol("aggr__2__8__4__key_0", "c21"),
				model.NewQueryResultCol("aggr__2__8__4__count", int64(17)),
				model.NewQueryResultCol("aggr__2__8__4__order_1", 17),
				model.NewQueryResultCol("metric__2__8__4__1_col_0", 399126.7496833334),
				model.NewQueryResultCol("metric__2__8__4__5_col_0", 337246.82201666664),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", 34290),
				model.NewQueryResultCol("aggr__2__key_0", "a2"),
				model.NewQueryResultCol("aggr__2__count", uint64(34)),
				model.NewQueryResultCol("aggr__2__order_1", 34),
				model.NewQueryResultCol("metric__2__1_col_0", 231143.3279666666),
				model.NewQueryResultCol("aggr__2__8__parent_count", 34),
				model.NewQueryResultCol("aggr__2__8__key_0", "b22"),
				model.NewQueryResultCol("aggr__2__8__count", int64(17)),
				model.NewQueryResultCol("aggr__2__8__order_1", 17),
				model.NewQueryResultCol("metric__2__8__1_col_0", 231143.3279666666),
				model.NewQueryResultCol("aggr__2__8__4__parent_count", 17),
				model.NewQueryResultCol("aggr__2__8__4__key_0", "c22"),
				model.NewQueryResultCol("aggr__2__8__4__count", int64(17)),
				model.NewQueryResultCol("aggr__2__8__4__order_1", 17),
				model.NewQueryResultCol("metric__2__8__4__1_col_0", 231143.3279666666),
				model.NewQueryResultCol("metric__2__8__4__5_col_0", 205408.48849999998),
			}},
		},
		ExpectedSQLs: []string{
			`WITH cte_1 AS ` +
				`(SELECT "surname" AS "cte_1_1", count() AS "cte_1_cnt" ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`WHERE "surname" IS NOT NULL ` +
				`GROUP BY "surname" ` +
				`ORDER BY count() DESC, "surname" ` +
				`LIMIT 200) ` +
				`SELECT "surname", sumOrNull("total") ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`INNER JOIN "cte_1" ON "surname" = "cte_1_1" ` +
				`WHERE "surname" IS NOT NULL ` +
				`GROUP BY "surname", cte_1_cnt ` +
				`ORDER BY cte_1_cnt DESC, "surname"`,
			`WITH cte_1 AS ` +
				`(SELECT "surname" AS "cte_1_1", count() AS "cte_1_cnt" ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`WHERE "surname" IS NOT NULL ` +
				`GROUP BY "surname" ` +
				`ORDER BY count() DESC, "surname" ` +
				`LIMIT 200), ` +
				`cte_2 AS ` +
				`(SELECT "surname" AS "cte_2_1", COALESCE("limbName",'__missing__') AS "cte_2_2", count() AS "cte_2_cnt" ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`WHERE "surname" IS NOT NULL ` +
				`GROUP BY "surname", COALESCE("limbName",'__missing__') ` +
				`ORDER BY count() DESC, COALESCE("limbName",'__missing__') ` +
				`LIMIT 20 BY "surname") ` +
				`SELECT "surname", COALESCE("limbName",'__missing__'), sumOrNull("total") ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`INNER JOIN "cte_1" ON "surname" = "cte_1_1" ` +
				`INNER JOIN "cte_2" ON "surname" = "cte_2_1" AND COALESCE("limbName",'__missing__') = "cte_2_2" ` +
				`WHERE "surname" IS NOT NULL ` +
				`GROUP BY "surname", COALESCE("limbName",'__missing__'), cte_1_cnt, cte_2_cnt ` +
				`ORDER BY cte_1_cnt DESC, "surname", cte_2_cnt DESC, COALESCE("limbName",'__missing__')`,
			`WITH cte_1 AS ` +
				`(SELECT "surname" AS "cte_1_1", count() AS "cte_1_cnt" ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`WHERE "surname" IS NOT NULL ` +
				`GROUP BY "surname" ` +
				`ORDER BY count() DESC, "surname" ` +
				`LIMIT 200), ` +
				`cte_2 AS ` +
				`(SELECT "surname" AS "cte_2_1", COALESCE("limbName",'__missing__') AS "cte_2_2", count() AS "cte_2_cnt" ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`WHERE "surname" IS NOT NULL ` +
				`GROUP BY "surname", COALESCE("limbName",'__missing__') ` +
				`ORDER BY count() DESC, COALESCE("limbName",'__missing__') ` +
				`LIMIT 20 BY "surname"), ` +
				`cte_3 AS ` +
				`(SELECT "surname" AS "cte_3_1", COALESCE("limbName",'__missing__') AS "cte_3_2", "organName" AS "cte_3_3", count() AS "cte_3_cnt" ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`WHERE ("surname" IS NOT NULL AND "organName" IS NOT NULL) ` +
				`GROUP BY "surname", COALESCE("limbName",'__missing__'), "organName" ` +
				`ORDER BY count() DESC, "organName" ` +
				`LIMIT 1 BY "surname", COALESCE("limbName",'__missing__')) ` +
				`SELECT "surname", COALESCE("limbName",'__missing__'), "organName", sumOrNull("total") ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`INNER JOIN "cte_1" ON "surname" = "cte_1_1" ` +
				`INNER JOIN "cte_2" ON "surname" = "cte_2_1" AND COALESCE("limbName",'__missing__') = "cte_2_2" ` +
				`INNER JOIN "cte_3" ON "surname" = "cte_3_1" AND COALESCE("limbName",'__missing__') = "cte_3_2" AND "organName" = "cte_3_3" ` +
				`WHERE ("surname" IS NOT NULL AND "organName" IS NOT NULL) ` +
				`GROUP BY "surname", COALESCE("limbName",'__missing__'), "organName", cte_1_cnt, cte_2_cnt, cte_3_cnt ` +
				`ORDER BY cte_1_cnt DESC, "surname", cte_2_cnt DESC, COALESCE("limbName",'__missing__'), cte_3_cnt DESC, "organName"`,
			`WITH cte_1 AS ` +
				`(SELECT "surname" AS "cte_1_1", count() AS "cte_1_cnt" ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`WHERE "surname" IS NOT NULL ` +
				`GROUP BY "surname" ` +
				`ORDER BY count() DESC, "surname" ` +
				`LIMIT 200), ` +
				`cte_2 AS ` +
				`(SELECT "surname" AS "cte_2_1", COALESCE("limbName",'__missing__') AS "cte_2_2", count() AS "cte_2_cnt" ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`WHERE "surname" IS NOT NULL ` +
				`GROUP BY "surname", COALESCE("limbName",'__missing__') ` +
				`ORDER BY count() DESC, COALESCE("limbName",'__missing__') ` +
				`LIMIT 20 BY "surname"), ` +
				`cte_3 AS ` +
				`(SELECT "surname" AS "cte_3_1", COALESCE("limbName",'__missing__') AS "cte_3_2", "organName" AS "cte_3_3", count() AS "cte_3_cnt" ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`WHERE ("surname" IS NOT NULL AND "organName" IS NOT NULL) ` +
				`GROUP BY "surname", COALESCE("limbName",'__missing__'), "organName" ` +
				`ORDER BY count() DESC, "organName" ` +
				`LIMIT 1 BY "surname", COALESCE("limbName",'__missing__')) ` +
				`SELECT "surname", COALESCE("limbName",'__missing__'), "organName", sumOrNull("some") ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`INNER JOIN "cte_1" ON "surname" = "cte_1_1" ` +
				`INNER JOIN "cte_2" ON "surname" = "cte_2_1" AND COALESCE("limbName",'__missing__') = "cte_2_2" ` +
				`INNER JOIN "cte_3" ON "surname" = "cte_3_1" AND COALESCE("limbName",'__missing__') = "cte_3_2" AND "organName" = "cte_3_3" ` +
				`WHERE ("surname" IS NOT NULL AND "organName" IS NOT NULL) ` +
				`GROUP BY "surname", COALESCE("limbName",'__missing__'), "organName", cte_1_cnt, cte_2_cnt, cte_3_cnt ` +
				`ORDER BY cte_1_cnt DESC, "surname", cte_2_cnt DESC, COALESCE("limbName",'__missing__'), cte_3_cnt DESC, "organName"`,
			`WITH cte_1 AS ` +
				`(SELECT "surname" AS "cte_1_1", count() AS "cte_1_cnt" ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`WHERE "surname" IS NOT NULL ` +
				`GROUP BY "surname" ` +
				`ORDER BY count() DESC, "surname" ` +
				`LIMIT 200), ` +
				`cte_2 AS ` +
				`(SELECT "surname" AS "cte_2_1", COALESCE("limbName",'__missing__') AS "cte_2_2", count() AS "cte_2_cnt" ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`WHERE "surname" IS NOT NULL ` +
				`GROUP BY "surname", COALESCE("limbName",'__missing__') ` +
				`ORDER BY count() DESC, COALESCE("limbName",'__missing__') ` +
				`LIMIT 20 BY "surname") ` +
				`SELECT "surname", COALESCE("limbName",'__missing__'), "organName", count() ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`INNER JOIN "cte_1" ON "surname" = "cte_1_1" ` +
				`INNER JOIN "cte_2" ON "surname" = "cte_2_1" AND COALESCE("limbName",'__missing__') = "cte_2_2" ` +
				`WHERE ("surname" IS NOT NULL AND "organName" IS NOT NULL) ` +
				`GROUP BY "surname", COALESCE("limbName",'__missing__'), "organName", cte_1_cnt, cte_2_cnt ` +
				`ORDER BY cte_1_cnt DESC, "surname", cte_2_cnt DESC, COALESCE("limbName",'__missing__'), count() DESC, "organName" ` +
				`LIMIT 1 BY "surname", COALESCE("limbName",'__missing__')`,
			`WITH cte_1 AS ` +
				`(SELECT "surname" AS "cte_1_1", count() AS "cte_1_cnt" ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`WHERE "surname" IS NOT NULL ` +
				`GROUP BY "surname" ` +
				`ORDER BY count() DESC, "surname" ` +
				`LIMIT 200) ` +
				`SELECT "surname", COALESCE("limbName",'__missing__'), count() ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`INNER JOIN "cte_1" ON "surname" = "cte_1_1" ` +
				`WHERE "surname" IS NOT NULL ` +
				`GROUP BY "surname", COALESCE("limbName",'__missing__'), cte_1_cnt ` +
				`ORDER BY cte_1_cnt DESC, "surname", count() DESC, COALESCE("limbName",'__missing__') ` +
				`LIMIT 20 BY "surname"`,
			`SELECT "surname", count() ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`WHERE "surname" IS NOT NULL ` +
				`GROUP BY "surname" ` +
				`ORDER BY count() DESC, "surname" ` +
				`LIMIT 200`,
		},
		ExpectedPancakeSQL: `
			SELECT "aggr__2__parent_count", "aggr__2__key_0", "aggr__2__count",
			  "aggr__2__order_1", "metric__2__1_col_0", "aggr__2__8__parent_count",
			  "aggr__2__8__key_0", "aggr__2__8__count", "aggr__2__8__order_1",
			  "metric__2__8__1_col_0", "aggr__2__8__4__parent_count",
			  "aggr__2__8__4__key_0", "aggr__2__8__4__count", "aggr__2__8__4__order_1",
			  "metric__2__8__4__1_col_0", "metric__2__8__4__5_col_0"
			FROM (
			  SELECT "aggr__2__parent_count", "aggr__2__key_0", "aggr__2__count",
				"aggr__2__order_1", "metric__2__1_col_0", "aggr__2__8__parent_count",
				"aggr__2__8__key_0", "aggr__2__8__count", "aggr__2__8__order_1",
				"metric__2__8__1_col_0", "aggr__2__8__4__parent_count",
				"aggr__2__8__4__key_0", "aggr__2__8__4__count", "aggr__2__8__4__order_1",
				"metric__2__8__4__1_col_0", "metric__2__8__4__5_col_0",
				dense_rank() OVER (ORDER BY "aggr__2__order_1" DESC, "aggr__2__key_0" ASC)
				AS "aggr__2__order_1_rank",
				dense_rank() OVER (PARTITION BY "aggr__2__key_0" ORDER BY
				"aggr__2__8__order_1" DESC, "aggr__2__8__key_0" ASC) AS
				"aggr__2__8__order_1_rank",
				dense_rank() OVER (PARTITION BY "aggr__2__key_0", "aggr__2__8__key_0" ORDER
				BY "aggr__2__8__4__order_1" DESC, "aggr__2__8__4__key_0" ASC) AS
				"aggr__2__8__4__order_1_rank"
			  FROM (
				SELECT sum(count(*)) OVER () AS "aggr__2__parent_count",
				  "surname" AS "aggr__2__key_0",
				  sum(count(*)) OVER (PARTITION BY "aggr__2__key_0") AS "aggr__2__count",
				  sum(count()) OVER (PARTITION BY "aggr__2__key_0") AS "aggr__2__order_1",
				  sumOrNull(sumOrNull("total")) OVER (PARTITION BY "aggr__2__key_0") AS
				  "metric__2__1_col_0",
				  sum(count(*)) OVER (PARTITION BY "aggr__2__key_0") AS
				  "aggr__2__8__parent_count",
				  COALESCE("limbName", '__missing__') AS "aggr__2__8__key_0",
				  sum(count(*)) OVER (PARTITION BY "aggr__2__key_0", "aggr__2__8__key_0") AS
				  "aggr__2__8__count",
				  sum(count()) OVER (PARTITION BY "aggr__2__key_0", "aggr__2__8__key_0") AS
				  "aggr__2__8__order_1",
				  sumOrNull(sumOrNull("total")) OVER (PARTITION BY "aggr__2__key_0",
				  "aggr__2__8__key_0") AS "metric__2__8__1_col_0",
				  sum(count(*)) OVER (PARTITION BY "aggr__2__key_0", "aggr__2__8__key_0") AS
				  "aggr__2__8__4__parent_count", "organName" AS "aggr__2__8__4__key_0",
				  count(*) AS "aggr__2__8__4__count", count() AS "aggr__2__8__4__order_1",
				  sumOrNull("total") AS "metric__2__8__4__1_col_0",
				  sumOrNull("some") AS "metric__2__8__4__5_col_0"
				FROM "logs-generic-default"
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
									"sum_other_doc_count": 504
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
		ExpectedResults: [][]model.QueryResultRow{
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a1"),
					model.NewQueryResultCol(`sumOrNull("total")`, 1091661.7608666667),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a2"),
					model.NewQueryResultCol(`sumOrNull("total")`, 630270.07765),
				}},
			},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a1"),
					model.NewQueryResultCol("limbName", "b11"),
					model.NewQueryResultCol(`sumOrNull("total")`, 51891.94613333333),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a1"),
					model.NewQueryResultCol("limbName", "b12"),
					model.NewQueryResultCol(`sumOrNull("total")`, 45774.291766666654),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a2"),
					model.NewQueryResultCol("limbName", "b21"),
					model.NewQueryResultCol(`sumOrNull("total")`, 399126.7496833334),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a2"),
					model.NewQueryResultCol("limbName", "b22"),
					model.NewQueryResultCol(`sumOrNull("total")`, 231143.3279666666),
				}},
			},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a1"),
					model.NewQueryResultCol("limbName", "b11"),
					model.NewQueryResultCol("organName", "c11"),
					model.NewQueryResultCol(`sumOrNull("total")`, 51891.94613333333),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a1"),
					model.NewQueryResultCol("limbName", "b12"),
					model.NewQueryResultCol("organName", "c12"),
					model.NewQueryResultCol(`sumOrNull("total")`, 45774.291766666654),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a2"),
					model.NewQueryResultCol("limbName", "b21"),
					model.NewQueryResultCol("organName", "c21"),
					model.NewQueryResultCol(`sumOrNull("total")`, 399126.7496833334),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a2"),
					model.NewQueryResultCol("limbName", "b22"),
					model.NewQueryResultCol("organName", "c22"),
					model.NewQueryResultCol(`sumOrNull("total")`, 231143.3279666666),
				}},
			},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a1"),
					model.NewQueryResultCol("limbName", "b11"),
					model.NewQueryResultCol("organName", "c11"),
					model.NewQueryResultCol(`sumOrNull("some")`, 37988.09523333333),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a1"),
					model.NewQueryResultCol("limbName", "b12"),
					model.NewQueryResultCol("organName", "c12"),
					model.NewQueryResultCol(`sumOrNull("some")`, 36577.89516666666),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a2"),
					model.NewQueryResultCol("limbName", "b21"),
					model.NewQueryResultCol("organName", "c21"),
					model.NewQueryResultCol(`sumOrNull("some")`, 337246.82201666664),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a2"),
					model.NewQueryResultCol("limbName", "b22"),
					model.NewQueryResultCol("organName", "c22"),
					model.NewQueryResultCol(`sumOrNull("some")`, 205408.48849999998),
				}},
			},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a1"),
					model.NewQueryResultCol("limbName", "b11"),
					model.NewQueryResultCol("organName", "c11"),
					model.NewQueryResultCol("count()", 21),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a1"),
					model.NewQueryResultCol("limbName", "b12"),
					model.NewQueryResultCol("organName", "c12"),
					model.NewQueryResultCol("count()", 24),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a2"),
					model.NewQueryResultCol("limbName", "b21"),
					model.NewQueryResultCol("organName", "c21"),
					model.NewQueryResultCol("count()", 17),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a2"),
					model.NewQueryResultCol("limbName", "b22"),
					model.NewQueryResultCol("organName", "c22"),
					model.NewQueryResultCol("count()", 17),
				}},
			},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a1"),
					model.NewQueryResultCol("limbName", "b11"),
					model.NewQueryResultCol("count()", 21),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a1"),
					model.NewQueryResultCol("limbName", "b12"),
					model.NewQueryResultCol("count()", 24),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a2"),
					model.NewQueryResultCol("limbName", "b21"),
					model.NewQueryResultCol("count()", 17),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a2"),
					model.NewQueryResultCol("limbName", "b22"),
					model.NewQueryResultCol("count()", 17),
				}},
			},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a1"),
					model.NewQueryResultCol("count()", 1036),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a2"),
					model.NewQueryResultCol("count()", 34),
				}},
			},
			{},
			{},
			{},
			{},
			{},
			{},
			{},
		},
		ExpectedSQLs: []string{
			`SELECT count() ` +
				`FROM (SELECT 1 ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`LIMIT 10000)`,
			`WITH cte_1 AS ` +
				`(SELECT "surname" AS "cte_1_1", count() AS "cte_1_cnt" ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`GROUP BY "surname" ` +
				`ORDER BY count() DESC, "surname" ` +
				`LIMIT 100) ` +
				`SELECT "surname", sumOrNull("total") ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`INNER JOIN "cte_1" ON "surname" = "cte_1_1" ` +
				`GROUP BY "surname", cte_1_cnt ` +
				`ORDER BY cte_1_cnt DESC, "surname"`,
			`WITH cte_1 AS ` +
				`(SELECT "surname" AS "cte_1_1", count() AS "cte_1_cnt" ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`GROUP BY "surname" ` +
				`ORDER BY count() DESC, "surname" ` +
				`LIMIT 100), ` +
				`cte_2 AS ` +
				`(SELECT "surname" AS "cte_2_1", COALESCE("limbName",'__missing__') AS "cte_2_2", count() AS "cte_2_cnt" ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`GROUP BY "surname", COALESCE("limbName",'__missing__') ` +
				`ORDER BY count() DESC, COALESCE("limbName",'__missing__') ` +
				`LIMIT 10 BY "surname") ` +
				`SELECT "surname", COALESCE("limbName",'__missing__'), sumOrNull("total") ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`INNER JOIN "cte_1" ON "surname" = "cte_1_1" ` +
				`INNER JOIN "cte_2" ON "surname" = "cte_2_1" AND COALESCE("limbName",'__missing__') = "cte_2_2" ` +
				`GROUP BY "surname", COALESCE("limbName",'__missing__'), cte_1_cnt, cte_2_cnt ` +
				`ORDER BY cte_1_cnt DESC, "surname", cte_2_cnt DESC, COALESCE("limbName",'__missing__')`,
			`WITH cte_1 AS ` +
				`(SELECT "surname" AS "cte_1_1", count() AS "cte_1_cnt" ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`GROUP BY "surname" ` +
				`ORDER BY count() DESC, "surname" ` +
				`LIMIT 100), ` +
				`cte_2 AS ` +
				`(SELECT "surname" AS "cte_2_1", COALESCE("limbName",'__missing__') AS "cte_2_2", count() AS "cte_2_cnt" ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`GROUP BY "surname", COALESCE("limbName",'__missing__') ` +
				`ORDER BY count() DESC, COALESCE("limbName",'__missing__') ` +
				`LIMIT 10 BY "surname"), ` +
				`cte_3 AS ` +
				`(SELECT "surname" AS "cte_3_1", COALESCE("limbName",'__missing__') AS "cte_3_2", COALESCE("organName",'__missing__') AS "cte_3_3", count() AS "cte_3_cnt" ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`GROUP BY "surname", COALESCE("limbName",'__missing__'), COALESCE("organName",'__missing__') ` +
				`ORDER BY count() DESC, COALESCE("organName",'__missing__') ` +
				`LIMIT 10 BY "surname", COALESCE("limbName",'__missing__')) ` +
				`SELECT "surname", COALESCE("limbName",'__missing__'), COALESCE("organName",'__missing__'), sumOrNull("total") ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`INNER JOIN "cte_1" ON "surname" = "cte_1_1" ` +
				`INNER JOIN "cte_2" ON "surname" = "cte_2_1" AND COALESCE("limbName",'__missing__') = "cte_2_2" ` +
				`INNER JOIN "cte_3" ON "surname" = "cte_3_1" AND COALESCE("limbName",'__missing__') = "cte_3_2" AND COALESCE("organName",'__missing__') = "cte_3_3" ` +
				`GROUP BY "surname", COALESCE("limbName",'__missing__'), COALESCE("organName",'__missing__'), cte_1_cnt, cte_2_cnt, cte_3_cnt ` +
				`ORDER BY cte_1_cnt DESC, "surname", cte_2_cnt DESC, COALESCE("limbName",'__missing__'), cte_3_cnt DESC, COALESCE("organName",'__missing__')`,
			`WITH cte_1 AS ` +
				`(SELECT "surname" AS "cte_1_1", count() AS "cte_1_cnt" ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`GROUP BY "surname" ` +
				`ORDER BY count() DESC, "surname" ` +
				`LIMIT 100), ` +
				`cte_2 AS ` +
				`(SELECT "surname" AS "cte_2_1", COALESCE("limbName",'__missing__') AS "cte_2_2", count() AS "cte_2_cnt" ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`GROUP BY "surname", COALESCE("limbName",'__missing__') ` +
				`ORDER BY count() DESC, COALESCE("limbName",'__missing__') ` +
				`LIMIT 10 BY "surname"), ` +
				`cte_3 AS ` +
				`(SELECT "surname" AS "cte_3_1", COALESCE("limbName",'__missing__') AS "cte_3_2", COALESCE("organName",'__missing__') AS "cte_3_3", count() AS "cte_3_cnt" ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`GROUP BY "surname", COALESCE("limbName",'__missing__'), COALESCE("organName",'__missing__') ` +
				`ORDER BY count() DESC, COALESCE("organName",'__missing__') ` +
				`LIMIT 10 BY "surname", COALESCE("limbName",'__missing__')) ` +
				`SELECT "surname", COALESCE("limbName",'__missing__'), COALESCE("organName",'__missing__'), sumOrNull("some") ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`INNER JOIN "cte_1" ON "surname" = "cte_1_1" ` +
				`INNER JOIN "cte_2" ON "surname" = "cte_2_1" AND COALESCE("limbName",'__missing__') = "cte_2_2" ` +
				`INNER JOIN "cte_3" ON "surname" = "cte_3_1" AND COALESCE("limbName",'__missing__') = "cte_3_2" AND COALESCE("organName",'__missing__') = "cte_3_3" ` +
				`GROUP BY "surname", COALESCE("limbName",'__missing__'), COALESCE("organName",'__missing__'), cte_1_cnt, cte_2_cnt, cte_3_cnt ` +
				`ORDER BY cte_1_cnt DESC, "surname", cte_2_cnt DESC, COALESCE("limbName",'__missing__'), cte_3_cnt DESC, COALESCE("organName",'__missing__')`,
			`WITH cte_1 AS ` +
				`(SELECT "surname" AS "cte_1_1", count() AS "cte_1_cnt" ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`GROUP BY "surname" ` +
				`ORDER BY count() DESC, "surname" ` +
				`LIMIT 200), ` +
				`cte_2 AS ` +
				`(SELECT "surname" AS "cte_2_1", COALESCE("limbName",'__missing__') AS "cte_2_2", count() AS "cte_2_cnt" ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`GROUP BY "surname", COALESCE("limbName",'__missing__') ` +
				`ORDER BY count() DESC, "surname", count() DESC, COALESCE("limbName",'__missing__') ` +
				`LIMIT 20 BY "surname") ` +
				`SELECT "surname", COALESCE("limbName",'__missing__'), COALESCE("organName",'__missing__'), count() ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`INNER JOIN "cte_1" ON "surname" = "cte_1_1" ` +
				`INNER JOIN "cte_2" ON "surname" = "cte_2_1" AND COALESCE("limbName",'__missing__') = "cte_2_2" ` +
				`GROUP BY "surname", COALESCE("limbName",'__missing__'), COALESCE("organName",'__missing__'), cte_1_cnt, cte_2_cnt ` +
				`ORDER BY cte_1_cnt DESC, "surname", cte_2_cnt DESC, COALESCE("limbName",'__missing__'), count() DESC, COALESCE("organName",'__missing__') ` +
				`LIMIT 10 BY "surname", COALESCE("limbName",'__missing__')`,
			`WITH cte_1 AS ` +
				`(SELECT "surname" AS "cte_1_1", count() AS "cte_1_cnt" ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`GROUP BY "surname" ` +
				`ORDER BY count() DESC, "surname" ` +
				`LIMIT 100) ` +
				`SELECT "surname", COALESCE(COALESCE("limbName",'__missing__'),'__missing__'), count() ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`INNER JOIN "cte_1" ON "surname" = "cte_1_1" ` +
				`GROUP BY "surname", COALESCE(COALESCE("limbName",'__missing__'),'__missing__'), cte_1_cnt ` +
				`ORDER BY cte_1_cnt DESC, "surname", count() DESC, COALESCE("limbName",'__missing__') ` +
				`LIMIT 10 BY "surname"`,
			`SELECT "surname", count() ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`GROUP BY "surname" ` +
				`ORDER BY count() DESC, "surname" ` +
				`LIMIT 100`,
			``,
			``,
			`WITH cte_1 AS ` +
				`(SELECT "surname" AS "cte_1_1", count() AS "cte_1_cnt" ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`GROUP BY "surname" ` +
				`ORDER BY count() DESC, "surname" ` +
				`LIMIT 100), ` +
				`cte_2 AS ` +
				`(SELECT "surname" AS "cte_2_1", COALESCE("limbName",'__missing__') AS "cte_2_2", count() AS "cte_2_cnt" ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`GROUP BY "surname", COALESCE("limbName",'__missing__') ` +
				`ORDER BY count() DESC, COALESCE("limbName",'__missing__') ` +
				`LIMIT 10 BY "surname"), ` +
				`cte_3 AS ` +
				`(SELECT "surname" AS "cte_3_1", COALESCE("limbName",'__missing__') AS "cte_3_2", COALESCE("organName",'__missing__') AS "cte_3_3", count() AS "cte_3_cnt" ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`GROUP BY "surname", COALESCE("limbName",'__missing__'), COALESCE("organName",'__missing__') ` +
				`ORDER BY count() DESC, COALESCE("organName",'__missing__') ` +
				`LIMIT 10 BY "surname", COALESCE("limbName",'__missing__')) ` +
				`SELECT "surname", COALESCE("limbName",'__missing__'), COALESCE("organName",'__missing__'), "doctorName", count() ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`INNER JOIN "cte_1" ON "surname" = "cte_1_1" ` +
				`INNER JOIN "cte_2" ON "surname" = "cte_2_1" AND COALESCE("limbName",'__missing__') = "cte_2_2" ` +
				`INNER JOIN "cte_3" ON "surname" = "cte_3_1" AND COALESCE("limbName",'__missing__') = "cte_3_2" AND COALESCE("organName",'__missing__') = "cte_3_3" ` +
				`GROUP BY "surname", COALESCE("limbName",'__missing__'), COALESCE("organName",'__missing__'), "doctorName", cte_1_cnt, cte_2_cnt, cte_3_cnt ` +
				`ORDER BY cte_1_cnt DESC, "surname", cte_2_cnt DESC, COALESCE("limbName",'__missing__'), cte_3_cnt DESC, COALESCE("organName",'__missing__'), count() DESC, "doctorName" ` +
				`LIMIT 6 BY "surname", COALESCE("limbName",'__missing__'), COALESCE("organName",'__missing__')`,
			`WITH cte_1 AS ` +
				`(SELECT "surname" AS "cte_1_1", count() AS "cte_1_cnt" ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`GROUP BY "surname" ` +
				`ORDER BY count() DESC, "surname" ` +
				`LIMIT 100), ` +
				`cte_2 AS ` +
				`(SELECT "surname" AS "cte_2_1", COALESCE("limbName",'__missing__') AS "cte_2_2", count() AS "cte_2_cnt" ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`GROUP BY "surname", COALESCE("limbName",'__missing__') ` +
				`ORDER BY count() DESC, COALESCE("limbName",'__missing__') ` +
				`LIMIT 10 BY "surname") ` +
				`SELECT "surname", COALESCE("limbName",'__missing__'), COALESCE("organName",'__missing__'), count() ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`INNER JOIN "cte_1" ON "surname" = "cte_1_1" ` +
				`INNER JOIN "cte_2" ON "surname" = "cte_2_1" AND COALESCE("limbName",'__missing__') = "cte_2_2" ` +
				`GROUP BY "surname", COALESCE("limbName",'__missing__'), COALESCE("organName",'__missing__'), cte_1_cnt, cte_2_cnt ` +
				`ORDER BY cte_1_cnt DESC, "surname", cte_2_cnt DESC, COALESCE("limbName",'__missing__'), count() DESC, COALESCE("organName",'__missing__') ` +
				`LIMIT 10 BY "surname", COALESCE("limbName",'__missing__')`,
			`WITH cte_1 AS ` +
				`(SELECT "surname" AS "cte_1_1", count() AS "cte_1_cnt" ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`GROUP BY "surname" ` +
				`ORDER BY count() DESC, "surname" ` +
				`LIMIT 100) ` +
				`SELECT "surname", COALESCE("limbName",'__missing__'), count() ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`INNER JOIN "cte_1" ON "surname" = "cte_1_1" ` +
				`GROUP BY "surname", COALESCE("limbName",'__missing__'), cte_1_cnt ` +
				`ORDER BY cte_1_cnt DESC, "surname", count() DESC, COALESCE("limbName",'__missing__') ` +
				`LIMIT 10 BY "surname"`,
			`SELECT "surname", count() ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`GROUP BY "surname" ` +
				`ORDER BY count() DESC, "surname" ` +
				`LIMIT 100`,
		},
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
		ExpectedResults: [][]model.QueryResultRow{
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a1"),
					model.NewQueryResultCol(`avgOrNull("total")`, 1091661.7608666667),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a2"),
					model.NewQueryResultCol(`avgOrNull("total")`, 630270.07765),
				}},
			},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a1"),
					model.NewQueryResultCol("limbName", "b12"),
					model.NewQueryResultCol("count()", 45774.291766666654),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a1"),
					model.NewQueryResultCol("limbName", "b11"),
					model.NewQueryResultCol("count()", 51891.94613333333),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a2"),
					model.NewQueryResultCol("limbName", "b22"),
					model.NewQueryResultCol("count()", 231143.3279666666),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a2"),
					model.NewQueryResultCol("limbName", "b21"),
					model.NewQueryResultCol("count()", 399126.7496833334),
				}},
			},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a1"),
					model.NewQueryResultCol("limbName", "b12"),
					model.NewQueryResultCol("organName", "c12"),
					model.NewQueryResultCol("count()", 24),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a1"),
					model.NewQueryResultCol("limbName", "b11"),
					model.NewQueryResultCol("organName", "c11"),
					model.NewQueryResultCol("count()", 21),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a2"),
					model.NewQueryResultCol("limbName", "b22"),
					model.NewQueryResultCol("organName", "c22"),
					model.NewQueryResultCol("count()", 17),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a2"),
					model.NewQueryResultCol("limbName", "b21"),
					model.NewQueryResultCol("organName", "c21"),
					model.NewQueryResultCol("count()", 17),
				}},
			},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a1"),
					model.NewQueryResultCol("limbName", "b12"),
					model.NewQueryResultCol("count()", 24),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a1"),
					model.NewQueryResultCol("limbName", "b11"),
					model.NewQueryResultCol("count()", 21),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a2"),
					model.NewQueryResultCol("limbName", "b22"),
					model.NewQueryResultCol("count()", 17),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a2"),
					model.NewQueryResultCol("limbName", "b21"),
					model.NewQueryResultCol("count()", 17),
				}},
			},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a1"),
					model.NewQueryResultCol("count()", 1036),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a2"),
					model.NewQueryResultCol("count()", 34),
				}},
			},
		},
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", 34290),
				model.NewQueryResultCol("aggr__2__key_0", "a1"),
				model.NewQueryResultCol("aggr__2__count", uint64(1036)),
				model.NewQueryResultCol("aggr__2__order_1", 1091661.7608666667),
				model.NewQueryResultCol("metric__2__1_col_0", 1091661.7608666667),
				model.NewQueryResultCol("aggr__2__8__parent_count", 1036),
				model.NewQueryResultCol("aggr__2__8__key_0", "b12"),
				model.NewQueryResultCol("aggr__2__8__count", int64(24)),
				model.NewQueryResultCol("aggr__2__8__order_1", 45774.291766666654),
				model.NewQueryResultCol("metric__2__8__1_col_0", 45774.291766666654),
				model.NewQueryResultCol("aggr__2__8__4__parent_count", 24),
				model.NewQueryResultCol("aggr__2__8__4__key_0", "c12"),
				model.NewQueryResultCol("aggr__2__8__4__count", int64(24)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", 34290),
				model.NewQueryResultCol("aggr__2__key_0", "a1"),
				model.NewQueryResultCol("aggr__2__count", uint64(1036)),
				model.NewQueryResultCol("aggr__2__order_1", 1091661.7608666667),
				model.NewQueryResultCol("metric__2__1_col_0", 1091661.7608666667),
				model.NewQueryResultCol("aggr__2__8__parent_count", 1036),
				model.NewQueryResultCol("aggr__2__8__key_0", "b11"),
				model.NewQueryResultCol("aggr__2__8__count", int64(21)),
				model.NewQueryResultCol("aggr__2__8__order_1", 51891.94613333333),
				model.NewQueryResultCol("metric__2__8__1_col_0", 51891.94613333333),
				model.NewQueryResultCol("aggr__2__8__4__parent_count", 21),
				model.NewQueryResultCol("aggr__2__8__4__key_0", "c11"),
				model.NewQueryResultCol("aggr__2__8__4__count", int64(21)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", 34290),
				model.NewQueryResultCol("aggr__2__key_0", "a2"),
				model.NewQueryResultCol("aggr__2__count", uint64(34)),
				model.NewQueryResultCol("aggr__2__order_1", 630270.07765),
				model.NewQueryResultCol("metric__2__1_col_0", 630270.07765),
				model.NewQueryResultCol("aggr__2__8__parent_count", 34),
				model.NewQueryResultCol("aggr__2__8__key_0", "b22"),
				model.NewQueryResultCol("aggr__2__8__count", int64(17)),
				model.NewQueryResultCol("aggr__2__8__order_1", 231143.3279666666),
				model.NewQueryResultCol("metric__2__8__1_col_0", 231143.3279666666),
				model.NewQueryResultCol("aggr__2__8__4__parent_count", 17),
				model.NewQueryResultCol("aggr__2__8__4__key_0", "c22"),
				model.NewQueryResultCol("aggr__2__8__4__count", int64(17)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", 34290),
				model.NewQueryResultCol("aggr__2__key_0", "a2"),
				model.NewQueryResultCol("aggr__2__count", uint64(34)),
				model.NewQueryResultCol("aggr__2__order_1", 630270.07765),
				model.NewQueryResultCol("metric__2__1_col_0", 630270.07765),
				model.NewQueryResultCol("aggr__2__8__parent_count", 34),
				model.NewQueryResultCol("aggr__2__8__key_0", "b21"),
				model.NewQueryResultCol("aggr__2__8__count", int64(17)),
				model.NewQueryResultCol("aggr__2__8__order_1", 399126.7496833334),
				model.NewQueryResultCol("metric__2__8__1_col_0", 399126.7496833334),
				model.NewQueryResultCol("aggr__2__8__4__parent_count", 17),
				model.NewQueryResultCol("aggr__2__8__4__key_0", "c21"),
				model.NewQueryResultCol("aggr__2__8__4__count", int64(17)),
			}},
		},
		ExpectedSQLs: []string{
			`WITH cte_1 AS ` +
				`(SELECT "surname" AS "cte_1_1", avgOrNull("total") AS "cte_1_cnt" ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`WHERE "surname" IS NOT NULL ` +
				`GROUP BY "surname" ` +
				`ORDER BY avgOrNull("total") DESC, "surname" ` +
				`LIMIT 200) ` +
				`SELECT "surname", avgOrNull("total") ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`INNER JOIN "cte_1" ON "surname" = "cte_1_1" ` +
				`WHERE "surname" IS NOT NULL ` +
				`GROUP BY "surname", cte_1_cnt ` +
				`ORDER BY cte_1_cnt DESC, "surname"`,
			`WITH cte_1 AS ` +
				`(SELECT "surname" AS "cte_1_1", avgOrNull("total") AS "cte_1_cnt" ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`WHERE "surname" IS NOT NULL ` +
				`GROUP BY "surname" ` +
				`ORDER BY avgOrNull("total") DESC, "surname" ` +
				`LIMIT 200), ` +
				`cte_2 AS ` +
				`(SELECT "surname" AS "cte_2_1", COALESCE("limbName",'__missing__') AS "cte_2_2", sumOrNull("total") AS "cte_2_cnt" ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`WHERE "surname" IS NOT NULL ` +
				`GROUP BY "surname", COALESCE("limbName",'__missing__') ` +
				`ORDER BY sumOrNull("total") ASC, COALESCE("limbName",'__missing__') ` +
				`LIMIT 20 BY "surname") ` +
				`SELECT "surname", COALESCE("limbName",'__missing__'), sumOrNull("total") ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`INNER JOIN "cte_1" ON "surname" = "cte_1_1" ` +
				`INNER JOIN "cte_2" ON "surname" = "cte_2_1" AND COALESCE("limbName",'__missing__') = "cte_2_2" ` +
				`WHERE "surname" IS NOT NULL ` +
				`GROUP BY "surname", COALESCE("limbName",'__missing__'), cte_1_cnt, cte_2_cnt ` +
				`ORDER BY cte_1_cnt DESC, "surname", cte_2_cnt DESC, COALESCE("limbName",'__missing__')`,
			`WITH cte_1 AS ` +
				`(SELECT "surname" AS "cte_1_1", avgOrNull("total") AS "cte_1_cnt" ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`WHERE "surname" IS NOT NULL ` +
				`GROUP BY "surname" ` +
				`ORDER BY avgOrNull("total") DESC, "surname" ` +
				`LIMIT 200), ` +
				`cte_2 AS ` +
				`(SELECT "surname" AS "cte_2_1", COALESCE("limbName",'__missing__') AS "cte_2_2", sumOrNull("total") AS "cte_2_cnt" ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`WHERE "surname" IS NOT NULL ` +
				`GROUP BY "surname", COALESCE("limbName",'__missing__') ` +
				`ORDER BY sumOrNull("total") ASC, COALESCE("limbName",'__missing__') ` +
				`LIMIT 20 BY "surname") ` +
				`SELECT "surname", COALESCE("limbName",'__missing__'), "organName", count() ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`INNER JOIN "cte_1" ON "surname" = "cte_1_1" ` +
				`INNER JOIN "cte_2" ON "surname" = "cte_2_1" AND COALESCE("limbName",'__missing__') = "cte_2_2" ` +
				`WHERE ("surname" IS NOT NULL AND "organName" IS NOT NULL) ` +
				`GROUP BY "surname", COALESCE("limbName",'__missing__'), "organName", cte_1_cnt, cte_2_cnt ` +
				`ORDER BY cte_1_cnt DESC, "surname", cte_2_cnt DESC, COALESCE("limbName",'__missing__'), "organName" DESC ` +
				`LIMIT 1 BY "surname", COALESCE("limbName",'__missing__')`,
			`WITH cte_1 AS ` +
				`(SELECT "surname" AS "cte_1_1", avgOrNull("total") AS "cte_1_cnt" ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`WHERE "surname" IS NOT NULL ` +
				`GROUP BY "surname" ` +
				`ORDER BY avgOrNull("total") DESC, "surname" ` +
				`LIMIT 200) ` +
				`SELECT "surname", COALESCE("limbName",'__missing__'), count() ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`INNER JOIN "cte_1" ON "surname" = "cte_1_1" ` +
				`WHERE "surname" IS NOT NULL ` +
				`GROUP BY "surname", COALESCE("limbName",'__missing__'), cte_1_cnt ` +
				`ORDER BY cte_1_cnt DESC, "surname", sumOrNull("total") ASC, COALESCE("limbName",'__missing__') ` +
				`LIMIT 20 BY "surname"`,
			`SELECT "surname", count() ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`WHERE "surname" IS NOT NULL ` +
				`GROUP BY "surname" ` +
				`ORDER BY avgOrNull("total") DESC, "surname" ` +
				`LIMIT 200`,
		},
		ExpectedPancakeSQL: `
			 SELECT "aggr__2__parent_count", "aggr__2__key_0", "aggr__2__count",
			  "aggr__2__order_1", "metric__2__1_col_0", "aggr__2__8__parent_count",
			  "aggr__2__8__key_0", "aggr__2__8__count", "aggr__2__8__order_1",
			  "metric__2__8__1_col_0", "aggr__2__8__4__parent_count",
			  "aggr__2__8__4__key_0", "aggr__2__8__4__count"
			FROM (
			  SELECT "aggr__2__parent_count", "aggr__2__key_0", "aggr__2__count",
				"aggr__2__order_1", "metric__2__1_col_0", "aggr__2__8__parent_count",
				"aggr__2__8__key_0", "aggr__2__8__count", "aggr__2__8__order_1",
				"metric__2__8__1_col_0", "aggr__2__8__4__parent_count",
				"aggr__2__8__4__key_0", "aggr__2__8__4__count",
				dense_rank() OVER (ORDER BY "aggr__2__order_1" DESC, "aggr__2__key_0" ASC)
				AS "aggr__2__order_1_rank",
				dense_rank() OVER (PARTITION BY "aggr__2__key_0" ORDER BY
				"aggr__2__8__order_1" ASC, "aggr__2__8__key_0" ASC) AS
				"aggr__2__8__order_1_rank",
				dense_rank() OVER (PARTITION BY "aggr__2__key_0", "aggr__2__8__key_0" ORDER
				BY "aggr__2__8__4__key_0" DESC) AS "aggr__2__8__4__order_1_rank"
			  FROM (
				SELECT sum(count(*)) OVER () AS "aggr__2__parent_count",
				  "surname" AS "aggr__2__key_0",
				  sum(count(*)) OVER (PARTITION BY "aggr__2__key_0") AS "aggr__2__count",
				  avgOrNullMerge(avgOrNullState("total")) OVER (PARTITION BY
				  "aggr__2__key_0") AS "aggr__2__order_1",
				  avgOrNullMerge(avgOrNullState("total")) OVER (PARTITION BY
				  "aggr__2__key_0") AS "metric__2__1_col_0",
				  sum(count(*)) OVER (PARTITION BY "aggr__2__key_0") AS
				  "aggr__2__8__parent_count",
				  COALESCE("limbName", '__missing__') AS "aggr__2__8__key_0",
				  sum(count(*)) OVER (PARTITION BY "aggr__2__key_0", "aggr__2__8__key_0") AS
				  "aggr__2__8__count",
				  sumOrNull(sumOrNull("total")) OVER (PARTITION BY "aggr__2__key_0",
				  "aggr__2__8__key_0") AS "aggr__2__8__order_1",
				  sumOrNull("total") AS "metric__2__8__1_col_0",
				  sum(count(*)) OVER (PARTITION BY "aggr__2__key_0", "aggr__2__8__key_0") AS
				  "aggr__2__8__4__parent_count", "organName" AS "aggr__2__8__4__key_0",
				  count(*) AS "aggr__2__8__4__count"
				FROM "logs-generic-default"
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
		ExpectedResults: [][]model.QueryResultRow{
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a2"),
					model.NewQueryResultCol("limbName", "b22"),
					model.NewQueryResultCol("count()", 231143.3279666666),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a2"),
					model.NewQueryResultCol("limbName", "b21"),
					model.NewQueryResultCol("count()", 399126.7496833334),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a1"),
					model.NewQueryResultCol("limbName", "b12"),
					model.NewQueryResultCol("count()", 45774.291766666654),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a1"),
					model.NewQueryResultCol("limbName", "b11"),
					model.NewQueryResultCol("count()", 51891.94613333333),
				}},
			},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a2"),
					model.NewQueryResultCol("limbName", "b22"),
					model.NewQueryResultCol("organName", "c22"),
					model.NewQueryResultCol("organName", "d22"),
					model.NewQueryResultCol("count()", 17),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a2"),
					model.NewQueryResultCol("limbName", "b21"),
					model.NewQueryResultCol("organName", "c21"),
					model.NewQueryResultCol("organName", "d21"),
					model.NewQueryResultCol("count()", 17),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a1"),
					model.NewQueryResultCol("limbName", "b12"),
					model.NewQueryResultCol("organName", "c12"),
					model.NewQueryResultCol("organName", "d12"),
					model.NewQueryResultCol("count()", 24),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a1"),
					model.NewQueryResultCol("limbName", "b11"),
					model.NewQueryResultCol("organName", "c11"),
					model.NewQueryResultCol("organName", "d11"),
					model.NewQueryResultCol("count()", 21),
				}},
			},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a2"),
					model.NewQueryResultCol("limbName", "b22"),
					model.NewQueryResultCol("organName", "c22"),
					model.NewQueryResultCol("count()", 17),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a2"),
					model.NewQueryResultCol("limbName", "b21"),
					model.NewQueryResultCol("organName", "c21"),
					model.NewQueryResultCol("count()", 17),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a1"),
					model.NewQueryResultCol("limbName", "b12"),
					model.NewQueryResultCol("organName", "c12"),
					model.NewQueryResultCol("count()", 24),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a1"),
					model.NewQueryResultCol("limbName", "b11"),
					model.NewQueryResultCol("organName", "c11"),
					model.NewQueryResultCol("count()", 21),
				}},
			},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a2"),
					model.NewQueryResultCol("limbName", "b22"),
					model.NewQueryResultCol("count()", 17),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a2"),
					model.NewQueryResultCol("limbName", "b21"),
					model.NewQueryResultCol("count()", 17),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a1"),
					model.NewQueryResultCol("limbName", "b12"),
					model.NewQueryResultCol("count()", 24),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a1"),
					model.NewQueryResultCol("limbName", "b11"),
					model.NewQueryResultCol("count()", 21),
				}},
			},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a2"),
					model.NewQueryResultCol("count()", 34),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a1"),
					model.NewQueryResultCol("count()", 1036),
				}},
			},
		},
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", 34290),
				model.NewQueryResultCol("aggr__2__key_0", "a2"),
				model.NewQueryResultCol("aggr__2__count", uint64(34)),
				model.NewQueryResultCol("aggr__2__8__parent_count", 34),
				model.NewQueryResultCol("aggr__2__8__key_0", "b22"),
				model.NewQueryResultCol("aggr__2__8__count", int64(17)),
				model.NewQueryResultCol("aggr__2__8__order_1", 231143.3279666666),
				model.NewQueryResultCol("metric__2__8__1_col_0", 231143.3279666666),
				model.NewQueryResultCol("aggr__2__8__4__parent_count", 17),
				model.NewQueryResultCol("aggr__2__8__4__key_0", "c22"),
				model.NewQueryResultCol("aggr__2__8__4__count", int64(17)),
				model.NewQueryResultCol("aggr__2__8__4__5__parent_count", 17),
				model.NewQueryResultCol("aggr__2__8__4__5__key_0", "d22"),
				model.NewQueryResultCol("aggr__2__8__4__5__count", int64(17)),
				model.NewQueryResultCol("aggr__2__8__4__5__order_1", 17),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", 34290),
				model.NewQueryResultCol("aggr__2__key_0", "a2"),
				model.NewQueryResultCol("aggr__2__count", uint64(34)),
				model.NewQueryResultCol("aggr__2__8__parent_count", 34),
				model.NewQueryResultCol("aggr__2__8__key_0", "b21"),
				model.NewQueryResultCol("aggr__2__8__count", int64(17)),
				model.NewQueryResultCol("aggr__2__8__order_1", 399126.7496833334),
				model.NewQueryResultCol("metric__2__8__1_col_0", 399126.7496833334),
				model.NewQueryResultCol("aggr__2__8__4__parent_count", 17),
				model.NewQueryResultCol("aggr__2__8__4__key_0", "c21"),
				model.NewQueryResultCol("aggr__2__8__4__count", int64(17)),
				model.NewQueryResultCol("aggr__2__8__4__5__parent_count", 17),
				model.NewQueryResultCol("aggr__2__8__4__5__key_0", "d21"),
				model.NewQueryResultCol("aggr__2__8__4__5__count", int64(17)),
				model.NewQueryResultCol("aggr__2__8__4__5__order_1", 17),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", 34290),
				model.NewQueryResultCol("aggr__2__key_0", "a1"),
				model.NewQueryResultCol("aggr__2__count", uint64(1036)),
				model.NewQueryResultCol("aggr__2__8__parent_count", 1036),
				model.NewQueryResultCol("aggr__2__8__key_0", "b12"),
				model.NewQueryResultCol("aggr__2__8__count", int64(24)),
				model.NewQueryResultCol("aggr__2__8__order_1", 45774.291766666654),
				model.NewQueryResultCol("metric__2__8__1_col_0", 45774.291766666654),
				model.NewQueryResultCol("aggr__2__8__4__parent_count", 24),
				model.NewQueryResultCol("aggr__2__8__4__key_0", "c12"),
				model.NewQueryResultCol("aggr__2__8__4__count", int64(24)),
				model.NewQueryResultCol("aggr__2__8__4__5__parent_count", 24),
				model.NewQueryResultCol("aggr__2__8__4__5__key_0", "d12"),
				model.NewQueryResultCol("aggr__2__8__4__5__count", int64(24)),
				model.NewQueryResultCol("aggr__2__8__4__5__order_1", 24),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", 34290),
				model.NewQueryResultCol("aggr__2__key_0", "a1"),
				model.NewQueryResultCol("aggr__2__count", uint64(1036)),
				model.NewQueryResultCol("aggr__2__8__parent_count", 1036),
				model.NewQueryResultCol("aggr__2__8__key_0", "b11"),
				model.NewQueryResultCol("aggr__2__8__count", int64(21)),
				model.NewQueryResultCol("aggr__2__8__order_1", 51891.94613333333),
				model.NewQueryResultCol("metric__2__8__1_col_0", 51891.94613333333),
				model.NewQueryResultCol("aggr__2__8__4__parent_count", 21),
				model.NewQueryResultCol("aggr__2__8__4__key_0", "c11"),
				model.NewQueryResultCol("aggr__2__8__4__count", int64(21)),
				model.NewQueryResultCol("aggr__2__8__4__order_1", "c11"),
				model.NewQueryResultCol("aggr__2__8__4__5__parent_count", 21),
				model.NewQueryResultCol("aggr__2__8__4__5__key_0", "d11"),
				model.NewQueryResultCol("aggr__2__8__4__5__count", int64(21)),
				model.NewQueryResultCol("aggr__2__8__4__5__order_1", 21),
			}},
		},
		ExpectedSQLs: []string{
			`WITH cte_1 AS ` +
				`(SELECT "surname" AS "cte_1_1", count() AS "cte_1_cnt" ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`WHERE "surname" IS NOT NULL ` +
				`GROUP BY "surname" ` +
				`ORDER BY "surname" DESC ` +
				`LIMIT 200), ` +
				`cte_2 AS ` +
				`(SELECT "surname" AS "cte_2_1", COALESCE("limbName",'__missing__') AS "cte_2_2", sumOrNull("total") AS "cte_2_cnt" ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`WHERE "surname" IS NOT NULL ` +
				`GROUP BY "surname", COALESCE("limbName",'__missing__') ` +
				`ORDER BY sumOrNull("total") ASC, COALESCE("limbName",'__missing__') ` +
				`LIMIT 20 BY "surname") ` +
				`SELECT "surname", COALESCE("limbName",'__missing__'), sumOrNull("total") ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`INNER JOIN "cte_1" ON "surname" = "cte_1_1" ` +
				`INNER JOIN "cte_2" ON "surname" = "cte_2_1" AND COALESCE("limbName",'__missing__') = "cte_2_2" ` +
				`WHERE "surname" IS NOT NULL ` +
				`GROUP BY "surname", COALESCE("limbName",'__missing__'), cte_1_cnt, cte_2_cnt ` +
				`ORDER BY "surname" DESC, cte_1_cnt DESC, COALESCE("limbName",'__missing__')`, // FIXME bug, should be cte_2_cnt DESC!
			`WITH cte_1 AS ` +
				`(SELECT "surname" AS "cte_1_1", count() AS "cte_1_cnt" ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`WHERE "surname" IS NOT NULL ` +
				`GROUP BY "surname" ` +
				`ORDER BY "surname" DESC ` +
				`LIMIT 200), ` +
				`cte_2 AS ` +
				`(SELECT "surname" AS "cte_2_1", COALESCE("limbName",'__missing__') AS "cte_2_2", sumOrNull("total") AS "cte_2_cnt" ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`WHERE "surname" IS NOT NULL ` +
				`GROUP BY "surname", COALESCE("limbName",'__missing__') ` +
				`ORDER BY sumOrNull("total") ASC, COALESCE("limbName",'__missing__') ` +
				`LIMIT 20 BY "surname"), ` +
				`cte_3 AS ` +
				`(SELECT "surname" AS "cte_3_1", COALESCE("limbName",'__missing__') AS "cte_3_2", ` +
				`"organName" AS "cte_3_3", count() AS "cte_3_cnt" ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`WHERE ("surname" IS NOT NULL AND "organName" IS NOT NULL) ` +
				`GROUP BY "surname", COALESCE("limbName",'__missing__'), "organName" ` +
				`ORDER BY COALESCE("limbName",'__missing__'), "organName" DESC ` +
				`LIMIT 1 BY "surname", COALESCE("limbName",'__missing__')) ` +
				`SELECT "surname", COALESCE("limbName",'__missing__'), "organName", "organName", count() ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`INNER JOIN "cte_1" ON "surname" = "cte_1_1" ` +
				`INNER JOIN "cte_2" ON "surname" = "cte_2_1" AND COALESCE("limbName",'__missing__') = "cte_2_2" ` +
				`INNER JOIN "cte_3" ON "surname" = "cte_3_1" AND COALESCE("limbName",'__missing__') = "cte_3_2" AND "organName" = "cte_3_3" ` +
				`WHERE (("surname" IS NOT NULL AND "organName" IS NOT NULL) AND "organName" IS NOT NULL) ` +
				`GROUP BY "surname", COALESCE("limbName",'__missing__'), "organName", "organName", cte_1_cnt, cte_2_cnt, cte_3_cnt ` +
				`ORDER BY "surname" DESC, cte_1_cnt DESC, COALESCE("limbName",'__missing__'), "organName" DESC, cte_2_cnt DESC, "organName" ` +
				`LIMIT 2 BY "surname", COALESCE("limbName",'__missing__'), "organName"`,
			`WITH cte_1 AS ` +
				`(SELECT "surname" AS "cte_1_1", count() AS "cte_1_cnt" ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`WHERE "surname" IS NOT NULL ` +
				`GROUP BY "surname" ` +
				`ORDER BY "surname" DESC ` +
				`LIMIT 200), ` +
				`cte_2 AS ` +
				`(SELECT "surname" AS "cte_2_1", COALESCE("limbName",'__missing__') AS "cte_2_2", sumOrNull("total") AS "cte_2_cnt" ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`WHERE "surname" IS NOT NULL ` +
				`GROUP BY "surname", COALESCE("limbName",'__missing__') ` +
				`ORDER BY sumOrNull("total") ASC, COALESCE("limbName",'__missing__') ` +
				`LIMIT 20 BY "surname") ` +
				`SELECT "surname", COALESCE("limbName",'__missing__'), "organName", count() ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`INNER JOIN "cte_1" ON "surname" = "cte_1_1" ` +
				`INNER JOIN "cte_2" ON "surname" = "cte_2_1" AND COALESCE("limbName",'__missing__') = "cte_2_2" ` +
				`WHERE ("surname" IS NOT NULL AND "organName" IS NOT NULL) ` +
				`GROUP BY "surname", COALESCE("limbName",'__missing__'), "organName", cte_1_cnt, cte_2_cnt ` +
				`ORDER BY "surname" DESC, cte_1_cnt DESC, COALESCE("limbName",'__missing__'), "organName" DESC ` +
				`LIMIT 1 BY "surname", COALESCE("limbName",'__missing__')`,
			`WITH cte_1 AS ` +
				`(SELECT "surname" AS "cte_1_1", count() AS "cte_1_cnt" ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`WHERE "surname" IS NOT NULL ` +
				`GROUP BY "surname" ` +
				`ORDER BY "surname" DESC ` +
				`LIMIT 200) ` +
				`SELECT "surname", COALESCE("limbName",'__missing__'), count() ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`INNER JOIN "cte_1" ON "surname" = "cte_1_1" ` +
				`WHERE "surname" IS NOT NULL ` +
				`GROUP BY "surname", COALESCE("limbName",'__missing__'), cte_1_cnt ` +
				`ORDER BY "surname" DESC, cte_1_cnt DESC, COALESCE("limbName",'__missing__') ` +
				`LIMIT 20 BY "surname"`,
			`SELECT "surname", count() ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`WHERE "surname" IS NOT NULL ` +
				`GROUP BY "surname" ` +
				`ORDER BY "surname" DESC ` +
				`LIMIT 200`,
		},
		ExpectedPancakeSQL: `
			SELECT "aggr__2__parent_count", "aggr__2__key_0", "aggr__2__count",
			  "aggr__2__8__parent_count", "aggr__2__8__key_0", "aggr__2__8__count",
			  "aggr__2__8__order_1", "metric__2__8__1_col_0", "aggr__2__8__4__parent_count",
			  "aggr__2__8__4__key_0", "aggr__2__8__4__count",
			  "aggr__2__8__4__5__parent_count", "aggr__2__8__4__5__key_0",
			  "aggr__2__8__4__5__count", "aggr__2__8__4__5__order_1"
			FROM (
			  SELECT "aggr__2__parent_count", "aggr__2__key_0", "aggr__2__count",
				"aggr__2__8__parent_count", "aggr__2__8__key_0", "aggr__2__8__count",
				"aggr__2__8__order_1", "metric__2__8__1_col_0",
				"aggr__2__8__4__parent_count", "aggr__2__8__4__key_0",
				"aggr__2__8__4__count", "aggr__2__8__4__5__parent_count",
				"aggr__2__8__4__5__key_0", "aggr__2__8__4__5__count",
				"aggr__2__8__4__5__order_1",
				dense_rank() OVER (ORDER BY "aggr__2__key_0" DESC) AS
				"aggr__2__order_1_rank",
				dense_rank() OVER (PARTITION BY "aggr__2__key_0" ORDER BY
				"aggr__2__8__order_1" ASC, "aggr__2__8__key_0" ASC) AS
				"aggr__2__8__order_1_rank",
				dense_rank() OVER (PARTITION BY "aggr__2__key_0", "aggr__2__8__key_0" ORDER
				BY "aggr__2__8__4__key_0" DESC) AS "aggr__2__8__4__order_1_rank",
				dense_rank() OVER (PARTITION BY "aggr__2__key_0", "aggr__2__8__key_0",
				"aggr__2__8__4__key_0" ORDER BY "aggr__2__8__4__5__order_1" DESC,
				"aggr__2__8__4__key_0" ASC, "aggr__2__8__4__5__key_0" ASC) AS
				"aggr__2__8__4__5__order_1_rank"
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
				  "aggr__2__8__key_0") AS "aggr__2__8__order_1",
				  sumOrNull(sumOrNull("total")) OVER (PARTITION BY "aggr__2__key_0",
				  "aggr__2__8__key_0") AS "metric__2__8__1_col_0",
				  sum(count(*)) OVER (PARTITION BY "aggr__2__key_0", "aggr__2__8__key_0") AS
				  "aggr__2__8__4__parent_count", "organName" AS "aggr__2__8__4__key_0",
				  sum(count(*)) OVER (PARTITION BY "aggr__2__key_0", "aggr__2__8__key_0",
				  "aggr__2__8__4__key_0") AS "aggr__2__8__4__count",
				  sum(count(*)) OVER (PARTITION BY "aggr__2__key_0", "aggr__2__8__key_0",
				  "aggr__2__8__4__key_0") AS "aggr__2__8__4__5__parent_count",
				  "organName" AS "aggr__2__8__4__5__key_0",
				  count(*) AS "aggr__2__8__4__5__count",
				  count() AS "aggr__2__8__4__5__order_1"
				FROM "logs-generic-default"
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
		ExpectedResults: [][]model.QueryResultRow{
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a1"),
					model.NewQueryResultCol(`sumOrNull("total")`, 1091661.7608666667),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a2"),
					model.NewQueryResultCol(`sumOrNull("total")`, 630270.07765),
				}},
			},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a1"),
					model.NewQueryResultCol("limbName", "b11"),
					model.NewQueryResultCol(`sumOrNull("total")`, 51891.94613333333),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a1"),
					model.NewQueryResultCol("limbName", "b12"),
					model.NewQueryResultCol(`sumOrNull("total")`, 45774.291766666654),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a2"),
					model.NewQueryResultCol("limbName", "b21"),
					model.NewQueryResultCol(`sumOrNull("total")`, 399126.7496833334),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a2"),
					model.NewQueryResultCol("limbName", "b22"),
					model.NewQueryResultCol(`sumOrNull("total")`, 231143.3279666666),
				}},
			},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a1"),
					model.NewQueryResultCol("limbName", "b11"),
					model.NewQueryResultCol("organName", "c11"),
					model.NewQueryResultCol(`sumOrNull("total")`, 51891.94613333333),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a1"),
					model.NewQueryResultCol("limbName", "b12"),
					model.NewQueryResultCol("organName", "c12"),
					model.NewQueryResultCol(`sumOrNull("total")`, 45774.291766666654),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a2"),
					model.NewQueryResultCol("limbName", "b21"),
					model.NewQueryResultCol("organName", "c21"),
					model.NewQueryResultCol(`sumOrNull("total")`, 399126.7496833334),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a2"),
					model.NewQueryResultCol("limbName", "b22"),
					model.NewQueryResultCol("organName", "c22"),
					model.NewQueryResultCol(`sumOrNull("total")`, 231143.3279666666),
				}},
			},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a1"),
					model.NewQueryResultCol("limbName", "b11"),
					model.NewQueryResultCol("organName", "c11"),
					model.NewQueryResultCol(`sumOrNull("some")`, 37988.09523333333),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a1"),
					model.NewQueryResultCol("limbName", "b12"),
					model.NewQueryResultCol("organName", "c12"),
					model.NewQueryResultCol(`sumOrNull("some")`, 36577.89516666666),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a2"),
					model.NewQueryResultCol("limbName", "b21"),
					model.NewQueryResultCol("organName", "c21"),
					model.NewQueryResultCol(`sumOrNull("some")`, 337246.82201666664),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a2"),
					model.NewQueryResultCol("limbName", "b22"),
					model.NewQueryResultCol("organName", "c22"),
					model.NewQueryResultCol(`sumOrNull("some")`, 205408.48849999998),
				}},
			},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a1"),
					model.NewQueryResultCol("limbName", "b11"),
					model.NewQueryResultCol("organName", "c11"),
					model.NewQueryResultCol("count()", 21),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a1"),
					model.NewQueryResultCol("limbName", "b12"),
					model.NewQueryResultCol("organName", "c12"),
					model.NewQueryResultCol("count()", 24),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a2"),
					model.NewQueryResultCol("limbName", "b21"),
					model.NewQueryResultCol("organName", "c21"),
					model.NewQueryResultCol("count()", 17),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a2"),
					model.NewQueryResultCol("limbName", "b22"),
					model.NewQueryResultCol("organName", "c22"),
					model.NewQueryResultCol("count()", 17),
				}},
			},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a1"),
					model.NewQueryResultCol("limbName", "b11"),
					model.NewQueryResultCol("count()", 21),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a1"),
					model.NewQueryResultCol("limbName", "b12"),
					model.NewQueryResultCol("count()", 24),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a2"),
					model.NewQueryResultCol("limbName", "b21"),
					model.NewQueryResultCol("count()", 17),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a2"),
					model.NewQueryResultCol("limbName", "b22"),
					model.NewQueryResultCol("count()", 17),
				}},
			},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a1"),
					model.NewQueryResultCol("count()", 1036),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a2"),
					model.NewQueryResultCol("count()", 34),
				}},
			},
		},
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", 34290),
				model.NewQueryResultCol("aggr__2__key_0", "a1"),
				model.NewQueryResultCol("aggr__2__count", uint64(1036)),
				model.NewQueryResultCol("aggr__2__order_1", 1091661.7608666667),
				model.NewQueryResultCol("metric__2__1_col_0", 1091661.7608666667),
				model.NewQueryResultCol("aggr__2__8__parent_count", 1036),
				model.NewQueryResultCol("aggr__2__8__key_0", "b11"),
				model.NewQueryResultCol("aggr__2__8__count", int64(21)),
				model.NewQueryResultCol("aggr__2__8__order_1", 51891.94613333333),
				model.NewQueryResultCol("metric__2__8__1_col_0", 51891.94613333333),
				model.NewQueryResultCol("aggr__2__8__4__parent_count", 21),
				model.NewQueryResultCol("aggr__2__8__4__key_0", "c11"),
				model.NewQueryResultCol("aggr__2__8__4__count", int64(21)),
				model.NewQueryResultCol("aggr__2__8__4__order_1", 51891.94613333333),
				model.NewQueryResultCol("metric__2__8__4__1_col_0", 51891.94613333333),
				model.NewQueryResultCol("metric__2__8__4__5_col_0", 37988.09523333333),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", 34290),
				model.NewQueryResultCol("aggr__2__key_0", "a1"),
				model.NewQueryResultCol("aggr__2__count", uint64(1036)),
				model.NewQueryResultCol("aggr__2__order_1", 1091661.7608666667),
				model.NewQueryResultCol("metric__2__1_col_0", 1091661.7608666667),
				model.NewQueryResultCol("aggr__2__8__parent_count", 1036),
				model.NewQueryResultCol("aggr__2__8__key_0", "b12"),
				model.NewQueryResultCol("aggr__2__8__count", int64(24)),
				model.NewQueryResultCol("aggr__2__8__order_1", 45774.291766666654),
				model.NewQueryResultCol("metric__2__8__1_col_0", 45774.291766666654),
				model.NewQueryResultCol("aggr__2__8__4__parent_count", 24),
				model.NewQueryResultCol("aggr__2__8__4__key_0", "c12"),
				model.NewQueryResultCol("aggr__2__8__4__count", int64(24)),
				model.NewQueryResultCol("aggr__2__8__4__order_1", 45774.291766666654),
				model.NewQueryResultCol("metric__2__8__4__1_col_0", 45774.291766666654),
				model.NewQueryResultCol("metric__2__8__4__5_col_0", 36577.89516666666),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", 34290),
				model.NewQueryResultCol("aggr__2__key_0", "a2"),
				model.NewQueryResultCol("aggr__2__count", uint64(34)),
				model.NewQueryResultCol("aggr__2__order_1", 630270.07765),
				model.NewQueryResultCol("metric__2__1_col_0", 630270.07765),
				model.NewQueryResultCol("aggr__2__8__parent_count", 34),
				model.NewQueryResultCol("aggr__2__8__key_0", "b21"),
				model.NewQueryResultCol("aggr__2__8__count", int64(17)),
				model.NewQueryResultCol("aggr__2__8__order_1", 399126.7496833334),
				model.NewQueryResultCol("metric__2__8__1_col_0", 399126.7496833334),
				model.NewQueryResultCol("aggr__2__8__4__parent_count", 17),
				model.NewQueryResultCol("aggr__2__8__4__key_0", "c21"),
				model.NewQueryResultCol("aggr__2__8__4__count", int64(17)),
				model.NewQueryResultCol("aggr__2__8__4__order_1", 399126.7496833334),
				model.NewQueryResultCol("metric__2__8__4__1_col_0", 399126.7496833334),
				model.NewQueryResultCol("metric__2__8__4__5_col_0", 337246.82201666664),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", 34290),
				model.NewQueryResultCol("aggr__2__key_0", "a2"),
				model.NewQueryResultCol("aggr__2__count", uint64(34)),
				model.NewQueryResultCol("aggr__2__order_1", 630270.07765),
				model.NewQueryResultCol("metric__2__1_col_0", 630270.07765),
				model.NewQueryResultCol("aggr__2__8__parent_count", 34),
				model.NewQueryResultCol("aggr__2__8__key_0", "b22"),
				model.NewQueryResultCol("aggr__2__8__count", int64(17)),
				model.NewQueryResultCol("aggr__2__8__order_1", 231143.3279666666),
				model.NewQueryResultCol("metric__2__8__1_col_0", 231143.3279666666),
				model.NewQueryResultCol("aggr__2__8__4__parent_count", 17),
				model.NewQueryResultCol("aggr__2__8__4__key_0", "c22"),
				model.NewQueryResultCol("aggr__2__8__4__count", int64(17)),
				model.NewQueryResultCol("aggr__2__8__4__order_1", 231143.3279666666),
				model.NewQueryResultCol("metric__2__8__4__1_col_0", 231143.3279666666),
				model.NewQueryResultCol("metric__2__8__4__5_col_0", 205408.48849999998),
			}},
		},
		ExpectedSQLs: []string{
			`WITH cte_1 AS ` +
				`(SELECT "surname" AS "cte_1_1", count() AS "cte_1_cnt" ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`GROUP BY "surname" ` +
				`ORDER BY count() DESC, "surname" ` +
				`LIMIT 200) ` +
				`SELECT "surname", sumOrNull("total") ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`INNER JOIN "cte_1" ON "surname" = "cte_1_1" ` +
				`GROUP BY "surname", cte_1_cnt ` +
				`ORDER BY cte_1_cnt DESC, "surname"`,
			`WITH cte_1 AS ` +
				`(SELECT "surname" AS "cte_1_1", count() AS "cte_1_cnt" ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`GROUP BY "surname" ` +
				`ORDER BY count() DESC, "surname" ` +
				`LIMIT 200), ` +
				`cte_2 AS ` +
				`(SELECT "surname" AS "cte_2_1", COALESCE("limbName",'__missing__') AS "cte_2_2", count() AS "cte_2_cnt" ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`GROUP BY "surname", COALESCE("limbName",'__missing__') ` +
				`ORDER BY count() DESC, COALESCE("limbName",'__missing__') ` +
				`LIMIT 20 BY "surname") ` +
				`SELECT "surname", COALESCE("limbName",'__missing__'), sumOrNull("total") ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`INNER JOIN "cte_1" ON "surname" = "cte_1_1" ` +
				`INNER JOIN "cte_2" ON "surname" = "cte_2_1" AND COALESCE("limbName",'__missing__') = "cte_2_2" ` +
				`GROUP BY "surname", COALESCE("limbName",'__missing__'), cte_1_cnt, cte_2_cnt ` +
				`ORDER BY cte_1_cnt DESC, "surname", cte_2_cnt DESC, COALESCE("limbName",'__missing__')`,
			`WITH cte_1 AS ` +
				`(SELECT "surname" AS "cte_1_1", count() AS "cte_1_cnt" ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`GROUP BY "surname" ` +
				`ORDER BY count() DESC, "surname" ` +
				`LIMIT 200), ` +
				`cte_2 AS ` +
				`(SELECT "surname" AS "cte_2_1", COALESCE("limbName",'__missing__') AS "cte_2_2", count() AS "cte_2_cnt" ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`GROUP BY "surname", COALESCE("limbName",'__missing__') ` +
				`ORDER BY count() DESC, COALESCE("limbName",'__missing__') ` +
				`LIMIT 20 BY "surname"), ` +
				`cte_3 AS ` +
				`(SELECT "surname" AS "cte_3_1", COALESCE("limbName",'__missing__') AS "cte_3_2", "organName" AS "cte_3_3", count() AS "cte_3_cnt" ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`GROUP BY "surname", COALESCE("limbName",'__missing__'), "organName" ` +
				`ORDER BY count() DESC, "organName" ` +
				`LIMIT 1 BY "surname", COALESCE("limbName",'__missing__')) ` +
				`SELECT "surname", COALESCE("limbName",'__missing__'), "organName", sumOrNull("total") ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`INNER JOIN "cte_1" ON "surname" = "cte_1_1" ` +
				`INNER JOIN "cte_2" ON "surname" = "cte_2_1" AND COALESCE("limbName",'__missing__') = "cte_2_2" ` +
				`INNER JOIN "cte_3" ON "surname" = "cte_3_1" AND COALESCE("limbName",'__missing__') = "cte_3_2" AND "organName" = "cte_3_3" ` +
				`GROUP BY "surname", COALESCE("limbName",'__missing__'), "organName", cte_1_cnt, cte_2_cnt, cte_3_cnt ` +
				`ORDER BY cte_1_cnt DESC, "surname", cte_2_cnt DESC, COALESCE("limbName",'__missing__'), cte_3_cnt DESC, "organName"`,
			`WITH cte_1 AS ` +
				`(SELECT "surname" AS "cte_1_1", count() AS "cte_1_cnt" ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`GROUP BY "surname" ` +
				`ORDER BY count() DESC, "surname" ` +
				`LIMIT 200), ` +
				`cte_2 AS ` +
				`(SELECT "surname" AS "cte_2_1", COALESCE("limbName",'__missing__') AS "cte_2_2", count() AS "cte_2_cnt" ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`GROUP BY "surname", COALESCE("limbName",'__missing__') ` +
				`ORDER BY count() DESC, COALESCE("limbName",'__missing__') ` +
				`LIMIT 20 BY "surname"), ` +
				`cte_3 AS ` +
				`(SELECT "surname" AS "cte_3_1", COALESCE("limbName",'__missing__') AS "cte_3_2", "organName" AS "cte_3_3", count() AS "cte_3_cnt" ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`GROUP BY "surname", COALESCE("limbName",'__missing__'), "organName" ` +
				`ORDER BY count() DESC, "organName" ` +
				`LIMIT 1 BY "surname", COALESCE("limbName",'__missing__')) ` +
				`SELECT "surname", COALESCE("limbName",'__missing__'), "organName", sumOrNull("some") ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`INNER JOIN "cte_1" ON "surname" = "cte_1_1" ` +
				`INNER JOIN "cte_2" ON "surname" = "cte_2_1" AND COALESCE("limbName",'__missing__') = "cte_2_2" ` +
				`INNER JOIN "cte_3" ON "surname" = "cte_3_1" AND COALESCE("limbName",'__missing__') = "cte_3_2" AND "organName" = "cte_3_3" ` +
				`GROUP BY "surname", COALESCE("limbName",'__missing__'), "organName", cte_1_cnt, cte_2_cnt, cte_3_cnt ` +
				`ORDER BY cte_1_cnt DESC, "surname", cte_2_cnt DESC, COALESCE("limbName",'__missing__'), cte_3_cnt DESC, "organName"`,
			`WITH cte_1 AS ` +
				`(SELECT "surname" AS "cte_1_1", count() AS "cte_1_cnt" ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`GROUP BY "surname" ` +
				`ORDER BY count() DESC, "surname" ` +
				`LIMIT 200), ` +
				`cte_2 AS ` +
				`(SELECT "surname" AS "cte_2_1", COALESCE("limbName",'__missing__') AS "cte_2_2", count() AS "cte_2_cnt" ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`GROUP BY "surname", COALESCE("limbName",'__missing__') ` +
				`ORDER BY count() DESC, COALESCE("limbName",'__missing__') ` +
				`LIMIT 20 BY "surname") ` +
				`SELECT "surname", COALESCE("limbName",'__missing__'), "organName", count() ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`INNER JOIN "cte_1" ON "surname" = "cte_1_1" ` +
				`INNER JOIN "cte_2" ON "surname" = "cte_2_1" AND COALESCE("limbName",'__missing__') = "cte_2_2" ` +
				`GROUP BY "surname", COALESCE("limbName",'__missing__'), "organName", cte_1_cnt, cte_2_cnt ` +
				`ORDER BY cte_1_cnt DESC, "surname", cte_2_cnt DESC, COALESCE("limbName",'__missing__'), count() DESC, "organName" ` +
				`LIMIT 1 BY "surname", COALESCE("limbName",'__missing__')`,
			`WITH cte_1 AS ` +
				`(SELECT "surname" AS "cte_1_1", count() AS "cte_1_cnt" ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`GROUP BY "surname" ` +
				`ORDER BY count() DESC, "surname" ` +
				`LIMIT 200) ` +
				`SELECT "surname", COALESCE("limbName",'__missing__'), count() ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`INNER JOIN "cte_1" ON "surname" = "cte_1_1" ` +
				`GROUP BY "surname", COALESCE("limbName",'__missing__'), cte_1_cnt ` +
				`ORDER BY cte_1_cnt DESC, "surname", count() DESC, COALESCE("limbName",'__missing__') ` +
				`LIMIT 20 BY "surname"`,
			`SELECT "surname", count() ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`GROUP BY "surname" ` +
				`ORDER BY count() DESC, "surname" ` +
				`LIMIT 200`,
		},
		ExpectedPancakeSQL: `
			SELECT "aggr__2__parent_count", "aggr__2__key_0", "aggr__2__count",
			  "aggr__2__order_1", "metric__2__1_col_0", "aggr__2__8__parent_count",
			  "aggr__2__8__key_0", "aggr__2__8__count", "aggr__2__8__order_1",
			  "metric__2__8__1_col_0", "aggr__2__8__4__parent_count",
			  "aggr__2__8__4__key_0", "aggr__2__8__4__count", "aggr__2__8__4__order_1",
			  "metric__2__8__4__1_col_0", "metric__2__8__4__5_col_0"
			FROM (
			  SELECT "aggr__2__parent_count", "aggr__2__key_0", "aggr__2__count",
				"aggr__2__order_1", "metric__2__1_col_0", "aggr__2__8__parent_count",
				"aggr__2__8__key_0", "aggr__2__8__count", "aggr__2__8__order_1",
				"metric__2__8__1_col_0", "aggr__2__8__4__parent_count",
				"aggr__2__8__4__key_0", "aggr__2__8__4__count", "aggr__2__8__4__order_1",
				"metric__2__8__4__1_col_0", "metric__2__8__4__5_col_0",
				dense_rank() OVER (ORDER BY "aggr__2__order_1" DESC, "aggr__2__key_0" ASC)
				AS "aggr__2__order_1_rank",
				dense_rank() OVER (PARTITION BY "aggr__2__key_0" ORDER BY
				"aggr__2__8__order_1" DESC, "aggr__2__8__key_0" ASC) AS
				"aggr__2__8__order_1_rank",
				dense_rank() OVER (PARTITION BY "aggr__2__key_0", "aggr__2__8__key_0" ORDER
				BY "aggr__2__8__4__order_1" DESC, "aggr__2__8__4__key_0" ASC) AS
				"aggr__2__8__4__order_1_rank"
			  FROM (
				SELECT sum(count(*)) OVER () AS "aggr__2__parent_count",
				  "surname" AS "aggr__2__key_0",
				  sum(count(*)) OVER (PARTITION BY "aggr__2__key_0") AS "aggr__2__count",
				  sumOrNull(sumOrNull("total")) OVER (PARTITION BY "aggr__2__key_0") AS
				  "aggr__2__order_1",
				  sumOrNull(sumOrNull("total")) OVER (PARTITION BY "aggr__2__key_0") AS
				  "metric__2__1_col_0",
				  sum(count(*)) OVER (PARTITION BY "aggr__2__key_0") AS
				  "aggr__2__8__parent_count",
				  COALESCE("limbName", '__missing__') AS "aggr__2__8__key_0",
				  sum(count(*)) OVER (PARTITION BY "aggr__2__key_0", "aggr__2__8__key_0") AS
				  "aggr__2__8__count",
				  sumOrNull(sumOrNull("total")) OVER (PARTITION BY "aggr__2__key_0",
				  "aggr__2__8__key_0") AS "aggr__2__8__order_1",
				  sumOrNull(sumOrNull("total")) OVER (PARTITION BY "aggr__2__key_0",
				  "aggr__2__8__key_0") AS "metric__2__8__1_col_0",
				  sum(count(*)) OVER (PARTITION BY "aggr__2__key_0", "aggr__2__8__key_0") AS
				  "aggr__2__8__4__parent_count", "organName" AS "aggr__2__8__4__key_0",
				  count(*) AS "aggr__2__8__4__count",
				  sumOrNull("total") AS "aggr__2__8__4__order_1",
				  sumOrNull("total") AS "metric__2__8__4__1_col_0",
				  sumOrNull("some") AS "metric__2__8__4__5_col_0"
				FROM "logs-generic-default"
				GROUP BY "surname" AS "aggr__2__key_0",
				  COALESCE("limbName", '__missing__') AS "aggr__2__8__key_0",
				  "organName" AS "aggr__2__8__4__key_0"))
			WHERE (("aggr__2__order_1_rank"<=201 AND "aggr__2__8__order_1_rank"<=20) AND
			  "aggr__2__8__4__order_1_rank"<=2)
			ORDER BY "aggr__2__order_1_rank" ASC, "aggr__2__8__order_1_rank" ASC,
			  "aggr__2__8__4__order_1_rank" ASC`,
	},
	{ // [6]
		TestName: "Ophelia Test 7: 5x terms + a lot of other aggregations",
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
									"sum_other_doc_count": 504
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
		ExpectedResults: [][]model.QueryResultRow{
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a1"),
					model.NewQueryResultCol(`sumOrNull("total")`, 1091661.7608666667),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a2"),
					model.NewQueryResultCol(`sumOrNull("total")`, 630270.07765),
				}},
			},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a1"),
					model.NewQueryResultCol("limbName", "b11"),
					model.NewQueryResultCol(`sumOrNull("total")`, 51891.94613333333),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a1"),
					model.NewQueryResultCol("limbName", "b12"),
					model.NewQueryResultCol(`sumOrNull("total")`, 45774.291766666654),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a2"),
					model.NewQueryResultCol("limbName", "b21"),
					model.NewQueryResultCol(`sumOrNull("total")`, 399126.7496833334),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a2"),
					model.NewQueryResultCol("limbName", "b22"),
					model.NewQueryResultCol(`sumOrNull("total")`, 231143.3279666666),
				}},
			},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a1"),
					model.NewQueryResultCol("limbName", "b11"),
					model.NewQueryResultCol("organName", "c11"),
					model.NewQueryResultCol(`sumOrNull("total")`, 51891.94613333333),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a1"),
					model.NewQueryResultCol("limbName", "b12"),
					model.NewQueryResultCol("organName", "c12"),
					model.NewQueryResultCol(`sumOrNull("total")`, 45774.291766666654),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a2"),
					model.NewQueryResultCol("limbName", "b21"),
					model.NewQueryResultCol("organName", "c21"),
					model.NewQueryResultCol(`sumOrNull("total")`, 399126.7496833334),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a2"),
					model.NewQueryResultCol("limbName", "b22"),
					model.NewQueryResultCol("organName", "c22"),
					model.NewQueryResultCol(`sumOrNull("total")`, 231143.3279666666),
				}},
			},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a1"),
					model.NewQueryResultCol("limbName", "b11"),
					model.NewQueryResultCol("organName", "c11"),
					model.NewQueryResultCol(`sumOrNull("some")`, 37988.09523333333),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a1"),
					model.NewQueryResultCol("limbName", "b12"),
					model.NewQueryResultCol("organName", "c12"),
					model.NewQueryResultCol(`sumOrNull("some")`, 36577.89516666666),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a2"),
					model.NewQueryResultCol("limbName", "b21"),
					model.NewQueryResultCol("organName", "c21"),
					model.NewQueryResultCol(`sumOrNull("some")`, 337246.82201666664),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a2"),
					model.NewQueryResultCol("limbName", "b22"),
					model.NewQueryResultCol("organName", "c22"),
					model.NewQueryResultCol(`sumOrNull("some")`, 205408.48849999998),
				}},
			},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a1"),
					model.NewQueryResultCol("limbName", "b11"),
					model.NewQueryResultCol("organName", "c11"),
					model.NewQueryResultCol("count()", 21),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a1"),
					model.NewQueryResultCol("limbName", "b12"),
					model.NewQueryResultCol("organName", "c12"),
					model.NewQueryResultCol("count()", 24),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a2"),
					model.NewQueryResultCol("limbName", "b21"),
					model.NewQueryResultCol("organName", "c21"),
					model.NewQueryResultCol("count()", 17),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a2"),
					model.NewQueryResultCol("limbName", "b22"),
					model.NewQueryResultCol("organName", "c22"),
					model.NewQueryResultCol("count()", 17),
				}},
			},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a1"),
					model.NewQueryResultCol("limbName", "b11"),
					model.NewQueryResultCol("count()", 21),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a1"),
					model.NewQueryResultCol("limbName", "b12"),
					model.NewQueryResultCol("count()", 24),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a2"),
					model.NewQueryResultCol("limbName", "b21"),
					model.NewQueryResultCol("count()", 17),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a2"),
					model.NewQueryResultCol("limbName", "b22"),
					model.NewQueryResultCol("count()", 17),
				}},
			},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a1"),
					model.NewQueryResultCol("count()", 1036),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a2"),
					model.NewQueryResultCol("count()", 34),
				}},
			},
			{},
			{},
			{},
			{},
			{},
			{},
			{},
		},
		ExpectedSQLs: []string{
			`SELECT count() ` +
				`FROM (SELECT 1 ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`LIMIT 10000)`,
			`WITH cte_1 AS ` +
				`(SELECT "surname" AS "cte_1_1", count() AS "cte_1_cnt" ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`GROUP BY "surname" ` +
				`ORDER BY count() DESC, "surname" ` +
				`LIMIT 100) ` +
				`SELECT "surname", sumOrNull("total") ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`INNER JOIN "cte_1" ON "surname" = "cte_1_1" ` +
				`GROUP BY "surname", cte_1_cnt ` +
				`ORDER BY cte_1_cnt DESC, "surname"`,
			`WITH cte_1 AS ` +
				`(SELECT "surname" AS "cte_1_1", count() AS "cte_1_cnt" ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`GROUP BY "surname" ` +
				`ORDER BY count() DESC, "surname" ` +
				`LIMIT 100), ` +
				`cte_2 AS ` +
				`(SELECT "surname" AS "cte_2_1", COALESCE("limbName",'__missing__') AS "cte_2_2", count() AS "cte_2_cnt" ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`GROUP BY "surname", COALESCE("limbName",'__missing__') ` +
				`ORDER BY count() DESC, COALESCE("limbName",'__missing__') ` +
				`LIMIT 10 BY "surname") ` +
				`SELECT "surname", COALESCE("limbName",'__missing__'), sumOrNull("total") ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`INNER JOIN "cte_1" ON "surname" = "cte_1_1" ` +
				`INNER JOIN "cte_2" ON "surname" = "cte_2_1" AND COALESCE("limbName",'__missing__') = "cte_2_2" ` +
				`GROUP BY "surname", COALESCE("limbName",'__missing__'), cte_1_cnt, cte_2_cnt ` +
				`ORDER BY cte_1_cnt DESC, "surname", cte_2_cnt DESC, COALESCE("limbName",'__missing__')`,
			`WITH cte_1 AS ` +
				`(SELECT "surname" AS "cte_1_1", count() AS "cte_1_cnt" ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`GROUP BY "surname" ` +
				`ORDER BY count() DESC, "surname" ` +
				`LIMIT 100), ` +
				`cte_2 AS ` +
				`(SELECT "surname" AS "cte_2_1", COALESCE("limbName",'__missing__') AS "cte_2_2", count() AS "cte_2_cnt" ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`GROUP BY "surname", COALESCE("limbName",'__missing__') ` +
				`ORDER BY count() DESC, COALESCE("limbName",'__missing__') ` +
				`LIMIT 10 BY "surname"), ` +
				`cte_3 AS ` +
				`(SELECT "surname" AS "cte_3_1", COALESCE("limbName",'__missing__') AS "cte_3_2", COALESCE("organName",'__missing__') AS "cte_3_3", count() AS "cte_3_cnt" ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`GROUP BY "surname", COALESCE("limbName",'__missing__'), COALESCE("organName",'__missing__') ` +
				`ORDER BY count() DESC, COALESCE("organName",'__missing__') ` +
				`LIMIT 10 BY "surname", COALESCE("limbName",'__missing__')) ` +
				`SELECT "surname", COALESCE("limbName",'__missing__'), COALESCE("organName",'__missing__'), sumOrNull("total") ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`INNER JOIN "cte_1" ON "surname" = "cte_1_1" ` +
				`INNER JOIN "cte_2" ON "surname" = "cte_2_1" AND COALESCE("limbName",'__missing__') = "cte_2_2" ` +
				`INNER JOIN "cte_3" ON "surname" = "cte_3_1" AND COALESCE("limbName",'__missing__') = "cte_3_2" AND COALESCE("organName",'__missing__') = "cte_3_3" ` +
				`GROUP BY "surname", COALESCE("limbName",'__missing__'), COALESCE("organName",'__missing__'), cte_1_cnt, cte_2_cnt, cte_3_cnt ` +
				`ORDER BY cte_1_cnt DESC, "surname", cte_2_cnt DESC, COALESCE("limbName",'__missing__'), cte_3_cnt DESC, COALESCE("organName",'__missing__')`,
			`WITH cte_1 AS ` +
				`(SELECT "surname" AS "cte_1_1", count() AS "cte_1_cnt" ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`GROUP BY "surname" ` +
				`ORDER BY count() DESC, "surname" ` +
				`LIMIT 100), ` +
				`cte_2 AS ` +
				`(SELECT "surname" AS "cte_2_1", COALESCE("limbName",'__missing__') AS "cte_2_2", count() AS "cte_2_cnt" ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`GROUP BY "surname", COALESCE("limbName",'__missing__') ` +
				`ORDER BY count() DESC, COALESCE("limbName",'__missing__') ` +
				`LIMIT 10 BY "surname"), ` +
				`cte_3 AS ` +
				`(SELECT "surname" AS "cte_3_1", COALESCE("limbName",'__missing__') AS "cte_3_2", COALESCE("organName",'__missing__') AS "cte_3_3", count() AS "cte_3_cnt" ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`GROUP BY "surname", COALESCE("limbName",'__missing__'), COALESCE("organName",'__missing__') ` +
				`ORDER BY count() DESC, COALESCE("organName",'__missing__') ` +
				`LIMIT 10 BY "surname", COALESCE("limbName",'__missing__')) ` +
				`SELECT "surname", COALESCE("limbName",'__missing__'), COALESCE("organName",'__missing__'), sumOrNull("some") ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`INNER JOIN "cte_1" ON "surname" = "cte_1_1" ` +
				`INNER JOIN "cte_2" ON "surname" = "cte_2_1" AND COALESCE("limbName",'__missing__') = "cte_2_2" ` +
				`INNER JOIN "cte_3" ON "surname" = "cte_3_1" AND COALESCE("limbName",'__missing__') = "cte_3_2" AND COALESCE("organName",'__missing__') = "cte_3_3" ` +
				`GROUP BY "surname", COALESCE("limbName",'__missing__'), COALESCE("organName",'__missing__'), cte_1_cnt, cte_2_cnt, cte_3_cnt ` +
				`ORDER BY cte_1_cnt DESC, "surname", cte_2_cnt DESC, COALESCE("limbName",'__missing__'), cte_3_cnt DESC, COALESCE("organName",'__missing__')`,
			`WITH cte_1 AS ` +
				`(SELECT "surname" AS "cte_1_1", count() AS "cte_1_cnt" ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`GROUP BY "surname" ` +
				`ORDER BY count() DESC, "surname" ` +
				`LIMIT 200), ` +
				`cte_2 AS ` +
				`(SELECT "surname" AS "cte_2_1", "limbName" AS "cte_2_2", count() AS "cte_2_cnt" ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`GROUP BY "surname", "limbName" ` +
				`ORDER BY count() DESC, "surname", count() DESC, "limbName" ` +
				`LIMIT 20 BY "surname") ` +
				`SELECT "surname", "limbName", sumOrNull("total") ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`INNER JOIN "cte_1" ON "surname" = "cte_1_1" ` +
				`INNER JOIN "cte_2" ON "surname" = "cte_2_1" AND "limbName" = "cte_2_2" ` +
				`GROUP BY "surname", "limbName", cte_1_cnt, cte_2_cnt ` +
				`ORDER BY cte_1_cnt DESC, "surname", cte_2_cnt DESC, "limbName"`,
			`WITH cte_1 AS ` +
				`(SELECT "surname" AS "cte_1_1", count() AS "cte_1_cnt" ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`GROUP BY "surname" ` +
				`ORDER BY count() DESC, "surname" ` +
				`LIMIT 200), ` +
				`cte_2 AS ` +
				`(SELECT "surname" AS "cte_2_1", "limbName" AS "cte_2_2", count() AS "cte_2_cnt" ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`GROUP BY "surname", "limbName" ` +
				`ORDER BY count() DESC, "surname", count() DESC, "limbName" ` +
				`LIMIT 20 BY "surname"), ` +
				`cte_3 AS ` +
				`(SELECT "surname" AS "cte_3_1", "limbName" AS "cte_3_2", "organName" AS "cte_3_3", count() AS "cte_3_cnt" ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`GROUP BY "surname", "limbName", "organName" ` +
				`ORDER BY count() DESC, "surname", count() DESC, "limbName", count() DESC, "organName" ` +
				`LIMIT 1 BY "surname", "limbName") ` +
				`SELECT "surname", "limbName", "organName", sumOrNull("total") ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`INNER JOIN "cte_1" ON "surname" = "cte_1_1" ` +
				`INNER JOIN "cte_2" ON "surname" = "cte_2_1" AND "limbName" = "cte_2_2" ` +
				`INNER JOIN "cte_3" ON "surname" = "cte_3_1" AND "limbName" = "cte_3_2" AND "organName" = "cte_3_3" ` +
				`GROUP BY "surname", "limbName", "organName", cte_1_cnt, cte_2_cnt, cte_3_cnt ` +
				`ORDER BY cte_1_cnt DESC, "surname", cte_2_cnt DESC, "limbName", cte_3_cnt DESC, "organName"`,
			`WITH cte_1 AS ` +
				`(SELECT "surname" AS "cte_1_1", count() AS "cte_1_cnt" ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`GROUP BY "surname" ` +
				`ORDER BY count() DESC, "surname" ` +
				`LIMIT 200), ` +
				`cte_2 AS ` +
				`(SELECT "surname" AS "cte_2_1", "limbName" AS "cte_2_2", count() AS "cte_2_cnt" ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`GROUP BY "surname", "limbName" ` +
				`ORDER BY count() DESC, "surname", count() DESC, "limbName" ` +
				`LIMIT 20 BY "surname"), ` +
				`cte_3 AS ` +
				`(SELECT "surname" AS "cte_3_1", "limbName" AS "cte_3_2", "organName" AS "cte_3_3", count() AS "cte_3_cnt" ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`GROUP BY "surname", "limbName", "organName" ` +
				`ORDER BY count() DESC, "surname", count() DESC, "limbName", count() DESC, "organName" ` +
				`LIMIT 1 BY "surname", "limbName") ` +
				`SELECT "surname", "limbName", "organName", sumOrNull("some") ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`INNER JOIN "cte_1" ON "surname" = "cte_1_1" ` +
				`INNER JOIN "cte_2" ON "surname" = "cte_2_1" AND "limbName" = "cte_2_2" ` +
				`INNER JOIN "cte_3" ON "surname" = "cte_3_1" AND "limbName" = "cte_3_2" AND "organName" = "cte_3_3" ` +
				`GROUP BY "surname", "limbName", "organName", cte_1_cnt, cte_2_cnt, cte_3_cnt ` +
				`ORDER BY cte_1_cnt DESC, "surname", cte_2_cnt DESC, "limbName", cte_3_cnt DESC, "organName"`,
			`WITH cte_1 AS ` +
				`(SELECT "surname" AS "cte_1_1", count() AS "cte_1_cnt" ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`GROUP BY "surname" ` +
				`ORDER BY count() DESC, "surname" ` +
				`LIMIT 200), ` +
				`cte_2 AS ` +
				`(SELECT "surname" AS "cte_2_1", "limbName" AS "cte_2_2", count() AS "cte_2_cnt" ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`GROUP BY "surname", "limbName" ` +
				`ORDER BY count() DESC, "surname", count() DESC, "limbName" ` +
				`LIMIT 20 BY "surname") ` +
				`SELECT "surname", "limbName", "organName", count() ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`INNER JOIN "cte_1" ON "surname" = "cte_1_1" ` +
				`INNER JOIN "cte_2" ON "surname" = "cte_2_1" AND "limbName" = "cte_2_2" ` +
				`GROUP BY "surname", "limbName", "organName", cte_1_cnt, cte_2_cnt ` +
				`ORDER BY cte_1_cnt DESC, "surname", cte_2_cnt DESC, "limbName", count() DESC, "organName" ` +
				`LIMIT 1 BY "surname", "limbName"`,
			`WITH cte_1 AS ` +
				`(SELECT "surname" AS "cte_1_1", count() AS "cte_1_cnt" ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`GROUP BY "surname" ` +
				`ORDER BY count() DESC, "surname" ` +
				`LIMIT 200) ` +
				`SELECT "surname", "limbName", count() ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`INNER JOIN "cte_1" ON "surname" = "cte_1_1" ` +
				`GROUP BY "surname", "limbName", cte_1_cnt ` +
				`ORDER BY cte_1_cnt DESC, "surname", count() DESC, "limbName" ` +
				`LIMIT 20 BY "surname"`,
			`SELECT "surname", count() ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`GROUP BY "surname" ` +
				`ORDER BY count() DESC, "surname" ` +
				`LIMIT 200`,
			``,
			``,
			`WITH cte_1 AS ` +
				`(SELECT "surname" AS "cte_1_1", count() AS "cte_1_cnt" ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`GROUP BY "surname" ` +
				`ORDER BY count() DESC, "surname" ` +
				`LIMIT 100), ` +
				`cte_2 AS ` +
				`(SELECT "surname" AS "cte_2_1", COALESCE("limbName",'__missing__') AS "cte_2_2", count() AS "cte_2_cnt" ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`GROUP BY "surname", COALESCE("limbName",'__missing__') ` +
				`ORDER BY count() DESC, COALESCE("limbName",'__missing__') ` +
				`LIMIT 10 BY "surname"), ` +
				`cte_3 AS ` +
				`(SELECT "surname" AS "cte_3_1", COALESCE("limbName",'__missing__') AS "cte_3_2", COALESCE("organName",'__missing__') AS "cte_3_3", count() AS "cte_3_cnt" ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`GROUP BY "surname", COALESCE("limbName",'__missing__'), COALESCE("organName",'__missing__') ` +
				`ORDER BY count() DESC, COALESCE("organName",'__missing__') ` +
				`LIMIT 10 BY "surname", COALESCE("limbName",'__missing__')) ` +
				`SELECT "surname", COALESCE("limbName",'__missing__'), COALESCE("organName",'__missing__'), "doctorName", count() ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`INNER JOIN "cte_1" ON "surname" = "cte_1_1" ` +
				`INNER JOIN "cte_2" ON "surname" = "cte_2_1" AND COALESCE("limbName",'__missing__') = "cte_2_2" ` +
				`INNER JOIN "cte_3" ON "surname" = "cte_3_1" AND COALESCE("limbName",'__missing__') = "cte_3_2" AND COALESCE("organName",'__missing__') = "cte_3_3" ` +
				`GROUP BY "surname", COALESCE("limbName",'__missing__'), COALESCE("organName",'__missing__'), "doctorName", cte_1_cnt, cte_2_cnt, cte_3_cnt ` +
				`ORDER BY cte_1_cnt DESC, "surname", cte_2_cnt DESC, COALESCE("limbName",'__missing__'), cte_3_cnt DESC, COALESCE("organName",'__missing__'), count() DESC, "doctorName" ` +
				`LIMIT 6 BY "surname", COALESCE("limbName",'__missing__'), COALESCE("organName",'__missing__')`,
			`WITH cte_1 AS ` +
				`(SELECT "surname" AS "cte_1_1", count() AS "cte_1_cnt" ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`GROUP BY "surname" ` +
				`ORDER BY count() DESC, "surname" ` +
				`LIMIT 100), ` +
				`cte_2 AS ` +
				`(SELECT "surname" AS "cte_2_1", COALESCE("limbName",'__missing__') AS "cte_2_2", count() AS "cte_2_cnt" ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`GROUP BY "surname", COALESCE("limbName",'__missing__') ` +
				`ORDER BY count() DESC, COALESCE("limbName",'__missing__') ` +
				`LIMIT 10 BY "surname") ` +
				`SELECT "surname", COALESCE("limbName",'__missing__'), COALESCE("organName",'__missing__'), count() ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`INNER JOIN "cte_1" ON "surname" = "cte_1_1" ` +
				`INNER JOIN "cte_2" ON "surname" = "cte_2_1" AND COALESCE("limbName",'__missing__') = "cte_2_2" ` +
				`GROUP BY "surname", COALESCE("limbName",'__missing__'), COALESCE("organName",'__missing__'), cte_1_cnt, cte_2_cnt ` +
				`ORDER BY cte_1_cnt DESC, "surname", cte_2_cnt DESC, COALESCE("limbName",'__missing__'), count() DESC, COALESCE("organName",'__missing__') ` +
				`LIMIT 10 BY "surname", COALESCE("limbName",'__missing__')`,
			`WITH cte_1 AS ` +
				`(SELECT "surname" AS "cte_1_1", count() AS "cte_1_cnt" ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`GROUP BY "surname" ` +
				`ORDER BY count() DESC, "surname" ` +
				`LIMIT 100) ` +
				`SELECT "surname", COALESCE("limbName",'__missing__'), count() ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`INNER JOIN "cte_1" ON "surname" = "cte_1_1" ` +
				`GROUP BY "surname", COALESCE("limbName",'__missing__'), cte_1_cnt ` +
				`ORDER BY cte_1_cnt DESC, "surname", count() DESC, COALESCE("limbName",'__missing__') ` +
				`LIMIT 10 BY "surname"`,
			`SELECT "surname", count() ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`GROUP BY "surname" ` +
				`ORDER BY count() DESC, "surname" ` +
				`LIMIT 100`,
		},
	},
}
