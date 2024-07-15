// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package clients

import (
	"quesma/model"
	"quesma/testdata"
)

var OpheliaTests = []testdata.AggregationTestCase{
	{ // [0]
		TestName: "Ophelia Test 1: triple terms",
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
									"sum_other_doc_count": 504
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
		ExpectedSQLs: []string{
			`WITH cte_1 AS ` +
				`(SELECT "surname" AS "cte_1_1", count() AS "cte_1_cnt" ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`GROUP BY "surname" ` +
				`ORDER BY count() DESC, "surname" ` +
				`LIMIT 200), cte_2 AS ` +
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
	},
	{ // [1]
		TestName: "Ophelia Test 2: triple terms + other aggregations + order by another aggregations",
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
}
