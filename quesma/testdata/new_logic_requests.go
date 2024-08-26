// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package testdata

import "quesma/model"

var NewLogicTestCases = []AggregationTestCase{
	{
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
										},
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
					model.NewQueryResultCol("cnt_1", 1036),
					model.NewQueryResultCol("metric_2_1", 1091661.7608666667),
					model.NewQueryResultCol(`COALESCE("limbName",'__missing__')`, "b11"),
					model.NewQueryResultCol("cnt_2", 21),
					model.NewQueryResultCol("metric_2_8_1", 51891.94613333333),
					model.NewQueryResultCol("organName", "c11"),
					model.NewQueryResultCol("cnt_3", 21),
					model.NewQueryResultCol("dense_rank_1", 1),
					model.NewQueryResultCol("dense_rank_2", 1),
					model.NewQueryResultCol("dense_rank_3", 1),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a1"),
					model.NewQueryResultCol("cnt_1", 1036),
					model.NewQueryResultCol("metric_2_1", 1091661.7608666667),
					model.NewQueryResultCol(`COALESCE("limbName",'__missing__')`, "b12"),
					model.NewQueryResultCol("cnt_2", 24),
					model.NewQueryResultCol("metric_2_8_1", 45774.291766666654),
					model.NewQueryResultCol("organName", "c12"),
					model.NewQueryResultCol("cnt_3", 24),
					model.NewQueryResultCol("dense_rank_1", 1),
					model.NewQueryResultCol("dense_rank_2", 2),
					model.NewQueryResultCol("dense_rank_3", 1),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a2"),
					model.NewQueryResultCol("cnt_1", 34),
					model.NewQueryResultCol("metric_2_1", 630270.07765),
					model.NewQueryResultCol(`COALESCE("limbName",'__missing__')`, "b21"),
					model.NewQueryResultCol("cnt_2", 17),
					model.NewQueryResultCol("metric_2_8_1", 399126.7496833334),
					model.NewQueryResultCol("organName", "c21"),
					model.NewQueryResultCol("cnt_3", 17),
					model.NewQueryResultCol("dense_rank_1", 2),
					model.NewQueryResultCol("dense_rank_2", 1),
					model.NewQueryResultCol("dense_rank_3", 1),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a2"),
					model.NewQueryResultCol("cnt_1", 34),
					model.NewQueryResultCol("metric_2_1", 1091661.7608666667),
					model.NewQueryResultCol(`COALESCE("limbName",'__missing__')`, "b22"),
					model.NewQueryResultCol("cnt_2", 17),
					model.NewQueryResultCol("metric_2_8_1", 231143.3279666666),
					model.NewQueryResultCol("organName", "c22"),
					model.NewQueryResultCol("cnt_3", 17),
					model.NewQueryResultCol("dense_rank_1", 2),
					model.NewQueryResultCol("dense_rank_2", 2),
					model.NewQueryResultCol("dense_rank_3", 1),
				}},
			},
		},
		ExpectedSQLs: []string{
			`WITH cte AS ` +
				`(SELECT "surname", sum(count()) OVER (PARTITION BY "surname"), ` +
				`avgOrNull(avgOrNull_total) OVER (PARTITION BY "surname") AS "metric_2_1", ` +
				`avgOrNull("total") AS "avgOrNull_total", ` +
				`COALESCE("limbName",'__missing__'), sum(count()) OVER (PARTITION BY "surname", COALESCE("limbName",'__missing__')), ` +
				`sumOrNull(sumOrNull_total) OVER (PARTITION BY "surname", COALESCE("limbName",'__missing__')) AS "metric_2_8_1", ` +
				`sumOrNull("total") AS "sumOrNull_total", ` +
				`"organName", sum(count()) OVER (PARTITION BY "surname", COALESCE("limbName",'__missing__'), "organName") ` +
				`FROM ` + TableName + ` ` +
				`GROUP BY "surname", COALESCE("limbName",'__missing__'), "organName") ` +
				`SELECT "surname", sum(count()) OVER (PARTITION BY "surname"), "metric_2_1", ` +
				`COALESCE("limbName",'__missing__'), sum(count()) OVER (PARTITION BY "surname", COALESCE("limbName",'__missing__')), ` +
				`"metric_2_8_1", ` +
				`"organName", sum(count()) OVER (PARTITION BY "surname", COALESCE("limbName",'__missing__'), "organName"), ` +
				`DENSE_RANK() OVER (ORDER BY metric_2_1 DESC, "surname") AS dense_rank_1, ` +
				`DENSE_RANK() OVER (PARTITION BY "surname" ORDER BY metric_2_8_1, COALESCE("limbName",'__missing__')) AS dense_rank_2, ` +
				`DENSE_RANK() OVER (PARTITION BY "surname", COALESCE("limbName",'__missing__') ORDER BY "organName" DESC) AS dense_rank_3 ` +
				`FROM cte ` +
				`QUALIFY dense_rank_1 <= 200 AND dense_rank_2 <= 20 AND dense_rank_3 <= 1 ` +
				`ORDER BY dense_rank_1, dense_rank_2, dense_rank_3`,
		},
	},
}
