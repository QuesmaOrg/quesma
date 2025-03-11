// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package testdata

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
	},
}
