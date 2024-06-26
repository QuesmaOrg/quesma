// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package kunkka

import (
	"quesma/model"
	"quesma/testdata"
)

var KunkkaTests = []testdata.AggregationTestCase{
	{ // [0]
		TestName: "clients/kunkka/test_0, used to be broken before aggregations merge fix" +
			"Output more or less works, but is different and worse than what Elastic returns." +
			"If it starts failing, maybe that's a good thing",
		QueryRequestJson: `
		{
			"aggs": {
				"0": {
					"date_histogram": {
						"field": "@timestamp",
						"calendar_interval": "1h",
						"time_zone": "Europe/Warsaw"
					},
					"aggs": {
						"1": {
							"sum": {
								"field": "spent"
							}
						},
						"2-bucket": {
							"filter": {
								"bool": {
									"must": [],
									"filter": [
										{
											"multi_match": {
												"type": "best_fields",
												"query": "started",
												"lenient": true
											}
										}
									],
									"should": [],
									"must_not": []
								}
							},
							"aggs": {
								"2-metric": {
									"sum": {
										"field": "multiplier"
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
					"field": "timestamp",
					"format": "date_time"
				}
			],
			"script_fields": {},
			"stored_fields": [
				"*"
			],
			"runtime_mappings": {
				"hour_utc": {
					"type": "double",
					"script": {
						"source": "emit(doc['@timestamp'].value.hour)"
					}
				}
			},
			"_source": {
				"excludes": []
			},
		}`,
		ExpectedResponse: `
		{
			"completion_time_in_millis": 1718983683782,
			"expiration_time_in_millis": 1719415683775,
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
									"value": 6.600000023841858
								},
								"doc_count": 2,
								"key": 1718794800000,
								"key_as_string": "2024-06-19T11:00:00.000"
							},
							{
								"1": {
									"value": 12.100000143051147
								},
								"doc_count": 3,
								"key": 1718798400000,
								"key_as_string": "2024-06-19T12:00:00.000"
							},
							{
								"1": {
									"value": 4.399999976158142
								},
								"2-bucket": {
									"2-metric": {
										"value": 1.0
									},
									"doc_count": 1
								},
								"doc_count": 2,
								"key": 1718802000000,
								"key_as_string": "2024-06-19T13:00:00.000"
							}
						]
					}
				},
				"hits": {
					"hits": [],
					"max_score": null,
					"total": {
						"relation": "eq",
						"value": 37
					}
				},
				"timed_out": false,
				"took": 7
			},
			"start_time_in_millis": 1718983683775
		}`,
		ExpectedResults: [][]model.QueryResultRow{
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol("value", uint64(37))}}},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol(`toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000)`, int64(1718794800000/3600000)),
					model.NewQueryResultCol(`sumOrNull("spent")`, 6.600000023841858),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol(`toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000)`, int64(1718798400000/3600000)),
					model.NewQueryResultCol(`sumOrNull("spent")`, 12.100000143051147),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol(`toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000)`, int64(1718802000000/3600000)),
					model.NewQueryResultCol(`sumOrNull("spent")`, 4.399999976158142),
				}},
			},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol(`toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000)`, int64(1718802000000/3600000)),
					model.NewQueryResultCol(`sumOrNull("spent")`, 1.0),
				}},
			},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol(`toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000)`, int64(1718802000000/3600000)),
					model.NewQueryResultCol(`count()`, 1),
				}},
			},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol(`toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000)`, int64(1718794800000/3600000)),
					model.NewQueryResultCol(`count()`, 2),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol(`toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000)`, int64(1718798400000/3600000)),
					model.NewQueryResultCol(`count()`, 3),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol(`toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000)`, int64(1718802000000/3600000)),
					model.NewQueryResultCol(`count()`, 2),
				}},
			},
		},
		ExpectedSQLs: []string{
			`SELECT count() FROM (SELECT 1 FROM ` + testdata.QuotedTableName + ` LIMIT 10000)`,
			`SELECT toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000), sumOrNull("spent") ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`GROUP BY toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000) ` +
				`ORDER BY toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000)`,
			`SELECT toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000), sumOrNull("multiplier") ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`WHERE "message" iLIKE '%started%' ` +
				`GROUP BY toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000) ` +
				`ORDER BY toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000)`,
			`SELECT toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000), count() ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`WHERE "message" iLIKE '%started%' ` +
				`GROUP BY toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000) ` +
				`ORDER BY toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000)`,
			`SELECT toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000), count() ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`GROUP BY toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000) ` +
				`ORDER BY toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000)`,
		},
	},
	{ // [0]
		TestName: "it's the same input as in previous test, but with the original output from Elastic." +
			"Skipped for now, as our response is different in 2 things: key_as_string date (probably not important) + we don't return 0's (e.g. doc_count: 0)." +
			"If we need clients/kunkka/test_0, used to be broken before aggregations merge fix",
		QueryRequestJson: `
		{
			"aggs": {
				"0": {
					"date_histogram": {
						"field": "@timestamp",
						"calendar_interval": "1h",
						"time_zone": "Europe/Warsaw"
					},
					"aggs": {
						"1": {
							"sum": {
								"field": "spent"
							}
						},
						"2-bucket": {
							"filter": {
								"bool": {
									"must": [],
									"filter": [
										{
											"multi_match": {
												"type": "best_fields",
												"query": "started",
												"lenient": true
											}
										}
									],
									"should": [],
									"must_not": []
								}
							},
							"aggs": {
								"2-metric": {
									"sum": {
										"field": "multiplier"
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
					"field": "timestamp",
					"format": "date_time"
				}
			],
			"script_fields": {},
			"stored_fields": [
				"*"
			],
			"runtime_mappings": {
				"hour_utc": {
					"type": "double",
					"script": {
						"source": "emit(doc['@timestamp'].value.hour)"
					}
				}
			},
			"_source": {
				"excludes": []
			},
		}`,
		ExpectedResponse: `
		{
			"completion_time_in_millis": 1718983683782,
			"expiration_time_in_millis": 1719415683775,
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
									"value": 6.600000023841858
								},
								"2-bucket": {
									"2-metric": {
										"value": 0.0
									},
									"doc_count": 0
								},
								"doc_count": 2,
								"key": 1718794800000,
								"key_as_string": "2024/06/19 13:00:00"
							},
							{
								"1": {
									"value": 12.100000143051147
								},
								"2-bucket": {
									"2-metric": {
										"value": 0.0
									},
									"doc_count": 0
								},
								"doc_count": 3,
								"key": 1718798400000,
								"key_as_string": "2024/06/19 14:00:00"
							},
							{
								"1": {
									"value": 4.399999976158142
								},
								"2-bucket": {
									"2-metric": {
										"value": 1.0
									},
									"doc_count": 1
								},
								"doc_count": 2,
								"key": 1718802000000,
								"key_as_string": "2024/06/19 15:00:00"
							}
						]
					}
				},
				"hits": {
					"hits": [],
					"max_score": null,
					"total": {
						"relation": "eq",
						"value": 37
					}
				},
				"timed_out": false,
				"took": 7
			},
			"start_time_in_millis": 1718983683775
		}`,
		ExpectedResults: [][]model.QueryResultRow{
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol("value", uint64(37))}}},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol(`toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000)`, int64(1718794800000/3600000)),
					model.NewQueryResultCol(`sumOrNull("spent")`, 6.600000023841858),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol(`toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000)`, int64(1718798400000/3600000)),
					model.NewQueryResultCol(`sumOrNull("spent")`, 12.100000143051147),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol(`toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000)`, int64(1718802000000/3600000)),
					model.NewQueryResultCol(`sumOrNull("spent")`, 4.399999976158142),
				}},
			},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol(`toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000)`, int64(1718802000000/3600000)),
					model.NewQueryResultCol(`sumOrNull("spent")`, 1.0),
				}},
			},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol(`toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000)`, int64(1718802000000/3600000)),
					model.NewQueryResultCol(`count()`, 1),
				}},
			},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol(`toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000)`, int64(1718794800000/3600000)),
					model.NewQueryResultCol(`count()`, 2),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol(`toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000)`, int64(1718798400000/3600000)),
					model.NewQueryResultCol(`count()`, 3),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol(`toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000)`, int64(1718802000000/3600000)),
					model.NewQueryResultCol(`count()`, 2),
				}},
			},
		},
		ExpectedSQLs: []string{
			`SELECT count() FROM (SELECT 1 FROM ` + testdata.QuotedTableName + ` LIMIT 10000)`,
			`SELECT toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000), sumOrNull("spent") ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`GROUP BY toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000) ` +
				`ORDER BY toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000)`,
			`SELECT toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000), sumOrNull("multiplier") ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`WHERE "message" iLIKE '%started%' ` +
				`GROUP BY toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000) ` +
				`ORDER BY toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000)`,
			`SELECT toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000), count() ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`WHERE "message" iLIKE '%started%' ` +
				`GROUP BY toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000) ` +
				`ORDER BY toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000)`,
			`SELECT toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000), count() ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`GROUP BY toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000) ` +
				`ORDER BY toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000)`,
		},
	},
	{
		TestName: "clients/kunkka/test_1, used to be broken before aggregations merge fix",
		QueryRequestJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"time_offset_split": {
					"aggs": {
						"0": {
							"aggs": {
								"1": {
									"sum": {
										"field": "earned"
									}
								},
								"2-bucket": {
									"aggs": {
										"2-metric": {
											"sum": {
												"field": "multiplier"
											}
										}
									},
									"filter": {
										"bool": {
											"filter": [
												{
													"multi_match": {
														"lenient": true,
														"query": "abc",
														"type": "best_fields"
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
									"aggs": {
										"3-metric": {
											"sum": {
												"field": "multiplier"
											}
										}
									},
									"filter": {
										"bool": {
											"filter": [
												{
													"multi_match": {
														"lenient": true,
														"query": "bcd",
														"type": "best_fields"
													}
												}
											],
											"must": [],
											"must_not": [],
											"should": []
										}
									}
								},
								"4-bucket": {
									"aggs": {
										"4-metric": {
											"sum": {
												"field": "multiplier"
											}
										}
									},
									"filter": {
										"bool": {
											"filter": [
												{
													"multi_match": {
														"lenient": true,
														"query": "cde",
														"type": "best_fields"
													}
												}
											],
											"must": [],
											"must_not": [],
											"should": []
										}
									}
								},
								"5-bucket": {
									"aggs": {
										"5-metric": {
											"sum": {
												"field": "multiplier"
											}
										}
									},
									"filter": {
										"bool": {
											"filter": [
												{
													"multi_match": {
														"lenient": true,
														"query": "abc",
														"type": "best_fields"
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
								"calendar_interval": "1h",
								"field": "@timestamp",
								"time_zone": "Europe/Warsaw"
							}
						}
					},
					"filters": {
						"filters": {
							"0": {
								"range": {
									"@timestamp": {
										"format": "strict_date_optional_time",
										"gte": "2024-06-02T17:16:16.749Z",
										"lte": "2024-06-21T21:59:59.999Z"
									}
								}
							},
							"86400000": {
								"range": {
									"@timestamp": {
										"format": "strict_date_optional_time",
										"gte": "2024-06-01T17:16:16.749Z",
										"lte": "2024-06-20T21:59:59.999Z"
									}
								}
							}
						}
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
				},
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
								"isOK": false
							}
						}
					],
					"must": [],
					"must_not": [],
					"should": []
				}
			},
			"runtime_mappings": {
				"hour_utc": {
					"script": {
						"source": "emit(doc['@timestamp'].value.hour)"
					},
					"type": "double"
				}
			},
			"script_fields": {},
			"size": 0,
			"stored_fields": [
				"*"
			],
			"track_total_hits": true
		}`,
		ExpectedResponse: `{
			"completion_time_in_millis": 1718989977680,
			"expiration_time_in_millis": 1718990317146,
			"id": "FkNRVjlieDBsUlRDMnhKdVV1TzJiMVEccFhLWWYwaThRUmFXNWRFWmY1b0tPZzozMzU5NQ==",
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
								"0": {
									"buckets": [
										{
											"1": {
												"value": 4.400000095367432
											},
											"2-bucket": {
												"2-metric": {
													"value": 2.0
												},
												"doc_count": 1
											},
											"3-bucket": {
												"3-metric": {
													"value": 0.0
												},
												"doc_count": 0
											},
											"4-bucket": {
												"4-metric": {
													"value": 0.0
												},
												"doc_count": 0
											},
											"5-bucket": {
												"5-metric": {
													"value": 2.0
												},
												"doc_count": 1
											},
											"doc_count": 3,
											"key": 1718409600000,
											"key_as_string": "2024/06/15 02:00:00"
										},
										{
											"1": {
												"value": 4.400000095367432
											},
											"2-bucket": {
												"2-metric": {
													"value": 0.0
												},
												"doc_count": 0
											},
											"3-bucket": {
												"3-metric": {
													"value": 0.0
												},
												"doc_count": 0
											},
											"4-bucket": {
												"4-metric": {
													"value": 0.0
												},
												"doc_count": 0
											},
											"5-bucket": {
												"5-metric": {
													"value": 0.0
												},
												"doc_count": 0
											},
											"doc_count": 2,
											"key": 1718413200000,
											"key_as_string": "2024/06/15 03:00:00"
										},
										{
											"1": {
												"value": 15.400000095367432
											},
											"2-bucket": {
												"2-metric": {
													"value": 3.0
												},
												"doc_count": 1
											},
											"3-bucket": {
												"3-metric": {
													"value": 1.0
												},
												"doc_count": 1
											},
											"4-bucket": {
												"4-metric": {
													"value": 1.0
												},
												"doc_count": 1
											},
											"5-bucket": {
												"5-metric": {
													"value": 3.0
												},
												"doc_count": 1
											},
											"doc_count": 6,
											"key": 1718496000000,
											"key_as_string": "2024/06/16 02:00:00"
										},
										{
											"1": {
												"value": 6.6000001430511475
											},
											"2-bucket": {
												"2-metric": {
													"value": 0.0
												},
												"doc_count": 0
											},
											"3-bucket": {
												"3-metric": {
													"value": 0.0
												},
												"doc_count": 0
											},
											"4-bucket": {
												"4-metric": {
													"value": 0.0
												},
												"doc_count": 0
											},
											"5-bucket": {
												"5-metric": {
													"value": 0.0
												},
												"doc_count": 0
											},
											"doc_count": 2,
											"key": 1718503200000,
											"key_as_string": "2024/06/16 04:00:00"
										},
										{
											"1": {
												"value": 0.0
											},
											"2-bucket": {
												"2-metric": {
													"value": 0.0
												},
												"doc_count": 0
											},
											"3-bucket": {
												"3-metric": {
													"value": 0.0
												},
												"doc_count": 0
											},
											"4-bucket": {
												"4-metric": {
													"value": 0.0
												},
												"doc_count": 0
											},
											"5-bucket": {
												"5-metric": {
													"value": 0.0
												},
												"doc_count": 0
											},
											"doc_count": 1,
											"key": 1718578800000,
											"key_as_string": "2024/06/17 01:00:00"
										}
									]
								},
								"doc_count": 181
							},
							"86400000": {
								"0": {
									"buckets": [
										{
											"1": {
												"value": 15.400000095367432
											},
											"2-bucket": {
												"2-metric": {
													"value": 3.0
												},
												"doc_count": 1
											},
											"3-bucket": {
												"3-metric": {
													"value": 0.0
												},
												"doc_count": 0
											},
											"4-bucket": {
												"4-metric": {
													"value": 1.0
												},
												"doc_count": 1
											},
											"5-bucket": {
												"5-metric": {
													"value": 3.0
												},
												"doc_count": 1
											},
											"doc_count": 6,
											"key": 1718496000000,
											"key_as_string": "2024/06/16 02:00:00"
										},
										{
											"1": {
												"value": 5.5
											},
											"key_as_string": "2024/06/16 15:00:00",
											"key": 1718542800000,
											"doc_count": 3,
											"5-bucket": {
												"doc_count": 0,
												"5-metric": {
													"value": 0
												}
											},
											"2-bucket": {
												"doc_count": 3,
												"2-metric": {
													"value": 9
												}
											},
											"3-bucket": {
												"doc_count": 0,
												"3-metric": {
													"value": 0.0
												}
											},
											"4-bucket": {
												"doc_count": 3,
												"4-metric": {
													"value": 9
												}
											}
										}
									]
								},
								"doc_count": 181
							}
						}
					}
				},
				"hits": {
					"hits": [],
					"max_score": null,
					"total": {
						"relation": "eq",
						"value": 181
					}
				},
				"timed_out": false,
				"took": 9
			},
			"start_time_in_millis": 1718989977671
		}`,
		ExpectedResults: [][]model.QueryResultRow{
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol("value", uint64(181))}}},
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol("count()", uint64(181))}}},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol(`toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000)`, int64(1718409600000/3600000)),
					model.NewQueryResultCol(`count()`, 3),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol(`toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000)`, int64(1718413200000/3600000)),
					model.NewQueryResultCol(`count()`, 2),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol(`toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000)`, int64(1718496000000/3600000)),
					model.NewQueryResultCol(`count()`, 6),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol(`toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000)`, int64(1718503200000/3600000)),
					model.NewQueryResultCol(`count()`, 2),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol(`toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000)`, int64(1718578800000/3600000)),
					model.NewQueryResultCol(`count()`, 1),
				}},
			},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol(`toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000)`, int64(1718409600000/3600000)),
					model.NewQueryResultCol(`sumOrNull("earned")`, 4.400000095367432),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol(`toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000)`, int64(1718413200000/3600000)),
					model.NewQueryResultCol(`sumOrNull("earned")`, 4.400000095367432),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol(`toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000)`, int64(1718496000000/3600000)),
					model.NewQueryResultCol(`sumOrNull("earned")`, 15.400000095367432),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol(`toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000)`, int64(1718503200000/3600000)),
					model.NewQueryResultCol(`sumOrNull("earned")`, 6.6000001430511475),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol(`toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000)`, int64(1718578800000/3600000)),
					model.NewQueryResultCol(`sumOrNull("earned")`, 0.0),
				}},
			},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol(`toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000)`, int64(1718409600000/3600000)),
					model.NewQueryResultCol(`count()`, 1),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol(`toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000)`, int64(1718496000000/3600000)),
					model.NewQueryResultCol(`count()`, 1),
				}},
			},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol(`toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000)`, int64(1718409600000/3600000)),
					model.NewQueryResultCol(`sumOrNull("multiplier")`, 2.0),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol(`toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000)`, int64(1718496000000/3600000)),
					model.NewQueryResultCol(`sumOrNull("multiplier")`, 3.0),
				}},
			},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol(`toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000)`, int64(1718496000000/3600000)),
					model.NewQueryResultCol(`count()`, 1),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol(`toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000)`, int64(1718496000000/3600000)),
					model.NewQueryResultCol(`count()`, 1),
				}},
			},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol(`toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000)`, int64(1718496000000/3600000)),
					model.NewQueryResultCol(`sumOrNull("multiplier")`, 1.0),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol(`toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000)`, int64(1718496000000/3600000)),
					model.NewQueryResultCol(`sumOrNull("multiplier")`, 1.0),
				}},
			},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol(`toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000)`, int64(1718496000000/3600000)),
					model.NewQueryResultCol(`count()`, 1),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol(`toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000)`, int64(1718496000000/3600000)),
					model.NewQueryResultCol(`count()`, 1),
				}},
			},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol(`toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000)`, int64(1718496000000/3600000)),
					model.NewQueryResultCol(`sumOrNull("multiplier")`, 1.0),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol(`toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000)`, int64(1718496000000/3600000)),
					model.NewQueryResultCol(`sumOrNull("multiplier")`, 1.0),
				}},
			},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol(`toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000)`, int64(1718409600000/3600000)),
					model.NewQueryResultCol(`count()`, 1),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol(`toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000)`, int64(1718496000000/3600000)),
					model.NewQueryResultCol(`count()`, 1),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol(`toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000)`, int64(1718496000000/3600000)),
					model.NewQueryResultCol(`count()`, 1),
				}},
			},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol(`toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000)`, int64(1718409600000/3600000)),
					model.NewQueryResultCol(`sumOrNull("multiplier")`, 2.0),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol(`toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000)`, int64(1718496000000/3600000)),
					model.NewQueryResultCol(`sumOrNull("multiplier")`, 3.0),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol(`toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000)`, int64(1718496000000/3600000)),
					model.NewQueryResultCol(`sumOrNull("multiplier")`, 3.0),
				}},
			},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol(`toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000)`, int64(1718409600000/3600000)),
					model.NewQueryResultCol(`count()`, 6),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol(`toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000)`, int64(1718542800000/3600000)),
					model.NewQueryResultCol(`count()`, 3),
				}},
			},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol(`toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000)`, int64(1718409600000/3600000)),
					model.NewQueryResultCol(`sumOrNull("earned")`, 15.400000095367432),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol(`toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000)`, int64(1718542800000/3600000)),
					model.NewQueryResultCol(`sumOrNull("earned")`, 5.5),
				}},
			},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol(`toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000)`, int64(1718409600000/3600000)),
					model.NewQueryResultCol(`count()`, 1),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol(`toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000)`, int64(1718542800000/3600000)),
					model.NewQueryResultCol(`count()`, 3),
				}},
			},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol(`toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000)`, int64(1718409600000/3600000)),
					model.NewQueryResultCol(`sumOrNull("multiplier")`, 3.0),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol(`toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000)`, int64(1718542800000/3600000)),
					model.NewQueryResultCol(`sumOrNull("multiplier")`, 9.0),
				}},
			},
			{},
			{},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol(`toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000)`, int64(1718409600000/3600000)),
					model.NewQueryResultCol(`count()`, 1),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol(`toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000)`, int64(1718542800000/3600000)),
					model.NewQueryResultCol(`count()`, 3),
				}},
			},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol(`toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000)`, int64(1718409600000/3600000)),
					model.NewQueryResultCol(`sumOrNull("multiplier")`, 1.0),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol(`toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000)`, int64(1718542800000/3600000)),
					model.NewQueryResultCol(`sumOrNull("multiplier")`, 9.0),
				}},
			},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol(`toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000)`, int64(1718409600000/3600000)),
					model.NewQueryResultCol(`count()`, 1),
				}},
			},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol(`toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000)`, int64(1718409600000/3600000)),
					model.NewQueryResultCol(`sumOrNull("multiplier")`, 3.0),
				}},
			},
			{},
		},
		ExpectedSQLs: []string{
			`SELECT count() ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`WHERE "isOK"==false`,
			`SELECT count() ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`WHERE ("isOK"==false AND ("@timestamp">=parseDateTime64BestEffort('2024-06-02T17:16:16.749Z') ` +
				`AND "@timestamp"<=parseDateTime64BestEffort('2024-06-21T21:59:59.999Z')))`,
			`SELECT toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000), count() ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`WHERE ("isOK"==false AND ("@timestamp">=parseDateTime64BestEffort('2024-06-02T17:16:16.749Z') AND ` +
				`"@timestamp"<=parseDateTime64BestEffort('2024-06-21T21:59:59.999Z'))) ` +
				`GROUP BY toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000) ` +
				`ORDER BY toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000)`,
			`SELECT toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000), sumOrNull("earned") ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`WHERE ("isOK"==false AND ("@timestamp">=parseDateTime64BestEffort('2024-06-02T17:16:16.749Z') AND ` +
				`"@timestamp"<=parseDateTime64BestEffort('2024-06-21T21:59:59.999Z'))) ` +
				`GROUP BY toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000) ` +
				`ORDER BY toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000)`,
			`SELECT toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000), count() ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`WHERE (("isOK"==false AND ("@timestamp">=parseDateTime64BestEffort('2024-06-02T17:16:16.749Z') AND ` +
				`"@timestamp"<=parseDateTime64BestEffort('2024-06-21T21:59:59.999Z'))) ` +
				`AND "message" iLIKE '%abc%') ` +
				`GROUP BY toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000) ` +
				`ORDER BY toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000)`,
			`SELECT toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000), sumOrNull("multiplier") ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`WHERE (("isOK"==false AND ("@timestamp">=parseDateTime64BestEffort('2024-06-02T17:16:16.749Z') AND ` +
				`"@timestamp"<=parseDateTime64BestEffort('2024-06-21T21:59:59.999Z'))) ` +
				`AND "message" iLIKE '%abc%') ` +
				`GROUP BY toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000) ` +
				`ORDER BY toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000)`,
			`SELECT toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000), count() ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`WHERE (("isOK"==false AND ("@timestamp">=parseDateTime64BestEffort('2024-06-02T17:16:16.749Z') AND ` +
				`"@timestamp"<=parseDateTime64BestEffort('2024-06-21T21:59:59.999Z'))) ` +
				`AND "message" iLIKE '%bcd%') ` +
				`GROUP BY toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000) ` +
				`ORDER BY toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000)`,
			`SELECT toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000), sumOrNull("multiplier") ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`WHERE (("isOK"==false AND ("@timestamp">=parseDateTime64BestEffort('2024-06-02T17:16:16.749Z') AND ` +
				`"@timestamp"<=parseDateTime64BestEffort('2024-06-21T21:59:59.999Z'))) ` +
				`AND "message" iLIKE '%bcd%') ` +
				`GROUP BY toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000) ` +
				`ORDER BY toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000)`,
			`SELECT toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000), count() ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`WHERE (("isOK"==false AND ("@timestamp">=parseDateTime64BestEffort('2024-06-02T17:16:16.749Z') AND ` +
				`"@timestamp"<=parseDateTime64BestEffort('2024-06-21T21:59:59.999Z'))) ` +
				`AND "message" iLIKE '%cde%') ` +
				`GROUP BY toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000) ` +
				`ORDER BY toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000)`,
			`SELECT toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000), sumOrNull("multiplier") ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`WHERE (("isOK"==false AND ("@timestamp">=parseDateTime64BestEffort('2024-06-02T17:16:16.749Z') AND ` +
				`"@timestamp"<=parseDateTime64BestEffort('2024-06-21T21:59:59.999Z'))) ` +
				`AND "message" iLIKE '%cde%') ` +
				`GROUP BY toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000) ` +
				`ORDER BY toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000)`,
			`SELECT toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000), count() ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`WHERE (("isOK"==false AND ("@timestamp">=parseDateTime64BestEffort('2024-06-02T17:16:16.749Z') AND ` +
				`"@timestamp"<=parseDateTime64BestEffort('2024-06-21T21:59:59.999Z'))) ` +
				`AND "message" iLIKE '%abc%') ` +
				`GROUP BY toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000) ` +
				`ORDER BY toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000)`,
			`SELECT toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000), sumOrNull("multiplier") ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`WHERE (("isOK"==false AND ("@timestamp">=parseDateTime64BestEffort('2024-06-02T17:16:16.749Z') AND ` +
				`"@timestamp"<=parseDateTime64BestEffort('2024-06-21T21:59:59.999Z'))) ` +
				`AND "message" iLIKE '%abc%') ` +
				`GROUP BY toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000) ` +
				`ORDER BY toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000)`,
			`SELECT count() ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`WHERE ("isOK"==false AND ("@timestamp">=parseDateTime64BestEffort('2024-06-01T17:16:16.749Z') AND ` +
				`"@timestamp"<=parseDateTime64BestEffort('2024-06-20T21:59:59.999Z')))`,
			`SELECT toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000), count() ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`WHERE ("isOK"==false AND ("@timestamp">=parseDateTime64BestEffort('2024-06-01T17:16:16.749Z') AND ` +
				`"@timestamp"<=parseDateTime64BestEffort('2024-06-20T21:59:59.999Z'))) ` +
				`GROUP BY toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000) ` +
				`ORDER BY toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000)`,
			`SELECT toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000), sumOrNull("earned") ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`WHERE ("isOK"==false AND ("@timestamp">=parseDateTime64BestEffort('2024-06-01T17:16:16.749Z') AND ` +
				`"@timestamp"<=parseDateTime64BestEffort('2024-06-20T21:59:59.999Z'))) ` +
				`GROUP BY toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000) ` +
				`ORDER BY toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000)`,
			`SELECT toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000), count() ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`WHERE (("isOK"==false AND ("@timestamp">=parseDateTime64BestEffort('2024-06-01T17:16:16.749Z') AND ` +
				`"@timestamp"<=parseDateTime64BestEffort('2024-06-20T21:59:59.999Z'))) ` +
				`AND "message" iLIKE '%abc%') ` +
				`GROUP BY toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000) ` +
				`ORDER BY toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000)`,
			`SELECT toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000), sumOrNull("multiplier") ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`WHERE (("isOK"==false AND ("@timestamp">=parseDateTime64BestEffort('2024-06-01T17:16:16.749Z') AND ` +
				`"@timestamp"<=parseDateTime64BestEffort('2024-06-20T21:59:59.999Z'))) ` +
				`AND "message" iLIKE '%abc%') ` +
				`GROUP BY toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000) ` +
				`ORDER BY toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000)`,
			`SELECT toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000), count() ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`WHERE (("isOK"==false AND ("@timestamp">=parseDateTime64BestEffort('2024-06-01T17:16:16.749Z') AND ` +
				`"@timestamp"<=parseDateTime64BestEffort('2024-06-20T21:59:59.999Z'))) ` +
				`AND "message" iLIKE '%bcd%') ` +
				`GROUP BY toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000) ` +
				`ORDER BY toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000)`,
			`SELECT toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000), sumOrNull("multiplier") ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`WHERE (("isOK"==false AND ("@timestamp">=parseDateTime64BestEffort('2024-06-01T17:16:16.749Z') AND ` +
				`"@timestamp"<=parseDateTime64BestEffort('2024-06-20T21:59:59.999Z'))) ` +
				`AND "message" iLIKE '%bcd%') ` +
				`GROUP BY toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000) ` +
				`ORDER BY toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000)`,
			`SELECT toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000), count() ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`WHERE (("isOK"==false AND ("@timestamp">=parseDateTime64BestEffort('2024-06-01T17:16:16.749Z') AND ` +
				`"@timestamp"<=parseDateTime64BestEffort('2024-06-20T21:59:59.999Z'))) ` +
				`AND "message" iLIKE '%cde%') ` +
				`GROUP BY toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000) ` +
				`ORDER BY toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000)`,
			`SELECT toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000), sumOrNull("multiplier") ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`WHERE (("isOK"==false AND ("@timestamp">=parseDateTime64BestEffort('2024-06-01T17:16:16.749Z') AND ` +
				`"@timestamp"<=parseDateTime64BestEffort('2024-06-20T21:59:59.999Z'))) ` +
				`AND "message" iLIKE '%cde%') ` +
				`GROUP BY toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000) ` +
				`ORDER BY toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000)`,
			`SELECT toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000), count() ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`WHERE (("isOK"==false AND ("@timestamp">=parseDateTime64BestEffort('2024-06-01T17:16:16.749Z') AND ` +
				`"@timestamp"<=parseDateTime64BestEffort('2024-06-20T21:59:59.999Z'))) ` +
				`AND "message" iLIKE '%abc%') ` +
				`GROUP BY toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000) ` +
				`ORDER BY toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000)`,
			`SELECT toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000), sumOrNull("multiplier") ` +
				`FROM ` + testdata.QuotedTableName + ` ` +
				`WHERE (("isOK"==false AND ("@timestamp">=parseDateTime64BestEffort('2024-06-01T17:16:16.749Z') AND ` +
				`"@timestamp"<=parseDateTime64BestEffort('2024-06-20T21:59:59.999Z'))) ` +
				`AND "message" iLIKE '%abc%') ` +
				`GROUP BY toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000) ` +
				`ORDER BY toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000)`,
		},
	},
}
