// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package testdata

// FIXME I'll restore this tests very soon. I need to merge this PR + #63 first, as I need changes from both of them to do so.
var PipelineAggregationTests = []AggregationTestCase{
	{
		TestName: "Kibana 8.15, Metrics: Aggregation: Rate, invalid Unit (10)", //reason [eaggs] > reason
		QueryRequestJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"1": {
					"rate": {
						"field": "DistanceKilometers",
						"unit": "10"
					}
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
			"error": {
				"caused_by": {
					"reason": "Unsupported unit 10",
					"type": "illegal_argument_exception"
				},
				"reason": "[1:59] [rate] failed to parse field [unit]",
				"root_cause": [
					{
						"reason": "[1:59] [rate] failed to parse field [unit]",
						"type": "x_content_parse_exception"
					}
				],
				"type": "x_content_parse_exception"
			},
			"status": 400
		} (400 status code)`,
	},
	{
		TestName: "Kibana 8.15, Metrics: Aggregation: Rate, invalid Unit (abc)", //reason [eaggs] > reason
		QueryRequestJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"1": {
					"rate": {
						"field": "DistanceKilometers",
						"unit": "abc"
					}
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
			"error": {
				"caused_by": {
					"reason": "Unsupported unit 10",
					"type": "illegal_argument_exception"
				},
				"reason": "[1:59] [rate] failed to parse field [unit]",
				"root_cause": [
					{
						"reason": "[1:59] [rate] failed to parse field [unit]",
						"type": "x_content_parse_exception"
					}
				],
				"type": "x_content_parse_exception"
			},
			"status": 400
		} (400 status code)`,
	},
	{
		TestName: "Kibana 8.15, Metrics: Aggregation: Rate, valid Unit (month), but bad surrounding aggregations", //reason [eaggs] > reason
		QueryRequestJson: `
{
    "_source": {
        "excludes": []
    },
    "aggs": {
        "1": {
            "rate": {
                "field": "DistanceKilometers",
                "unit": "month"
            }
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
    "completion_time_in_millis": 1731585426907,
    "error": {
        "caused_by": {
            "caused_by": {
                "caused_by": {
                    "reason": "The rate aggregation can only be used inside a date histogram aggregation or composite aggregation with one date histogram value source",
                    "type": "illegal_argument_exception"
                },
                "reason": "The rate aggregation can only be used inside a date histogram aggregation or composite aggregation with one date histogram value source",
                "type": "illegal_argument_exception"
            },
            "failed_shards": [
                {
                    "index": "kibana_sample_data_flights",
                    "node": "SqOwBNLfS0yt1lgl8XzEdA",
                    "reason": {
                        "reason": "The rate aggregation can only be used inside a date histogram aggregation or composite aggregation with one date histogram value source",
                        "type": "illegal_argument_exception"
                    },
                    "shard": 0
                }
            ],
            "grouped": true,
            "phase": "query",
            "reason": "all shards failed",
            "type": "search_phase_execution_exception"
        },
        "reason": "error while executing search",
        "type": "status_exception"
    },
    "expiration_time_in_millis": 1731585486899,
    "id": "FnoxVjUxSnRJUnZHNmVCUHZaLTQwbXccU3FPd0JOTGZTMHl0MWxnbDhYekVkQToxMDIzMQ==",
    "is_partial": true,
    "is_running": false,
    "response": {
        "_shards": {
            "failed": 1,
            "failures": [
                {
                    "index": "kibana_sample_data_flights",
                    "node": "SqOwBNLfS0yt1lgl8XzEdA",
                    "reason": {
                        "reason": "The rate aggregation can only be used inside a date histogram aggregation or composite aggregation with one date histogram value source",
                        "type": "illegal_argument_exception"
                    },
                    "shard": 0
                }
            ],
            "skipped": 0,
            "successful": 0,
            "total": 1
        },
        "hits": {
            "hits": [],
            "max_score": null,
            "total": {
                "relation": "gte",
                "value": 0
            }
        },
        "num_reduce_phases": 0,
        "terminated_early": false,
        "timed_out": false,
        "took": 8
    },
    "start_time_in_millis": 1731585426899
} (400 status code)`,
	},
	{
		TestName: "Kibana 8.15, Metrics: Aggregation: Rate, invalid Unit (10)", //reason [eaggs] > reason
		QueryRequestJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"2": {
					"aggs": {
						"1": {
							"rate": {
								"field": "DistanceKilometers",
								"unit": "month"
							}
						}
					},
					"date_histogram": {
						"field": "timestamp",
						"fixed_interval": "30s",
						"min_doc_count": 1,
						"time_zone": "Europe/Warsaw"
					}
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
			"completion_time_in_millis": 1731585496445,
			"error": {
				"caused_by": {
					"caused_by": {
						"caused_by": {
							"reason": "Cannot use month-based rate unit [month] with fixed interval based histogram, only week, day, hour, minute and second are supported for this histogram",
							"type": "illegal_argument_exception"
						},
						"reason": "Cannot use month-based rate unit [month] with fixed interval based histogram, only week, day, hour, minute and second are supported for this histogram",
						"type": "illegal_argument_exception"
					},
					"failed_shards": [
						{
							"index": "kibana_sample_data_flights",
							"node": "SqOwBNLfS0yt1lgl8XzEdA",
							"reason": {
								"reason": "Cannot use month-based rate unit [month] with fixed interval based histogram, only week, day, hour, minute and second are supported for this histogram",
								"type": "illegal_argument_exception"
							},
							"shard": 0
						}
					],
					"grouped": true,
					"phase": "query",
					"reason": "all shards failed",
					"type": "search_phase_execution_exception"
				},
				"reason": "error while executing search",
				"type": "status_exception"
			},
			"expiration_time_in_millis": 1731585556279,
			"id": "FlU1MWhKNzZsVDh1RGhCS2xpeGFqUXccU3FPd0JOTGZTMHl0MWxnbDhYekVkQToxMTA3Ng==",
			"is_partial": true,
			"is_running": false,
			"response": {
				"_shards": {
					"failed": 1,
					"failures": [
						{
							"index": "kibana_sample_data_flights",
							"node": "SqOwBNLfS0yt1lgl8XzEdA",
							"reason": {
								"reason": "Cannot use month-based rate unit [month] with fixed interval based histogram, only week, day, hour, minute and second are supported for this histogram",
								"type": "illegal_argument_exception"
							},
							"shard": 0
						}
					],
					"skipped": 0,
					"successful": 0,
					"total": 1
				},
				"hits": {
					"hits": [],
					"max_score": null,
					"total": {
						"relation": "gte",
						"value": 0
					}
				},
				"num_reduce_phases": 0,
				"terminated_early": false,
				"timed_out": false,
				"took": 166
			},
			"start_time_in_millis": 1731585496279
		}`,
	},
}

/*
	{
		TestName: "pipeline simple count",
		QueryRequestJson: `
		{
			"aggs": {
				"q": {
					"aggs": {
						"time_buckets": {
							"aggs": {
								"count": {
									"bucket_script": {
										"buckets_path": "_count",
										"script": {
											"lang": "expression",
											"source": "_value"
										}
									}
								}
							},
							"date_histogram": {
								"extended_bounds": {
									"max": 1713978908651,
									"min": 1713978008651
								},
								"field": "@timestamp",
								"fixed_interval": "1h",
								"min_doc_count": 0,
								"time_zone": "Europe/Warsaw"
							},
							"meta": {
								"type": "time_buckets"
							}
						}
					},
					"filters": {
						"filters": {
							"*": {
								"query_string": {
									"query": "*"
								}
							}
						}
					},
					"meta": {
						"type": "split"
					}
				}
			},
			"query": {
				"bool": {
					"filter": {
						"bool": {
							"filter": [],
							"must": [],
							"must_not": [],
							"should": []
						}
					},
					"must": []
				}
			},
			"runtime_mappings": {},
			"size": 0,
			"timeout": "30000ms",
			"track_total_hits": true
		}`,
		ExpectedResponse: `
		{
			"is_partial": false,
			"is_running": false,
			"start_time_in_millis": 1713982993490,
			"expiration_time_in_millis": 1714414993490,
			"completion_time_in_millis": 1713982993639,
			"response": {
				"took": 149,
				"timed_out": false,
				"_shards": {
					"total": 1,
					"successful": 1,
					"skipped": 0,
					"failed": 0
				},
				"hits": {
					"total": {
						"value": 13014,
						"relation": "eq"
					},
					"max_score": null,
					"hits": []
				},
				"aggregations": {
					"q": {
						"meta": {
							"type": "split"
						},
						"buckets": {
							"*": {
								"doc_count": 1000,
								"time_buckets": {
									"meta": {
										"type": "time_buckets"
									},
									"buckets": [
										{
											"key_as_string": "2024-04-15T00:00:00.000",
											"key": 1713139200000,
											"doc_count": 10,
											"count": {
												"value": 10
											}
										},
										{
											"key_as_string": "2024-04-15T01:00:00.000",
											"key": 1713142800000,
											"doc_count": 0,
											"count": {
												"value": 0
											}
										},
										{
											"key_as_string": "2024-04-15T02:00:00.000",
											"key": 1713146400000,
											"doc_count": 0,
											"count": {
												"value": 0
											}
										},
										{
											"key_as_string": "2024-04-15T03:00:00.000",
											"key": 1713150000000,
											"doc_count": 9,
											"count": {
												"value": 9
											}
										}
									]
								}
							}
						}
					}
				}
			}
		}`,
		ExpectedResults: [][]model.QueryResultRow{
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol("hits", uint64(13014))}}},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("key", int64(1713139200000/1000/60/60)),
					model.NewQueryResultCol("doc_count", 10),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("key", int64(1713150000000/1000/60/60)),
					model.NewQueryResultCol("doc_count", 9),
				}},
			},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("key", int64(1713139200000/1000/60/60)),
					model.NewQueryResultCol("doc_count", 10),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("key", int64(1713150000000/1000/60/60)),
					model.NewQueryResultCol("doc_count", 9),
				}},
			},
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol("hits", uint64(1000))}}},
		},
		ExpectedSQLs: []string{
			`SELECT count() FROM ` + QuotedTableName + ` `,
			"SELECT toInt64(toUnixTimestamp64Milli(`@timestamp`)/3600000), count() " +
				"FROM " + QuotedTableName + ` WHERE "message" ILIKE '%'  ` +
				"GROUP BY (toInt64(toUnixTimestamp64Milli(`@timestamp`)/3600000)) " +
				"ORDER BY (toInt64(toUnixTimestamp64Milli(`@timestamp`)/3600000))",
			"SELECT toInt64(toUnixTimestamp64Milli(`@timestamp`)/3600000), count() " +
				"FROM " + QuotedTableName + ` WHERE "message" ILIKE '%'  ` +
				"GROUP BY (toInt64(toUnixTimestamp64Milli(`@timestamp`)/3600000)) " +
				"ORDER BY (toInt64(toUnixTimestamp64Milli(`@timestamp`)/3600000))",
			`SELECT count() FROM ` + QuotedTableName + ` WHERE "message" ILIKE '%' `,
		},
	},
	/*
		{ // [1]
			TestName: "pipeline from elasticsearch docs TODO",
			QueryRequestJson: `
			{
				"size": 0,
					"aggs": {
						"sales_per_month": {
							"date_histogram": {
								"field": "date",
								"calendar_interval": "month"
							},
							"aggs": {
								"total_sales": {
									"sum": {
										"field": "price"
									}
								},
								"t-shirts": {
									"filter": {
										"term": {
											"type": "t-shirt"
										}
									},
									"aggs": {
										"sales": {
											"sum": {
												"field": "price"
											}
										}
									}
								},
								"t-shirt-percentage": {
									"bucket_script": {
										"buckets_path": {
											"tShirtSales": "t-shirts>sales",
											"totalSales": "total_sales"
										},
									"script": "params.tShirtSales / params.totalSales * 100"
								}
							}
						}
					}
				}
			}`,
			ExpectedResponse: `
			{
				"took": 11,
				"timed_out": false,
				"_shards": ...,
				"hits": ...,
				"aggregations": {
					"sales_per_month": {
						"buckets": [
							{
								"key_as_string": "2015/01/01 00:00:00",
								"key": 1420070400000,
								"doc_count": 3,
								"total_sales": {
									"value": 550.0
								},
								"t-shirts": {
									"doc_count": 1,
									"sales": {
										"value": 200.0
									}
								},
								"t-shirt-percentage": {
									"value": 36.36363636363637
								}
							},
							{
								"key_as_string": "2015/02/01 00:00:00",
								"key": 1422748800000,
								"doc_count": 2,
								"total_sales": {
									"value": 60.0
								},
								"t-shirts": {
									"doc_count": 1,
									"sales": {
										"value": 10.0
									}
								},
								"t-shirt-percentage": {
									"value": 16.666666666666664
								}
							},
							{
								"key_as_string": "2015/03/01 00:00:00",
								"key": 1425168000000,
								"doc_count": 2,
								"total_sales": {
									"value": 375.0
								},
								"t-shirts": {
									"doc_count": 1,
									"sales": {
										"value": 175.0
									}
								},
								"t-shirt-percentage": {
									"value": 46.666666666666664
								}
							}
						]
					}
				}
			}`,
			ExpectedResults: make([][]model.QueryResultRow, 0),
			ExpectedSQLs:    make([]string, 0),
		},
}
*/
