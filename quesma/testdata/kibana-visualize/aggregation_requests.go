package kibana_visualize

import (
	"mitmproxy/quesma/model"
	"mitmproxy/quesma/testdata"
)

var AggregationTests = []testdata.AggregationTestCase{
	{ // [0]
		TestName: "Multi_terms without subaggregations. Visualize: Bar Vertical: Horizontal Axis: Date Histogram, Vertical Axis: Count of records, Breakdown: Top values (2 values)",
		QueryRequestJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"0": {
					"aggs": {
						"1": {
							"multi_terms": {
								"order": {
									"_count": "desc"
								},
								"size": 3,
								"terms": [
									{
										"field": "severity"
									},
									{
										"field": "source"
									}
								]
							}
						}
					},
					"date_histogram": {
						"extended_bounds": {
							"max": 1716812096627,
							"min": 1716811196627
						},
						"field": "@timestamp",
						"fixed_interval": "30s",
						"time_zone": "Europe/Warsaw"
					}
				}
			},
			"fields": [
				{
					"field": "@timestamp",
					"format": "date_time"
				}
			],
			"query": {
				"bool": {
					"filter": [
						{
							"range": {
								"@timestamp": {
									"format": "strict_date_optional_time",
									"gte": "2024-05-27T11:59:56.627Z",
									"lte": "2024-05-27T12:14:56.627Z"
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
		// I erased empty date_histogram buckets, we don't support extended_bounds yet
		ExpectedResponse: `
		{
			"completion_time_in_millis": 1716834974737,
			"expiration_time_in_millis": 1716835034732,
			"id": "FnFPQm5xWDFEU2gtVlBOZnBkX3RNeFEcRVZINklxc1VTQ2lhVEtwMnpmZjNEZzoyNDM3OQ==",
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
									"buckets": [
										{
											"doc_count": 1,
											"key": [
												"artemis",
												"error"
											],
											"key_as_string": "artemis|error"
										},
										{
											"doc_count": 1,
											"key": [
												"artemis",
												"info"
											],
											"key_as_string": "artemis|info"
										},
										{
											"doc_count": 1,
											"key": [
												"jupiter",
												"info"
											],
											"key_as_string": "jupiter|info"
										}
									],
									"doc_count_error_upper_bound": 0,
									"sum_other_doc_count": 1
								},
								"doc_count": 4,
								"key": 1716834210000,
								"key_as_string": "2024-05-27T20:23:30.000+02:00"
							},
							{
								"1": {
									"buckets": [
										{
											"doc_count": 2,
											"key": [
												"apollo",
												"info"
											],
											"key_as_string": "apollo|info"
										},
										{
											"doc_count": 1,
											"key": [
												"cassandra",
												"debug"
											],
											"key_as_string": "cassandra|debug"
										}
									],
									"doc_count_error_upper_bound": 0,
									"sum_other_doc_count": 12
								},
								"doc_count": 16,
								"key": 1716834270000,
								"key_as_string": "2024-05-27T20:24:30.000+02:00"
							}
						]
					}
				},
				"hits": {
					"hits": [],
					"max_score": null,
					"total": {
						"relation": "eq",
						"value": 378
					}
				},
				"timed_out": false,
				"took": 5
			},
			"start_time_in_millis": 1716834974732
		}`,
		ExpectedResults: [][]model.QueryResultRow{},
		ExpectedSQLs:    []string{},
	},
	{ // [1]
		TestName: "Multi_terms with simple count. Visualize: Bar Vertical: Horizontal Axis: Top values (2 values), Vertical: Count of records, Breakdown: @timestamp",
		QueryRequestJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"0": {
					"aggs": {
						"1": {
							"date_histogram": {
								"extended_bounds": {
									"max": 1716812073493,
									"min": 1716811173493
								},
								"field": "@timestamp",
								"fixed_interval": "30s",
								"time_zone": "Europe/Warsaw"
							}
						}
					},
					"multi_terms": {
						"order": {
							"_count": "desc"
						},
						"size": 3,
						"terms": [
							{
								"field": "message"
							},
							{
								"field": "host.name"
							}
						]
					}
				}
			},
			"fields": [
				{
					"field": "@timestamp",
					"format": "date_time"
				}
			],
			"query": {
				"bool": {
					"filter": [
						{
							"range": {
								"@timestamp": {
									"format": "strict_date_optional_time",
									"gte": "2024-05-27T11:59:33.493Z",
									"lte": "2024-05-27T12:14:33.493Z"
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
		// I erased empty date_histogram buckets, we don't support extended_bounds yet
		ExpectedResponse: `
		{
			"completion_time_in_millis": 1716834668794,
			"expiration_time_in_millis": 1716834728791,
			"id": "FkpjTm9UNHhVUUNlY3Z5cVNfTk5Db3ccRVZINklxc1VTQ2lhVEtwMnpmZjNEZzoxNjMxMA==",
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
									"buckets": [
										{
											"doc_count": 1,
											"key": 1716834420000,
											"key_as_string": "2024-05-27T20:27:00.000+02:00"
										},
										{
											"doc_count": 1,
											"key": 1716834450000,
											"key_as_string": "2024-05-27T20:27:30.000+02:00"
										},
										{
											"doc_count": 2,
											"key": 1716834510000,
											"key_as_string": "2024-05-27T20:28:30.000+02:00"
										}
									]
								},
								"doc_count": 13,
								"key": [
									"info",
									"redhat"
								],
								"key_as_string": "info|redhat"
							}
						],
						"doc_count_error_upper_bound": 0,
						"sum_other_doc_count": 188
					}
				},
				"hits": {
					"hits": [],
					"max_score": null,
					"total": {
						"relation": "eq",
						"value": 217
					}
				},
				"timed_out": false,
				"took": 3
			},
			"start_time_in_millis": 1716834668791
		}`,
		ExpectedResults: [][]model.QueryResultRow{},
		ExpectedSQLs:    []string{``},
	},
	{ //[2],
		TestName: "Multi_terms with double-nested subaggregations. Visualize: Bar Vertical: Horizontal Axis: Top values (2 values), Vertical: Unique count, Breakdown: @timestamp",
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
									"cardinality": {
										"field": "severity"
									}
								}
							},
							"date_histogram": {
								"extended_bounds": {
									"max": 1716834478178,
									"min": 1716833578178
								},
								"field": "@timestamp",
								"fixed_interval": "30s",
								"time_zone": "Europe/Warsaw"
							}
						},
						"2": {
							"cardinality": {
								"field": "severity"
							}
						}
					},
					"multi_terms": {
						"order": {
							"2": "desc"
						},
						"size": 3,
						"terms": [
							{
								"field": "severity"
							},
							{
								"field": "source"
							}
						]
					}
				}
			},
			"fields": [
				{
					"field": "@timestamp",
					"format": "date_time"
				}
			],
			"query": {
				"bool": {
					"filter": [],
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
		// I erased empty date_histogram buckets, we don't support extended_bounds yet
		ExpectedResponse: `
		{
			"completion_time_in_millis": 1716834482828,
			"expiration_time_in_millis": 1716834542815,
			"id": "FlhQOUVMZDhSU1V1azdxbW9rREE2a2ccRVZINklxc1VTQ2lhVEtwMnpmZjNEZzoxMTUwNA==",
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
									"buckets": [
										{
											"2": {
												"value": 1
											},
											"doc_count": 1,
											"key": 1716834300000,
											"key_as_string": "2024-05-27T20:25:00.000+02:00"
										},
										{
											"2": {
												"value": 1
											},
											"doc_count": 1,
											"key": 1716834390000,
											"key_as_string": "2024-05-27T20:26:30.000+02:00"
										}
									]
								},
								"2": {
									"value": 1
								},
								"doc_count": 2,
								"key": [
									"critical",
									"alpine"
								],
								"key_as_string": "critical|alpine"
							},
							{
								"1": {
									"buckets": [
										{
											"2": {
												"value": 1
											},
											"doc_count": 1,
											"key": 1716834270000,
											"key_as_string": "2024-05-27T20:24:30.000+02:00"
										}
									]
								},
								"2": {
									"value": 1
								},
								"doc_count": 1,
								"key": [
									"critical",
									"fedora"
								],
								"key_as_string": "critical|fedora"
							}
						],
						"doc_count_error_upper_bound": -1,
						"sum_other_doc_count": 121
					}
				},
				"hits": {
					"hits": [],
					"max_score": null,
					"total": {
						"relation": "eq",
						"value": 126
					}
				},
				"timed_out": false,
				"took": 13
			},
			"start_time_in_millis": 1716834482815
		}`,
		ExpectedResults: [][]model.QueryResultRow{},
		ExpectedSQLs:    []string{""},
	},
}
