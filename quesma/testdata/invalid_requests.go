// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package testdata

var InvalidAggregationTests = []AggregationTestCase{
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
