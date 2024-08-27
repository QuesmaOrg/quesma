// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package testdata

import (
	"quesma/clickhouse"
	"quesma/model"
	"time"
)

var TestsAsyncSearch = []AsyncSearchTestCase{
	{ // [0]
		"Facets: aggregate by field + additionally match user (filter)",
		`{
    "aggs": {
        "sample": {
            "aggs": {
                "sample_count": {
                    "value_count": {
                        "field": "host.name"
                    }
                },
                "top_values": {
                    "terms": {
                        "field": "host.name",
                        "shard_size": 25,
                        "size": 10
                    }
                }
            },
            "sampler": {
                "shard_size": 5000
            }
        }
    },
    "query": {
        "bool": {
            "filter": [
                {
                    "range": {
                        "@timestamp": {
                            "format": "strict_date_optional_time",
                            "gte": "2024-01-23T11:27:16.820Z",
                            "lte": "2024-01-23T11:42:16.820Z"
                        }
                    }
                },
                {
                    "bool": {
                        "filter": [
                            {
                                "multi_match": {
                                    "lenient": true,
                                    "query": "user",
                                    "type": "best_fields"
                                }
                            }
                        ],
                        "must": [],
                        "must_not": [],
                        "should": []
                    }
                }
            ]
        }
    },
    "runtime_mappings": {},
    "size": 0,
    "track_total_hits": false
}`,
		`{
    "completion_time_in_millis": 1706010201967,
    "expiration_time_in_millis": 1706010261964,
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
            "sample": {
                "doc_count": 442,
                "sample_count": {
                    "value": 442
                },
                "top_values": {
                    "buckets": [
                        {
                            "doc_count": 30,
                            "key": "hephaestus"
                        },
                        {
                            "doc_count": 29,
                            "key": "poseidon"
                        },
                        {
                            "doc_count": 28,
                            "key": "jupiter"
                        },
                        {
                            "doc_count": 26,
                            "key": "selen"
                        },
                        {
                            "doc_count": 24,
                            "key": "demeter"
                        },
                        {
                            "doc_count": 24,
                            "key": "iris"
                        },
                        {
                            "doc_count": 24,
                            "key": "pan"
                        },
                        {
                            "doc_count": 22,
                            "key": "hades"
                        },
                        {
                            "doc_count": 22,
                            "key": "hermes"
                        },
                        {
                            "doc_count": 21,
                            "key": "persephone"
                        }
                    ],
                    "doc_count_error_upper_bound": 0,
                    "sum_other_doc_count": 192
                }
            }
        },
        "hits": {
            "hits": [],
            "max_score": null,
            "total": {
                "relation": "eq",
                "value": 442
            }
        },
        "timed_out": false,
        "took": 3
    },
    "start_time_in_millis": 1706010201964
}`,
		"no comment yet",
		model.SearchQueryInfo{Typ: model.Facets, FieldName: "host.name", I1: 10, I2: 5000},
		[]string{`SELECT "host.name" AS "key", count() AS "doc_count" FROM (SELECT "host.name" FROM ` + TableName + `  WHERE (("@timestamp".=parseDateTime64BestEffort('2024-01-23T11:27:16.820Z') AND "@timestamp".=parseDateTime64BestEffort('2024-01-23T11:42:16.820Z')) AND "message" iLIKE '%user%') LIMIT ` + queryparserFacetsSampleSize + `) GROUP BY "host.name" ORDER BY count() DESC`},
		true,
	},
	{ // [1]
		"ListByField: query one field, last 'size' results, return list of just that field, no timestamp, etc.",
		`{
    "_source": false,
    "fields": [
        {
            "field": "message"
        }
    ],
	"sort": [
		{
			"@timestamp": {
				"format": "strict_date_optional_time",
				"order": "desc",
				"unmapped_type": "boolean"
			}
		}
	],
    "query": {
        "bool": {
            "filter": [
                {
                    "range": {
                        "@timestamp": {
                            "format": "strict_date_optional_time",
                            "gte": "2024-01-23T14:43:19.481Z",
                            "lte": "2024-01-23T14:58:19.481Z"
                        }
                    }
                },
                {
                    "bool": {
                        "filter": [
                            {
                                "multi_match": {
                                    "lenient": true,
                                    "query": "user",
                                    "type": "best_fields"
                                }
                            }
                        ],
                        "must": [],
                        "must_not": [],
                        "should": []
                    }
                },
                {
                    "exists": {
                        "field": "message"
                    }
                }
            ]
        }
    },
    "runtime_mappings": {},
    "size": 100,
    "track_total_hits": true
}
`,
		`{
    "completion_time_in_millis": 1706021975541,
    "expiration_time_in_millis": 1706022035538,
    "is_partial": false,
    "is_running": false,
    "response": {
        "_shards": {
            "failed": 0,
            "skipped": 0,
            "successful": 1,
            "total": 1
        },
        "hits": {
            "hits": [
                {
                    "_id": "DuLTNo0Bxs2mnbSyknVe",
                    "_index": ".ds-logs-generic-default-2024.01.23-000001",
                    "_score": 0.0,
                    "fields": {
                        "message": [
                            "User logged in"
                        ]
                    }
                },
                {
                    "_id": "B-LTNo0Bxs2mnbSydXWB",
                    "_index": ".ds-logs-generic-default-2024.01.23-000001",
                    "_score": 0.0,
                    "fields": {
                        "message": [
                            "User password reset requested"
                        ]
                    }
                },
                {
                    "_id": "CeLTNo0Bxs2mnbSyfnWi",
                    "_index": ".ds-logs-generic-default-2024.01.23-000001",
                    "_score": 0.0,
                    "fields": {
                        "message": [
                            "User password reset requested"
                        ]
                    }
                },
                {
                    "_id": "C-LTNo0Bxs2mnbSyiXUd",
                    "_index": ".ds-logs-generic-default-2024.01.23-000001",
                    "_score": 0.0,
                    "fields": {
                        "message": [
                            "User logged out"
                        ]
                    }
                },
                {
                    "_id": "DeLTNo0Bxs2mnbSyjXV7",
                    "_index": ".ds-logs-generic-default-2024.01.23-000001",
                    "_score": 0.0,
                    "fields": {
                        "message": [
                            "User password changed"
                        ]
                    }
                }
            ],
            "max_score": 0.0,
            "total": {
                "relation": "eq",
                "value": 97
            }
        },
        "timed_out": false,
        "took": 3
    },
    "start_time_in_millis": 1706021975538
}
`, "there should be 97 results, I truncated most of them",
		model.SearchQueryInfo{Typ: model.ListByField, RequestedFields: []string{"message"}, FieldName: "message", I1: 0, I2: 100},
		[]string{
			`SELECT count() FROM ` + TableName + ` WHERE ((("@timestamp">=parseDateTime64BestEffort('2024-01-23T14:43:19.481Z') ` +
				`AND "@timestamp"<=parseDateTime64BestEffort('2024-01-23T14:58:19.481Z')) AND "message" iLIKE '%user%') AND "message" IS NOT NULL)`,
			`SELECT "message" FROM ` + TableName + ` WHERE ((("@timestamp".=parseDateTime64BestEffort('2024-01-23T14:..:19.481Z') ` +
				`AND "@timestamp".=parseDateTime64BestEffort('2024-01-23T14:..:19.481Z')) AND "message" iLIKE '%user%') ` +
				`AND "message" IS NOT NULL) ` +
				`ORDER BY "@timestamp" DESC ` +
				`LIMIT 100`},
		false,
	},
	{ // [2]
		"ListAllFields: search all fields, return JSON + count (we don't return count atm)",
		`{
    "_source": false,
    "fields": [
        {
            "field": "*",
            "include_unmapped": "true"
        },
        {
            "field": "@timestamp",
            "format": "strict_date_optional_time"
        }
    ],
    "highlight": {
        "fields": {
            "*": {}
        },
        "fragment_size": 2147483647,
        "post_tags": [
            "@/kibana-highlighted-field@"
        ],
        "pre_tags": [
            "@kibana-highlighted-field@"
        ]
    },
    "query": {
        "bool": {
            "filter": [
                {
                    "multi_match": {
                        "lenient": true,
                        "query": "user",
                        "type": "best_fields"
                    }
                },
                {
                    "range": {
                        "@timestamp": {
                            "format": "strict_date_optional_time",
                            "gte": "2024-01-23T14:43:19.481Z",
                            "lte": "2024-01-23T14:58:19.481Z"
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
    "size": 500,
    "sort": [
        {
            "@timestamp": {
                "format": "strict_date_optional_time",
                "order": "desc",
                "unmapped_type": "boolean"
            }
        },
        {
            "_doc": {
                "order": "desc",
                "unmapped_type": "boolean"
            }
        }
    ],
    "stored_fields": [
        "*"
    ],
    "track_total_hits": false,
    "version": true
}
`,
		`{
    "completion_time_in_millis": 1706021899599,
    "expiration_time_in_millis": 1706021959593,
    "id": "FlpxWGNLeG9rVF82aTJEYXItU1BGVncdNVFvOUloYTBUZ3U0Q25MRTJtQTA0dzoyMTEyNjk=",
    "is_partial": false,
    "is_running": false,
    "response": {
        "_shards": {
            "failed": 0,
            "skipped": 0,
            "successful": 1,
            "total": 1
        },
        "hits": {
            "hits": [
                {
                    "_id": "PeLUNo0Bxs2mnbSyS3Wk",
                    "_index": ".ds-logs-generic-default-2024.01.23-000001",
                    "_score": null,
                    "_version": 1,
                    "fields": {
                        "@timestamp": [
                            "2024-01-23T14:58:19.172Z"
                        ],
                        "data_stream.type": [
                            "logs"
                        ],
                        "host.name": [
                            "athena"
                        ],
                        "host.name.text": [
                            "athena"
                        ],
                        "message": [
                            "User password changed"
                        ],
                        "service.name": [
                            "backend"
                        ],
                        "service.name.text": [
                            "backend"
                        ],
                        "severity": [
                            "error"
                        ],
                        "source": [
                            "alpine"
                        ],
                        "timestamp": [
                            "2024-01-23T14:58:19.168Z"
                        ]
                    },
                    "highlight": {
                        "message": [
                            "@kibana-highlighted-field@User@/kibana-highlighted-field@ password changed"
                        ]
                    },
                    "sort": [
                        "2024-01-23T14:58:19.172Z",
                        0
                    ]
                },
                {
                    "_id": "OuLUNo0Bxs2mnbSyRXX_",
                    "_index": ".ds-logs-generic-default-2024.01.23-000001",
                    "_score": null,
                    "_version": 1,
                    "fields": {
                        "@timestamp": [
                            "2024-01-23T14:58:17.726Z"
                        ],
                        "data_stream.type": [
                            "logs"
                        ],
                        "host.name": [
                            "apollo"
                        ],
                        "host.name.text": [
                            "apollo"
                        ],
                        "message": [
                            "User password reset failed"
                        ],
                        "service.name": [
                            "worker"
                        ],
                        "service.name.text": [
                            "worker"
                        ],
                        "severity": [
                            "info"
                        ],
                        "source": [
                            "debian"
                        ],
                        "timestamp": [
                            "2024-01-23T14:58:17.714Z"
                        ]
                    },
                    "highlight": {
                        "message": [
                            "@kibana-highlighted-field@User@/kibana-highlighted-field@ password reset failed"
                        ]
                    },
                    "sort": [
                        "2024-01-23T14:58:17.726Z",
                        1
                    ]
                },
                {
                    "_id": "OeLUNo0Bxs2mnbSyP3Xl",
                    "_index": ".ds-logs-generic-default-2024.01.23-000001",
                    "_score": null,
                    "_version": 1,
                    "fields": {
                        "@timestamp": [
                            "2024-01-23T14:58:16.165Z"
                        ],
                        "data_stream.type": [
                            "logs"
                        ],
                        "host.name": [
                            "hestia"
                        ],
                        "host.name.text": [
                            "hestia"
                        ],
                        "message": [
                            "User logged out"
                        ],
                        "service.name": [
                            "cron"
                        ],
                        "service.name.text": [
                            "cron"
                        ],
                        "severity": [
                            "info"
                        ],
                        "source": [
                            "suse"
                        ],
                        "timestamp": [
                            "2024-01-23T14:58:16.154Z"
                        ]
                    },
                    "highlight": {
                        "message": [
                            "@kibana-highlighted-field@User@/kibana-highlighted-field@ logged out"
                        ]
                    },
                    "sort": [
                        "2024-01-23T14:58:16.165Z",
                        2
                    ]
                }
			]
		}
	}`,
		"Truncated most results. TODO Check what's at the end of response, probably count?",
		model.SearchQueryInfo{Typ: model.ListAllFields, RequestedFields: []string{"*"}, FieldName: "*", I1: 0, I2: 500},
		[]string{`SELECT ` + selectFieldsInAnyOrderAsRegex([]string{"@timestamp", "message", "host.name", "properties::isreg"}) + ` FROM ` + TableName + ` WHERE "message" iLIKE '%user%' AND ("@timestamp".=parseDateTime64BestEffort('2024-01-23T14:..:19.481Z') AND "@timestamp".=parseDateTime64BestEffort('2024-01-23T14:..:19.481Z')) ORDER BY "@timestamp" desc LIMIT 500`},
		false,
	},
	{ // [3]
		"Histogram: possible query nr 1",
		`{
    "_source": {
        "excludes": []
    },
    "aggs": {
        "0": {
            "date_histogram": {
                "field": "@timestamp",
                "fixed_interval": "30s",
                "min_doc_count": 1,
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
                    "multi_match": {
                        "lenient": true,
                        "query": "user",
                        "type": "best_fields"
                    }
                },
                {
                    "range": {
                        "@timestamp": {
                            "format": "strict_date_optional_time",
                            "gte": "2024-01-23T14:43:19.481Z",
                            "lte": "2024-01-23T14:58:19.481Z"
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
    "size": 100,
    "stored_fields": [
        "*"
    ],
    "track_total_hits": 1000
}
`, `{
    "completion_time_in_millis": 1706021899595,
    "expiration_time_in_millis": 1706021959594,
    "id": "FjFQMlBUNnJmUU1pWml0WkllNmJWYXcdNVFvOUloYTBUZ3U0Q25MRTJtQTA0dzoyMTEyNzI=",
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
                        "doc_count": 2,
                        "key": 1706021670000,
                        "key_as_string": "2024-01-23T15:54:30.000+01:00"
                    },
                    {
                        "doc_count": 13,
                        "key": 1706021700000,
                        "key_as_string": "2024-01-23T15:55:00.000+01:00"
                    },
                    {
                        "doc_count": 14,
                        "key": 1706021730000,
                        "key_as_string": "2024-01-23T15:55:30.000+01:00"
                    },
                    {
                        "doc_count": 14,
                        "key": 1706021760000,
                        "key_as_string": "2024-01-23T15:56:00.000+01:00"
                    },
                    {
                        "doc_count": 15,
                        "key": 1706021790000,
                        "key_as_string": "2024-01-23T15:56:30.000+01:00"
                    },
                    {
                        "doc_count": 13,
                        "key": 1706021820000,
                        "key_as_string": "2024-01-23T15:57:00.000+01:00"
                    },
                    {
                        "doc_count": 15,
                        "key": 1706021850000,
                        "key_as_string": "2024-01-23T15:57:30.000+01:00"
                    },
                    {
                        "doc_count": 11,
                        "key": 1706021880000,
                        "key_as_string": "2024-01-23T15:58:00.000+01:00"
                    }
                ]
            }
        },
        "hits": {
            "hits": [],
            "max_score": null,
            "total": {
                "relation": "eq",
                "value": 97
            }
        },
        "timed_out": false,
        "took": 1
    },
    "start_time_in_millis": 1706021899594
}
`,
		"no comment yet",
		model.SearchQueryInfo{Typ: model.ListByField, RequestedFields: []string{"@timestamp"}, FieldName: "@timestamp", I2: 100},
		[]string{
			`SELECT count() FROM (SELECT 1 ` +
				`FROM ` + TableName + ` ` +
				`WHERE ("message" iLIKE '%user%' AND ("@timestamp">=parseDateTime64BestEffort('2024-01-23T14:43:19.481Z') ` +
				`AND "@timestamp"<=parseDateTime64BestEffort('2024-01-23T14:58:19.481Z'))) ` +
				`LIMIT 1000)`,
			`SELECT toInt64(toUnixTimestamp64Milli("@timestamp") / 30000), count() ` +
				`FROM ` + TableName + ` ` +
				`WHERE ("message" iLIKE '%user%' ` +
				`AND ("@timestamp".=parseDateTime64BestEffort('2024-01-23T14:..:19.481Z') ` +
				`AND "@timestamp".=parseDateTime64BestEffort('2024-01-23T14:..:19.481Z'))) ` +
				`GROUP BY toInt64(toUnixTimestamp64Milli("@timestamp") / 30000) ` +
				`ORDER BY toInt64(toUnixTimestamp64Milli("@timestamp") / 30000)`,
			`SELECT "@timestamp" ` +
				`FROM ` + TableName + ` ` +
				`WHERE ("message" iLIKE '%user%' ` +
				`AND ("@timestamp">=parseDateTime64BestEffort('2024-01-23T14:43:19.481Z') ` +
				`AND "@timestamp"<=parseDateTime64BestEffort('2024-01-23T14:58:19.481Z'))) ` +
				`LIMIT 100`,
		},
		true,
	},
	{ // [4]
		"Histogram: possible query nr 2",
		`{
	"size":0,
	"query": {
		"range": {
			"@timestamp": {
				"gt": "2024-01-25T14:53:59.033Z",
				"lte": "2024-01-25T15:08:59.033Z",
				"format": "strict_date_optional_time"
			}
		}
	},
	"aggs": {
		"stats": {
			"terms": {
				"field": "event.dataset",
				"size": 4,
				"missing": "unknown"
			},
			"aggs": {
				"series": {
					"date_histogram": {
						"field": "@timestamp",
						"fixed_interval": "60s"
					}
				}
			}
		}
	},
	"track_total_hits":false
}`,
		`{}`,
		"no comment yet",
		model.SearchQueryInfo{Typ: model.Normal},
		[]string{
			`WITH cte_1 AS ` +
				`(SELECT COALESCE("event.dataset",'unknown') AS "cte_1_1", count() AS "cte_1_cnt" ` +
				`FROM ` + TableName + ` ` +
				`WHERE ("@timestamp">parseDateTime64BestEffort('2024-01-25T14:53:59.033Z') ` +
				`AND "@timestamp"<=parseDateTime64BestEffort('2024-01-25T15:08:59.033Z')) ` +
				`GROUP BY COALESCE("event.dataset",'unknown') ` +
				`ORDER BY count() DESC, COALESCE("event.dataset",'unknown') ` +
				`LIMIT 4) ` +
				`SELECT COALESCE("event.dataset",'unknown'), toInt64(toUnixTimestamp64Milli("@timestamp") / 60000), count() ` +
				`FROM ` + TableName + ` ` +
				`INNER JOIN "cte_1" ON COALESCE("event.dataset",'unknown') = "cte_1_1" ` +
				`WHERE ("@timestamp">parseDateTime64BestEffort('2024-01-25T14:53:59.033Z') ` +
				`AND "@timestamp"<=parseDateTime64BestEffort('2024-01-25T15:08:59.033Z')) ` +
				`GROUP BY COALESCE("event.dataset",'unknown'), toInt64(toUnixTimestamp64Milli("@timestamp") / 60000), cte_1_cnt ` +
				`ORDER BY cte_1_cnt DESC, COALESCE("event.dataset",'unknown'), toInt64(toUnixTimestamp64Milli("@timestamp") / 60000)`,
			`SELECT COALESCE("event.dataset",'unknown'), count() FROM ` + TableName + ` ` +
				`WHERE ("@timestamp".*parseDateTime64BestEffort('2024-01-25T1.:..:59.033Z') ` +
				`AND "@timestamp".*parseDateTime64BestEffort('2024-01-25T1.:..:59.033Z')) ` +
				`GROUP BY COALESCE("event.dataset",'unknown') ` +
				`ORDER BY count() DESC, COALESCE("event.dataset",'unknown') ` +
				`LIMIT 4`,
		},
		true,
	},
	{ // [5]
		"Earliest/latest timestamp",
		`{
			"aggs": {
				"earliest_timestamp": {
					"min": {
						"field": "@timestamp"
					}
				},
				"latest_timestamp": {
					"max": {
						"field": "@timestamp"
					}
				}
			},
			"query": {
				"bool": {
					"filter": [
						{
							"multi_match": {
								"lenient": true,
								"query": "posei",
								"type": "best_fields"
							}
						},
						{
							"match_phrase": {
								"message": "User logged out"
							}
						},
						{
							"match_phrase": {
								"host.name": "poseidon"
							}
						}
					],
					"must": [],
					"must_not": [],
					"should": []
				}
			},
			"size": 0,
			"track_total_hits": 1
		}`,
		`{
			"completion_time_in_millis": 1706551812667,
			"expiration_time_in_millis": 1706551872665,
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
					"earliest_timestamp": {
						"value": null
					},
					"latest_timestamp": {
						"value": null
					}
				},
				"hits": {
					"hits": [],
					"max_score": null,
					"total": {
						"relation": "eq",
						"value": 0
					}
				},
				"timed_out": false,
				"took": 2
			},
			"start_time_in_millis": 1706551812665
		}`,
		"no comment yet",
		model.SearchQueryInfo{Typ: model.Normal},
		[]string{
			`SELECT count() FROM (SELECT 1 FROM ` + TableName + ` ` +
				`WHERE (("message" iLIKE '%posei%' AND "message" iLIKE '%User logged out%') AND "host.name" iLIKE '%poseidon%') LIMIT 1)`,
			`SELECT m..OrNull("@timestamp") FROM ` + TableName + ` WHERE (("message" iLIKE '%posei%' AND "message" iLIKE '%User logged out%') AND "host.name" iLIKE '%poseidon%')`,
			`SELECT m..OrNull("@timestamp") FROM ` + TableName + ` WHERE (("message" iLIKE '%posei%' AND "message" iLIKE '%User logged out%') AND "host.name" iLIKE '%poseidon%')`,
		},
		true,
	},
	{ // [6]
		"VERY simple ListAllFields",
		`{
			"fields": [
				"*"
			],
			"size": 50,
			"track_total_hits": false
		}`,
		``,
		"no comment yet",
		model.SearchQueryInfo{Typ: model.ListAllFields, RequestedFields: []string{"*"}, FieldName: "*", I1: 0, I2: 50},
		[]string{`SELECT ` + selectFieldsInAnyOrderAsRegex([]string{"@timestamp", "message", "host.name", "properties::isreg"}) + ` FROM ` + TableName + ` LIMIT 50`},
		false,
	},
	{ // [7]
		"Timestamp in epoch_millis + select one field",
		`{
			"_source": false,
			"fields": [
				"properties::isreg"
			],
			"query": {
				"bool": {
					"filter": [
						{
							"range": {
								"epoch_time": {
									"format": "epoch_millis",
									"gte": 1710171234276,
									"lte": 1710172134276
								}
							}
						},
						{
							"bool": {
								"filter": [
									{
										"range": {
											"epoch_time": {
												"format": "epoch_millis",
												"gte": 1710171234276,
												"lte": 1710172134276
											}
										}
									},
									{
										"bool": {
											"filter": [],
											"must": [
												{
													"match_all": {}
												}
											],
											"must_not": []
										}
									}
								]
							}
						},
						{
							"exists": {
								"field": "properties::isreg"
							}
						}
					]
				}
			},
			"size": 100,
			"track_total_hits": false
		}`,
		``,
		"happens e.g. in Explorer > Field Statistics view",
		model.SearchQueryInfo{Typ: model.ListByField, RequestedFields: []string{"properties::isreg"}, FieldName: "properties::isreg", I1: 0, I2: 100},
		[]string{
			`SELECT "properties::isreg" ` +
				`FROM ` + TableName + ` ` +
				`WHERE (((toUnixTimestamp64Milli("epoch_time").=1.71017.......e.12 ` +
				`AND toUnixTimestamp64Milli("epoch_time").=1.71017.......e.12) ` +
				`AND (toUnixTimestamp64Milli("epoch_time").=1.71017.......e.12 ` +
				`AND toUnixTimestamp64Milli("epoch_time").=1.71017.......e.12)) ` +
				`AND "properties::isreg" IS NOT NULL) LIMIT 100`,
		},
		false,
	},
}

var TestsSearch = []SearchTestCase{
	{ // [0]
		"Match all",
		`
		{
			"query": {
				"match_all": {}
			},
			"track_total_hits": false
		}`,
		[]string{""},
		model.ListAllFields,
		////[]model.Query{newSimplestQuery()},
		[]string{
			`SELECT "message" FROM ` + TableName + ` LIMIT 10`,
		},
	},
	{ // [1]
		"Term as dictionary",
		`
		{
			"query": {
				"bool": {
					"filter":
					{
						"term": {
							"type": "task"
						}
					}
				}
			},
			"track_total_hits": true
		}`,
		[]string{`"type"='task'`},
		model.ListAllFields,
		////[]model.Query{justSimplestWhere(`"type"='task'`)},
		[]string{
			`SELECT "message" FROM ` + TableName + ` WHERE "type"='task' LIMIT 10`,
			`SELECT count() FROM ` + TableName,
		},
	},
	{ // [2]
		"Term as array",
		`
		{
			"query": {
				"bool": {
					"filter": [
						{
							"term": {
								"type": "task"
							}
						},
						{
							"terms": {
								"task.enabled": [true, 54]
							}
						}
					]
				}
			},
			"track_total_hits": true
		}`,
		[]string{`("type"='task' AND "task.enabled" IN (true,54))`},
		model.ListAllFields,
		//[]model.Query{
		//	justSimplestWhere(`("type"='task' AND "task.enabled" IN (true,54))`),
		//},
		[]string{
			`SELECT "message" FROM ` + TableName + ` WHERE ("type"='task' AND "task.enabled" IN (true,54)) LIMIT 10`,
			`SELECT count() FROM ` + TableName,
		},
	},
	{ // [3]
		"Sample log query",
		`
		{
			"query": {
				"bool": {
					"must": [],
					"filter": [
					{
						"multi_match": {
							"type": "best_fields",
							"query": "user",
							"lenient": true
						}
					},
					{
						"range": {
							"@timestamp": {
								"format": "strict_date_optional_time",
								"gte": "2024-01-17T10:28:18.815Z",
								"lte": "2024-01-17T10:43:18.815Z"
							}
						}
					}],
					"should": [],
					"must_not": []
				}
			},
			"track_total_hits": true
		}`,
		[]string{
			`(` + fullTextFieldName + ` iLIKE '%user%' AND ("@timestamp">=parseDateTime64BestEffort('2024-01-17T10:28:18.815Z') AND "@timestamp"<=parseDateTime64BestEffort('2024-01-17T10:43:18.815Z')))`,
		},
		model.ListAllFields,
		//[]model.Query{
		//	justSimplestWhere(`("message" iLIKE '%user%' AND ("@timestamp">=parseDateTime64BestEffort('2024-01-17T10:28:18.815Z') AND "@timestamp"<=parseDateTime64BestEffort('2024-01-17T10:43:18.815Z')))`),
		//},
		[]string{
			`SELECT "message" FROM ` + TableName + ` WHERE ("message" iLIKE '%user%' ` +
				`AND ("@timestamp".=parseDateTime64BestEffort('2024-01-17T10:..:18.815Z') ` +
				`AND "@timestamp".=parseDateTime64BestEffort('2024-01-17T10:..:18.815Z'))) ` +
				`LIMIT 10`,
			`SELECT count() FROM ` + TableName,
		},
	},
	{ // [4]
		"Multiple bool query",
		`
		{
			"query": {
				"bool" : {
					"must" : {
						"term" : { "user.id" : "kimchy" }
					},
					"filter": {
						"term" : { "tags" : "production" }
					},
					"must_not" : {
						"range" : {
							"age" : { "gte" : 10, "lte" : 20 }
						}
					},
					"should" : [
						{ "term" : { "tags" : "env1" } },
						{ "term" : { "tags" : "deployed" } }
					],
					"minimum_should_match" : 1,
					"boost" : 1.0
				}
			},
			"track_total_hits": true
		}`,
		[]string{
			`((("user.id"='kimchy' AND "tags"='production') AND ("tags"='env1' OR "tags"='deployed')) AND NOT (("age">=10 AND "age"<=20)))`,
		},
		model.ListAllFields,
		//[]model.Query{
		//	justSimplestWhere(`((("user.id"='kimchy' AND "tags"='production') AND ("tags"='env1' OR "tags"='deployed')) AND NOT (("age">=10 AND "age"<=20)))`),
		//	justSimplestWhere(`((("user.id"='kimchy' AND "tags"='production') AND ("tags"='env1' OR "tags"='deployed')) AND NOT (("age">=10 AND "age"<=20)))`),
		//},
		[]string{
			`SELECT "message" FROM ` + TableName + ` WHERE ((("user.id"='kimchy' AND "tags"='production') ` +
				`AND ("tags"='env1' OR "tags"='deployed')) AND NOT (("age".=.0 AND "age".=.0))) ` +
				`LIMIT 10`,
			`SELECT count() FROM ` + TableName + ` ` +
				`WHERE ((("user.id"='kimchy' AND "tags"='production') ` +
				`AND ("tags"='env1' OR "tags"='deployed')) AND NOT (("age".=.0 AND "age".=.0)))`,
		},
	},
	{ // [5]
		"Match phrase",
		`
		{
			"query": {
				"bool": {
					"filter": [
						{
							"bool": {
								"must": [],
								"filter": [
									{
										"match_phrase": {
											"host_name.keyword": "prometheus"
										}
									}
								],
								"should": [],
								"must_not": []
							}
						}
					]
				}
			},
			"track_total_hits": false
		}`,
		[]string{`"host_name" iLIKE '%prometheus%'`},
		model.ListAllFields,
		////[]model.Query{justSimplestWhere(`"host_name" iLIKE '%prometheus%'`)},
		[]string{`SELECT "message" FROM ` + TableName + ` WHERE "host_name" iLIKE '%prometheus%' LIMIT 10`},
	},
	{ // [6]
		"Match",
		`
		{
			"query": {
				"match": {
					"message": "this is a test"
				}
			},
			"size": 100,
			"track_total_hits": false
		}`,
		[]string{`((("message" iLIKE '%this%' OR "message" iLIKE '%is%') OR "message" iLIKE '%a%') OR "message" iLIKE '%test%')`},
		model.ListAllFields,
		////[]model.Query{justSimplestWhere(`((("message" iLIKE '%this%' OR "message" iLIKE '%is%') OR "message" iLIKE '%a%') OR "message" iLIKE '%test%')`)},
		[]string{
			`SELECT "message" FROM ` + TableName + ` WHERE ((("message" iLIKE '%this%' OR "message" iLIKE '%is%') ` +
				`OR "message" iLIKE '%a%') OR "message" iLIKE '%test%') ` +
				`LIMIT 100`,
		},
	},
	{ // [7]
		"Terms",
		`
		{
			"query": {
				"bool": {
					"must": [
						{
							"terms": {
								"status": ["pending"]
							}
						}
					]
				}
			},
			"track_total_hits": false
		}`,
		[]string{`"status"='pending'`},
		model.ListAllFields,
		////[]model.Query{justSimplestWhere(`"status"='pending'`)},
		[]string{`SELECT "message" FROM ` + TableName + ` WHERE "status"='pending'`},
	},
	{ // [8]
		"Exists",
		`
		{
			"query": {
				"bool": {
					"filter": [
						{
							"bool": {
								"should": [
									{
										"bool": {
											"must": [
												{
													"term": {
														"type": "upgrade-assistant-reindex-operation"
													}
												}
											],
											"must_not": [
												{
													"exists": {
														"field": "namespace"
													}
												},
												{
													"exists": {
														"field": "namespaces"
													}
												}
											]
										}
									}
								],
								"minimum_should_match": 1
							}
						}
					]
				}
			},
			"track_total_hits": false
		}`,
		[]string{
			`("type"='upgrade-assistant-reindex-operation' AND NOT ` +
				`(((has("attributes_string_key","namespace") AND "attributes_string_value"[indexOf("attributes_string_key","namespace")] IS NOT NULL) ` +
				`OR (has("attributes_string_key","namespaces") AND "attributes_string_value"[indexOf("attributes_string_key","namespaces")] IS NOT NULL))))`},
		model.ListAllFields,
		////[]model.Query{
		//	justSimplestWhere(`("type"='upgrade-assistant-reindex-operation' AND (NOT ((has("attributes_string_key","namespace") AND "attributes_string_value"[indexOf("attributes_string_key","namespace")] IS NOT NULL)) OR NOT ((has("attributes_string_key","namespaces") AND "attributes_string_value"[indexOf("attributes_string_key","namespaces")] IS NOT NULL))))`),
		//},
		[]string{
			`SELECT "message" ` +
				`FROM ` + TableName + ` ` +
				`WHERE ("type"='upgrade-assistant-reindex-operation' ` +
				`AND NOT (((has("attributes_string_key","namespace") ` +
				`AND "attributes_string_value"[indexOf("attributes_string_key","namespace")] IS NOT NULL) ` +
				`OR (has("attributes_string_key","namespaces") ` +
				`AND "attributes_string_value"[indexOf("attributes_string_key","namespaces")] IS NOT NULL))))`,
		},
	},
	{ // [9]
		"Simple query string",
		`
		{
			"query": {
				"bool": {
					"must": [
						{
							"simple_query_string": {
								"query": "endpoint_event_filters",
								"fields": [
									"exception-list-agnostic.list_id"
								],
								"default_operator": "OR"
							}
						}
					]
				}
			},
			"track_total_hits": false
		}`,
		[]string{`"exception-list-agnostic.list_id" = 'endpoint_event_filters'`},
		model.ListAllFields,
		//[]model.Query{justSimplestWhere(`"exception-list-agnostic.list_id" = 'endpoint_event_filters'`)},
		[]string{`SELECT "message" FROM ` + TableName + ` WHERE "exception-list-agnostic.list_id" = 'endpoint_event_filters'`},
	},
	{ // [10]
		"Simple query string wildcard",
		`
		{
			"query": {
				"bool": {
					"must": [
						{
							"simple_query_string": {
								"query": "ingest-agent-policies",
								"lenient": true,
								"fields": [
									"*"
								],
								"default_operator": "OR"
							}
						}
					]
				}
			},
			"track_total_hits": false
		}`,
		[]string{fullTextFieldName + ` = 'ingest-agent-policies'`},
		model.ListAllFields,
		//[]model.Query{justSimplestWhere(`"message" = 'ingest-agent-policies'`)},
		[]string{`SELECT "message" FROM ` + TableName + ` WHERE ` + fullTextFieldName + ` = 'ingest-agent-policies'`},
	},
	{ // [11]
		"Simple wildcard",
		`
		{
			"query": {
				"bool": {
					"must": [
						{
							"wildcard": {
								"task.taskType": {
									"value": "alerting:*"
								}
							}
						}
					]
				}
			},
			"track_total_hits": false
		}`,
		[]string{`"task.taskType" iLIKE 'alerting:%'`},
		model.ListAllFields,
		//[]model.Query{justSimplestWhere(`"task.taskType" iLIKE 'alerting:%'`)},
		[]string{`SELECT "message" FROM ` + TableName + ` WHERE "task.taskType" iLIKE 'alerting:%'`},
	},
	{ // [12]
		"Simple prefix ver1",
		`
		{
			"query": {
				"bool": {
					"must": [
						{
							"prefix": {
								"alert.actions.actionRef": {
									"value": "preconfigured:"
								}
							}
						}
					]
				}
			},
			"track_total_hits": false
		}`,
		[]string{`"alert.actions.actionRef" iLIKE 'preconfigured:%'`},
		model.ListAllFields,
		//[]model.Query{justSimplestWhere(`"alert.actions.actionRef" iLIKE 'preconfigured:%'`)},
		[]string{`SELECT "message" FROM ` + TableName + ` WHERE "alert.actions.actionRef" iLIKE 'preconfigured:%'`},
	},
	{ // [13]
		"Simple prefix ver2",
		`
		{
			"query": {
				"prefix" : { "user" : "ki" }
			},
			"track_total_hits": false,
			"size": 10
		}`,
		[]string{`"user" iLIKE 'ki%'`},
		model.ListAllFields,
		//[]model.Query{justSimplestWhere(`"user" iLIKE 'ki%'`)},
		[]string{`SELECT "message" FROM ` + TableName + ` WHERE "user" iLIKE 'ki%'`},
	},
	{ // [14]
		"Query string, wildcards don't work properly",
		`
		{
			"query": {
				"query_string": {
					"fields": [
						"message"
					],
					"query": "\"* logged\""
				}
			},
			"track_total_hits": false,
			"size": 1
		}`,
		[]string{`"message" ILIKE '% logged'`},
		model.ListAllFields,
		//[]model.Query{justSimplestWhere(`"message" ILIKE '% logged'`)},
		[]string{`SELECT "message" FROM ` + TableName + ` WHERE "message" ILIKE '% logged'`},
	},
	{ // [15]
		"Empty bool",
		`
		{
			"query": {
				"bool": {
					"must": [],
					"filter": [],
					"should": [],
					"must_not": []
				}
			},
			"track_total_hits": true
		}`,
		[]string{""},
		model.ListAllFields,
		//[]model.Query{newSimplestQuery()},
		[]string{
			`SELECT count() FROM ` + TableName,
			`SELECT "message" FROM ` + TableName,
		},
	},
	{ // [16]
		"Simplest 'match_phrase'",
		`{
			"query": {
				"match_phrase": {
					"message": "this is a test"
				}
			},
			"track_total_hits": false
		}`,
		[]string{`"message" iLIKE '%this is a test%'`},
		model.ListAllFields,
		//[]model.Query{justSimplestWhere(`"message" iLIKE '%this is a test%'`)},
		[]string{`SELECT "message" FROM ` + TableName + ` WHERE "message" iLIKE '%this is a test%'`},
	},
	{ // [17]
		"More nested 'match_phrase'",
		`{
			"query": {
				"match_phrase": {
					"message": {
						"query": "this is a test",
						"analyzer": "my_analyzer"
					}
				}
			},
			"track_total_hits": false
		}`,
		[]string{`"message" iLIKE '%this is a test%'`},
		model.ListAllFields,
		//[]model.Query{justSimplestWhere(`"message" iLIKE '%this is a test%'`)},
		[]string{`SELECT "message" FROM ` + TableName + ` WHERE "message" iLIKE '%this is a test%'`},
	},
	{ // [18]
		"Simple nested",
		`
		{
			"query": {
				"bool": {
					"must": [
						{
							"nested": {
								"path": "references",
								"query": {
									"bool": {
										"must": [
											{
												"term": {
													"references.type": "tag"
												}
											}
										]
									}
								}
							}
						}
					]
				}
			},
			"track_total_hits": false
		}`,
		[]string{`"references.type"='tag'`},
		model.ListAllFields,
		////[]model.Query{justSimplestWhere(`"references.type"='tag'`)},
		[]string{`SELECT "message" FROM ` + TableName + ` WHERE "references.type"='tag'`},
	},
	{ // [19]
		"random simple test",
		`
		{
			"size": 0,
			"timeout": "1000ms",
			"terminate_after": 100000,
			"query": {
			  "bool": {
				"filter": [
				  {
					"bool": {
					  "must": [],
					  "filter": [
						{
						  "multi_match": {
							"type": "best_fields",
							"query": "user",
							"lenient": true
						  }
						},
						{
						  "range": {
							"@timestamp": {
							  "format": "strict_date_optional_time",
							  "gte": "2024-01-22T09:26:10.299Z",
							  "lte": "2024-01-22T09:41:10.299Z"
							}
						  }
						}
					  ],
					  "should": [],
					  "must_not": []
					}
				  }
				]
			  }
			},
			"aggs": {
			  "suggestions": {
				"terms": {
				  "size": 10,
				  "field": "data_stream.namespace",
				  "shard_size": 10,
				  "order": {
					"_count": "desc"
				  }
				}
			  },
			  "unique_terms": {
				"cardinality": {
				  "field": "data_stream.namespace"
				}
			  }
			},
			"runtime_mappings": {},
			"track_total_hits": true
		  }
		`,
		[]string{
			`(` + fullTextFieldName + ` iLIKE '%user%' AND ("@timestamp">=parseDateTime64BestEffort('2024-01-22T09:26:10.299Z') ` +
				`AND "@timestamp"<=parseDateTime64BestEffort('2024-01-22T09:41:10.299Z')))`,
			`((` + fullTextFieldName + ` iLIKE '%user%' AND ("@timestamp">=parseDateTime64BestEffort('2024-01-22T09:26:10.299Z') ` +
				`AND "@timestamp"<=parseDateTime64BestEffort('2024-01-22T09:41:10.299Z'))) ` +
				`AND "data_stream.namespace" IS NOT NULL)`,
		},
		model.Facets,
		////[]model.Query{
		//	justSimplestWhere(`("message" iLIKE '%user%' AND ("@timestamp">=parseDateTime64BestEffort('2024-01-22T09:26:10.299Z') AND "@timestamp"<=parseDateTime64BestEffort('2024-01-22T09:41:10.299Z')))`),
		//},
		[]string{
			`SELECT count() ` +
				`FROM ` + TableName + ` ` +
				`WHERE (` + fullTextFieldName + ` iLIKE '%user%' ` +
				`AND ("@timestamp".=parseDateTime64BestEffort('2024-01-22T09:..:10.299Z') ` +
				`AND "@timestamp".=parseDateTime64BestEffort('2024-01-22T09:..:10.299Z')))`,
		},
	},
	{ // [20]
		"termWithCompoundValue",
		`
		{
			"size": 0,
			"track_total_hits": false,
			"timeout": "1000ms",
			"terminate_after": 100000,
			"query": {
			  "bool": {
				"filter": [
				  {
					"bool": {
					  "must": [],
					  "filter": [
						{
						  "bool": {
							"should": [
							  {
								"term": {
								  "service.name": {
									"value": "admin"
								  }
								}
							  }
							],
							"minimum_should_match": 1
						  }
						},
						{
						  "range": {
							"@timestamp": {
							  "format": "strict_date_optional_time",
							  "gte": "2024-01-22T14:34:35.873Z",
							  "lte": "2024-01-22T14:49:35.873Z"
							}
						  }
						}
					  ],
					  "should": [],
					  "must_not": []
					}
				  }
				]
			  }
			},
			"aggs": {
			  "suggestions": {
				"terms": {
				  "size": 10,
				  "field": "namespace",
				  "shard_size": 10,
				  "order": {
					"_count": "desc"
				  }
				}
			  },
			  "unique_terms": {
				"cardinality": {
				  "field": "namespace"
				}
			  }
			},
			"runtime_mappings": {}
		  }
		`,
		[]string{
			`(("service.name"='admin' AND ("@timestamp">=parseDateTime64BestEffort('2024-01-22T14:34:35.873Z') ` +
				`AND "@timestamp"<=parseDateTime64BestEffort('2024-01-22T14:49:35.873Z'))) ` +
				`AND "namespace" IS NOT NULL)`,
			`("service.name"='admin' AND ("@timestamp">=parseDateTime64BestEffort('2024-01-22T14:34:35.873Z') ` +
				`AND "@timestamp"<=parseDateTime64BestEffort('2024-01-22T14:49:35.873Z')))`,
		},
		model.Facets,
		////[]model.Query{
		//	justSimplestWhere(`("service.name"='admin' AND ("@timestamp">=parseDateTime64BestEffort('2024-01-22T14:34:35.873Z') AND "@timestamp"<=parseDateTime64BestEffort('2024-01-22T14:49:35.873Z')))`),
		//},
		[]string{
			`SELECT count(DISTINCT "namespace") ` +
				`FROM ` + TableName + ` ` +
				`WHERE ("service.name"='admin' ` +
				`AND ("@timestamp".=parseDateTime64BestEffort('2024-01-22T14:..:35.873Z') ` +
				`AND "@timestamp".=parseDateTime64BestEffort('2024-01-22T14:..:35.873Z')))`,
			`SELECT "namespace", count() ` +
				`FROM ` + TableName + ` ` +
				`WHERE (("service.name"='admin' ` +
				`AND ("@timestamp".=parseDateTime64BestEffort('2024-01-22T14:..:35.873Z') ` +
				`AND "@timestamp".=parseDateTime64BestEffort('2024-01-22T14:..:35.873Z'))) ` +
				`AND "namespace" IS NOT NULL) ` +
				`GROUP BY "namespace" ` +
				`ORDER BY count() DESC, "namespace" ` +
				`LIMIT 10`,
		},
	},
	{ // [21]
		"count() as /_search query. With filter", // response should be just ["hits"]["total"]["value"] == result of count()
		`{
		"aggs": {
			"suggestions": {
				"terms": {
					"field": "data_stream.namespace",
					"order": {
						"_count": "desc"
					},
					"shard_size": 10,
					"size": 10
				}
			},
			"unique_terms": {
				"cardinality": {
					"field": "data_stream.namespace"
				}
			}
		},
		"query": {
			"bool": {
				"filter": [
					{
						"bool": {
							"filter": [
								{
									"match_phrase": {
										"message": "User logged out"
									}
								},
								{
									"match_phrase": {
										"host.name": "poseidon"
									}
								},
								{
									"range": {
										"@timestamp": {
											"format": "strict_date_optional_time",
											"gte": "2024-01-29T15:36:36.491Z",
											"lte": "2024-01-29T18:11:36.491Z"
										}
									}
								}
							],
							"must": [],
							"must_not": [],
							"should": []
						}
					}
				]
			}
		},
		"runtime_mappings": {},
		"size": 0,
		"terminate_after": 100000,
		"timeout": "1000ms",
		"track_total_hits": true
	}`,
		[]string{
			`(("message" iLIKE '%User logged out%' AND "host.name" iLIKE '%poseidon%') ` +
				`AND ("@timestamp">=parseDateTime64BestEffort('2024-01-29T15:36:36.491Z') ` +
				`AND "@timestamp"<=parseDateTime64BestEffort('2024-01-29T18:11:36.491Z')))`,
			`((("message" iLIKE '%User logged out%' AND "host.name" iLIKE '%poseidon%') ` +
				`AND ("@timestamp">=parseDateTime64BestEffort('2024-01-29T15:36:36.491Z') ` +
				`AND "@timestamp"<=parseDateTime64BestEffort('2024-01-29T18:11:36.491Z'))) ` +
				`AND "data_stream.namespace" IS NOT NULL)`,
		},
		model.Facets,
		////[]model.Query{
		//	justSimplestWhere(`(("message" iLIKE '%User logged out%' AND "host.name" iLIKE '%poseidon%') AND ("@timestamp">=parseDateTime64BestEffort('2024-01-29T15:36:36.491Z') AND "@timestamp"<=parseDateTime64BestEffort('2024-01-29T18:11:36.491Z')))`),
		//},
		[]string{
			`SELECT count() ` +
				`FROM ` + TableName + ` ` +
				`WHERE (("message" iLIKE '%User logged out%' AND "host.name" iLIKE '%poseidon%') ` +
				`AND ("@timestamp".=parseDateTime64BestEffort('2024-01-29T1.:..:36.491Z') ` +
				`AND "@timestamp".=parseDateTime64BestEffort('2024-01-29T1.:..:36.491Z')))`,
		},
	},
	{ // [22]
		"count() as /_search or /logs-*-/_search query. Without filter", // response should be just ["hits"]["total"]["value"] == result of count()
		`{
			"aggs": {
				"suggestions": {
					"terms": {
						"field": "namespace",
						"order": {
							"_count": "desc"
						},
						"shard_size": 10,
						"size": 10
					}
				},
				"unique_terms": {
					"cardinality": {
						"field": "namespace"
					}
				}
			},
			"query": {
				"bool": {
					"filter": [
						{
							"bool": {
								"filter": [
									{
										"multi_match": {
											"lenient": true,
											"query": "user",
											"type": "best_fields"
										}
									},
									{
										"range": {
											"@timestamp": {
												"format": "strict_date_optional_time",
												"gte": "2024-01-22T09:26:10.299Z",
												"lte": "2024-01-22T09:41:10.299Z"
											}
										}
									}
								],
								"must": [],
								"must_not": [],
								"should": []
							}
						}
					]
				}
			},
			"runtime_mappings": {},
			"size": 0,
			"track_total_hits": false,
			"terminate_after": 100000,
			"timeout": "1000ms"
		}`,
		[]string{
			`((` + fullTextFieldName + ` iLIKE '%user%' AND ("@timestamp">=parseDateTime64BestEffort('2024-01-22T09:26:10.299Z') ` +
				`AND "@timestamp"<=parseDateTime64BestEffort('2024-01-22T09:41:10.299Z'))) ` +
				`AND "namespace" IS NOT NULL)`,
			`(` + fullTextFieldName + ` iLIKE '%user%' AND ("@timestamp">=parseDateTime64BestEffort('2024-01-22T09:26:10.299Z') ` +
				`AND "@timestamp"<=parseDateTime64BestEffort('2024-01-22T09:41:10.299Z')))`,
		},
		model.Facets,
		////[]model.Query{
		//	justSimplestWhere(`("message" iLIKE '%user%' AND ("@timestamp">=parseDateTime64BestEffort('2024-01-22T09:26:10.299Z') AND "@timestamp"<=parseDateTime64BestEffort('2024-01-22T09:41:10.299Z')))`),
		//},
		[]string{
			`SELECT "namespace", count() ` +
				`FROM ` + TableName + ` ` +
				`WHERE ((` + fullTextFieldName + ` iLIKE '%user%' ` +
				`AND ("@timestamp">=parseDateTime64BestEffort('2024-01-22T09:26:10.299Z') ` +
				`AND "@timestamp"<=parseDateTime64BestEffort('2024-01-22T09:41:10.299Z'))) ` +
				`AND "namespace" IS NOT NULL) ` +
				`GROUP BY "namespace" ` +
				`ORDER BY count() DESC, "namespace" ` +
				`LIMIT 10`,
			`SELECT count(DISTINCT "namespace") ` +
				`FROM ` + TableName + ` ` +
				`WHERE (` + fullTextFieldName + ` iLIKE '%user%' ` +
				`AND ("@timestamp">=parseDateTime64BestEffort('2024-01-22T09:26:10.299Z') ` +
				`AND "@timestamp"<=parseDateTime64BestEffort('2024-01-22T09:41:10.299Z')))`,
		},
	},
	{ // [23]
		"count() as /_search query. With filter", // response should be just ["hits"]["total"]["value"] == result of count()
		`{
		"aggs": {
			"suggestions": {
				"terms": {
					"field": "namespace",
					"order": {
						"_count": "desc"
					},
					"shard_size": 10,
					"size": 10
				}
			},
			"unique_terms": {
				"cardinality": {
					"field": "namespace"
				}
			}
		},
		"query": {
			"bool": {
				"filter": [
					{
						"bool": {
							"filter": [
								{
									"match_phrase": {
										"message": "User logged out"
									}
								},
								{
									"match_phrase": {
										"host.name": "poseidon"
									}
								},
								{
									"range": {
										"@timestamp": {
											"format": "strict_date_optional_time",
											"gte": "2024-01-29T15:36:36.491Z",
											"lte": "2024-01-29T18:11:36.491Z"
										}
									}
								}
							],
							"must": [],
							"must_not": [],
							"should": []
						}
					}
				]
			}
		},
		"runtime_mappings": {},
		"size": 0,
		"track_total_hits": false,
		"terminate_after": 100000,
		"timeout": "1000ms"
	}`,
		[]string{
			`((("message" iLIKE '%User logged out%' AND "host.name" iLIKE '%poseidon%') ` +
				`AND ("@timestamp">=parseDateTime64BestEffort('2024-01-29T15:36:36.491Z') ` +
				`AND "@timestamp"<=parseDateTime64BestEffort('2024-01-29T18:11:36.491Z'))) ` +
				`AND "namespace" IS NOT NULL)`,
			`(("message" iLIKE '%User logged out%' AND "host.name" iLIKE '%poseidon%') ` +
				`AND ("@timestamp">=parseDateTime64BestEffort('2024-01-29T15:36:36.491Z') ` +
				`AND "@timestamp"<=parseDateTime64BestEffort('2024-01-29T18:11:36.491Z')))`,
		},
		model.Facets,
		////[]model.Query{
		//	justSimplestWhere(`(("message" iLIKE '%User logged out%' AND "host.name" iLIKE '%poseidon%') AND ("@timestamp">=parseDateTime64BestEffort('2024-01-29T15:36:36.491Z') AND "@timestamp"<=parseDateTime64BestEffort('2024-01-29T18:11:36.491Z')))`),
		//},
		[]string{
			`SELECT "namespace", count() ` +
				`FROM ` + TableName + ` ` +
				`WHERE ((("message" iLIKE '%User logged out%' AND "host.name" iLIKE '%poseidon%') ` +
				`AND ("@timestamp">=parseDateTime64BestEffort('2024-01-29T15:36:36.491Z') ` +
				`AND "@timestamp"<=parseDateTime64BestEffort('2024-01-29T18:11:36.491Z'))) ` +
				`AND "namespace" IS NOT NULL) ` +
				`GROUP BY "namespace" ` +
				`ORDER BY count() DESC, "namespace" ` +
				`LIMIT 10`,
			`SELECT count(DISTINCT "namespace") ` +
				`FROM ` + TableName + ` ` +
				`WHERE (("message" iLIKE '%User logged out%' AND "host.name" iLIKE '%poseidon%') ` +
				`AND ("@timestamp">=parseDateTime64BestEffort('2024-01-29T15:36:36.491Z') ` +
				`AND "@timestamp"<=parseDateTime64BestEffort('2024-01-29T18:11:36.491Z')))`,
		},
	},
	{ // [24]
		"count() as /_search or /logs-*-/_search query. Without filter", // response should be just ["hits"]["total"]["value"] == result of count()
		`{
			"aggs": {
				"suggestions": {
					"terms": {
						"field": "namespace",
						"order": {
							"_count": "desc"
						},
						"shard_size": 10,
						"size": 10
					}
				},
				"unique_terms": {
					"cardinality": {
						"field": "namespace"
					}
				}
			},
			"query": {
				"bool": {
					"filter": [
						{
							"bool": {
								"filter": [
									{
										"multi_match": {
											"lenient": true,
											"query": "user",
											"type": "best_fields"
										}
									},
									{
										"range": {
											"@timestamp": {
												"format": "strict_date_optional_time",
												"gte": "2024-01-22T09:26:10.299Z",
												"lte": "2024-01-22T09:41:10.299Z"
											}
										}
									}
								],
								"must": [],
								"must_not": [],
								"should": []
							}
						}
					]
				}
			},
			"runtime_mappings": {},
			"size": 0,
			"track_total_hits": false,
			"terminate_after": 100000,
			"timeout": "1000ms"
		}`,
		[]string{
			`((` + fullTextFieldName + ` iLIKE '%user%' AND ("@timestamp">=parseDateTime64BestEffort('2024-01-22T09:26:10.299Z') ` +
				`AND "@timestamp"<=parseDateTime64BestEffort('2024-01-22T09:41:10.299Z'))) ` +
				`AND "namespace" IS NOT NULL)`,
			`(` + fullTextFieldName + ` iLIKE '%user%' AND ("@timestamp">=parseDateTime64BestEffort('2024-01-22T09:26:10.299Z') ` +
				`AND "@timestamp"<=parseDateTime64BestEffort('2024-01-22T09:41:10.299Z')))`,
		},
		model.Facets,
		////[]model.Query{
		//	justSimplestWhere(`("message" iLIKE '%user%' AND ("@timestamp">=parseDateTime64BestEffort('2024-01-22T09:26:10.299Z') AND "@timestamp"<=parseDateTime64BestEffort('2024-01-22T09:41:10.299Z')))`),
		//},
		[]string{
			`SELECT "namespace", count() ` +
				`FROM ` + TableName + ` ` +
				`WHERE (("message" iLIKE '%user%' ` +
				`AND ("@timestamp">=parseDateTime64BestEffort('2024-01-22T09:26:10.299Z') ` +
				`AND "@timestamp"<=parseDateTime64BestEffort('2024-01-22T09:41:10.299Z'))) ` +
				`AND "namespace" IS NOT NULL) ` +
				`GROUP BY "namespace" ` +
				`ORDER BY count() DESC, "namespace" ` +
				`LIMIT 10`,
			`SELECT count(DISTINCT "namespace") ` +
				`FROM ` + TableName + ` ` +
				`WHERE ("message" iLIKE '%user%' ` +
				`AND ("@timestamp">=parseDateTime64BestEffort('2024-01-22T09:26:10.299Z') ` +
				`AND "@timestamp"<=parseDateTime64BestEffort('2024-01-22T09:41:10.299Z')))`,
		},
	},
	{ // [25]
		"_search, only one so far with fields, we're not sure if SELECT * is correct, or should be SELECT @timestamp",
		`{
			"_source": {
				"excludes": []
			},
			"fields": [
				{
					"field": "message",
					"format": "date_time"
				}
			],
			"pit": {
				"id": "gcSHBAEqLmRzLWxvZ3MtZ2VuZXJpYy1kZWZhdWx0LTIwMjQuMDMuMjYtMDAwMDAxFndZdXQ5SG5wU0RTZExnV0lkXzRWT1EAFkRwRTBHbm5uVDlpLTR0MnZONXY0dFEAAAAAAAAALAUWTTBidDdzcWJTWGlZamxpTGE3WW5IUQABFndZdXQ5SG5wU0RTZExnV0lkXzRWT1EAAA==",
				"keep_alive": "30s"
			},
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
			"size": 500,
			"track_total_hits": false,
			"stored_fields": [
				"*"
			],
			"timeout": "30000ms",
			"track_total_hits": true
		}`,
		[]string{""},
		model.ListByField,
		//[]model.Query{withLimit(newSimplestQuery(), 500)},
		[]string{
			`SELECT count() FROM ` + TableName,
			`SELECT "message" FROM ` + TableName + ` LIMIT 500`,
		},
	},
	{ // [26]
		"Empty must",
		`
		{
			"query": {
				"bool": {
					"must": {}
				}
			},
            "track_total_hits": true
		}`,
		[]string{``},
		model.ListAllFields,
		//[]model.Query{justSimplestWhere(``)},
		[]string{
			`SELECT count() FROM ` + TableName,
			`SELECT "message" FROM ` + TableName + ` LIMIT 10`,
		},
	},
	{ // [27]
		"Empty must not",
		`
		{
			"query": {
				"bool": {
					"must_not": {}
				}
			},
			"track_total_hits": false
		}`,
		[]string{``},
		model.ListAllFields,
		//[]model.Query{justSimplestWhere(``)},
		[]string{
			`SELECT "message" FROM ` + TableName + ` LIMIT 10`,
		},
	},
	{ // [28]
		"Empty should",
		`
		{
			"query": {
				"bool": {
					"should": {}
				}
			},
			"track_total_hits": false
		}`,
		[]string{``},
		model.ListAllFields,
		//[]model.Query{justSimplestWhere(``)},
		[]string{`SELECT "message" FROM ` + TableName},
	},
	{ // [29]
		"Empty all bools",
		`
		{
			"query": {
				"bool": {
					"should": {},
					"must": {},
					"must_not": {},
					"filter": {}
				}
			},
			"track_total_hits": true
		}`,
		[]string{``},
		model.ListAllFields,
		//[]model.Query{justSimplestWhere(``)},
		[]string{
			`SELECT count() FROM ` + TableName,
			`SELECT "message" FROM ` + TableName,
		},
	},
	{ // [30]
		"Some bools empty, some not",
		`
		{
			"query": {
				"bool": {
					"should": [],
					"must": {
						"match_phrase": {
							"message": "User logged out"
						}
					},
					"must_not": {},
					"filter": [
						{
							"match_phrase": {
								"message": "User logged out"
							}
						}
					]
				}
			},
			"track_total_hits": false,
			"size": 12
		}`,
		[]string{`("message" iLIKE '%User logged out%' AND "message" iLIKE '%User logged out%')`},
		model.ListAllFields,
		//[]model.Query{justSimplestWhere(`("message" iLIKE '%User logged out%' AND "message" iLIKE '%User logged out%')`)},
		[]string{
			`SELECT "message" ` +
				`FROM ` + TableName + ` ` +
				`WHERE ("message" iLIKE '%User logged out%' AND "message" iLIKE '%User logged out%')`,
		},
	},
	{ // [31]
		"Match all (empty query)",
		`{}`,
		[]string{""},
		model.ListAllFields,
		//[]model.Query{newSimplestQuery()},
		[]string{
			`SELECT count() FROM (SELECT 1 FROM ` + TableName + ` LIMIT 10000)`,
			`SELECT "message" FROM ` + TableName,
		},
	},
	{ // [32]
		"Constant score query",
		`{
			"query": {
 				"constant_score": {
 					"filter": {
 						"term": { "user.id": "kimchy" }
 					},
 					"boost": 1.2
 				}
 			},
			"track_total_hits": false
		}`,
		[]string{`"user.id"='kimchy'`},
		model.ListAllFields,
		//[]model.Query{justSimplestWhere(`"user.id"='kimchy'`)},
		[]string{`SELECT "message" FROM ` + TableName + ` WHERE "user.id"='kimchy'`},
	},
	{ // [33] this is a snowflake case as `_id` is a special field in ES and in clickhouse we compute
		"Match phrase using _id field",
		`{
			  "query": {
				"bool": {
				  "filter": [
					{
					  "range": {
						"@timestamp": {
						  "format": "strict_date_optional_time",
							"gte": "2024-01-22T09:26:10.299Z"
						}
					  }
					},
					{
					  "match_phrase": {
						"_id": "323032342d30352d32342031333a33323a34372e333037202b3030303020555443q1"
					  }
					}
				  ]
				}
			  },
			  "track_total_hits": false
			}`,
		[]string{
			`("@timestamp">=parseDateTime64BestEffort('2024-01-22T09:26:10.299Z') AND "@timestamp" = toDateTime64('2024-05-24 13:32:47.307',3))`,
		},
		model.ListAllFields,
		//[]model.Query{
		//	justSimplestWhere(`("@timestamp">=parseDateTime64BestEffort('2024-01-22T09:26:10.299Z') AND "@timestamp" = toDateTime64('2024-05-24 13:32:47.307',3))`),
		//},
		// TestSearchHandler is pretty blunt with config loading so the test below can't be used.
		// We will probably refactor it as we move forwards with schema which will get even more side-effecting
		[]string{`SELECT "message" FROM ` + TableName + ` WHERE "@timestamp".=parseDateTime64BestEffort('2024-01-22T09:..:10.299Z')`},
	},
	{ // [34] Comments in queries
		"Comments in filter",
		`{
			"query": { /*one comment */
				"bool": {
					"must": {
 						"term": { "user.id": "kimchy" } // One comment
                     }
				}
			},
			"track_total_hits": false
		}`,
		[]string{`"user.id"='kimchy'`},
		model.ListAllFields,
		//[]model.Query{justSimplestWhere(`"user.id"='kimchy'`)},
		[]string{`SELECT "message" FROM ` + TableName + ` WHERE "user.id"='kimchy'`},
	},
	{ // [35] terms with range
		"Terms with range",
		`{
		  "size": 1,
		  "query": {
			"bool": {
			  "filter": [
				{
				  "terms": {
					"cliIP": [
					  "2601:204:c503:c240:9c41:5531:ad94:4d90",
					  "50.116.43.98",
					  "75.246.0.64"
					]
				  }
				},
				{
				  "range": {
					"@timestamp": {
					  "gte": "2024-05-16T00:00:00",
					  "lte": "2024-05-17T23:59:59"
					}
				  }
				}
			  ]
			}
		  },
		  "track_total_hits": false
		}`,
		[]string{`("cliIP" IN ('2601:204:c503:c240:9c41:5531:ad94:4d90','50.116.43.98','75.246.0.64') AND ("@timestamp">=parseDateTime64BestEffort('2024-05-16T00:00:00') AND "@timestamp"<=parseDateTime64BestEffort('2024-05-17T23:59:59')))`},
		model.ListAllFields,
		//[]model.Query{withLimit(justSimplestWhere(`("cliIP" IN ('2601:204:c503:c240:9c41:5531:ad94:4d90','50.116.43.98','75.246.0.64') AND ("@timestamp">=parseDateTime64BestEffort('2024-05-16T00:00:00') AND "@timestamp"<=parseDateTime64BestEffort('2024-05-17T23:59:59')))`), 1)},
		[]string{
			`SELECT "message" ` +
				`FROM ` + TableName + ` ` +
				`WHERE ("cliIP" IN ('2601:204:c503:c240:9c41:5531:ad94:4d90','50.116.43.98','75.246.0.64') ` +
				`AND ("@timestamp">=parseDateTime64BestEffort('2024-05-16T00:00:00') ` +
				`AND "@timestamp"<=parseDateTime64BestEffort('2024-05-17T23:59:59'))) ` +
				`LIMIT 1`,
		},
	},
	{ // [36]
		"Simple regexp (can be simply transformed to one LIKE)",
		`{
			"query": {
				"bool": {
					 "filter": [
						{
							"regexp": {
								"field": {
									"value": ".*-abb-all-li.mit.*s-5"
								}
							}
						}
					]
				}
			},
			"track_total_hits": false
		}`,
		[]string{`"field" LIKE '%-abb-all-li_mit%s-5'`},
		model.ListAllFields,
		[]string{
			`SELECT "message" ` +
				`FROM ` + TableName + ` ` +
				`WHERE "field" LIKE '%-abb-all-li_mit%s-5' ` +
				`LIMIT 10`,
		},
	},
	{ // [37]
		"Simple regexp (can be simply transformed to one LIKE), with _, which needs to be escaped",
		`{
			"query": {
				"bool": {
					 "filter": [
						{
							"regexp": {
								"field": {
									"value": ".*_.."
								}
							}
						}
					]
				}
			},
			"track_total_hits": false
		}`,
		[]string{`"field" LIKE '%\___'`},
		model.ListAllFields,
		[]string{
			`SELECT "message" ` +
				`FROM ` + TableName + ` ` +
				`WHERE "field" LIKE '%\\___' ` +
				`LIMIT 10`,
		},
	},
	{ // [38]
		"Complex regexp 1 (can't be transformed to LIKE)",
		`{
			"query": {
				"bool": {
					 "filter": [
						{
							"regexp": {
								"field": {
									"value": "a*-abb-all-li.mit.*s-5"
								}
							}
						}
					]
				}
			},
			"track_total_hits": false
		}`,
		[]string{`"field" REGEXP 'a*-abb-all-li.mit.*s-5'`},
		model.ListAllFields,
		[]string{
			`SELECT "message" ` +
				`FROM ` + TableName + ` ` +
				`WHERE "field" REGEXP 'a*-abb-all-li.mit.*s-5' ` +
				`LIMIT 10`,
		},
	},
	{ // [39]
		"Complex regexp 2 (can't be transformed to LIKE)",
		`{
			"query": {
				"bool": {
					 "filter": [
						{
							"regexp": {
								"field": {
									"value": "a?"
								}
							}
						}
					]
				}
			},
			"track_total_hits": false
		}`,
		[]string{`"field" REGEXP 'a?'`},
		model.ListAllFields,
		[]string{
			`SELECT "message" ` +
				`FROM ` + TableName + ` ` +
				`WHERE "field" REGEXP 'a\?' ` +
				`LIMIT 10`,
		},
	},
}

var TestsSearchNoAttrs = []SearchTestCase{
	{
		"Test empty ANDs, ORs and NOTs",
		`
		{
			"query": {
				"bool": {
				  "filter": [
					{
					  "range": {
						"@timestamp": {
						  "gte": "2024-01-25T13:22:45.968Z",
						  "lte": "2024-01-25T13:37:45.968Z"
						}
					  }
					},
					{
					  "exists": {
						"field": "summary"
					  }
					},
					{
					  "bool": {
						"must_not": {
						  "exists": {
							"field": "run_once"
						  }
						}
					  }
					}
				  ]
				}
			},
			"track_total_hits": false
		}`,
		[]string{
			`("@timestamp">=parseDateTime64BestEffort('2024-01-25T13:22:45.968Z') AND "@timestamp"<=parseDateTime64BestEffort('2024-01-25T13:37:45.968Z'))`,
		},
		model.ListAllFields,
		//[]model.Query{
		//	justSimplestWhere(`("@timestamp">=parseDateTime64BestEffort('2024-01-25T13:22:45.968Z') AND "@timestamp"<=parseDateTime64BestEffort('2024-01-25T13:37:45.968Z'))`),
		//},
		[]string{
			`SELECT "message" FROM ` + TableName + ` ` +
				`WHERE ((("@timestamp".=parseDateTime64BestEffort('2024-01-25T13:22:45.968Z') ` +
				`AND "@timestamp".=parseDateTime64BestEffort('2024-01-25T13:37:45.968Z')) ` +
				`AND (has("attributes_string_key","summary") AND "attributes_string_value"[indexOf("attributes_string_key","summary")] IS NOT NULL)) ` +
				`AND NOT ((has("attributes_string_key","run_once") AND "attributes_string_value"[indexOf("attributes_string_key","run_once")] IS NOT NULL))) ` +
				`LIMIT 10`,
		},
	},
}

var TestSearchFilter = []SearchTestCase{
	{ // [0]
		"Empty filter clause",
		`{
			  "_source": {
				"excludes": []
			  },
			  "aggs": {
				"0": {
				  "date_histogram": {
					"field": "@timestamp",
					"fixed_interval": "30s",
					"min_doc_count": 1,
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
				  ],
				  "must": [],
				  "must_not": [],
				  "should": []
				}
			  },
			  "runtime_mappings": {},
			  "script_fields": {},
			  "size": 0,
			  "track_total_hits": false,
			  "stored_fields": [
				"*"
			  ]
			}`,
		[]string{
			``,
			``,
		},
		model.Normal,
		//[]model.Query{
		//	justSimplestWhere(``),
		//	justSimplestWhere(``),
		//},
		[]string{
			"SELECT " + groupBySQL("@timestamp", clickhouse.DateTime64, 30*time.Second) + ", count() " +
				"FROM " + TableName + " " +
				"GROUP BY " + groupBySQL("@timestamp", clickhouse.DateTime64, 30*time.Second) + " " +
				"ORDER BY " + groupBySQL("@timestamp", clickhouse.DateTime64, 30*time.Second),
		},
	},
	{ // [1]
		"Filter with now in range",
		`{
		  "_source": {
			"excludes": []
		  },
		  "aggs": {
			"0": {
			  "date_histogram": {
				"field": "@timestamp",
				"fixed_interval": "30s",
				"min_doc_count": 1,
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
					  "gt": "now-15m"
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
		[]string{
			``,
			``,
		},
		model.Normal,
		//[]model.Query{
		//	justSimplestWhere(``),
		//	justSimplestWhere(``),
		//},
		[]string{
			"SELECT count() FROM " + TableName + ` WHERE "@timestamp">subDate(now(), INTERVAL 15 minute)`,
			"SELECT " + groupBySQL("@timestamp", clickhouse.DateTime64, 30*time.Second) + `, count() ` +
				`FROM ` + TableName + ` ` +
				`WHERE "@timestamp">subDate(now(), INTERVAL 15 minute) ` +
				`GROUP BY ` + groupBySQL("@timestamp", clickhouse.DateTime64, 30*time.Second) + ` ` +
				`ORDER BY ` + groupBySQL("@timestamp", clickhouse.DateTime64, 30*time.Second),
		},
	},
	{ // [2]
		"Empty filter",
		`
		{
			"query": {
				"bool": {
					"filter": {}
				}
			},
			"track_total_hits": false
		}`,
		[]string{``},
		model.Normal,
		//[]model.Query{justSimplestWhere(``)},
		[]string{`SELECT "message" FROM ` + TableName + ` LIMIT 10`},
	},
	{ // [3]
		"Empty filter with other clauses",
		`
		{
			"query": {
				"bool" : {
					"must" : {
						"term" : { "user.id" : "kimchy" }
					},
					"filter": {},
					"must_not" : {
						"range" : {
							"age" : { "gte" : 10, "lte" : 20 }
						}
					},
					"should" : [
						{ "term" : { "tags" : "env1" } },
						{ "term" : { "tags" : "deployed" } }
					],
					"minimum_should_match" : 1,
					"boost" : 1.0
				}
			},
			"track_total_hits": false
		}`,
		[]string{
			`("user.id"='kimchy' AND ("tags"='env1' OR "tags"='deployed')) AND NOT ("age"<=20 AND "age">=10)`,
			`("user.id"='kimchy' AND ("tags"='env1' OR "tags"='deployed')) AND NOT ("age">=10 AND "age"<=20)`,
		},
		model.Normal,
		//[]model.Query{
		//	justSimplestWhere(`("user.id"='kimchy' AND ("tags"='env1' OR "tags"='deployed')) AND NOT ("age"<=20 AND "age">=10)`),
		//	justSimplestWhere(`("user.id"='kimchy' AND ("tags"='env1' OR "tags"='deployed')) AND NOT ("age">=10 AND "age"<=20)`),
		//},
		[]string{
			`SELECT "message" ` +
				`FROM ` + TableName + ` ` +
				`WHERE (("user.id"='kimchy' AND ("tags"='env1' OR "tags"='deployed')) ` +
				`AND NOT (("age".=10 AND "age".=20)))`,
		},
	},
}
