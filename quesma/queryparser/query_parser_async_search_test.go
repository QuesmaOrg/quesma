package queryparser

import (
	"github.com/stretchr/testify/assert"
	"mitmproxy/quesma/clickhouse"
	"mitmproxy/quesma/model"
	"testing"
)

var testsAsyncResult = []struct {
	name              string
	queryJson         string
	resultJson        string // from ELK
	comment           string
	wantedParseResult model.QueryInfo
}{
	{
		"AggsByField (Facet): aggregate by field + additionally match user (filter)",
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
    "track_total_hits": true
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
		model.QueryInfo{Typ: model.AggsByField, FieldName: "host.name", I1: 10, I2: 5000}},
	{
		"ListByField: query one field, last 'size' results, return list of just that field, no timestamp, etc.",
		`{
    "_source": false,
    "fields": [
        {
            "field": "message"
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
		model.QueryInfo{Typ: model.ListByField, FieldName: "message", I1: 0, I2: 100}},
	{
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
                },
`, "Truncated most results. TODO Check what's at the end of response, probably count?",
		model.QueryInfo{Typ: model.ListAllFields, FieldName: "*", I1: 0, I2: 500},
	},
	{
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
    "size": 0,
    "stored_fields": [
        "*"
    ],
    "track_total_hits": true
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
		model.QueryInfo{Typ: model.Histogram, FieldName: "30s", I1: 0, I2: 0},
	},
	{
		"Histogram: possible query nr 2",
		`{
	"size":0,
	"query":
	{
		"range":
		{
			"@timestamp":
			{
				"gt": "2024-01-25T14:53:59.033Z",
				"lte": "2024-01-25T15:08:59.033Z",
				"format": "strict_date_optional_time"
			}
		}
	},
	"aggs":
	{
		"stats":
		{
			"terms":
			{
				"field": "event.dataset",
				"size": 4,
				"missing": "unknown"
			},
			"aggs":
			{
				"series":
				{
					"date_histogram":
					{
						"field": "@timestamp",
						"fixed_interval": "60s"
					}
				}
			}
		}
	},
	"track_total_hits":true
}`,
		`{}`,
		"no comment yet",
		model.QueryInfo{Typ: model.Histogram, FieldName: "60s", I1: 0, I2: 0},
	},
}

func TestQueryParserAsyncSearch(t *testing.T) {
	lm := clickhouse.NewLogManager(make(clickhouse.TableMap), make(clickhouse.TableMap))
	cw := ClickhouseQueryTranslator{lm}
	for _, tt := range testsAsyncResult {
		t.Run(tt.name, func(t *testing.T) {
			query, queryInfo := cw.parseQueryAsyncSearch(tt.queryJson)
			assert.True(t, query.CanParse)
			assert.Equal(t, tt.wantedParseResult, queryInfo)
		})
	}
}
