package testdata

import (
	"fmt"
	"mitmproxy/quesma/clickhouse"
	"mitmproxy/quesma/model"
	"strconv"
	"strings"
	"time"
)

// Used in at least 2 different packages/test files for now, so it's moved to a separate package.

const TableName = "logs-generic-default"
const quotedTableName = `"` + TableName + `"`
const queryparserFacetsSampleSize = "20000" // should be same value as queryparser.facetsSampleSize

const oneMinute = 60 * time.Second

func newSimplestQuery() model.Query {
	return model.Query{
		Fields:     []string{"*"},
		FromClause: strconv.Quote(TableName),
		CanParse:   true,
	}
}

// simple helper function to help fill out test cases
func qToStr(query model.Query) string {
	return strings.Replace(query.String(), "*", `"message"`, 1)
}

// simple helper function to help fill out test cases
func justWhere(whereClause string) model.Query {
	q := newSimplestQuery()
	q.WhereClause = whereClause
	return q
}

// EscapeBrackets is a simple helper function used in sqlmock's tests.
// Example usage: sqlmock.ExpectQuery(EscapeBrackets(`SELECT count() FROM "logs-generic-default" WHERE `))
func EscapeBrackets(s string) string {
	s = strings.ReplaceAll(s, `(`, `\(`)
	s = strings.ReplaceAll(s, `)`, `\)`)
	s = strings.ReplaceAll(s, `[`, `\[`)
	s = strings.ReplaceAll(s, `]`, `\]`)
	return s
}

type SearchTestCase struct {
	Name            string
	QueryJson       string
	WantedSql       []string // array because of non-determinism
	WantedQueryType model.SearchQueryType
	WantedQuery     []model.Query // array because of non-determinism
	WantedRegexes   []string      // regexes saying what SELECT queries to CH should look like (in order). A lot of '.' here because of non-determinism.
}

var TestsAsyncSearch = []struct {
	Name              string
	QueryJson         string
	ResultJson        string // from ELK
	Comment           string
	WantedParseResult model.QueryInfoAsyncSearch
	WantedRegexes     []string // queries might be a bit weird at times, because of non-determinism of our parser (need to use a lot of "." in regexes) (they also need to happen as ordered in this slice)
	IsAggregation     bool     // is it an aggregation query?
}{
	{ // [0]
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
		model.QueryInfoAsyncSearch{Typ: model.AggsByField, FieldName: "host.name", I1: 10, I2: 5000},
		[]string{`SELECT "host.name", count() FROM (SELECT \* FROM "logs-generic-default" LIMIT ` + queryparserFacetsSampleSize + `) WHERE ("@timestamp".=parseDateTime64BestEffort('2024-01-23T11:..:16.820Z') AND "@timestamp".=parseDateTime64BestEffort('2024-01-23T11:..:16.820Z')) AND "message" iLIKE '%user%' GROUP BY "host.name" ORDER BY count() DESC`},
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
		model.QueryInfoAsyncSearch{Typ: model.ListByField, FieldName: "message", I1: 0, I2: 100},
		[]string{`SELECT "message" FROM "logs-generic-default" WHERE ("@timestamp".=parseDateTime64BestEffort('2024-01-23T14:..:19.481Z') AND "@timestamp".=parseDateTime64BestEffort('2024-01-23T14:..:19.481Z')) AND "message" iLIKE '%user%' AND message IS NOT NULL ORDER BY "@timestamp" desc LIMIT 100`},
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
		model.QueryInfoAsyncSearch{Typ: model.ListAllFields, FieldName: "*", I1: 0, I2: 500},
		[]string{`SELECT .*"@timestamp".* FROM "logs-generic-default" WHERE "message" iLIKE '%user%' AND ("@timestamp".=parseDateTime64BestEffort('2024-01-23T14:..:19.481Z') AND "@timestamp".=parseDateTime64BestEffort('2024-01-23T14:..:19.481Z')) ORDER BY "@timestamp" desc LIMIT 500`},
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
		model.QueryInfoAsyncSearch{Typ: model.Histogram, FieldName: "@timestamp", Interval: "30s", I1: 0, I2: 0},
		[]string{
			`SELECT count() FROM "logs-generic-default" WHERE "message" iLIKE '%user%' AND ("@timestamp".=parseDateTime64BestEffort('2024-01-23T14:..:19.481Z') AND "@timestamp".=parseDateTime64BestEffort('2024-01-23T14:..:19.481Z')) `,
			`SELECT toInt64(toUnixTimestamp64Milli(` + "`@timestamp`" + `)/30000), count() FROM "logs-generic-default" WHERE "message" iLIKE '%user%' AND ("@timestamp".=parseDateTime64BestEffort('2024-01-23T14:..:19.481Z') AND "@timestamp".=parseDateTime64BestEffort('2024-01-23T14:..:19.481Z')) GROUP BY (toInt64(toUnixTimestamp64Milli(` + "`@timestamp`)/30000)) ORDER BY (toInt64(toUnixTimestamp64Milli(`@timestamp`)/30000))",
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
	"track_total_hits":true
}`,
		`{}`,
		"no comment yet",
		model.QueryInfoAsyncSearch{Typ: model.Histogram, FieldName: "@timestamp", Interval: fmt.Sprintf("%ds", int(oneMinute.Seconds())), I1: 0, I2: 0},
		[]string{
			`SELECT count() FROM "logs-generic-default" WHERE "@timestamp".*parseDateTime64BestEffort('2024-01-25T..:..:59.033Z') AND "@timestamp".*parseDateTime64BestEffort('2024-01-25T..:..:59.033Z') `,
			`SELECT "event.dataset", ` + clickhouse.TimestampGroupBy("@timestamp", clickhouse.DateTime64, time.Minute) + `, count() FROM "logs-generic-default" WHERE "@timestamp".*parseDateTime64BestEffort('2024-01-25T1.:..:59.033Z') AND "@timestamp".*parseDateTime64BestEffort('2024-01-25T1.:..:59.033Z') GROUP BY ("event.dataset", ` + clickhouse.TimestampGroupBy("@timestamp", clickhouse.DateTime64, time.Minute) + `) ORDER BY ("event.dataset", ` + clickhouse.TimestampGroupBy("@timestamp", clickhouse.DateTime64, time.Minute) + ")",
			`SELECT "event.dataset", count() FROM "logs-generic-default" WHERE "@timestamp".*parseDateTime64BestEffort('2024-01-25T1.:..:59.033Z') AND "@timestamp".*parseDateTime64BestEffort('2024-01-25T1.:..:59.033Z')  GROUP BY ("event.dataset") ORDER BY ("event.dataset")`,
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
			"track_total_hits": true
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
		model.QueryInfoAsyncSearch{Typ: model.EarliestLatestTimestamp, FieldName: "@timestamp"},
		[]string{
			`SELECT count() FROM "logs-generic-default" WHERE "message" iLIKE '%posei%' AND ("message" iLIKE '%User%' OR "message" iLIKE '%logged%' OR "message" iLIKE '%out%') AND "host.name" iLIKE '%poseidon%' `,
			`SELECT m..("@timestamp") FROM "logs-generic-default" WHERE "message" iLIKE '%posei%' AND ("message" iLIKE '%User%' OR "message" iLIKE '%logged%' OR "message" iLIKE '%out%') AND "host.name" iLIKE '%poseidon%' `,
			`SELECT m..("@timestamp") FROM "logs-generic-default" WHERE "message" iLIKE '%posei%' AND ("message" iLIKE '%User%' OR "message" iLIKE '%logged%' OR "message" iLIKE '%out%') AND "host.name" iLIKE '%poseidon%' `,
		},
		true,
	},
}

var TestsSearch = []SearchTestCase{
	{ // [0]
		"Match all",
		`
		{
			"query": {
				"match_all": {}
			}
		}`,
		[]string{""},
		model.Normal,
		[]model.Query{newSimplestQuery()},
		[]string{qToStr(newSimplestQuery())},
	},
	{ // [1]
		"Term as dictionary",
		`
		{
			"bool": {
				"filter":
				{
					"term": {
						"type": "task"
					}
				}
			}
		}`,
		[]string{`"type"='task'`},
		model.Normal,
		[]model.Query{justWhere(`"type"='task'`)},
		[]string{qToStr(justWhere(`"type"='task'`))},
	},
	{ // [2]
		"Term as array",
		`
		{
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
		}`,
		[]string{`"type"='task' AND ("task.enabled"=true OR "task.enabled"=54)`},
		model.Normal,
		[]model.Query{
			justWhere(`"type"='task' AND ("task.enabled"=true OR "task.enabled"=54)`),
		},
		[]string{qToStr(justWhere(`"type"='task' AND ("task.enabled"=true OR "task.enabled"=54)`))},
	},
	{ // [3]
		"Sample log query",
		`
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
							"gte": "2024-01-17T10:28:18.815Z",
							"lte": "2024-01-17T10:43:18.815Z"
						}
					}
				}],
				"should": [],
				"must_not": []
			}
		}`,
		[]string{
			`"message" iLIKE '%user%' AND ("@timestamp">=parseDateTime64BestEffort('2024-01-17T10:28:18.815Z') AND "@timestamp"<=parseDateTime64BestEffort('2024-01-17T10:43:18.815Z'))`,
			`"message" iLIKE '%user%' AND ("@timestamp"<=parseDateTime64BestEffort('2024-01-17T10:43:18.815Z') AND "@timestamp">=parseDateTime64BestEffort('2024-01-17T10:28:18.815Z'))`,
		},
		model.Normal,
		[]model.Query{
			justWhere(`"message" iLIKE '%user%' AND ("@timestamp">=parseDateTime64BestEffort('2024-01-17T10:28:18.815Z') AND "@timestamp"<=parseDateTime64BestEffort('2024-01-17T10:43:18.815Z'))`),
			justWhere(`"message" iLIKE '%user%' AND ("@timestamp"<=parseDateTime64BestEffort('2024-01-17T10:43:18.815Z') AND "@timestamp">=parseDateTime64BestEffort('2024-01-17T10:28:18.815Z'))`),
		},
		[]string{`SELECT "message" FROM "logs-generic-default" WHERE "message" iLIKE '%user%' AND ("@timestamp".=parseDateTime64BestEffort('2024-01-17T10:..:18.815Z') AND "@timestamp".=parseDateTime64BestEffort('2024-01-17T10:..:18.815Z'))`},
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
			}
		}`,
		[]string{
			`(("user.id"='kimchy' AND "tags"='production') AND ("tags"='env1' OR "tags"='deployed')) AND NOT ("age"<=20 AND "age">=10)`,
			`(("user.id"='kimchy' AND "tags"='production') AND ("tags"='env1' OR "tags"='deployed')) AND NOT ("age">=10 AND "age"<=20)`,
		},
		model.Normal,
		[]model.Query{
			justWhere(`(("user.id"='kimchy' AND "tags"='production') AND ("tags"='env1' OR "tags"='deployed')) AND NOT ("age"<=20 AND "age">=10)`),
			justWhere(`(("user.id"='kimchy' AND "tags"='production') AND ("tags"='env1' OR "tags"='deployed')) AND NOT ("age">=10 AND "age"<=20)`),
		},
		[]string{`SELECT "message" FROM "logs-generic-default" WHERE (("user.id"='kimchy' AND "tags"='production') AND ("tags"='env1' OR "tags"='deployed')) AND NOT ("age".=.0 AND "age".=.0)`},
	},
	{ // [5]
		"Match phrase",
		`
		{
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
		}`,
		[]string{`"host_name.keyword" iLIKE '%prometheus%'`},
		model.Normal,
		[]model.Query{justWhere(`"host_name.keyword" iLIKE '%prometheus%'`)},
		[]string{qToStr(justWhere(`"host_name.keyword" iLIKE '%prometheus%'`))},
	},
	{ // [6]
		"Match",
		`
		{
			"query": {
				"match": {
					"message": "this is a test"
				}
			}
		}`,
		[]string{`"message" iLIKE '%this%' OR "message" iLIKE '%is%' OR "message" iLIKE '%a%' OR "message" iLIKE '%test%'`},
		model.Normal,
		[]model.Query{justWhere(`"message" iLIKE '%this%' OR "message" iLIKE '%is%' OR "message" iLIKE '%a%' OR "message" iLIKE '%test%'`)},
		[]string{qToStr(justWhere(`"message" iLIKE '%this%' OR "message" iLIKE '%is%' OR "message" iLIKE '%a%' OR "message" iLIKE '%test%'`))},
	},
	{ // [7]
		"Terms",
		`
		{
			"bool": {
				"must": [
					{
						"terms": {
							"status": ["pending"]
						}
					}
				]
			}
		}`,
		[]string{`"status"='pending'`},
		model.Normal,
		[]model.Query{justWhere(`"status"='pending'`)},
		[]string{qToStr(justWhere(`"status"='pending'`))},
	},
	{ // [8]
		"Exists",
		`
		{
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
		}`,
		[]string{`"type"='upgrade-assistant-reindex-operation' AND NOT ((has("attributes_string_key","namespace") AND "attributes_string_value"[indexOf("attributes_string_key","namespace")] IS NOT NULL) OR (has("attributes_string_key","namespaces") AND "attributes_string_value"[indexOf("attributes_string_key","namespaces")] IS NOT NULL))`},
		model.Normal,
		[]model.Query{
			justWhere(`"type"='upgrade-assistant-reindex-operation' AND NOT ((has("attributes_string_key","namespace") AND "attributes_string_value"[indexOf("attributes_string_key","namespace")] IS NOT NULL) OR (has("attributes_string_key","namespaces") AND "attributes_string_value"[indexOf("attributes_string_key","namespaces")] IS NOT NULL))`),
		},
		[]string{qToStr(justWhere(`"type"='upgrade-assistant-reindex-operation' AND NOT ((has("attributes_string_key","namespace") AND "attributes_string_value"[indexOf("attributes_string_key","namespace")] IS NOT NULL) OR (has("attributes_string_key","namespaces") AND "attributes_string_value"[indexOf("attributes_string_key","namespaces")] IS NOT NULL))`))},
	},
	{ // [9]
		"Simple query string",
		`
		{
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
		}`,
		[]string{`"exception-list-agnostic.list_id" iLIKE '%endpoint_event_filters%'`},
		model.Normal,
		[]model.Query{justWhere(`"exception-list-agnostic.list_id" iLIKE '%endpoint_event_filters%'`)},
		[]string{qToStr(justWhere(`"exception-list-agnostic.list_id" iLIKE '%endpoint_event_filters%'`))},
	},
	{ // [10]
		"Simple query string wildcard",
		`
		{
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
		}`,
		[]string{`"message" iLIKE '%ingest-agent-policies%'`},
		model.Normal,
		[]model.Query{justWhere(`"message" iLIKE '%ingest-agent-policies%'`)},
		[]string{qToStr(justWhere(`"message" iLIKE '%ingest-agent-policies%'`))},
	},
	{ // [11]
		"Simple wildcard",
		`
		{
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
		}`,
		[]string{`"task.taskType" iLIKE 'alerting:%'`},
		model.Normal,
		[]model.Query{justWhere(`"task.taskType" iLIKE 'alerting:%'`)},
		[]string{qToStr(justWhere(`"task.taskType" iLIKE 'alerting:%'`))},
	},
	{ // [12]
		"Simple prefix ver1",
		`
		{
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
		}`,
		[]string{`"alert.actions.actionRef" iLIKE 'preconfigured:%'`},
		model.Normal,
		[]model.Query{justWhere(`"alert.actions.actionRef" iLIKE 'preconfigured:%'`)},
		[]string{qToStr(justWhere(`"alert.actions.actionRef" iLIKE 'preconfigured:%'`))},
	},
	{ // [13]
		"Simple prefix ver2",
		`
		{
			"query": {
				"prefix" : { "user" : "ki" }
			}
		}`,
		[]string{`"user" iLIKE 'ki%'`},
		model.Normal,
		[]model.Query{justWhere(`"user" iLIKE 'ki%'`)},
		[]string{qToStr(justWhere(`"user" iLIKE 'ki%'`))},
	},
	{ // [14]
		"Query string",
		`
		{
			"query_string": {
				"fields": [
					"message"
				],
				"query": "* logged"
			}
		}`,
		[]string{`"message" iLIKE '%%%' OR "message" iLIKE '%logged%'`},
		model.Normal,
		[]model.Query{justWhere(`"message" iLIKE '%%%' OR "message" iLIKE '%logged%'`)},
		[]string{qToStr(justWhere(`"message" iLIKE '%%%' OR "message" iLIKE '%logged%'`))},
	},
	{ // [15]
		"Empty bool",
		`
		{
			"bool": {
				"must": [],
				"filter": [],
				"should": [],
				"must_not": []
			}
		}`,
		[]string{""},
		model.Normal,
		[]model.Query{newSimplestQuery()},
		[]string{qToStr(newSimplestQuery())},
	},
	{ // [16]
		"Simplest 'match_phrase'",
		`{
			"query": {
				"match_phrase": {
					"message": "this is a test"
				}
			}
		}`,
		[]string{`"message" iLIKE '%this%' OR "message" iLIKE '%is%' OR "message" iLIKE '%a%' OR "message" iLIKE '%test%'`},
		model.Normal,
		[]model.Query{justWhere(`"message" iLIKE '%this%' OR "message" iLIKE '%is%' OR "message" iLIKE '%a%' OR "message" iLIKE '%test%'`)},
		[]string{qToStr(justWhere(`"message" iLIKE '%this%' OR "message" iLIKE '%is%' OR "message" iLIKE '%a%' OR "message" iLIKE '%test%'`))},
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
			}
		}`,
		[]string{`"message" iLIKE '%this%' OR "message" iLIKE '%is%' OR "message" iLIKE '%a%' OR "message" iLIKE '%test%'`},
		model.Normal,
		[]model.Query{justWhere(`"message" iLIKE '%this%' OR "message" iLIKE '%is%' OR "message" iLIKE '%a%' OR "message" iLIKE '%test%'`)},
		[]string{qToStr(justWhere(`"message" iLIKE '%this%' OR "message" iLIKE '%is%' OR "message" iLIKE '%a%' OR "message" iLIKE '%test%'`))},
	},
	{ // [18]
		"Simple nested",
		`
		{
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
		}`,
		[]string{`"references.type"='tag'`},
		model.Normal,
		[]model.Query{justWhere(`"references.type"='tag'`)},
		[]string{qToStr(justWhere(`"references.type"='tag'`))},
	},
	{ // [19]
		"TODO bad answer?",
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
			"runtime_mappings": {}
		  }
		`,
		[]string{
			`"message" iLIKE '%user%' AND ("@timestamp">=parseDateTime64BestEffort('2024-01-22T09:26:10.299Z') AND "@timestamp"<=parseDateTime64BestEffort('2024-01-22T09:41:10.299Z'))`,
			`"message" iLIKE '%user%' AND ("@timestamp"<=parseDateTime64BestEffort('2024-01-22T09:41:10.299Z') AND "@timestamp">=parseDateTime64BestEffort('2024-01-22T09:26:10.299Z'))`,
		},
		model.Count,
		[]model.Query{
			justWhere(`"message" iLIKE '%user%' AND ("@timestamp">=parseDateTime64BestEffort('2024-01-22T09:26:10.299Z') AND "@timestamp"<=parseDateTime64BestEffort('2024-01-22T09:41:10.299Z'))`),
			justWhere(`"message" iLIKE '%user%' AND ("@timestamp"<=parseDateTime64BestEffort('2024-01-22T09:41:10.299Z') AND "@timestamp">=parseDateTime64BestEffort('2024-01-22T09:26:10.299Z'))`),
		},
		[]string{`SELECT count() FROM "logs-generic-default" WHERE "message" iLIKE '%user%' AND ("@timestamp".=parseDateTime64BestEffort('2024-01-22T09:..:10.299Z') AND "@timestamp".=parseDateTime64BestEffort('2024-01-22T09:..:10.299Z'))`},
	},
	{ // [20]
		"termWithCompoundValue",
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
			"runtime_mappings": {}
		  }
		`,
		[]string{
			`"service.name"='admin' AND ("@timestamp"<=parseDateTime64BestEffort('2024-01-22T14:49:35.873Z') AND "@timestamp">=parseDateTime64BestEffort('2024-01-22T14:34:35.873Z'))`,
			`"service.name"='admin' AND ("@timestamp">=parseDateTime64BestEffort('2024-01-22T14:34:35.873Z') AND "@timestamp"<=parseDateTime64BestEffort('2024-01-22T14:49:35.873Z'))`,
		},
		model.Count,
		[]model.Query{
			justWhere(`"service.name"='admin' AND ("@timestamp"<=parseDateTime64BestEffort('2024-01-22T14:49:35.873Z') AND "@timestamp">=parseDateTime64BestEffort('2024-01-22T14:34:35.873Z'))`),
			justWhere(`"service.name"='admin' AND ("@timestamp">=parseDateTime64BestEffort('2024-01-22T14:34:35.873Z') AND "@timestamp"<=parseDateTime64BestEffort('2024-01-22T14:49:35.873Z'))`),
		},
		[]string{`SELECT count() FROM "logs-generic-default" WHERE "service.name"='admin' AND ("@timestamp".=parseDateTime64BestEffort('2024-01-22T14:..:35.873Z') AND "@timestamp".=parseDateTime64BestEffort('2024-01-22T14:..:35.873Z'))`},
	},
	{ // [21]
		"Count() as /_search query. With filter", // response should be just ["hits"]["total"]["value"] == result of count()
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
		"timeout": "1000ms"
	}`,
		[]string{`("message" iLIKE '%User%' OR "message" iLIKE '%logged%' OR "message" iLIKE '%out%') AND "host.name" iLIKE '%poseidon%' AND ("@timestamp">=parseDateTime64BestEffort('2024-01-29T15:36:36.491Z') AND "@timestamp"<=parseDateTime64BestEffort('2024-01-29T18:11:36.491Z'))`,
			`("message" iLIKE '%User%' OR "message" iLIKE '%logged%' OR "message" iLIKE '%out%') AND "host.name" iLIKE '%poseidon%' AND ("@timestamp"<=parseDateTime64BestEffort('2024-01-29T18:11:36.491Z') AND "@timestamp">=parseDateTime64BestEffort('2024-01-29T15:36:36.491Z'))`},
		model.Count,
		[]model.Query{
			justWhere(`("message" iLIKE '%User%' OR "message" iLIKE '%logged%' OR "message" iLIKE '%out%') AND "host.name" iLIKE '%poseidon%' AND ("@timestamp">=parseDateTime64BestEffort('2024-01-29T15:36:36.491Z') AND "@timestamp"<=parseDateTime64BestEffort('2024-01-29T18:11:36.491Z'))`),
			justWhere(`("message" iLIKE '%User%' OR "message" iLIKE '%logged%' OR "message" iLIKE '%out%') AND "host.name" iLIKE '%poseidon%' AND ("@timestamp"<=parseDateTime64BestEffort('2024-01-29T18:11:36.491Z') AND "@timestamp">=parseDateTime64BestEffort('2024-01-29T15:36:36.491Z'))`),
		},
		[]string{`SELECT count() FROM "logs-generic-default" WHERE ("message" iLIKE '%User%' OR "message" iLIKE '%logged%' OR "message" iLIKE '%out%') AND "host.name" iLIKE '%poseidon%' AND ("@timestamp".=parseDateTime64BestEffort('2024-01-29T1.:..:36.491Z') AND "@timestamp".=parseDateTime64BestEffort('2024-01-29T1.:..:36.491Z'))`},
	},
	{ // [22]
		"Count() as /_search or /logs-*-*/_search query. Without filter", // response should be just ["hits"]["total"]["value"] == result of count()
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
			"terminate_after": 100000,
			"timeout": "1000ms"
		}`,
		[]string{`"message" iLIKE '%user%' AND ("@timestamp">=parseDateTime64BestEffort('2024-01-22T09:26:10.299Z') AND "@timestamp"<=parseDateTime64BestEffort('2024-01-22T09:41:10.299Z'))`,
			`"message" iLIKE '%user%' AND ("@timestamp"<=parseDateTime64BestEffort('2024-01-22T09:41:10.299Z') AND "@timestamp">=parseDateTime64BestEffort('2024-01-22T09:26:10.299Z'))`},
		model.Count,
		[]model.Query{
			justWhere(`"message" iLIKE '%user%' AND ("@timestamp">=parseDateTime64BestEffort('2024-01-22T09:26:10.299Z') AND "@timestamp"<=parseDateTime64BestEffort('2024-01-22T09:41:10.299Z'))`),
			justWhere(`"message" iLIKE '%user%' AND ("@timestamp"<=parseDateTime64BestEffort('2024-01-22T09:41:10.299Z') AND "@timestamp">=parseDateTime64BestEffort('2024-01-22T09:26:10.299Z'))`),
		},
		[]string{`SELECT count() FROM "logs-generic-default" WHERE "message" iLIKE '%user%' AND ("@timestamp".=parseDateTime64BestEffort('2024-01-22T09:..:10.299Z') AND "@timestamp".=parseDateTime64BestEffort('2024-01-22T09:..:10.299Z'))`},
	},
	{ // [23]
		"Count() as /_search query. With filter", // response should be just ["hits"]["total"]["value"] == result of count()
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
		"timeout": "1000ms"
	}`,
		[]string{`("message" iLIKE '%User%' OR "message" iLIKE '%logged%' OR "message" iLIKE '%out%') AND "host.name" iLIKE '%poseidon%' AND ("@timestamp">=parseDateTime64BestEffort('2024-01-29T15:36:36.491Z') AND "@timestamp"<=parseDateTime64BestEffort('2024-01-29T18:11:36.491Z'))`,
			`("message" iLIKE '%User%' OR "message" iLIKE '%logged%' OR "message" iLIKE '%out%') AND "host.name" iLIKE '%poseidon%' AND ("@timestamp"<=parseDateTime64BestEffort('2024-01-29T18:11:36.491Z') AND "@timestamp">=parseDateTime64BestEffort('2024-01-29T15:36:36.491Z'))`},
		model.Count,
		[]model.Query{
			justWhere(`("message" iLIKE '%User%' OR "message" iLIKE '%logged%' OR "message" iLIKE '%out%') AND "host.name" iLIKE '%poseidon%' AND ("@timestamp">=parseDateTime64BestEffort('2024-01-29T15:36:36.491Z') AND "@timestamp"<=parseDateTime64BestEffort('2024-01-29T18:11:36.491Z'))`),
			justWhere(`("message" iLIKE '%User%' OR "message" iLIKE '%logged%' OR "message" iLIKE '%out%') AND "host.name" iLIKE '%poseidon%' AND ("@timestamp"<=parseDateTime64BestEffort('2024-01-29T18:11:36.491Z') AND "@timestamp">=parseDateTime64BestEffort('2024-01-29T15:36:36.491Z'))`),
		},
		[]string{`SELECT count() FROM "logs-generic-default" WHERE ("message" iLIKE '%User%' OR "message" iLIKE '%logged%' OR "message" iLIKE '%out%') AND "host.name" iLIKE '%poseidon%' AND ("@timestamp".=parseDateTime64BestEffort('2024-01-29T1.:..:36.491Z') AND "@timestamp".=parseDateTime64BestEffort('2024-01-29T1.:..:36.491Z'))`},
	},
	{ // [24]
		"Count() as /_search or /logs-*-*/_search query. Without filter", // response should be just ["hits"]["total"]["value"] == result of count()
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
			"terminate_after": 100000,
			"timeout": "1000ms"
		}`,
		[]string{`"message" iLIKE '%user%' AND ("@timestamp">=parseDateTime64BestEffort('2024-01-22T09:26:10.299Z') AND "@timestamp"<=parseDateTime64BestEffort('2024-01-22T09:41:10.299Z'))`,
			`"message" iLIKE '%user%' AND ("@timestamp"<=parseDateTime64BestEffort('2024-01-22T09:41:10.299Z') AND "@timestamp">=parseDateTime64BestEffort('2024-01-22T09:26:10.299Z'))`},
		model.Count,
		[]model.Query{
			justWhere(`"message" iLIKE '%user%' AND ("@timestamp">=parseDateTime64BestEffort('2024-01-22T09:26:10.299Z') AND "@timestamp"<=parseDateTime64BestEffort('2024-01-22T09:41:10.299Z'))`),
			justWhere(`"message" iLIKE '%user%' AND ("@timestamp"<=parseDateTime64BestEffort('2024-01-22T09:41:10.299Z') AND "@timestamp">=parseDateTime64BestEffort('2024-01-22T09:26:10.299Z'))`),
		},
		[]string{`SELECT count() FROM "logs-generic-default" WHERE "message" iLIKE '%user%' AND ("@timestamp".=parseDateTime64BestEffort('2024-01-22T09:..:10.299Z') AND "@timestamp".=parseDateTime64BestEffort('2024-01-22T09:..:10.299Z'))`},
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
			}
		}`,
		[]string{
			`"@timestamp">=parseDateTime64BestEffort('2024-01-25T13:22:45.968Z') AND "@timestamp"<=parseDateTime64BestEffort('2024-01-25T13:37:45.968Z')`,
			`"@timestamp"<=parseDateTime64BestEffort('2024-01-25T13:37:45.968Z') AND "@timestamp">=parseDateTime64BestEffort('2024-01-25T13:22:45.968Z')`,
		},
		model.Normal,
		[]model.Query{
			justWhere(`"@timestamp">=parseDateTime64BestEffort('2024-01-25T13:22:45.968Z') AND "@timestamp"<=parseDateTime64BestEffort('2024-01-25T13:37:45.968Z')`),
			justWhere(`"@timestamp"<=parseDateTime64BestEffort('2024-01-25T13:37:45.968Z') AND "@timestamp">=parseDateTime64BestEffort('2024-01-25T13:22:45.968Z')`),
		},
		[]string{`SELECT "message" FROM "logs-generic-default" WHERE ("@timestamp".=parseDateTime64BestEffort('2024-01-25T13:..:45.968Z') AND "@timestamp".=parseDateTime64BestEffort('2024-01-25T13:..:45.968Z')) AND (has("attributes_string_key","summary") AND "attributes_string_value"[indexOf("attributes_string_key","summary")] IS NOT NULL) AND NOT (has("attributes_string_key","run_once") AND "attributes_string_value"[indexOf("attributes_string_key","run_once")] IS NOT NULL)`},
	},
}

var TestSearchFilter = []SearchTestCase{
	{
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
		[]model.Query{
			justWhere(``),
			justWhere(``),
		},
		[]string{
			"SELECT count() FROM " + quotedTableName,
			"SELECT " + clickhouse.TimestampGroupBy("@timestamp", clickhouse.DateTime64, 30*time.Second) + ", count() FROM " + quotedTableName + "  GROUP BY (" + clickhouse.TimestampGroupBy("@timestamp", clickhouse.DateTime64, 30*time.Second) + ") ORDER BY (" + clickhouse.TimestampGroupBy("@timestamp", clickhouse.DateTime64, 30*time.Second) + ")",
		},
	},
	{
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
		[]model.Query{
			justWhere(``),
			justWhere(``),
		},
		[]string{
			"SELECT count() FROM " + quotedTableName + ` WHERE "@timestamp">subDate(now(), INTERVAL 15 minute)`,
			"SELECT " + clickhouse.TimestampGroupBy("@timestamp", clickhouse.DateTime64, 30*time.Second) + `, count() FROM ` + quotedTableName + ` WHERE "@timestamp">subDate(now(), INTERVAL 15 minute)  GROUP BY (` + clickhouse.TimestampGroupBy("@timestamp", clickhouse.DateTime64, 30*time.Second) + `) ORDER BY (` + clickhouse.TimestampGroupBy("@timestamp", clickhouse.DateTime64, 30*time.Second) + `)`,
		},
	},
}
