package queryparser

import (
	"mitmproxy/quesma/clickhouse"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

const sel = "SELECT * FROM " + TableName + " WHERE "

type testCase struct {
	name      string
	queryJson string
	wantSql   any
}

var testsStringAttr = []testCase{
	{
		"Match all",
		`{
			"query": {
				"match_all": {}
			}
		}`,
		"SELECT * FROM " + TableName,
	},
	{
		"Term as dictionary",
		`{
				"bool": {
					"filter":
					{
						"term": {
							"type": "task"
						}
					}
				}
			}`,
		sel + `"type"='task'`,
	},
	{
		"Term as array",
		`{
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
		sel + `("type"='task' AND ("task.enabled"=true OR "task.enabled"=54))`,
	},
	{
		"Sample log query",
		`{
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
					}
				],
				"should": [],
				"must_not": []
			}
		}`,
		[]string{sel + `("message" iLIKE '%user%' AND ("@timestamp">=parseDateTime64BestEffort('2024-01-17T10:28:18.815Z') AND "@timestamp"<=parseDateTime64BestEffort('2024-01-17T10:43:18.815Z')))`,
			sel + `("message" iLIKE '%user%' AND ("@timestamp"<=parseDateTime64BestEffort('2024-01-17T10:43:18.815Z') AND "@timestamp">=parseDateTime64BestEffort('2024-01-17T10:28:18.815Z')))`,
		},
	},
	{
		"Multiple bool query",
		`{
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
			sel + `((("user.id"='kimchy' AND "tags"='production') AND ("tags"='env1' OR "tags"='deployed')) AND NOT ("age"<=20 AND "age">=10))`,
			sel + `((("user.id"='kimchy' AND "tags"='production') AND ("tags"='env1' OR "tags"='deployed')) AND NOT ("age">=10 AND "age"<=20))`,
		},
	},
	{
		"Match phrase",
		`{
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
		sel + `"host_name.keyword" iLIKE '%prometheus%'`,
	},
	{
		"Match",
		`{
				"query": {
					"match": {
						"message": "this is a test"
					}
				}
			}`,
		sel + `("message" iLIKE '%this%' OR "message" iLIKE '%is%' OR "message" iLIKE '%a%' OR "message" iLIKE '%test%')`,
	},
	{
		"Terms",
		`{
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
		sel + `"status"='pending'`,
	},
	{
		"Exists",
		`{
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
		sel + `("type"='upgrade-assistant-reindex-operation' AND NOT (has("attributes_string_key","namespace") AND "attributes_string_value"[indexOf("attributes_string_key","namespace")] IS NOT NULL OR has("attributes_string_key","namespaces") AND "attributes_string_value"[indexOf("attributes_string_key","namespaces")] IS NOT NULL))`,
	},
	{
		"Simple query string",
		`{
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
		sel + `"exception-list-agnostic.list_id" iLIKE '%endpoint_event_filters%'`,
	},
	{
		"Simple query string wildcard",
		`{
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
		sel + `"message" iLIKE '%ingest-agent-policies%'`,
	},
	{
		"Simple wildcard",
		`{
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
		sel + `"task.taskType" iLIKE 'alerting:%'`,
	},
	{
		"Simple prefix ver1",
		`{
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
		sel + `"alert.actions.actionRef" iLIKE 'preconfigured:%'`,
	},
	{
		"Simple prefix ver2",
		`{
				"query": {
					"prefix" : { "user" : "ki" }
				}
			}`,
		sel + `"user" iLIKE 'ki%'`,
	},
	{
		"Query string",
		`{
				"query_string": {
					"fields": [
						"message"
					],
					"query": "* logged"
				}
			}`,
		sel + `("message" iLIKE '%%%' OR "message" iLIKE '%logged%')`,
	},
	{
		"Empty bool",
		`{
				"bool": {
					"must": [],
					"filter": [],
					"should": [],
					"must_not": []
				}
			}`,
		"SELECT * FROM " + TableName,
	},
	{
		"Simple nested",
		`{
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
		sel + `"references.type"='tag'`,
	},
	{
		"user",
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
								  "lte": "2024-12-22T09:41:10.299Z"
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
			sel + `("message" iLIKE '%user%' AND ("@timestamp">=parseDateTime64BestEffort('2024-01-22T09:26:10.299Z') AND "@timestamp"<=parseDateTime64BestEffort('2024-12-22T09:41:10.299Z')))`,
			sel + `("message" iLIKE '%user%' AND ("@timestamp"<=parseDateTime64BestEffort('2024-12-22T09:41:10.299Z') AND "@timestamp">=parseDateTime64BestEffort('2024-01-22T09:26:10.299Z')))`,
		},
	},
	{
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
			sel + `("service.name"='admin' AND ("@timestamp"<=parseDateTime64BestEffort('2024-01-22T14:49:35.873Z') AND "@timestamp">=parseDateTime64BestEffort('2024-01-22T14:34:35.873Z')))`,
			sel + `("service.name"='admin' AND ("@timestamp">=parseDateTime64BestEffort('2024-01-22T14:34:35.873Z') AND "@timestamp"<=parseDateTime64BestEffort('2024-01-22T14:49:35.873Z')))`,
		},
	},
}

var testsNoAttrs = []testCase{
	{
		"Test empty ANDs, ORs and NOTs",
		`{
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
			`SELECT * FROM "logs-generic-default" WHERE ("@timestamp">=parseDateTime64BestEffort('2024-01-25T13:22:45.968Z') AND "@timestamp"<=parseDateTime64BestEffort('2024-01-25T13:37:45.968Z'))`,
			`SELECT * FROM "logs-generic-default" WHERE ("@timestamp"<=parseDateTime64BestEffort('2024-01-25T13:37:45.968Z') AND "@timestamp">=parseDateTime64BestEffort('2024-01-25T13:22:45.968Z'))`,
		},
	},
}

// TODO:
// 1. 14th test, "Query string". "(message LIKE '%%%' OR message LIKE '%logged%')", is it really
//    what should be? According to docs, I think so... Maybe test in Kibana?

func TestQueryParserStringAttrConfig(t *testing.T) {
	testTable, err := clickhouse.NewTable(`CREATE TABLE `+TableName+`
		( "message" String, "timestamp" DateTime64(3, 'UTC') )
		ENGINE = Memory`,
		clickhouse.NewNoTimestampOnlyStringAttrCHConfig(),
	)
	if err != nil {
		t.Fatal(err)
	}
	lm := clickhouse.NewLogManager(clickhouse.TableMap{TableName: testTable}, make(clickhouse.TableMap))
	cw := ClickhouseQueryTranslator{lm}
	for _, tt := range testsStringAttr {
		t.Run(tt.name, func(t *testing.T) {
			query := cw.parseQuery(tt.queryJson)
			assert.True(t, query.CanParse)
			switch tt.wantSql.(type) {
			case string:
				assert.Equal(t, tt.wantSql, query.Sql)
			case []string:
				assert.Contains(t, tt.wantSql, query.Sql)
			}
		})
	}
}

func TestQueryParserNoAttrsConfig(t *testing.T) {
	testTable, err := clickhouse.NewTable(`CREATE TABLE `+TableName+`
		( "message" String, "timestamp" DateTime64(3, 'UTC') )
		ENGINE = Memory`,
		clickhouse.NewCHTableConfigNoAttrs(),
	)
	if err != nil {
		t.Fatal(err)
	}
	lm := clickhouse.NewLogManager(clickhouse.TableMap{TableName: testTable}, make(clickhouse.TableMap))
	cw := ClickhouseQueryTranslator{lm}
	for _, tt := range testsNoAttrs {
		t.Run(tt.name, func(t *testing.T) {
			query := cw.parseQuery(tt.queryJson)
			assert.True(t, query.CanParse)
			switch tt.wantSql.(type) {
			case string:
				assert.Equal(t, tt.wantSql, query.Sql)
			case []string:
				assert.Contains(t, tt.wantSql, query.Sql)
			}
		})
	}
}

func TestFilterNonEmpty(t *testing.T) {
	tests := []struct {
		array    []string
		filtered []string
	}{
		{
			[]string{"", "", ""},
			[]string{},
		},
		{
			[]string{"", "a", ""},
			[]string{"a"},
		},
		{
			[]string{"a", "b", "c"},
			[]string{"a", "b", "c"},
		},
	}

	for i, tt := range tests {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			assert.Equal(t, tt.filtered, filterNonEmpty(tt.array))
		})
	}
}

func TestOrAndAnd(t *testing.T) {
	tests := []struct {
		stmts []string
		want  string
	}{
		{
			[]string{"a", "b", "c"},
			"(a AND b AND c)",
		},
		{
			[]string{"a", "", "", "b"},
			"(a AND b)",
		},
		{
			[]string{"", "", "a", "", "", "", ""},
			"a",
		},
		{
			[]string{"", "", "", "", "", "", "", "", "", "", "", ""},
			"",
		},
	}

	// copy, because and() and or() modify the slice
	for i, tt := range tests {
		t.Run("AND "+strconv.Itoa(i), func(t *testing.T) {
			b := make([]string, len(tt.stmts))
			copy(b, tt.stmts)
			assert.Equal(t, tt.want, and(b))
		})
	}
	for i, tt := range tests {
		t.Run("OR "+strconv.Itoa(i), func(t *testing.T) {
			assert.Equal(t, strings.ReplaceAll(tt.want, "AND", "OR"), or(tt.stmts))
		})
	}
}
