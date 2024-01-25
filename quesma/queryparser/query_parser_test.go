package queryparser

import (
	"mitmproxy/quesma/clickhouse"
	"testing"

	"github.com/stretchr/testify/assert"
)

const sel = "SELECT * FROM " + TableName + " WHERE "

var testsQueryParser = []struct {
	name      string
	queryJson string
	wantSql   any
}{
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

// TODO:
// 1. 14th test, "Query string". "(message LIKE '%%%' OR message LIKE '%logged%')", is it really
//    what should be? According to docs, I think so... Maybe test in Kibana?

func TestQueryParser(t *testing.T) {
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
	for _, tt := range testsQueryParser {
		t.Run(tt.name, func(t *testing.T) {
			query := cw.parseQuery(tt.queryJson)
			assert.True(t, query.CanParse)
			switch tt.wantSql.(type) {
			case string:
				assert.Equal(t, tt.wantSql, query.Sql)
			case []string:
				assert.Contains(t, tt.wantSql, query.Sql)
			}
			//fmt.Println(i, ":", query.sql)
		})
	}
}
