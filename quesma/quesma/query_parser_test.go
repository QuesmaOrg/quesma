package quesma

import (
	"github.com/stretchr/testify/assert"
	"slices"
	"testing"
)

const sel = "SELECT * FROM " + tableName + " WHERE "

var tests = []struct {
	name      string
	queryJson string
	wantSql   string
}{
	{
		"Match all",
		`{
		"query": {
			"match_all": {}
		}
	}`,
		"SELECT * FROM " + tableName,
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
		sel + "type=task",
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
					"term": {
				  		"task.enabled": true
					}
			  	}
			]
		}
	}`,
		sel + "type=task AND task.enabled=true",
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
		"TODO",
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
		"TODO",
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
		"TODO",
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
		"TODO",
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
		sel + "status=pending",
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
		"TODO",
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
		"TODO",
	},
	{
		"A bit harder simple query string, but seems doable",
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
		"TODO",
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
		"TODO",
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
		"TODO",
	},
	{
		"Simple prefix ver2",
		`{
			"query": {
				"prefix" : { "user" : "ki" }
			}
		}`,
		"TODO",
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
		"TODO",
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
		"TODO",
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
		"SELECT * FROM " + tableName,
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
		}
	}`,
		"TODO",
	},
}

func Test(t *testing.T) {
	supported := []int{0, 1, 2}
	for i, tt := range tests {
		if !slices.Contains(supported, i) {
			continue
		}
		t.Run(tt.name, func(t *testing.T) {
			query := parseQuery(tt.queryJson)
			assert.True(t, query.canParse)
			assert.Equal(t, tt.wantSql, query.sql)
		})
	}
}
