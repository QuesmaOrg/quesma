// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package frontend_connectors

import (
	"github.com/QuesmaOrg/quesma/platform/types"
	"github.com/QuesmaOrg/quesma/platform/util"
	mux "github.com/QuesmaOrg/quesma/platform/v2/core"
	"github.com/stretchr/testify/assert"
	"testing"
)

const kibanaAlerts = `{
  "aggs": {
    "endpoint_alert_count": {
      "cardinality": {
        "field": "event.id"
      }
    }
  },
  "pit": {
    "id": "gcSHBAEvLmludGVybmFsLmFsZXJ0cy1zZWN1cml0eS5hbGVydHMtZGVmYXVsdC0wMDAwMDEWRWdvdFQwblRUN0tNaFk4SWc3TDRSQQAWMEdVOVNnVk1TV0t3ckRxbUpkb3BzZwAAAAAAAASdvBZGQXQwWTUyTVRKQ29zaDJ1elRhWFR3AAEWRWdvdFQwblRUN0tNaFk4SWc3TDRSQQAA"
  },
  "query": {
    "bool": {
      "filter": [
        {
          "bool": {
            "should": [
              {
                "match_phrase": {
                  "event.module": "endpoint"
                }
              }
            ]
          }
        },
        {
          "bool": {
            "should": [
              {
                "match_phrase": {
                  "kibana.alert.rule.parameters.immutable": "true"
                }
              }
            ]
          }
        },
        {
          "range": {
            "@timestamp": {
              "gte": "now-3h",
              "lte": "now"
            }
          }
        }
      ]
    }
  },
  "size": 1000,
  "sort": [
    {
      "@timestamp": {
        "format": "strict_date_optional_time_nanos",
        "order": "asc"
      }
    },
    {
      "_shard_doc": "desc"
    }
  ],
  "track_total_hits": false
}
`

const nonKibanaAlerts = `
{
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
							"gte": "2022-01-23T14:43:19.481Z",
							"lte": "2025-01-23T14:58:19.481Z"
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

`

const migrationQuery = `{
"query": {
	"bool": {
		"should": [
			{
				"bool": {
					"must": [
						{
							"term": {
								"type": "core-usage-diag"
							}
						},
						{
							"bool": {
								"should": [
									{
										"bool": {
											"must_not": [
												{
													"exists": {
														"field": "typeMigrationVersion"
													}
												},
												{
													"exists": {
														"field": "migrationVersion.core-usage-diag"
													}
												}
											]
										}
									},
									{
										"bool": {
											"must": {
												"exists": {
													"field": "migrationVersion"
												}
											},
											"must_not": {
												"term": {
													"migrationVersion.core-usage-diag": "7.14.1"
												}
											}
										}
									},
									{
										"range": {
											"typeMigrationVersion": {
												"lt": "7.14.1"
											}
										}
									}
								]
							}
						}
					]
				}
			},
			{
				"bool": {
					"must": [
						{
							"term": {
								"type": "legacy-url-alias"
							}
						},
						{
							"bool": {
								"should": [
									{
										"bool": {
											"must_not": [
												{
													"exists": {
														"field": "typeMigrationVersion"
													}
												},
												{
													"exists": {
														"field": "migrationVersion.legacy-url-alias"
													}
												}
											]
										}
									},
									{
										"bool": {
											"must": {
												"exists": {
													"field": "migrationVersion"
												}
											},
											"must_not": {
												"term": {
													"migrationVersion.legacy-url-alias": "8.2.0"
												}
											}
										}
									},
									{
										"range": {
											"typeMigrationVersion": {
												"lt": "8.2.0"
											}
										}
									}
								]
							}
						}
					]
				}
			},
			{
				"bool": {
					"must": [
						{
							"term": {
								"type": "config"
							}
						},
						{
							"bool": {
								"should": [
									{
										"bool": {
											"must_not": [
												{
													"exists": {
														"field": "typeMigrationVersion"
													}
												},
												{
													"exists": {
														"field": "migrationVersion.config"
													}
												}
											]
										}
									},
									{
										"bool": {
											"must": {
												"exists": {
													"field": "migrationVersion"
												}
											},
											"must_not": {
												"term": {
													"migrationVersion.config": "8.9.0"
												}
											}
										}
									},
									{
										"range": {
											"typeMigrationVersion": {
												"lt": "8.9.0"
											}
										}
									}
								]
							}
						}
					]
				}
			},
			{
				"bool": {
					"must": [
						{
							"term": {
								"type": "task"
							}
						},
						{
							"bool": {
								"should": [
									{
										"bool": {
											"must_not": [
												{
													"exists": {
														"field": "typeMigrationVersion"
													}
												},
												{
													"exists": {
														"field": "migrationVersion.task"
													}
												}
											]
										}
									},
									{
										"bool": {
											"must": {
												"exists": {
													"field": "migrationVersion"
												}
											},
											"must_not": {
												"term": {
													"migrationVersion.task": "8.8.0"
												}
											}
										}
									},
									{
										"range": {
											"typeMigrationVersion": {
												"lt": "8.8.0"
											}
										}
									}
								]
							}
						}
					]
				}
			}
		]
	}
}
}`

func TestMatchAgainstKibanaAlerts(t *testing.T) {

	tests := []struct {
		name     string
		body     string
		expected bool
	}{
		{"{kibana alerts", kibanaAlerts, false},
		{"non kibana alerts", nonKibanaAlerts, true},
		{"migration", migrationQuery, false},
	}

	for i, test := range tests {
		t.Run(util.PrettyTestName(test.name, i), func(tt *testing.T) {

			req := &mux.Request{Body: test.body}

			req.ParsedBody = types.ParseRequestBody(test.body)

			actual := matchAgainstKibanaInternal().Matches(req)
			assert.Equal(tt, test.expected, actual.Matched)
		})

	}

}
