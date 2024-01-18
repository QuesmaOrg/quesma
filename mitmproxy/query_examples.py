import query
import json

LOG_QUERY_1="""
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
      }
    ],
    "should": [],
    "must_not": []
  }
}
"""

LOG_QUERY_2 = """
{
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
}
"""

LOG_QUERY_3 = """
{
  "bool": {
    "filter":
      {
        "term": {
          "type": "task"
        }
      }
  }
}
"""

LOG_QUERY_4 = """
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
}
"""

LOG_QUERY_5 ="""
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
}
"""

LOG_QUERY_6 = """
{
  "query": {
    "match": {
      "message": "this is a test"
    }
  }
}
"""

LOG_QUERY_7 = """
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
}
"""

LOG_QUERY_8 = """
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
}
"""

LOG_QUERY_9 = """
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
}
"""

LOG_QUERY_10 = """
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
}
"""

LOG_QUERY_11 = """
{
  "query": {
    "match_all": {}
  }
}
"""

LOG_QUERY_12 = """
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
}
"""

LOG_QUERY_13 = """
{
  "bool": {"must": [
     {
       "prefix": {
         "alert.actions.actionRef": {
           "value": "preconfigured:"
         }
       }
     }
   ]
  }
}
"""

LOG_QUERY_14 = """
{
  "query": {
    "prefix" : { "user" : "ki" }
  }
}
"""

LOG_QUERY_15 = """
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
}
"""

LOG_QUERY_16 = """
{
  "query_string": {
    "fields": [
      "message"
    ],
    "query": "* logged"
  }
}
"""

LOG_QUERY_17 = """
{
  "bool": {
    "must": [],
    "filter": [],
    "should": [],
    "must_not": []
  }
}
"""

LOG_QUERY_18 = """
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
}
"""

test_number = 1

def verify_result(human_readable_name, result):
    if not result.can_parse:
        print(test_number, "FAIL:", human_readable_name, "cannot be parsed", result)
    else:
        print(test_number, "PASS:", human_readable_name, "can be parsed", result)

def ensure_correct(human_readable_name, json_to_parse):
    global test_number
    result = query.safe_parse_query(json.loads(json_to_parse))
    verify_result(human_readable_name, result)
    test_number += 1

if __name__ == "__main__":
    ensure_correct("Sample log query", LOG_QUERY_1)
    ensure_correct("Term as array", LOG_QUERY_2)
    ensure_correct("Term as dictionary", LOG_QUERY_3)
    ensure_correct("Multiple bool query", LOG_QUERY_4)
    ensure_correct("Match phrase", LOG_QUERY_5)
    ensure_correct("Match", LOG_QUERY_6)
    ensure_correct("Terms", LOG_QUERY_7)
    ensure_correct("Exists", LOG_QUERY_8)
    ensure_correct("Simple query string", LOG_QUERY_9)
    
    ensure_correct("A bit harder simple query string, but seems doable", LOG_QUERY_10)
    ensure_correct("Match all", LOG_QUERY_11)
    ensure_correct("Simple wildcard", LOG_QUERY_12)
    ensure_correct("Simple prefix ver1", LOG_QUERY_13)
    ensure_correct("Simple prefix ver2", LOG_QUERY_14)
    ensure_correct("Simple query string wildcard", LOG_QUERY_15)
    ensure_correct("Query string", LOG_QUERY_16)
    ensure_correct("Empty bool", LOG_QUERY_17)
    ensure_correct("Simple Nested", LOG_QUERY_18)

