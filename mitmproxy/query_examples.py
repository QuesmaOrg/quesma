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

def verify_result(human_readable_name, result):
    if not result.can_parse:
        print("FAIL:", human_readable_name, "cannot parse", result)
    else:
        print("PASS:", human_readable_name, "can parse", result)

def ensure_correct(human_readable_name, json_to_parse):
    result = query.safe_parse_query(json.loads(json_to_parse))
    verify_result(human_readable_name, result)

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
