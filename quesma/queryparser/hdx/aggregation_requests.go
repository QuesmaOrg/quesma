package hdx

var a = `
{
"_source": {
"excludes": []
},
"aggs": {
"2": {
"filters": {
"filters": {
"@timestamp \u003c= now": {
"bool": {
"filter": [
{
"bool": {
"minimum_should_match": 1,
"should": [
{
"range": {
"@timestamp": {
"lte": "now",
"time_zone": "Europe/Warsaw"
}
}
}
]
}
}
],
"must": [],
"must_not": [],
"should": []
}
}
}
}
}
},
"fields": [
{
"field": "@timestamp",
"format": "date_time"
},
{
"field": "customer_birth_date",
"format": "date_time"
},
"..."
],
"query": {
"bool": {
"filter": [
{
"range": {
"order_date": {
"format": "strict_date_optional_time",
"gte": "2024-10-02T12:35:43.015Z",
"lte": "2024-10-02T12:50:43.015Z"
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
}`

var b = `
{
  "_source": {
    "excludes": []
  },
  "aggs": {
    "2": {
      "filter": {
        "bool": {
          "filter": [
            {
              "bool": {
                "minimum_should_match": 1,
                "should": [
                  {
                    "range": {
                      "@timestamp": {
                        "lte": "now",
                        "time_zone": "Europe/Warsaw"
                      }
                    }
                  }
                ]
              }
            }
          ],
          "must": [],
          "must_not": [],
          "should": []
        }
      }
    }
  },
  "query": {
    "bool": {
      "filter": [
        {
          "range": {
            "order_date": {
              "format": "strict_date_optional_time",
              "gte": "2024-10-02T12:35:43.015Z",
              "lte": "2024-10-02T12:50:43.015Z"
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
`
