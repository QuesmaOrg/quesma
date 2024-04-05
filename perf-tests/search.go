package main

import "time"

func getSearchAggregateQuery(duration time.Duration) []byte {
	now := time.Now()

	body := []byte(`{
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
              "format": "strict_date_optional_time",
              "gte": ` + `"` + now.Add(duration).Format("2006-01-02T15:04:05.726Z") + `"` + `,
              "lte": ` + `"` + now.Format("2006-01-02T15:04:05.726Z") + `"` + `
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
}`)
	return body
}
