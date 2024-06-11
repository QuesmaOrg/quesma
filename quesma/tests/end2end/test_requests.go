package end2end

var testRequests = []string{
	`{
		"_source": {
			"excludes": []
		},
		"aggs": {
			"0": {
				"date_histogram": {
					"field": "timestamp",
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
	},
	{
		"field": "timestamp",
		"format": "date_time"
	},
	{
		"field": "utc_time",
		"format": "date_time"
	}
	],
		"query": {
		"bool": {
		"filter": [
	{
		"range": {
		"timestamp": {
		"format": "strict_date_optional_time",
		"gte": "2022-05-11T12:17:14.373Z",
		"lte": "2024-06-11T17:32:14.373Z"
	}
	}
	}
	],
		"must": [],
		"must_not": [],
		"should": []
	}
	},
		"runtime_mappings": {
		"hour_of_day": {
		"script": {
		"source": "emit(doc['timestamp'].value.getHour());"
	},
		"type": "long"
	}
	},
		"script_fields": {},
		"size": 0,
		"stored_fields": [
		"*"
	],
		"track_total_hits": true
	}`,
}
