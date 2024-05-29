package testdata

import (
	"mitmproxy/quesma/model"
)

var OpensearchSearchTests = []SearchTestCase{
	{
		Name: "Basic Explorer request",
		QueryJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"2": {
					"date_histogram": {
						"field": "-@timestamp",
						"fixed_interval": "30s",
						"min_doc_count": 1,
						"time_zone": "Europe/Warsaw"
					}
				}
			},
			"docvalue_fields": [
				{
					"field": "-@timestamp",
					"format": "date_time"
				}
			],
			"highlight": {
				"fields": {
					"*": {}
				},
				"fragment_size": 2147483647,
				"post_tags": [
					"@/opensearch-dashboards-highlighted-field@"
				],
				"pre_tags": [
					"@opensearch-dashboards-highlighted-field@"
				]
			},
			"query": {
				"bool": {
					"filter": [
						{
							"match_all": {}
						},
						{
							"range": {
								"-@timestamp": {
									"format": "strict_date_optional_time",
									"gte": "2024-04-04T13:18:18.149Z",
									"lte": "2024-04-04T13:33:18.149Z"
								}
							}
						}
					],
					"must": [],
					"must_not": [],
					"should": []
				}
			},
			"script_fields": {},
			"size": 500,
			"sort": [
				{
					"-@timestamp": {
						"order": "desc",
						"unmapped_type": "boolean"
					}
				}
			],
			"stored_fields": [
				"*"
			],
			"version": true
		}`,
		WantedSql: []string{
			`("-@timestamp">=parseDateTime64BestEffort('2024-04-04T13:18:18.149Z') AND "-@timestamp"<=parseDateTime64BestEffort('2024-04-04T13:33:18.149Z'))`,
		},
		WantedQueryType: model.Normal,
		WantedQuery:     []model.Query{}, // not needed
		WantedRegexes: []string{
			"SELECT count() FROM " + QuotedTableName + ` ` +
				`WHERE ("-@timestamp".=parseDateTime64BestEffort('2024-04-04T13:..:18.149Z') ` +
				`AND "-@timestamp".=parseDateTime64BestEffort('2024-04-04T13:..:18.149Z'))`,
			"SELECT toInt64(toUnixTimestamp64Milli(`-@timestamp`)/30000), count() " +
				`FROM ` + QuotedTableName + ` ` +
				`WHERE ("-@timestamp".=parseDateTime64BestEffort('2024-04-04T13:..:18.149Z') ` +
				`AND "-@timestamp".=parseDateTime64BestEffort('2024-04-04T13:..:18.149Z')) ` +
				"GROUP BY toInt64(toUnixTimestamp64Milli(`-@timestamp`)/30000) " +
				"ORDER BY toInt64(toUnixTimestamp64Milli(`-@timestamp`)/30000)",
			`SELECT.*"-@bytes".*FROM ` + QuotedTableName + ` ` +
				`WHERE ("-@timestamp".=parseDateTime64BestEffort('2024-04-04T13:..:18.149Z') ` +
				`AND "-@timestamp".=parseDateTime64BestEffort('2024-04-04T13:..:18.149Z')) ` +
				`ORDER BY "-@timestamp" DESC LIMIT 500`,
		},
	},
	{
		Name: "Basic Explorer request, but without SELECT *",
		QueryJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"2": {
					"date_histogram": {
						"field": "-@timestamp",
						"fixed_interval": "30s",
						"min_doc_count": 1,
						"time_zone": "Europe/Warsaw"
					}
				}
			},
			"docvalue_fields": [
				{
					"field": "-@timestamp",
					"format": "date_time"
				}
			],
			"highlight": {
				"fields": {
					"*": {}
				},
				"fragment_size": 2147483647,
				"post_tags": [
					"@/opensearch-dashboards-highlighted-field@"
				],
				"pre_tags": [
					"@opensearch-dashboards-highlighted-field@"
				]
			},
			"query": {
				"bool": {
					"filter": [
						{
							"match_all": {}
						},
						{
							"range": {
								"-@timestamp": {
									"format": "strict_date_optional_time",
									"gte": "2024-04-04T13:18:18.149Z",
									"lte": "2024-04-04T13:33:18.149Z"
								}
							}
						}
					],
					"must": [],
					"must_not": [],
					"should": []
				}
			},
			"script_fields": {},
			"size": 0,
			"sort": [
				{
					"-@timestamp": {
						"order": "desc",
						"unmapped_type": "boolean"
					}
				}
			],
			"stored_fields": [
				"*"
			],
			"version": true
		}`,
		WantedSql: []string{
			`("-@timestamp">=parseDateTime64BestEffort('2024-04-04T13:18:18.149Z') AND "-@timestamp"<=parseDateTime64BestEffort('2024-04-04T13:33:18.149Z'))`,
		},
		WantedQueryType: model.Normal,
		WantedQuery:     []model.Query{}, // not needed
		WantedRegexes: []string{
			"SELECT count() FROM " + QuotedTableName + ` ` +
				`WHERE ("-@timestamp".=parseDateTime64BestEffort('2024-04-04T13:..:18.149Z') ` +
				`AND "-@timestamp".=parseDateTime64BestEffort('2024-04-04T13:..:18.149Z'))`,
			"SELECT toInt64(toUnixTimestamp64Milli(`-@timestamp`)/30000), count() FROM " + QuotedTableName + ` ` +
				`WHERE ("-@timestamp".=parseDateTime64BestEffort('2024-04-04T13:..:18.149Z') ` +
				`AND "-@timestamp".=parseDateTime64BestEffort('2024-04-04T13:..:18.149Z')) ` +
				"GROUP BY toInt64(toUnixTimestamp64Milli(`-@timestamp`)/30000) " +
				"ORDER BY toInt64(toUnixTimestamp64Milli(`-@timestamp`)/30000)",
		},
	},
}
