package testdata

import "mitmproxy/quesma/model"

var AggregationTestsWithSpecialCharactersInFieldNames = []AggregationTestCase{
	{
		TestName: "Top metrics",
		QueryRequestJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"0": {
					"aggs": {
						"1": {
							"top_metrics": {
								"metrics": {
									"field": "-@bytes"
								},
								"size": 1,
								"sort": {
									"-@timestamp": "desc"
								}
							}
						}
					},
					"date_histogram": {
						"field": "-@timestamp",
						"fixed_interval": "12h",
						"min_doc_count": 1,
						"time_zone": "Europe/Warsaw"
					}
				}
			},
			"fields": [
				{
					"field": "-@timestamp",
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
							"exists": {
								"field": "message$*%:;"
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
		ExpectedResponse: `
		{
			"is_partial": false,
			"is_running": false,
			"start_time_in_millis": 1711457737150,
			"expiration_time_in_millis": 1711889737150,
			"completion_time_in_millis": 1711457737169,
			"response": {
				"took": 19,
				"timed_out": false,
				"_shards": {
					"total": 1,
					"successful": 1,
					"skipped": 0,
					"failed": 0
				},
				"hits": {
					"total": {
						"value": 2190,
						"relation": "eq"
					},
					"max_score": null,
					"hits": []
				},
				"aggregations": {
					"0": {
						"buckets": [
							{
								"1": {
									"top": [
										{
											"sort": [
												"2024-03-17T10:59:44.162Z"
											],
											"metrics": {
												"bytes": 8279
											}
										}
									]
								},
								"key_as_string": "2024-03-17T00:00:00.000+01:00",
								"key": 1710630000000,
								"doc_count": 109
							},
							{
								"1": {
									"top": [
										{
											"sort": [
												"2024-03-17T21:48:16.637Z"
											],
											"metrics": {
												"bytes": 8909
											}
										}
									]
								},
								"key_as_string": "2024-03-17T12:00:00.000+01:00",
								"key": 1710673200000,
								"doc_count": 140
							}
						]
					}
				}
			}
		}`,
		ExpectedResults: [][]model.QueryResultRow{}, // checking only the SQLs is enough for now
		ExpectedSQLs: []string{
			`SELECT count() FROM ` + quotedTableName + ` WHERE "message\$\*\%\:\;" IS NOT NULL`,
			`SELECT toInt64(toUnixTimestamp64Milli(` + "`-@timestamp`" + `)/43200000), MIN("-@bytes") AS "windowed_-@bytes", MIN("-@timestamp") AS "windowed_-@timestamp" FROM (SELECT "-@bytes", "-@timestamp", ROW_NUMBER() OVER (PARTITION BY toInt64(toUnixTimestamp64Milli(` + "`-@timestamp`)/43200000) ORDER BY " + `"-@timestamp" desc) AS row_number FROM ` + quotedTableName + ` WHERE "message\$\*\%\:\;" IS NOT NULL) WHERE "message\$\*\%\:\;" IS NOT NULL AND row_number <= 1  GROUP BY (toInt64(toUnixTimestamp64Milli(` + "`-@timestamp`)/43200000)) ORDER BY (toInt64(toUnixTimestamp64Milli(`-@timestamp`)/43200000))",
			"SELECT toInt64(toUnixTimestamp64Milli(`-@timestamp`)/43200000), count() FROM " + quotedTableName + ` WHERE "message\$\*\%\:\;\" IS NOT NULL  GROUP BY (toInt64(toUnixTimestamp64Milli(` + "`-@timestamp`)/43200000)) ORDER BY (toInt64(toUnixTimestamp64Milli(`-@timestamp`)/43200000))",
		},
	},
}
