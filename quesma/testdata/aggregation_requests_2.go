// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package testdata

import "quesma/model"

// Goland lags a lot when you edit aggregation_requests.go file, so let's add new tests to this one.

var AggregationTests2 = []AggregationTestCase{
	{ // [42]
		// FIXME results for this test are not 100% correct for day/week intervals (maybe others too)
		// see https://github.com/QuesmaOrg/quesma/issues/307
		TestName: "histogram with all possible calendar_intervals",
		QueryRequestJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"minute1": {
					"date_histogram": {
						"calendar_interval": "1m",
						"field": "@timestamp",
						"min_doc_count": 1,
						"time_zone": "Europe/Warsaw"
					}
				},
				"minute2": {
					"date_histogram": {
						"calendar_interval": "minute",
						"field": "@timestamp",
						"min_doc_count": 1,
						"time_zone": "Europe/Warsaw"
					}
				},
				"hour1": {
					"date_histogram": {
						"calendar_interval": "1h",
						"field": "@timestamp",
						"min_doc_count": 1,
						"time_zone": "Europe/Warsaw"
					}
				},
				"hour2": {
					"date_histogram": {
						"calendar_interval": "hour",
						"field": "@timestamp",
						"min_doc_count": 1,
						"time_zone": "Europe/Warsaw"
					}
				},
				"day1": {
					"date_histogram": {
						"calendar_interval": "1d",
						"field": "@timestamp",
						"min_doc_count": 1,
						"time_zone": "Europe/Warsaw"
					}
				},
				"day2": {
					"date_histogram": {
						"calendar_interval": "day",
						"field": "@timestamp",
						"min_doc_count": 1,
						"time_zone": "Europe/Warsaw"
					}
				},
				"week1": {
					"date_histogram": {
						"calendar_interval": "1w",
						"field": "@timestamp",
						"min_doc_count": 1,
						"time_zone": "Europe/Warsaw"
					}
				},
				"week2": {
					"date_histogram": {
						"calendar_interval": "week",
						"field": "@timestamp",
						"min_doc_count": 1,
						"time_zone": "Europe/Warsaw"
					}
				},
				"month1": {
					"date_histogram": {
						"calendar_interval": "1M",
						"field": "@timestamp",
						"min_doc_count": 1,
						"time_zone": "Europe/Warsaw"
					}
				},
				"month2": {
					"date_histogram": {
						"calendar_interval": "month",
						"field": "@timestamp",
						"min_doc_count": 1,
						"time_zone": "Europe/Warsaw"
					}
				},
				"quarter1": {
					"date_histogram": {
						"calendar_interval": "1q",
						"field": "@timestamp",
						"min_doc_count": 1,
						"time_zone": "Europe/Warsaw"
					}
				},
				"quarter2": {
					"date_histogram": {
						"calendar_interval": "quarter",
						"field": "@timestamp",
						"min_doc_count": 1,
						"time_zone": "Europe/Warsaw"
					}
				},
				"year1": {
					"date_histogram": {
						"calendar_interval": "1y",
						"field": "@timestamp",
						"min_doc_count": 1,
						"time_zone": "Europe/Warsaw"
					}
				},
				"year2": {
					"date_histogram": {
						"calendar_interval": "year",
						"field": "@timestamp",
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
			"runtime_mappings": {},
			"script_fields": {},
			"size": 0,
			"stored_fields": [
				"*"
			],
			"track_total_hits": true
		}`,
		ExpectedResponse: `
		{
			"took": 30,
			"timed_out": false,
			"_shards": {
				"total": 1,
				"successful": 1,
				"skipped": 0,
				"failed": 0
			},
			"hits": {
				"total": {
					"value": 33,
					"relation": "eq"
				},
				"max_score": null,
				"hits": []
			},
			"aggregations": {
				"hour1": {
					"buckets": [
						{
							"key_as_string": "2024-06-10T13:00:00.000",
							"key": 1718024400000,
							"doc_count": 33
						}
					]
				},
				"month1": {
					"buckets": [
						{
							"key_as_string": "2024-05-31T22:00:00.000",
							"key": 1717192800000,
							"doc_count": 33
						}
					]
				},
				"week1": {
					"buckets": [
						{
							"key_as_string": "2024-06-09T22:00:00.000",
							"key": 1717970400000,
							"doc_count": 33
						}
					]
				},
				"month2": {
					"buckets": [
						{
							"key_as_string": "2024-05-31T22:00:00.000",
							"key": 1717192800000,
							"doc_count": 33
						}
					]
				},
				"week2": {
					"buckets": [
						{
							"key_as_string": "2024-06-09T22:00:00.000",
							"key": 1717970400000,
							"doc_count": 33
						}
					]
				},
				"hour2": {
					"buckets": [
						{
							"key_as_string": "2024-06-10T13:00:00.000",
							"key": 1718024400000,
							"doc_count": 33
						}
					]
				},
				"minute1": {
					"buckets": [
						{
							"key_as_string": "2024-06-10T13:24:00.000",
							"key": 1718025840000,
							"doc_count": 9
						},
						{
							"key_as_string": "2024-06-10T13:25:00.000",
							"key": 1718025900000,
							"doc_count": 24
						}
					]
				},
				"quarter1": {
					"buckets": [
						{
							"key_as_string": "2024-03-31T22:00:00.000",
							"key": 1711922400000,
							"doc_count": 33
						}
					]
				},
				"quarter2": {
					"buckets": [
						{
							"key_as_string": "2024-03-31T22:00:00.000",
							"key": 1711922400000,
							"doc_count": 33
						}
					]
				},
				"minute2": {
					"buckets": [
						{
							"key_as_string": "2024-06-10T13:24:00.000",
							"key": 1718025840000,
							"doc_count": 9
						},
						{
							"key_as_string": "2024-06-10T13:25:00.000",
							"key": 1718025900000,
							"doc_count": 24
						}
					]
				},
				"year1": {
					"buckets": [
						{
							"key_as_string": "2023-12-31T23:00:00.000",
							"key": 1704063600000,
							"doc_count": 33
						}
					]
				},
				"year2": {
					"buckets": [
						{
							"key_as_string": "2023-12-31T23:00:00.000",
							"key": 1704063600000,
							"doc_count": 33
						}
					]
				},
				"day2": {
					"buckets": [
						{
							"key_as_string": "2024-06-10T00:00:00.000",
							"key": 1717977600000,
							"doc_count": 33
						}
					]
				},
				"day1": {
					"buckets": [
						{
							"key_as_string": "2024-06-10T00:00:00.000",
							"key": 1717977600000,
							"doc_count": 33
						}
					]
				}
			}
		}`,
		ExpectedResults: [][]model.QueryResultRow{
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol("hits", uint64(33))}}},
			{{Cols: []model.QueryResultCol{
				model.NewQueryResultCol(`toInt64(toUnixTimestamp64Milli("@timestamp") / 86400000)`, int64(1717980400000/86400000)),
				model.NewQueryResultCol("count()", uint64(33)),
			}}},
			{{Cols: []model.QueryResultCol{
				model.NewQueryResultCol(`toInt64(toUnixTimestamp64Milli("@timestamp") / 86400000)`, int64(1717980400000/86400000)),
				model.NewQueryResultCol("count()", uint64(33)),
			}}},
			{{Cols: []model.QueryResultCol{
				model.NewQueryResultCol(`toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000)`, int64(1718024400000/3600000)),
				model.NewQueryResultCol("count()", uint64(33)),
			}}},
			{{Cols: []model.QueryResultCol{
				model.NewQueryResultCol(`toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000)`, int64(1718024400000/3600000)),
				model.NewQueryResultCol("count()", uint64(33)),
			}}},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol(`toInt64(toUnixTimestamp64Milli("@timestamp") / 60000)`, int64(1718025840000/60000)),
					model.NewQueryResultCol("count()", uint64(9)),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol(`toInt64(toUnixTimestamp64Milli("@timestamp") / 60000)`, int64(1718025900000/60000)),
					model.NewQueryResultCol("count()", uint64(24)),
				}},
			},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol(`toInt64(toUnixTimestamp64Milli("@timestamp") / 60000)`, int64(1718025840000/60000)),
					model.NewQueryResultCol("count()", uint64(9)),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol(`toInt64(toUnixTimestamp64Milli("@timestamp") / 60000)`, int64(1718025900000/60000)),
					model.NewQueryResultCol("count()", uint64(24)),
				}},
			},
			{{Cols: []model.QueryResultCol{
				model.NewQueryResultCol(`toInt64(toUnixTimestamp64Milli(toStartOfMonth("@timestamp")))`, int64(1717192800000)),
				model.NewQueryResultCol("count()", uint64(33)),
			}}},
			{{Cols: []model.QueryResultCol{
				model.NewQueryResultCol(`toInt64(toUnixTimestamp64Milli(toStartOfMonth("@timestamp")))`, int64(1717192800000)),
				model.NewQueryResultCol("count()", uint64(33)),
			}}},
			{{Cols: []model.QueryResultCol{
				model.NewQueryResultCol(`toInt64(toUnixTimestamp64Milli(toStartOfQuarter("@timestamp")))`, int64(1711922400000)),
				model.NewQueryResultCol("count()", uint64(33)),
			}}},
			{{Cols: []model.QueryResultCol{
				model.NewQueryResultCol(`toInt64(toUnixTimestamp64Milli(toStartOfQuarter("@timestamp")))`, int64(1711922400000)),
				model.NewQueryResultCol("count()", uint64(33)),
			}}},
			{{Cols: []model.QueryResultCol{
				model.NewQueryResultCol(`toInt64(toUnixTimestamp64Milli(toStartOfWeek("@timestamp")))`, int64(1717970400000)),
				model.NewQueryResultCol("count()", uint64(33)),
			}}},
			{{Cols: []model.QueryResultCol{
				model.NewQueryResultCol(`toInt64(toUnixTimestamp64Milli(toStartOfWeek("@timestamp")))`, int64(1717970400000)),
				model.NewQueryResultCol("count()", uint64(33)),
			}}},
			{{Cols: []model.QueryResultCol{
				model.NewQueryResultCol(`toInt64(toUnixTimestamp64Milli(toStartOfYear("@timestamp")))`, int64(1704063600000)),
				model.NewQueryResultCol("count()", uint64(33)),
			}}},
			{{Cols: []model.QueryResultCol{
				model.NewQueryResultCol(`toInt64(toUnixTimestamp64Milli(toStartOfYear("@timestamp")))`, int64(1704063600000)),
				model.NewQueryResultCol("count()", uint64(33)),
			}}},
		},
		ExpectedPancakeResults: make([]model.QueryResultRow, 0),
		ExpectedSQLs: []string{
			`SELECT count() FROM ` + QuotedTableName,
			`SELECT toInt64(toUnixTimestamp64Milli("@timestamp") / 86400000), count() ` +
				`FROM ` + QuotedTableName + ` ` +
				`GROUP BY toInt64(toUnixTimestamp64Milli("@timestamp") / 86400000) ` +
				`ORDER BY toInt64(toUnixTimestamp64Milli("@timestamp") / 86400000)`,
			`SELECT toInt64(toUnixTimestamp64Milli("@timestamp") / 86400000), count() ` +
				`FROM ` + QuotedTableName + ` ` +
				`GROUP BY toInt64(toUnixTimestamp64Milli("@timestamp") / 86400000) ` +
				`ORDER BY toInt64(toUnixTimestamp64Milli("@timestamp") / 86400000)`,
			`SELECT toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000), count() ` +
				`FROM ` + QuotedTableName + ` ` +
				`GROUP BY toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000) ` +
				`ORDER BY toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000)`,
			`SELECT toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000), count() ` +
				`FROM ` + QuotedTableName + ` ` +
				`GROUP BY toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000) ` +
				`ORDER BY toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000)`,
			`SELECT toInt64(toUnixTimestamp64Milli("@timestamp") / 60000), count() ` +
				`FROM ` + QuotedTableName + ` ` +
				`GROUP BY toInt64(toUnixTimestamp64Milli("@timestamp") / 60000) ` +
				`ORDER BY toInt64(toUnixTimestamp64Milli("@timestamp") / 60000)`,
			`SELECT toInt64(toUnixTimestamp64Milli("@timestamp") / 60000), count() ` +
				`FROM ` + QuotedTableName + ` ` +
				`GROUP BY toInt64(toUnixTimestamp64Milli("@timestamp") / 60000) ` +
				`ORDER BY toInt64(toUnixTimestamp64Milli("@timestamp") / 60000)`,
			`SELECT toInt64(toUnixTimestamp(toStartOfMonth("@timestamp")))*1000, count() ` +
				`FROM ` + QuotedTableName + ` ` +
				`GROUP BY toInt64(toUnixTimestamp(toStartOfMonth("@timestamp")))*1000 ` +
				`ORDER BY toInt64(toUnixTimestamp(toStartOfMonth("@timestamp")))*1000`,
			`SELECT toInt64(toUnixTimestamp(toStartOfMonth("@timestamp")))*1000, count() ` +
				`FROM ` + QuotedTableName + ` ` +
				`GROUP BY toInt64(toUnixTimestamp(toStartOfMonth("@timestamp")))*1000 ` +
				`ORDER BY toInt64(toUnixTimestamp(toStartOfMonth("@timestamp")))*1000`,
			`SELECT toInt64(toUnixTimestamp(toStartOfQuarter("@timestamp")))*1000, count() ` +
				`FROM ` + QuotedTableName + ` ` +
				`GROUP BY toInt64(toUnixTimestamp(toStartOfQuarter("@timestamp")))*1000 ` +
				`ORDER BY toInt64(toUnixTimestamp(toStartOfQuarter("@timestamp")))*1000`,
			`SELECT toInt64(toUnixTimestamp(toStartOfQuarter("@timestamp")))*1000, count() ` +
				`FROM ` + QuotedTableName + ` ` +
				`GROUP BY toInt64(toUnixTimestamp(toStartOfQuarter("@timestamp")))*1000 ` +
				`ORDER BY toInt64(toUnixTimestamp(toStartOfQuarter("@timestamp")))*1000`,
			`SELECT toInt64(toUnixTimestamp(toStartOfWeek("@timestamp")))*1000, count() ` +
				`FROM ` + QuotedTableName + ` ` +
				`GROUP BY toInt64(toUnixTimestamp(toStartOfWeek("@timestamp")))*1000 ` +
				`ORDER BY toInt64(toUnixTimestamp(toStartOfWeek("@timestamp")))*1000`,
			`SELECT toInt64(toUnixTimestamp(toStartOfWeek("@timestamp")))*1000, count() ` +
				`FROM ` + QuotedTableName + ` ` +
				`GROUP BY toInt64(toUnixTimestamp(toStartOfWeek("@timestamp")))*1000 ` +
				`ORDER BY toInt64(toUnixTimestamp(toStartOfWeek("@timestamp")))*1000`,
			`SELECT toInt64(toUnixTimestamp(toStartOfYear("@timestamp")))*1000, count() ` +
				`FROM ` + QuotedTableName + ` ` +
				`GROUP BY toInt64(toUnixTimestamp(toStartOfYear("@timestamp")))*1000 ` +
				`ORDER BY toInt64(toUnixTimestamp(toStartOfYear("@timestamp")))*1000`,
			`SELECT toInt64(toUnixTimestamp(toStartOfYear("@timestamp")))*1000, count() ` +
				`FROM ` + QuotedTableName + ` ` +
				`GROUP BY toInt64(toUnixTimestamp(toStartOfYear("@timestamp")))*1000 ` +
				`ORDER BY toInt64(toUnixTimestamp(toStartOfYear("@timestamp")))*1000`,
		},
		ExpectedPancakeSQL: "TODO",
	},
}
