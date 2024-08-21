// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package testdata

import (
	"quesma/model"
	"quesma/util"
	"time"
)

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
		ExpectedPancakeSQL: `
			SELECT toInt64(toUnixTimestamp64Milli("@timestamp") / 86400000) AS
			  "aggr__day1__key_0", count(*) AS "aggr__day1__count"
			FROM "logs-generic-default"
			GROUP BY toInt64(toUnixTimestamp64Milli("@timestamp") / 86400000) AS
			  "aggr__day1__key_0"
			ORDER BY "aggr__day1__key_0" ASC`,
		ExpectedAdditionalPancakeSQLs: []string{
			`SELECT toInt64(toUnixTimestamp64Milli("@timestamp") / 86400000) AS
			  "aggr__day2__key_0", count(*) AS "aggr__day2__count"
			FROM "logs-generic-default"
			GROUP BY toInt64(toUnixTimestamp64Milli("@timestamp") / 86400000) AS
			  "aggr__day2__key_0"
			ORDER BY "aggr__day2__key_0" ASC`,
			`SELECT toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000) AS
			  "aggr__hour1__key_0", count(*) AS "aggr__hour1__count"
			FROM "logs-generic-default"
			GROUP BY toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000) AS
			  "aggr__hour1__key_0"
			ORDER BY "aggr__hour1__key_0" ASC`,
			`SELECT toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000) AS
			  "aggr__hour2__key_0", count(*) AS "aggr__hour2__count"
			FROM "logs-generic-default"
			GROUP BY toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000) AS
			  "aggr__hour2__key_0"
			ORDER BY "aggr__hour2__key_0" ASC`,
			`SELECT toInt64(toUnixTimestamp64Milli("@timestamp") / 60000) AS
			  "aggr__minute1__key_0", count(*) AS "aggr__minute1__count"
			FROM "logs-generic-default"
			GROUP BY toInt64(toUnixTimestamp64Milli("@timestamp") / 60000) AS
			  "aggr__minute1__key_0"
			ORDER BY "aggr__minute1__key_0" ASC`,
			`SELECT toInt64(toUnixTimestamp64Milli("@timestamp") / 60000) AS
			  "aggr__minute2__key_0", count(*) AS "aggr__minute2__count"
			FROM "logs-generic-default"
			GROUP BY toInt64(toUnixTimestamp64Milli("@timestamp") / 60000) AS
			  "aggr__minute2__key_0"
			ORDER BY "aggr__minute2__key_0" ASC`,
			`SELECT toInt64(toUnixTimestamp(toStartOfMonth("@timestamp")))*1000 AS
			  "aggr__month1__key_0", count(*) AS "aggr__month1__count"
			FROM "logs-generic-default"
			GROUP BY toInt64(toUnixTimestamp(toStartOfMonth("@timestamp")))*1000 AS
			  "aggr__month1__key_0"
			ORDER BY "aggr__month1__key_0" ASC`,
			`SELECT toInt64(toUnixTimestamp(toStartOfMonth("@timestamp")))*1000 AS
			  "aggr__month2__key_0", count(*) AS "aggr__month2__count"
			FROM "logs-generic-default"
			GROUP BY toInt64(toUnixTimestamp(toStartOfMonth("@timestamp")))*1000 AS
			  "aggr__month2__key_0"
			ORDER BY "aggr__month2__key_0" ASC`,
			`SELECT toInt64(toUnixTimestamp(toStartOfQuarter("@timestamp")))*1000 AS
			  "aggr__quarter1__key_0", count(*) AS "aggr__quarter1__count"
			FROM "logs-generic-default"
			GROUP BY toInt64(toUnixTimestamp(toStartOfQuarter("@timestamp")))*1000 AS
			  "aggr__quarter1__key_0"
			ORDER BY "aggr__quarter1__key_0" ASC`,
			`SELECT toInt64(toUnixTimestamp(toStartOfQuarter("@timestamp")))*1000 AS
			  "aggr__quarter2__key_0", count(*) AS "aggr__quarter2__count"
			FROM "logs-generic-default"
			GROUP BY toInt64(toUnixTimestamp(toStartOfQuarter("@timestamp")))*1000 AS
			  "aggr__quarter2__key_0"
			ORDER BY "aggr__quarter2__key_0" ASC`,
			`SELECT toInt64(toUnixTimestamp(toStartOfWeek("@timestamp")))*1000 AS
			  "aggr__week1__key_0", count(*) AS "aggr__week1__count"
			FROM "logs-generic-default"
			GROUP BY toInt64(toUnixTimestamp(toStartOfWeek("@timestamp")))*1000 AS
			  "aggr__week1__key_0"
			ORDER BY "aggr__week1__key_0" ASC`,
			`SELECT toInt64(toUnixTimestamp(toStartOfWeek("@timestamp")))*1000 AS
			  "aggr__week2__key_0", count(*) AS "aggr__week2__count"
			FROM "logs-generic-default"
			GROUP BY toInt64(toUnixTimestamp(toStartOfWeek("@timestamp")))*1000 AS
			  "aggr__week2__key_0"
			ORDER BY "aggr__week2__key_0" ASC`,
			`SELECT toInt64(toUnixTimestamp(toStartOfYear("@timestamp")))*1000 AS
			  "aggr__year1__key_0", count(*) AS "aggr__year1__count"
			FROM "logs-generic-default"
			GROUP BY toInt64(toUnixTimestamp(toStartOfYear("@timestamp")))*1000 AS
			  "aggr__year1__key_0"
			ORDER BY "aggr__year1__key_0" ASC`,
			`SELECT toInt64(toUnixTimestamp(toStartOfYear("@timestamp")))*1000 AS
			  "aggr__year2__key_0", count(*) AS "aggr__year2__count"
			FROM "logs-generic-default"
			GROUP BY toInt64(toUnixTimestamp(toStartOfYear("@timestamp")))*1000 AS
			  "aggr__year2__key_0"
			ORDER BY "aggr__year2__key_0" ASC`,
		},
	},
	{ // [43]
		TestName: "Percentiles with another metric aggregation. It might get buggy after introducing pancakes.",
		QueryRequestJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"2": {
					"aggs": {
						"1": {
							"percentiles": {
								"field": "timestamp",
								"keyed": false,
								"percents": [1, 2]
							}
						},
						"2": {
							"sum": {
								"field": "count"
							}
						}
					},
					"significant_terms": {
						"field": "response.keyword",
						"size": 3
					}
				}
			},
			"docvalue_fields": [
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
									"gte": "2024-04-18T00:51:15.845Z",
									"lte": "2024-05-03T00:51:15.845Z"
								}
							}
						}
					],
					"must": [
						{
							"match_all": {}
						}
					],
					"must_not": [],
					"should": []
				}
			},
			"script_fields": {
				"hour_of_day": {
					"script": {
						"lang": "painless",
						"source": "doc['timestamp'].value.getHour()"
					}
				}
			},
			"size": 0,
			"stored_fields": [
				"*"
			],
			"track_total_hits": true
		}`,
		ExpectedResponse: `
		{
			"_shards": {
				"failed": 0,
				"skipped": 0,
				"successful": 1,
				"total": 1
			},
			"aggregations": {
				"2": {
					"bg_count": 2786,
					"buckets": [
						{
							"1": {
								"values": [
									{
										"key": 1.0,
										"value": 1713679873619.0,
										"value_as_string": "2024-04-21T06:11:13.619Z"
									},
									{
										"key": 2,
										"value": 1713702073414.0,
										"value_as_string": "2024-04-21T12:21:13.414Z"
									}
								]
							},
							"2": {
								"value": 10
							},
							"bg_count": 2570,
							"doc_count": 2570,
							"key": "200",
							"score": 2570
						}
					],
					"sum_other_doc_count": 2786,
					"doc_count_error_upper_bound": 0
				}
			},
			"hits": {
				"hits": [],
				"max_score": null,
				"total": {
					"relation": "eq",
					"value": 2786
				}
			},
			"timed_out": false,
			"took": 9
		}`,
		ExpectedResults: [][]model.QueryResultRow{
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol("value", uint64(2786))}}},
			{{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("response", "200"),
				model.NewQueryResultCol(`quantile_1`, []time.Time{util.ParseTime("2024-04-21T06:11:13.619Z")}),
				model.NewQueryResultCol(`quantile_2`, []time.Time{util.ParseTime("2024-04-21T12:21:13.414Z")}),
			}}},
			{{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("response", "200"),
				model.NewQueryResultCol(`sumOrNull("count")`, 10),
			}}},
			{{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("response", "200"),
				model.NewQueryResultCol(`doc_count`, 2570),
			}}},
		},
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", 2786),
				model.NewQueryResultCol("aggr__2__key_0", "200"),
				model.NewQueryResultCol("aggr__2__count", 2570),
				model.NewQueryResultCol("aggr__2__order_1", 2570),
				model.NewQueryResultCol("metric__2__1_col_0", []time.Time{util.ParseTime("2024-04-21T06:11:13.619Z")}),
				model.NewQueryResultCol("metric__2__1_col_1", []time.Time{util.ParseTime("2024-04-21T12:21:13.414Z")}),
				model.NewQueryResultCol("metric__2__2_col_0", 10),
			}},
		},
		ExpectedSQLs: []string{
			`SELECT count() FROM ` + QuotedTableName + ` ` +
				`WHERE ("timestamp">=parseDateTime64BestEffort('2024-04-18T00:51:15.845Z') ` +
				`AND "timestamp"<=parseDateTime64BestEffort('2024-05-03T00:51:15.845Z'))`,
			`WITH cte_1 AS ` +
				`(SELECT "response" AS "cte_1_1", count() AS "cte_1_cnt" ` +
				`FROM ` + QuotedTableName + ` ` +
				`WHERE (("timestamp">=parseDateTime64BestEffort('2024-04-18T00:51:15.845Z') ` +
				`AND "timestamp"<=parseDateTime64BestEffort('2024-05-03T00:51:15.845Z')) ` +
				`AND "response" IS NOT NULL) ` +
				`GROUP BY "response" ` +
				`ORDER BY count() DESC, "response" ` +
				`LIMIT 3) ` +
				`SELECT "response", ` +
				"quantiles(0.010000)(\"timestamp\") AS \"quantile_1\", " +
				"quantiles(0.020000)(\"timestamp\") AS \"quantile_2\" " +
				`FROM ` + QuotedTableName + ` ` +
				`INNER JOIN "cte_1" ON "response" = "cte_1_1" ` +
				`WHERE (("timestamp">=parseDateTime64BestEffort('2024-04-18T00:51:15.845Z') ` +
				`AND "timestamp"<=parseDateTime64BestEffort('2024-05-03T00:51:15.845Z')) ` +
				`AND "response" IS NOT NULL) ` +
				`GROUP BY "response", cte_1_cnt ` +
				`ORDER BY cte_1_cnt DESC, "response"`,
			`WITH cte_1 AS ` +
				`(SELECT "response" AS "cte_1_1", count() AS "cte_1_cnt" ` +
				`FROM ` + QuotedTableName + ` ` +
				`WHERE (("timestamp">=parseDateTime64BestEffort('2024-04-18T00:51:15.845Z') ` +
				`AND "timestamp"<=parseDateTime64BestEffort('2024-05-03T00:51:15.845Z')) ` +
				`AND "response" IS NOT NULL) ` +
				`GROUP BY "response" ` +
				`ORDER BY count() DESC, "response" ` +
				`LIMIT 3) ` +
				`SELECT "response", sumOrNull("count") ` +
				`FROM ` + QuotedTableName + ` ` +
				`INNER JOIN "cte_1" ON "response" = "cte_1_1" ` +
				`WHERE (("timestamp">=parseDateTime64BestEffort('2024-04-18T00:51:15.845Z') ` +
				`AND "timestamp"<=parseDateTime64BestEffort('2024-05-03T00:51:15.845Z')) ` +
				`AND "response" IS NOT NULL) ` +
				`GROUP BY "response", cte_1_cnt ` +
				`ORDER BY cte_1_cnt DESC, "response"`,
			`SELECT "response", count() FROM ` + QuotedTableName + ` ` +
				`WHERE (("timestamp">=parseDateTime64BestEffort('2024-04-18T00:51:15.845Z') ` +
				`AND "timestamp"<=parseDateTime64BestEffort('2024-05-03T00:51:15.845Z')) ` +
				`AND "response" IS NOT NULL) ` +
				`GROUP BY "response" ` +
				`ORDER BY count() DESC, "response" ` +
				`LIMIT 3`,
		},
		ExpectedPancakeSQL: `
			SELECT
			  sum(count(*)) OVER () AS "aggr__2__parent_count",
			  "response" AS "aggr__2__key_0",
			  count(*) AS "aggr__2__count",
			  count() AS "aggr__2__order_1",
			  quantiles(0.010000)("timestamp") AS "quantile_1" AS "metric__2__1_col_0",
			  quantiles(0.020000)("timestamp") AS "quantile_2" AS "metric__2__1_col_1",
			  sumOrNull("count") AS "metric__2__2_col_0"
			FROM "logs-generic-default"
			WHERE ("timestamp">=parseDateTime64BestEffort('2024-04-18T00:51:15.845Z') AND
			  "timestamp"<=parseDateTime64BestEffort('2024-05-03T00:51:15.845Z'))
			GROUP BY "response" AS "aggr__2__key_0"
			ORDER BY "aggr__2__order_1" DESC, "aggr__2__key_0" ASC
			LIMIT 4`,
	},
	{ // [44]
		TestName: "2x terms with nulls 1/4, nulls in second aggregation, with missing parameter",
		QueryRequestJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"2": {
					"aggs": {
						"8": {
							"terms": {
								"field": "limbName",
								"missing": "__missing__",
								"size": 20
							}
						}
					},
					"terms": {
						"field": "surname",
						"shard_size": 1000,
						"size": 200
					}
				}
			},
			"fields": [],
			"runtime_mappings": {},
			"script_fields": {},
			"size": 0,
			"stored_fields": [
				"*"
			],
			"track_total_hits": false
		}`,
		ExpectedResponse: `
		{
			"completion_time_in_millis": 1720352002293,
			"expiration_time_in_millis": 1720352062445,
			"id": "FnpTUXdfTTZLUlBtQVo1YzBTVFBseEEcM19IaHdFWG5RN1d1eV9VaUcxenYwdzo0MTc0MA==",
			"is_partial": false,
			"is_running": false,
			"response": {
				"_shards": {
					"failed": 0,
					"skipped": 0,
					"successful": 1,
					"total": 1
				},
				"aggregations": {
					"2": {
						"buckets": [
							{
								"8": {
									"buckets": [
										{
											"doc_count": 21,
											"key": "__missing__"
										},
										{
											"doc_count": 24,
											"key": "b12"
										}
									],
									"doc_count_error_upper_bound": -1,
									"sum_other_doc_count": 991
								},
								"doc_count": 1036,
								"key": "a1"
							},
							{
								"8": {
									"buckets": [
										{
											"doc_count": 17,
											"key": "b21"
										},
										{
											"doc_count": 17,
											"key": "__missing__"
										}
									],
									"doc_count_error_upper_bound": 0,
									"sum_other_doc_count": 0
								},
								"doc_count": 34,
								"key": "a2"
							}
						],
						"doc_count_error_upper_bound": -1,
						"sum_other_doc_count": 33220
					}
				},
				"hits": {
					"hits": [],
					"max_score": null,
					"total": {
						"relation": "eq",
						"value": 50427
					}
				},
				"timed_out": false,
				"took": 554
			},
			"start_time_in_millis": 1720352001739
		}`,
		ExpectedResults: [][]model.QueryResultRow{
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a1"),
					model.NewQueryResultCol("limbName", "__missing__"),
					model.NewQueryResultCol("count()", 21),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a1"),
					model.NewQueryResultCol("limbName", "b12"),
					model.NewQueryResultCol("count()", 24),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a2"),
					model.NewQueryResultCol("limbName", "b21"),
					model.NewQueryResultCol("count()", 17),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a2"),
					model.NewQueryResultCol("limbName", "__missing__"),
					model.NewQueryResultCol("count()", 17),
				}},
			},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a1"),
					model.NewQueryResultCol("count()", 1036),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a2"),
					model.NewQueryResultCol("count()", 34),
				}},
			},
		},
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", 34290),
				model.NewQueryResultCol("aggr__2__key_0", "a1"),
				model.NewQueryResultCol("aggr__2__count", int64(1036)),
				model.NewQueryResultCol("aggr__2__order_1", 1036),
				model.NewQueryResultCol("aggr__2__8__parent_count", 1036),
				model.NewQueryResultCol("aggr__2__8__key_0", "__missing__"),
				model.NewQueryResultCol("aggr__2__8__count", int64(21)),
				model.NewQueryResultCol("aggr__2__8__order_1", 21),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", 34290),
				model.NewQueryResultCol("aggr__2__key_0", "a1"),
				model.NewQueryResultCol("aggr__2__count", int64(1036)),
				model.NewQueryResultCol("aggr__2__order_1", 1036),
				model.NewQueryResultCol("aggr__2__8__parent_count", 1036),
				model.NewQueryResultCol("aggr__2__8__key_0", "b12"),
				model.NewQueryResultCol("aggr__2__8__count", int64(24)),
				model.NewQueryResultCol("aggr__2__8__order_1", 24),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", 34290),
				model.NewQueryResultCol("aggr__2__key_0", "a2"),
				model.NewQueryResultCol("aggr__2__count", int64(34)),
				model.NewQueryResultCol("aggr__2__order_1", 34),
				model.NewQueryResultCol("aggr__2__8__parent_count", 34),
				model.NewQueryResultCol("aggr__2__8__key_0", "b21"),
				model.NewQueryResultCol("aggr__2__8__count", int64(17)),
				model.NewQueryResultCol("aggr__2__8__order_1", 17),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", 34290),
				model.NewQueryResultCol("aggr__2__key_0", "a2"),
				model.NewQueryResultCol("aggr__2__count", int64(34)),
				model.NewQueryResultCol("aggr__2__order_1", 34),
				model.NewQueryResultCol("aggr__2__8__parent_count", 34),
				model.NewQueryResultCol("aggr__2__8__key_0", "__missing__"),
				model.NewQueryResultCol("aggr__2__8__count", int64(17)),
				model.NewQueryResultCol("aggr__2__8__order_1", 17),
			}},
		},
		ExpectedSQLs: []string{
			`WITH cte_1 AS (` +
				`SELECT "surname" AS "cte_1_1", count() AS "cte_1_cnt" ` +
				`FROM "logs-generic-default" ` +
				`WHERE "surname" IS NOT NULL ` +
				`GROUP BY "surname" ` +
				`ORDER BY count() DESC, "surname" ` +
				`LIMIT 200) ` +
				`SELECT "surname", COALESCE("limbName",'__missing__'), count() ` +
				`FROM "logs-generic-default" INNER JOIN "cte_1" ON "surname" = "cte_1_1" ` +
				`WHERE "surname" IS NOT NULL ` +
				`GROUP BY "surname", COALESCE("limbName",'__missing__'), cte_1_cnt ` +
				`ORDER BY cte_1_cnt DESC, "surname", count() DESC, COALESCE("limbName",'__missing__') ` +
				`LIMIT 20 BY "surname"`,
			`SELECT "surname", count() ` +
				`FROM "logs-generic-default" ` +
				`WHERE "surname" IS NOT NULL ` +
				`GROUP BY "surname" ` +
				`ORDER BY count() DESC, "surname" ` +
				`LIMIT 200`,
		},
		ExpectedPancakeSQL: `
			SELECT "aggr__2__parent_count", "aggr__2__key_0", "aggr__2__count",
			  "aggr__2__order_1", "aggr__2__8__parent_count", "aggr__2__8__key_0",
			  "aggr__2__8__count", "aggr__2__8__order_1"
			FROM (
			  SELECT "aggr__2__parent_count", "aggr__2__key_0", "aggr__2__count",
				"aggr__2__order_1", "aggr__2__8__parent_count", "aggr__2__8__key_0",
				"aggr__2__8__count", "aggr__2__8__order_1",
				dense_rank() OVER (ORDER BY "aggr__2__order_1" DESC, "aggr__2__key_0" ASC)
				AS "aggr__2__order_1_rank",
				dense_rank() OVER (PARTITION BY "aggr__2__key_0" ORDER BY
				"aggr__2__8__order_1" DESC, "aggr__2__8__key_0" ASC) AS
				"aggr__2__8__order_1_rank"
			  FROM (
				SELECT sum(count(*)) OVER () AS "aggr__2__parent_count",
				  "surname" AS "aggr__2__key_0",
				  sum(count(*)) OVER (PARTITION BY "aggr__2__key_0") AS "aggr__2__count",
				  sum(count()) OVER (PARTITION BY "aggr__2__key_0") AS "aggr__2__order_1",
				  sum(count(*)) OVER (PARTITION BY "aggr__2__key_0") AS
				  "aggr__2__8__parent_count",
				  COALESCE("limbName", '__missing__') AS "aggr__2__8__key_0",
				  count(*) AS "aggr__2__8__count", count() AS "aggr__2__8__order_1"
				FROM "logs-generic-default"
				GROUP BY "surname" AS "aggr__2__key_0",
				  COALESCE("limbName", '__missing__') AS "aggr__2__8__key_0"))
			WHERE ("aggr__2__order_1_rank"<=201 AND "aggr__2__8__order_1_rank"<=20)
			ORDER BY "aggr__2__order_1_rank" ASC, "aggr__2__8__order_1_rank" ASC`,
	},
	{ // [45]
		TestName: "2x terms with nulls 2/4, nulls in the second aggregation, but no missing parameter",
		QueryRequestJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"2": {
					"aggs": {
						"8": {
							"terms": {
								"field": "limbName",
								"size": 20
							}
						}
					},
					"terms": {
						"field": "surname",
						"shard_size": 1000,
						"size": 200
					}
				}
			},
			"fields": [],
			"runtime_mappings": {},
			"script_fields": {},
			"size": 0,
			"stored_fields": [
				"*"
			],
			"track_total_hits": false
		}`,
		ExpectedResponse: `
		{
			"completion_time_in_millis": 1720352002293,
			"expiration_time_in_millis": 1720352062445,
			"id": "FnpTUXdfTTZLUlBtQVo1YzBTVFBseEEcM19IaHdFWG5RN1d1eV9VaUcxenYwdzo0MTc0MA==",
			"is_partial": false,
			"is_running": false,
			"response": {
				"_shards": {
					"failed": 0,
					"skipped": 0,
					"successful": 1,
					"total": 1
				},
				"aggregations": {
					"2": {
						"buckets": [
							{
								"8": {
									"buckets": [
										{
											"doc_count": 21,
											"key": "b11"
										},
										{
											"doc_count": 24,
											"key": "b12"
										}
									],
									"doc_count_error_upper_bound": -1,
									"sum_other_doc_count": 991
								},
								"doc_count": 1036,
								"key": "a1"
							},
							{
								"8": {
									"buckets": [
										{
											"doc_count": 17,
											"key": "b21"
										},
										{
											"doc_count": 17,
											"key": "b22"
										}
									],
									"doc_count_error_upper_bound": 0,
									"sum_other_doc_count": 0
								},
								"doc_count": 34,
								"key": "a2"
							},
							{
								"8": {
									"buckets": [
										{
											"doc_count": 17,
											"key": "b31"
										},
										{
											"doc_count": 17,
											"key": "b32"
										}
									],
									"doc_count_error_upper_bound": 0,
									"sum_other_doc_count": 0
								},
								"doc_count": 34,
								"key": "a3"
							}
						],
						"doc_count_error_upper_bound": -1,
						"sum_other_doc_count": 33220
					}
				},
				"hits": {
					"hits": [],
					"max_score": null,
					"total": {
						"relation": "eq",
						"value": 50427
					}
				},
				"timed_out": false,
				"took": 554
			},
			"start_time_in_millis": 1720352001739
		}`,
		ExpectedResults: [][]model.QueryResultRow{
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a1"),
					model.NewQueryResultCol("limbName", "b11"),
					model.NewQueryResultCol("count()", 21),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a1"),
					model.NewQueryResultCol("limbName", "b12"),
					model.NewQueryResultCol("count()", 24),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a2"),
					model.NewQueryResultCol("limbName", "b21"),
					model.NewQueryResultCol("count()", 17),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a2"),
					model.NewQueryResultCol("limbName", "b22"),
					model.NewQueryResultCol("count()", 17),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a3"),
					model.NewQueryResultCol("limbName", "b31"),
					model.NewQueryResultCol("count()", 17),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a3"),
					model.NewQueryResultCol("limbName", "b32"),
					model.NewQueryResultCol("count()", 17),
				}},
			},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a1"),
					model.NewQueryResultCol("count()", 1036),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a2"),
					model.NewQueryResultCol("count()", 34),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a3"),
					model.NewQueryResultCol("count()", 34),
				}},
			},
		},
		ExpectedPancakeResults: []model.QueryResultRow{
			// nil in the middle
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", 34324),
				model.NewQueryResultCol("aggr__2__key_0", "a1"),
				model.NewQueryResultCol("aggr__2__count", int64(1036)),
				model.NewQueryResultCol("aggr__2__order_1", 1036),
				model.NewQueryResultCol("aggr__2__8__parent_count", 1036),
				model.NewQueryResultCol("aggr__2__8__key_0", "b11"),
				model.NewQueryResultCol("aggr__2__8__count", int64(21)),
				model.NewQueryResultCol("aggr__2__8__order_1", 21),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", 34324),
				model.NewQueryResultCol("aggr__2__key_0", "a1"),
				model.NewQueryResultCol("aggr__2__count", int64(1036)),
				model.NewQueryResultCol("aggr__2__order_1", 1036),
				model.NewQueryResultCol("aggr__2__8__parent_count", 1036),
				model.NewQueryResultCol("aggr__2__8__key_0", nil),
				model.NewQueryResultCol("aggr__2__8__count", int64(17)),
				model.NewQueryResultCol("aggr__2__8__order_1", 17),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", 34324),
				model.NewQueryResultCol("aggr__2__key_0", "a1"),
				model.NewQueryResultCol("aggr__2__count", int64(1036)),
				model.NewQueryResultCol("aggr__2__order_1", 1036),
				model.NewQueryResultCol("aggr__2__8__parent_count", 1036),
				model.NewQueryResultCol("aggr__2__8__key_0", "b12"),
				model.NewQueryResultCol("aggr__2__8__count", int64(24)),
				model.NewQueryResultCol("aggr__2__8__order_1", 24),
			}},
			// nil at the beginningą
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", 34290),
				model.NewQueryResultCol("aggr__2__key_0", "a2"),
				model.NewQueryResultCol("aggr__2__count", int64(34)),
				model.NewQueryResultCol("aggr__2__order_1", 34),
				model.NewQueryResultCol("aggr__2__8__parent_count", 34),
				model.NewQueryResultCol("aggr__2__8__key_0", nil),
				model.NewQueryResultCol("aggr__2__8__count", int64(57)),
				model.NewQueryResultCol("aggr__2__8__order_1", 57),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", 34290),
				model.NewQueryResultCol("aggr__2__key_0", "a2"),
				model.NewQueryResultCol("aggr__2__count", int64(34)),
				model.NewQueryResultCol("aggr__2__order_1", 34),
				model.NewQueryResultCol("aggr__2__8__parent_count", 34),
				model.NewQueryResultCol("aggr__2__8__key_0", "b21"),
				model.NewQueryResultCol("aggr__2__8__count", int64(17)),
				model.NewQueryResultCol("aggr__2__8__order_1", 17),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", 34290),
				model.NewQueryResultCol("aggr__2__key_0", "a2"),
				model.NewQueryResultCol("aggr__2__count", int64(34)),
				model.NewQueryResultCol("aggr__2__order_1", 34),
				model.NewQueryResultCol("aggr__2__8__parent_count", 34),
				model.NewQueryResultCol("aggr__2__8__key_0", "b22"),
				model.NewQueryResultCol("aggr__2__8__count", int64(17)),
				model.NewQueryResultCol("aggr__2__8__order_1", 17),
			}},
			// nil at the end
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", 34290),
				model.NewQueryResultCol("aggr__2__key_0", "a3"),
				model.NewQueryResultCol("aggr__2__count_1", int64(34)),
				model.NewQueryResultCol("aggr__2__order_1", 34),
				model.NewQueryResultCol("aggr__2__8__parent_count", 34),
				model.NewQueryResultCol("aggr__2__8__key_0", "b31"),
				model.NewQueryResultCol("aggr__2__8__count_1", int64(17)),
				model.NewQueryResultCol("aggr__2__8__order_1", 17),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", 34290),
				model.NewQueryResultCol("aggr__2__key_0", "a3"),
				model.NewQueryResultCol("aggr__2__count_1", int64(34)),
				model.NewQueryResultCol("aggr__2__order_1", 34),
				model.NewQueryResultCol("aggr__2__8__parent_count", 34),
				model.NewQueryResultCol("aggr__2__8__key_0", "b32"),
				model.NewQueryResultCol("aggr__2__8__count_1", int64(17)),
				model.NewQueryResultCol("aggr__2__8__order_1", 17),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", 34290),
				model.NewQueryResultCol("aggr__2__key_0", "a3"),
				model.NewQueryResultCol("aggr__2__count_1", int64(34)),
				model.NewQueryResultCol("aggr__2__order_1", 34),
				model.NewQueryResultCol("aggr__2__8__parent_count", 34),
				model.NewQueryResultCol("aggr__2__8__key_0", nil),
				model.NewQueryResultCol("aggr__2__8__count_1", int64(17)),
				model.NewQueryResultCol("aggr__2__8__order_1", 17),
			}},
		},
		ExpectedSQLs: []string{
			`WITH cte_1 AS (` +
				`SELECT "surname" AS "cte_1_1", count() AS "cte_1_cnt" ` +
				`FROM "logs-generic-default" ` +
				`WHERE "surname" IS NOT NULL ` +
				`GROUP BY "surname" ` +
				`ORDER BY count() DESC, "surname" ` +
				`LIMIT 200) ` +
				`SELECT "surname", "limbName", count() ` +
				`FROM "logs-generic-default" INNER JOIN "cte_1" ON "surname" = "cte_1_1" ` +
				`WHERE ("surname" IS NOT NULL AND "limbName" IS NOT NULL) ` +
				`GROUP BY "surname", "limbName", cte_1_cnt ` +
				`ORDER BY cte_1_cnt DESC, "surname", count() DESC, "limbName" ` +
				`LIMIT 20 BY "surname"`,
			`SELECT "surname", count() ` +
				`FROM "logs-generic-default" ` +
				`WHERE "surname" IS NOT NULL ` +
				`GROUP BY "surname" ` +
				`ORDER BY count() DESC, "surname" ` +
				`LIMIT 200`,
		},
		ExpectedPancakeSQL: `
			SELECT "aggr__2__parent_count", "aggr__2__key_0", "aggr__2__count",
			  "aggr__2__order_1", "aggr__2__8__parent_count", "aggr__2__8__key_0",
			  "aggr__2__8__count", "aggr__2__8__order_1"
			FROM (
			  SELECT "aggr__2__parent_count", "aggr__2__key_0", "aggr__2__count",
				"aggr__2__order_1", "aggr__2__8__parent_count", "aggr__2__8__key_0",
				"aggr__2__8__count", "aggr__2__8__order_1",
				dense_rank() OVER (ORDER BY "aggr__2__order_1" DESC, "aggr__2__key_0" ASC)
				AS "aggr__2__order_1_rank",
				dense_rank() OVER (PARTITION BY "aggr__2__key_0" ORDER BY
				"aggr__2__8__order_1" DESC, "aggr__2__8__key_0" ASC) AS
				"aggr__2__8__order_1_rank"
			  FROM (
				SELECT sum(count(*)) OVER () AS "aggr__2__parent_count",
				  "surname" AS "aggr__2__key_0",
				  sum(count(*)) OVER (PARTITION BY "aggr__2__key_0") AS "aggr__2__count",
				  sum(count()) OVER (PARTITION BY "aggr__2__key_0") AS "aggr__2__order_1",
				  sum(count(*)) OVER (PARTITION BY "aggr__2__key_0") AS
				  "aggr__2__8__parent_count", "limbName" AS "aggr__2__8__key_0",
				  count(*) AS "aggr__2__8__count", count() AS "aggr__2__8__order_1"
				FROM "logs-generic-default"
				GROUP BY "surname" AS "aggr__2__key_0", "limbName" AS "aggr__2__8__key_0"))
			WHERE ("aggr__2__order_1_rank"<=201 AND "aggr__2__8__order_1_rank"<=21)
			ORDER BY "aggr__2__order_1_rank" ASC, "aggr__2__8__order_1_rank" ASC`,
	},
	{ // [46]
		TestName: "2x terms with nulls 3/4, nulls in the first aggregation, with missing parameter",
		QueryRequestJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"2": {
					"aggs": {
						"8": {
							"terms": {
								"field": "limbName",
								"missing": "__missing__",
								"size": 20
							}
						}
					},
					"terms": {
						"field": "surname",
						"shard_size": 1000,
						"missing": "miss",
						"size": 200
					}
				}
			},
			"fields": [],
			"runtime_mappings": {},
			"script_fields": {},
			"size": 0,
			"stored_fields": [
				"*"
			],
			"track_total_hits": false
		}`,
		ExpectedResponse: `
		{
			"completion_time_in_millis": 1720352002293,
			"expiration_time_in_millis": 1720352062445,
			"id": "FnpTUXdfTTZLUlBtQVo1YzBTVFBseEEcM19IaHdFWG5RN1d1eV9VaUcxenYwdzo0MTc0MA==",
			"is_partial": false,
			"is_running": false,
			"response": {
				"_shards": {
					"failed": 0,
					"skipped": 0,
					"successful": 1,
					"total": 1
				},
				"aggregations": {
					"2": {
						"buckets": [
							{
								"8": {
									"buckets": [
										{
											"doc_count": 21,
											"key": "__missing__"
										},
										{
											"doc_count": 24,
											"key": "b12"
										}
									],
									"doc_count_error_upper_bound": -1,
									"sum_other_doc_count": 991
								},
								"doc_count": 1036,
								"key": "miss"
							},
							{
								"8": {
									"buckets": [
										{
											"doc_count": 17,
											"key": "b21"
										},
										{
											"doc_count": 17,
											"key": "__missing__"
										}
									],
									"doc_count_error_upper_bound": 0,
									"sum_other_doc_count": 0
								},
								"doc_count": 34,
								"key": "a2"
							}
						],
						"doc_count_error_upper_bound": -1,
						"sum_other_doc_count": 33220
					}
				},
				"hits": {
					"hits": [],
					"max_score": null,
					"total": {
						"relation": "eq",
						"value": 50427
					}
				},
				"timed_out": false,
				"took": 554
			},
			"start_time_in_millis": 1720352001739
		}`,
		ExpectedResults: [][]model.QueryResultRow{
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "miss"),
					model.NewQueryResultCol("limbName", "__missing__"),
					model.NewQueryResultCol("count()", 21),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "miss"),
					model.NewQueryResultCol("limbName", "b12"),
					model.NewQueryResultCol("count()", 24),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a2"),
					model.NewQueryResultCol("limbName", "b21"),
					model.NewQueryResultCol("count()", 17),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a2"),
					model.NewQueryResultCol("limbName", "__missing__"),
					model.NewQueryResultCol("count()", 17),
				}},
			},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "miss"),
					model.NewQueryResultCol("count()", 1036),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a2"),
					model.NewQueryResultCol("count()", 34),
				}},
			},
		},
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", 34290),
				model.NewQueryResultCol("aggr__2__key_0", "miss"),
				model.NewQueryResultCol("aggr__2__count", int64(1036)),
				model.NewQueryResultCol("aggr__2__order_1", 1036),
				model.NewQueryResultCol("aggr__2__8__parent_count", 1036),
				model.NewQueryResultCol("aggr__2__8__key_0", "__missing__"),
				model.NewQueryResultCol("aggr__2__8__count", int64(21)),
				model.NewQueryResultCol("aggr__2__8__order_1", 21),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", 34290),
				model.NewQueryResultCol("aggr__2__key_0", "miss"),
				model.NewQueryResultCol("aggr__2__count", int64(1036)),
				model.NewQueryResultCol("aggr__2__order_1", 1036),
				model.NewQueryResultCol("aggr__2__8__parent_count", 1036),
				model.NewQueryResultCol("aggr__2__8__key_0", "b12"),
				model.NewQueryResultCol("aggr__2__8__count", int64(24)),
				model.NewQueryResultCol("aggr__2__8__order_1", 24),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", 34290),
				model.NewQueryResultCol("aggr__2__key_0", "a2"),
				model.NewQueryResultCol("aggr__2__count", int64(34)),
				model.NewQueryResultCol("aggr__2__order_1", 34),
				model.NewQueryResultCol("aggr__2__8__parent_count", 34),
				model.NewQueryResultCol("aggr__2__8__key_0", "b21"),
				model.NewQueryResultCol("aggr__2__8__count", int64(17)),
				model.NewQueryResultCol("aggr__2__8__order_1", 17),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", 34290),
				model.NewQueryResultCol("aggr__2__key_0", "a2"),
				model.NewQueryResultCol("aggr__2__count", int64(34)),
				model.NewQueryResultCol("aggr__2__order_1", 34),
				model.NewQueryResultCol("aggr__2__8__parent_count", 34),
				model.NewQueryResultCol("aggr__2__8__key_0", "__missing__"),
				model.NewQueryResultCol("aggr__2__8__count", int64(17)),
				model.NewQueryResultCol("aggr__2__8__order_1", 17),
			}},
		},
		ExpectedSQLs: []string{
			`WITH cte_1 AS (` +
				`SELECT COALESCE("surname",'miss') AS "cte_1_1", count() AS "cte_1_cnt" ` +
				`FROM "logs-generic-default" ` +
				`GROUP BY COALESCE("surname",'miss') ` +
				`ORDER BY count() DESC, COALESCE("surname",'miss') ` +
				`LIMIT 200) ` +
				`SELECT COALESCE("surname",'miss'), COALESCE("limbName",'__missing__'), count() ` +
				`FROM "logs-generic-default" INNER JOIN "cte_1" ON COALESCE("surname",'miss') = "cte_1_1" ` +
				`GROUP BY COALESCE("surname",'miss'), COALESCE("limbName",'__missing__'), cte_1_cnt ` +
				`ORDER BY cte_1_cnt DESC, COALESCE("surname",'miss'), count() DESC, COALESCE("limbName",'__missing__') ` +
				`LIMIT 20 BY COALESCE("surname",'miss')`,
			`SELECT COALESCE("surname",'miss'), count() ` +
				`FROM "logs-generic-default" ` +
				`GROUP BY COALESCE("surname",'miss') ` +
				`ORDER BY count() DESC, COALESCE("surname",'miss') ` +
				`LIMIT 200`,
		},
		ExpectedPancakeSQL: `
			SELECT "aggr__2__parent_count", "aggr__2__key_0", "aggr__2__count",
			  "aggr__2__order_1", "aggr__2__8__parent_count", "aggr__2__8__key_0",
			  "aggr__2__8__count", "aggr__2__8__order_1"
			FROM (
			  SELECT "aggr__2__parent_count", "aggr__2__key_0", "aggr__2__count",
				"aggr__2__order_1", "aggr__2__8__parent_count", "aggr__2__8__key_0",
				"aggr__2__8__count", "aggr__2__8__order_1",
				dense_rank() OVER (ORDER BY "aggr__2__order_1" DESC, "aggr__2__key_0" ASC)
				AS "aggr__2__order_1_rank",
				dense_rank() OVER (PARTITION BY "aggr__2__key_0" ORDER BY
				"aggr__2__8__order_1" DESC, "aggr__2__8__key_0" ASC) AS
				"aggr__2__8__order_1_rank"
			  FROM (
				SELECT sum(count(*)) OVER () AS "aggr__2__parent_count",
				  COALESCE("surname", 'miss') AS "aggr__2__key_0",
				  sum(count(*)) OVER (PARTITION BY "aggr__2__key_0") AS "aggr__2__count",
				  sum(count()) OVER (PARTITION BY "aggr__2__key_0") AS "aggr__2__order_1",
				  sum(count(*)) OVER (PARTITION BY "aggr__2__key_0") AS
				  "aggr__2__8__parent_count",
				  COALESCE("limbName", '__missing__') AS "aggr__2__8__key_0",
				  count(*) AS "aggr__2__8__count", count() AS "aggr__2__8__order_1"
				FROM "logs-generic-default"
				GROUP BY COALESCE("surname", 'miss') AS "aggr__2__key_0",
				  COALESCE("limbName", '__missing__') AS "aggr__2__8__key_0"))
			WHERE ("aggr__2__order_1_rank"<=200 AND "aggr__2__8__order_1_rank"<=20)
			ORDER BY "aggr__2__order_1_rank" ASC, "aggr__2__8__order_1_rank" ASC`,
	},
	{ // [47]
		TestName: "2x terms with nulls 4/4, nulls in the first aggregation, without missing parameter",
		QueryRequestJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"2": {
					"aggs": {
						"8": {
							"terms": {
								"field": "limbName",
								"size": 20
							}
						}
					},
					"terms": {
						"field": "surname",
						"shard_size": 1000,
						"size": 200
					}
				}
			},
			"fields": [],
			"runtime_mappings": {},
			"script_fields": {},
			"size": 0,
			"stored_fields": [
				"*"
			],
			"track_total_hits": false
		}`,
		ExpectedResponse: `
		{
			"completion_time_in_millis": 1720352002293,
			"expiration_time_in_millis": 1720352062445,
			"id": "FnpTUXdfTTZLUlBtQVo1YzBTVFBseEEcM19IaHdFWG5RN1d1eV9VaUcxenYwdzo0MTc0MA==",
			"is_partial": false,
			"is_running": false,
			"response": {
				"_shards": {
					"failed": 0,
					"skipped": 0,
					"successful": 1,
					"total": 1
				},
				"aggregations": {
					"2": {
						"buckets": [
							{
								"8": {
									"buckets": [
										{
											"doc_count": 21,
											"key": "b11"
										},
										{
											"doc_count": 24,
											"key": "b12"
										}
									],
									"doc_count_error_upper_bound": -1,
									"sum_other_doc_count": 991
								},
								"doc_count": 1036,
								"key": "a1"
							},
							{
								"8": {
									"buckets": [
										{
											"doc_count": 17,
											"key": "b21"
										},
										{
											"doc_count": 17,
											"key": "b22"
										}
									],
									"doc_count_error_upper_bound": 0,
									"sum_other_doc_count": 0
								},
								"doc_count": 34,
								"key": "a2"
							}
						],
						"doc_count_error_upper_bound": -1,
						"sum_other_doc_count": 33220
					}
				},
				"hits": {
					"hits": [],
					"max_score": null,
					"total": {
						"relation": "eq",
						"value": 50427
					}
				},
				"timed_out": false,
				"took": 554
			},
			"start_time_in_millis": 1720352001739
		}`,
		ExpectedResults: [][]model.QueryResultRow{
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a1"),
					model.NewQueryResultCol("limbName", "b11"),
					model.NewQueryResultCol("count()", 21),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a1"),
					model.NewQueryResultCol("limbName", "b12"),
					model.NewQueryResultCol("count()", 24),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a2"),
					model.NewQueryResultCol("limbName", "b21"),
					model.NewQueryResultCol("count()", 17),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a2"),
					model.NewQueryResultCol("limbName", "b22"),
					model.NewQueryResultCol("count()", 17),
				}},
			},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a1"),
					model.NewQueryResultCol("count()", 1036),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a2"),
					model.NewQueryResultCol("count()", 34),
				}},
			},
		},
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", 34290),
				model.NewQueryResultCol("aggr__2__key_0", "a1"),
				model.NewQueryResultCol("aggr__2__count", int64(1036)),
				model.NewQueryResultCol("aggr__2__order_1", 1036),
				model.NewQueryResultCol("aggr__2__8__parent_count", 1036),
				model.NewQueryResultCol("aggr__2__8__key_0", "b11"),
				model.NewQueryResultCol("aggr__2__8__count", int64(21)),
				model.NewQueryResultCol("aggr__2__8__order_1", 21),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", 34290),
				model.NewQueryResultCol("aggr__2__key_0", "a1"),
				model.NewQueryResultCol("aggr__2__count", int64(1036)),
				model.NewQueryResultCol("aggr__2__order_1", 1036),
				model.NewQueryResultCol("aggr__2__8__parent_count", 1036),
				model.NewQueryResultCol("aggr__2__8__key_0", "b12"),
				model.NewQueryResultCol("aggr__2__8__count", int64(24)),
				model.NewQueryResultCol("aggr__2__8__order_1", 24),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", 34290),
				model.NewQueryResultCol("aggr__2__key_0", nil),
				model.NewQueryResultCol("aggr__2__count", int64(55)),
				model.NewQueryResultCol("aggr__2__order_1", 55),
				model.NewQueryResultCol("aggr__2__8__parent_count", 34),
				model.NewQueryResultCol("aggr__2__8__key_0", "__missing__"),
				model.NewQueryResultCol("aggr__2__8__count", int64(21)),
				model.NewQueryResultCol("aggr__2__8__order_1", 21),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", 34290),
				model.NewQueryResultCol("aggr__2__key_0", nil),
				model.NewQueryResultCol("aggr__2__count", int64(55)),
				model.NewQueryResultCol("aggr__2__order_1", 55),
				model.NewQueryResultCol("aggr__2__8__parent_count", 34),
				model.NewQueryResultCol("aggr__2__8__key_0", "lala"),
				model.NewQueryResultCol("aggr__2__8__count", int64(21)),
				model.NewQueryResultCol("aggr__2__8__order_1", 21),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", 34290),
				model.NewQueryResultCol("aggr__2__key_0", "a2"),
				model.NewQueryResultCol("aggr__2__count", int64(34)),
				model.NewQueryResultCol("aggr__2__order_1", 34),
				model.NewQueryResultCol("aggr__2__8__parent_count", 34),
				model.NewQueryResultCol("aggr__2__8__key_0", "b21"),
				model.NewQueryResultCol("aggr__2__8__count", int64(17)),
				model.NewQueryResultCol("aggr__2__8__order_1", 17),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", 34290),
				model.NewQueryResultCol("aggr__2__key_0", "a2"),
				model.NewQueryResultCol("aggr__2__count", int64(34)),
				model.NewQueryResultCol("aggr__2__order_1", 34),
				model.NewQueryResultCol("aggr__2__8__parent_count", 34290),
				model.NewQueryResultCol("aggr__2__8__key_0", nil),
				model.NewQueryResultCol("aggr__2__8__count", uint64(17)),
				model.NewQueryResultCol("aggr__2__8__order_1", 17),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", 34290),
				model.NewQueryResultCol("aggr__2__key_0", "a2"),
				model.NewQueryResultCol("aggr__2__count", uint64(34)),
				model.NewQueryResultCol("aggr__2__order_1", 34),
				model.NewQueryResultCol("aggr__2__8__parent_count", 34),
				model.NewQueryResultCol("aggr__2__8__key_0", "b22"),
				model.NewQueryResultCol("aggr__2__8__count", int64(17)),
				model.NewQueryResultCol("aggr__2__8__order_1", 17),
			}},
		},
		ExpectedSQLs: []string{
			`WITH cte_1 AS (` +
				`SELECT "surname" AS "cte_1_1", count() AS "cte_1_cnt" ` +
				`FROM "logs-generic-default" ` +
				`WHERE "surname" IS NOT NULL ` +
				`GROUP BY "surname" ` +
				`ORDER BY count() DESC, "surname" ` +
				`LIMIT 200) ` +
				`SELECT "surname", "limbName", count() ` +
				`FROM "logs-generic-default" INNER JOIN "cte_1" ON "surname" = "cte_1_1" ` +
				`WHERE ("surname" IS NOT NULL AND "limbName" IS NOT NULL) ` +
				`GROUP BY "surname", "limbName", cte_1_cnt ` +
				`ORDER BY cte_1_cnt DESC, "surname", count() DESC, "limbName" ` +
				`LIMIT 20 BY "surname"`,
			`SELECT "surname", count() ` +
				`FROM "logs-generic-default" ` +
				`WHERE "surname" IS NOT NULL ` +
				`GROUP BY "surname" ` +
				`ORDER BY count() DESC, "surname" ` +
				`LIMIT 200`,
		},
		ExpectedPancakeSQL: `
			SELECT "aggr__2__parent_count", "aggr__2__key_0", "aggr__2__count",
			  "aggr__2__order_1", "aggr__2__8__parent_count", "aggr__2__8__key_0",
			  "aggr__2__8__count", "aggr__2__8__order_1"
			FROM (
			  SELECT "aggr__2__parent_count", "aggr__2__key_0", "aggr__2__count",
				"aggr__2__order_1", "aggr__2__8__parent_count", "aggr__2__8__key_0",
				"aggr__2__8__count", "aggr__2__8__order_1",
				dense_rank() OVER (ORDER BY "aggr__2__order_1" DESC, "aggr__2__key_0" ASC)
				AS "aggr__2__order_1_rank",
				dense_rank() OVER (PARTITION BY "aggr__2__key_0" ORDER BY
				"aggr__2__8__order_1" DESC, "aggr__2__8__key_0" ASC) AS
				"aggr__2__8__order_1_rank"
			  FROM (
				SELECT sum(count(*)) OVER () AS "aggr__2__parent_count",
				  "surname" AS "aggr__2__key_0",
				  sum(count(*)) OVER (PARTITION BY "aggr__2__key_0") AS "aggr__2__count",
				  sum(count()) OVER (PARTITION BY "aggr__2__key_0") AS "aggr__2__order_1",
				  sum(count(*)) OVER (PARTITION BY "aggr__2__key_0") AS
				  "aggr__2__8__parent_count", "limbName" AS "aggr__2__8__key_0",
				  count(*) AS "aggr__2__8__count", count() AS "aggr__2__8__order_1"
				FROM "logs-generic-default"
				GROUP BY "surname" AS "aggr__2__key_0", "limbName" AS "aggr__2__8__key_0"))
			WHERE ("aggr__2__order_1_rank"<=201 AND "aggr__2__8__order_1_rank"<=21)
			ORDER BY "aggr__2__order_1_rank" ASC, "aggr__2__8__order_1_rank" ASC`,
	},
	{ // [48], "old" test, also can be found in testdata/requests.go TestAsyncSearch[3]
		// Copied it also here to be more sure we do not create some regression
		TestName: "2x date_histogram",
		QueryRequestJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"2": {
					"date_histogram": {
						"field": "@timestamp",
						"fixed_interval": "30s",
						"min_doc_count": 1,
						"time_zone": "Europe/Warsaw"
					},
					"aggs": {
						"3": {
							"date_histogram": {
								"field": "@timestamp",
								"fixed_interval": "40s",
								"min_doc_count": 1,
								"time_zone": "Europe/Warsaw"
							}
						}
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
			"size": 5,
			"stored_fields": [
				"*"
			],
			"track_total_hits": true
		}`,
		ExpectedResponse: `
		{
			"completion_time_in_millis": 1706021899595,
			"expiration_time_in_millis": 1706021959594,
			"id": "FjFQMlBUNnJmUU1pWml0WkllNmJWYXcdNVFvOUloYTBUZ3U0Q25MRTJtQTA0dzoyMTEyNzI=",
			"is_partial": false,
			"is_running": false,
			"response": {
				"_shards": {
					"failed": 0,
					"skipped": 0,
					"successful": 1,
					"total": 1
				},
				"aggregations": {
					"2": {
						"buckets": [
							{
								"doc_count": 2,
								"key": 1706021670000,
								"key_as_string": "2024-01-23T14:54:30.000",
								"3": {
									"buckets": [
										{
											"doc_count": 13,
											"key": 1706021800000,
											"key_as_string": "2024-01-23T14:56:40.000"
										}
									]
								}
							},
							{
								"doc_count": 13,
								"key": 1706021820000,
								"key_as_string": "2024-01-23T14:57:00.000",
								"3": {
									"buckets": [
										{
											"doc_count": 2,
											"key": 1706021640000,
											"key_as_string": "2024-01-23T14:54:00.000"
										}
									]
								}
							}
						]
					}
				},
				"hits": {
					"hits": [],
					"max_score": null,
					"total": {
						"relation": "eq",
						"value": 97
					}
				},
				"timed_out": false,
				"took": 1
			},
			"start_time_in_millis": 1706021899594
		}`,
		/*
			ExpectedResults: [][]model.QueryResultRow{
				{{Cols: []model.QueryResultCol{model.NewQueryResultCol("hits", uint64(97))}}},
				{}, // TODO non-aggregation query, maybe fill in results later: now we don't check them
				{
					{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", int64(1706021670000/30000)), model.NewQueryResultCol("doc_count", 2)}},
					{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", int64(1706021700000/30000)), model.NewQueryResultCol("doc_count", 13)}},
					{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", int64(1706021730000/30000)), model.NewQueryResultCol("doc_count", 14)}},
					{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", int64(1706021760000/30000)), model.NewQueryResultCol("doc_count", 14)}},
					{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", int64(1706021790000/30000)), model.NewQueryResultCol("doc_count", 15)}},
					{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", int64(1706021820000/30000)), model.NewQueryResultCol("doc_count", 13)}},
					{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", int64(1706021850000/30000)), model.NewQueryResultCol("doc_count", 15)}},
					{Cols: []model.QueryResultCol{model.NewQueryResultCol("key", int64(1706021880000/30000)), model.NewQueryResultCol("doc_count", 11)}},
				},
			},
		*/
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__key_0", int64(1706021670000/30000)),
				model.NewQueryResultCol("aggr__2__count", 2),
				model.NewQueryResultCol("aggr__2__3__key_0", int64(1706021820000/40000)),
				model.NewQueryResultCol("aggr__2__3__count", 13),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__key_0", int64(1706021820000/30000)),
				model.NewQueryResultCol("aggr__2__count", 13),
				model.NewQueryResultCol("aggr__2__3__key_0", int64(1706021670000/40000)),
				model.NewQueryResultCol("aggr__2__3__count", 2),
			}},
		},
		/*
			ExpectedSQLs: []string{
				`SELECT count() ` +
					`FROM ` + QuotedTableName + ` ` +
					`WHERE ("message" iLIKE '%user%' ` +
					`AND ("@timestamp">=parseDateTime64BestEffort('2024-01-23T14:43:19.481Z') ` +
					`AND "@timestamp"<=parseDateTime64BestEffort('2024-01-23T14:58:19.481Z')))`,
				`SELECT "@timestamp" ` +
					`FROM ` + QuotedTableName + ` ` +
					`WHERE ("message" iLIKE '%user%' AND ("@timestamp">=parseDateTime64BestEffort('2024-01-23T14:43:19.481Z') ` +
					`AND "@timestamp"<=parseDateTime64BestEffort('2024-01-23T14:58:19.481Z'))) ` +
					`LIMIT 5`,
				`SELECT ` + timestampGroupByClause + `, count() ` +
					`FROM ` + QuotedTableName + ` ` +
					`WHERE ("message" iLIKE '%user%' ` +
					`AND ("@timestamp">=parseDateTime64BestEffort('2024-01-23T14:43:19.481Z') ` +
					`AND "@timestamp"<=parseDateTime64BestEffort('2024-01-23T14:58:19.481Z'))) ` +
					`GROUP BY ` + timestampGroupByClause + ` ` +
					`ORDER BY ` + timestampGroupByClause,
			},
		*/
		ExpectedPancakeSQL: `
			SELECT "aggr__2__key_0", "aggr__2__count", "aggr__2__3__key_0",
			  "aggr__2__3__count"
			FROM (
			  SELECT "aggr__2__key_0", "aggr__2__count", "aggr__2__3__key_0",
				"aggr__2__3__count",
				dense_rank() OVER (ORDER BY "aggr__2__key_0" ASC) AS "aggr__2__order_1_rank"
				,
				dense_rank() OVER (PARTITION BY "aggr__2__key_0" ORDER BY
				"aggr__2__3__key_0" ASC) AS "aggr__2__3__order_1_rank"
			  FROM (
				SELECT toInt64(toUnixTimestamp64Milli("@timestamp") / 30000) AS
				  "aggr__2__key_0",
				  sum(count(*)) OVER (PARTITION BY "aggr__2__key_0") AS "aggr__2__count",
				  toInt64(toUnixTimestamp64Milli("@timestamp") / 40000) AS
				  "aggr__2__3__key_0", count(*) AS "aggr__2__3__count"
				FROM "logs-generic-default"
				GROUP BY toInt64(toUnixTimestamp64Milli("@timestamp") / 30000) AS
				  "aggr__2__key_0",
				  toInt64(toUnixTimestamp64Milli("@timestamp") / 40000) AS
				  "aggr__2__3__key_0"))
			ORDER BY "aggr__2__order_1_rank" ASC, "aggr__2__3__order_1_rank" ASC`,
	},
	{ // [49] TODO should null be in the response? Maybe try to replicate and see if it's fine as is.
		TestName: "2x histogram",
		QueryRequestJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"2": {
					"histogram": {
						"field": "bytes",
						"interval": 100,
						"min_doc_count": 1
					},
					"aggs": {
						"3": {
							"histogram": {
								"field": "bytes2",
								"interval": 5,
								"min_doc_count": 1
							}
						}
					}
				}
			},
			"docvalue_fields": [
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
									"gte": "2024-05-10T13:47:56.077Z",
									"lte": "2024-05-10T14:02:56.077Z"
								}
							}
						}
					],
					"must": [
						{
							"match_all": {}
						}
					],
					"must_not": [],
					"should": []
				}
			},
			"script_fields": {
				"hour_of_day": {
					"script": {
						"lang": "painless",
						"source": "doc['timestamp'].value.getHour()"
					}
				}
			},
			"size": 0,
			"stored_fields": [
				"*"
			],
			"track_total_hits": true
		}`,
		ExpectedResponse: `
		{
			"_shards": {
				"failed": 0,
				"skipped": 0,
				"successful": 1,
				"total": 1
			},
			"aggregations": {
				"2": {
					"buckets": [
						{
							"doc_count": 1,
							"key": 9100.0,
							"3": {
								"buckets": [
									{
										"key": 12,
										"doc_count": 1
									}
								]
							}
						},
						{
							"doc_count": 2,
							"key": 9700.0,
							"3": {
								"buckets": [
									{
										"key": null,
										"doc_count": 1
									}
								]
							}
						}
					]
				}
			},
			"hits": {
				"hits": [],
				"max_score": null,
				"total": {
					"relation": "eq",
					"value": 6
				}
			},
			"timed_out": false,
			"took": 10
		}`,
		ExpectedResults: [][]model.QueryResultRow{
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol("hits", uint64(6))}}},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("key", 9100.0),
					model.NewQueryResultCol("doc_count", 1),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("key", 9700.0),
					model.NewQueryResultCol("doc_count", 2),
				}},
			},
		},
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__key_0", 9100.0),
				model.NewQueryResultCol("aggr__2__count", 1),
				model.NewQueryResultCol("aggr__2__3__key_0", 12),
				model.NewQueryResultCol("aggr__2__3__count", 1),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__key_0", 9700.0),
				model.NewQueryResultCol("aggr__2__count", 2),
				model.NewQueryResultCol("aggr__2__3__key_0", nil),
				model.NewQueryResultCol("aggr__2__3__count", 1),
			}},
		},
		ExpectedSQLs: []string{
			`SELECT count() ` +
				`FROM ` + QuotedTableName + ` ` +
				`WHERE ("timestamp">=parseDateTime64BestEffort('2024-05-10T13:47:56.077Z') ` +
				`AND "timestamp"<=parseDateTime64BestEffort('2024-05-10T14:02:56.077Z'))`,
			`SELECT floor("bytes"/100.000000)*100.000000, count() ` +
				`FROM ` + QuotedTableName + ` ` +
				`WHERE ("timestamp">=parseDateTime64BestEffort('2024-05-10T13:47:56.077Z') ` +
				`AND "timestamp"<=parseDateTime64BestEffort('2024-05-10T14:02:56.077Z')) ` +
				`GROUP BY floor("bytes"/100.000000)*100.000000 ` +
				`ORDER BY floor("bytes"/100.000000)*100.000000`,
		},
		ExpectedPancakeSQL: `
			SELECT "aggr__2__key_0", "aggr__2__count", "aggr__2__3__key_0",
			  "aggr__2__3__count"
			FROM (
			  SELECT "aggr__2__key_0", "aggr__2__count", "aggr__2__3__key_0",
				"aggr__2__3__count",
				dense_rank() OVER (ORDER BY "aggr__2__key_0" ASC) AS "aggr__2__order_1_rank"
				,
				dense_rank() OVER (PARTITION BY "aggr__2__key_0" ORDER BY
				"aggr__2__3__key_0" ASC) AS "aggr__2__3__order_1_rank"
			  FROM (
				SELECT floor("bytes"/100.000000)*100.000000 AS "aggr__2__key_0",
				  sum(count(*)) OVER (PARTITION BY "aggr__2__key_0") AS "aggr__2__count",
				  floor("bytes2"/5.000000)*5.000000 AS "aggr__2__3__key_0",
				  count(*) AS "aggr__2__3__count"
				FROM "logs-generic-default"
				WHERE ("timestamp">=parseDateTime64BestEffort('2024-05-10T13:47:56.077Z')
				  AND "timestamp"<=parseDateTime64BestEffort('2024-05-10T14:02:56.077Z'))
				GROUP BY floor("bytes"/100.000000)*100.000000 AS "aggr__2__key_0",
				  floor("bytes2"/5.000000)*5.000000 AS "aggr__2__3__key_0"))
			ORDER BY "aggr__2__order_1_rank" ASC, "aggr__2__3__order_1_rank" ASC`,
	},
	{ // [50] TODO: what about nulls in histogram? Maybe they should be treated like in terms?
		TestName: "2x histogram with min_doc_count 0",
		QueryRequestJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"2": {
					"histogram": {
						"field": "bytes",
						"interval": 100,
						"min_doc_count": 0
					},
					"aggs": {
						"3": {
							"histogram": {
								"field": "bytes2",
								"interval": 5,
								"min_doc_count": 1
							}
						}
					}
				}
			},
			"docvalue_fields": [
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
									"gte": "2024-05-10T13:47:56.077Z",
									"lte": "2024-05-10T14:02:56.077Z"
								}
							}
						}
					],
					"must": [
						{
							"match_all": {}
						}
					],
					"must_not": [],
					"should": []
				}
			},
			"script_fields": {
				"hour_of_day": {
					"script": {
						"lang": "painless",
						"source": "doc['timestamp'].value.getHour()"
					}
				}
			},
			"size": 0,
			"stored_fields": [
				"*"
			],
			"track_total_hits": true
		}`,
		ExpectedResponse: `
		{
			"_shards": {
				"failed": 0,
				"skipped": 0,
				"successful": 1,
				"total": 1
			},
			"aggregations": {
				"2": {
					"buckets": [
						{
							"doc_count": 1,
							"key": 9100.0,
							"3": {
								"buckets": [
									{
										"key": 12,
										"doc_count": 1
									}
								]
							}
						},
						{
							"doc_count": 0,
							"key": 9200.0
						},
						{
							"doc_count": 0,
							"key": 9300.0
						},
						{
							"doc_count": 0,
							"key": 9400.0
						},
						{
							"doc_count": 0,
							"key": 9500.0
						},
						{
							"doc_count": 0,
							"key": 9600.0
						},
						{
							"doc_count": 2,
							"key": 9700.0,
							"3": {
								"buckets": [
									{
										"key": null,
										"doc_count": 1
									}
								]
							}
						}
					]
				}
			},
			"hits": {
				"hits": [],
				"max_score": null,
				"total": {
					"relation": "eq",
					"value": 6
				}
			},
			"timed_out": false,
			"took": 10
		}`,
		ExpectedResults: [][]model.QueryResultRow{
			{{Cols: []model.QueryResultCol{model.NewQueryResultCol("hits", uint64(6))}}},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("key", 9100.0),
					model.NewQueryResultCol("doc_count", 1),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("key", 9700.0),
					model.NewQueryResultCol("doc_count", 2),
				}},
			},
		},
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__key_0", 9100.0),
				model.NewQueryResultCol("aggr__2__count", 1),
				model.NewQueryResultCol("aggr__2__3__key_0", 12),
				model.NewQueryResultCol("aggr__2__3__count", 1),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__key_0", 9700.0),
				model.NewQueryResultCol("aggr__2__count", 2),
				model.NewQueryResultCol("aggr__2__3__key_0", nil),
				model.NewQueryResultCol("aggr__2__3__count", 1),
			}},
		},
		ExpectedSQLs: []string{
			`SELECT count() ` +
				`FROM ` + QuotedTableName + ` ` +
				`WHERE ("timestamp">=parseDateTime64BestEffort('2024-05-10T13:47:56.077Z') ` +
				`AND "timestamp"<=parseDateTime64BestEffort('2024-05-10T14:02:56.077Z'))`,
			`SELECT floor("bytes"/100.000000)*100.000000, count() ` +
				`FROM ` + QuotedTableName + ` ` +
				`WHERE ("timestamp">=parseDateTime64BestEffort('2024-05-10T13:47:56.077Z') ` +
				`AND "timestamp"<=parseDateTime64BestEffort('2024-05-10T14:02:56.077Z')) ` +
				`GROUP BY floor("bytes"/100.000000)*100.000000 ` +
				`ORDER BY floor("bytes"/100.000000)*100.000000`,
		},
		ExpectedPancakeSQL: `
			SELECT "aggr__2__key_0", "aggr__2__count", "aggr__2__3__key_0",
			  "aggr__2__3__count"
			FROM (
			  SELECT "aggr__2__key_0", "aggr__2__count", "aggr__2__3__key_0",
				"aggr__2__3__count",
				dense_rank() OVER (ORDER BY "aggr__2__key_0" ASC) AS "aggr__2__order_1_rank"
				,
				dense_rank() OVER (PARTITION BY "aggr__2__key_0" ORDER BY
				"aggr__2__3__key_0" ASC) AS "aggr__2__3__order_1_rank"
			  FROM (
				SELECT floor("bytes"/100.000000)*100.000000 AS "aggr__2__key_0",
				  sum(count(*)) OVER (PARTITION BY "aggr__2__key_0") AS "aggr__2__count",
				  floor("bytes2"/5.000000)*5.000000 AS "aggr__2__3__key_0",
				  count(*) AS "aggr__2__3__count"
				FROM "logs-generic-default"
				WHERE ("timestamp">=parseDateTime64BestEffort('2024-05-10T13:47:56.077Z')
				  AND "timestamp"<=parseDateTime64BestEffort('2024-05-10T14:02:56.077Z'))
				GROUP BY floor("bytes"/100.000000)*100.000000 AS "aggr__2__key_0",
				  floor("bytes2"/5.000000)*5.000000 AS "aggr__2__3__key_0"))
			ORDER BY "aggr__2__order_1_rank" ASC, "aggr__2__3__order_1_rank" ASC`,
	},
	{ // [51]
		TestName: "2x terms with sampler in the middle",
		QueryRequestJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"2": {
					"aggs": {
						"8": {
							"sampler": {
								"shard_size": 3333
							},
							"aggs": {
								"5": {
									"terms": {
										"field": "limbName",
										"missing": "__missing__",
										"size": 20
									}
								}
							}
						}
					},
					"terms": {
						"field": "surname",
						"shard_size": 1000,
						"size": 200
					}
				}
			},
			"fields": [],
			"runtime_mappings": {},
			"script_fields": {},
			"size": 0,
			"stored_fields": [
				"*"
			],
			"track_total_hits": false
		}`,
		ExpectedResponse: `
		{
			"completion_time_in_millis": 1720352002293,
			"expiration_time_in_millis": 1720352062445,
			"id": "FnpTUXdfTTZLUlBtQVo1YzBTVFBseEEcM19IaHdFWG5RN1d1eV9VaUcxenYwdzo0MTc0MA==",
			"is_partial": false,
			"is_running": false,
			"response": {
				"_shards": {
					"failed": 0,
					"skipped": 0,
					"successful": 1,
					"total": 1
				},
				"aggregations": {
					"2": {
						"buckets": [
							{
								"8": {
									"doc_count": 1036,
									"5": {
										"buckets": [
											{
												"doc_count": 21,
												"key": "__missing__"
											},
											{
												"doc_count": 24,
												"key": "b12"
											}
										],
										"doc_count_error_upper_bound": -1,
										"sum_other_doc_count": 991
									}
								},
								"doc_count": 1036,
								"key": "a1"
							},
							{
								"8": {
									"doc_count": 34,
									"5": {
										"buckets": [
											{
												"doc_count": 17,
												"key": "b21"
											},
											{
												"doc_count": 17,
												"key": "__missing__"
											}
										],
										"doc_count_error_upper_bound": 0,
										"sum_other_doc_count": 0
									}
								},
								"doc_count": 34,
								"key": "a2"
							}
						],
						"doc_count_error_upper_bound": -1,
						"sum_other_doc_count": 33220
					}
				},
				"hits": {
					"hits": [],
					"max_score": null,
					"total": {
						"relation": "eq",
						"value": 50427
					}
				},
				"timed_out": false,
				"took": 554
			},
			"start_time_in_millis": 1720352001739
		}`,
		ExpectedResults: [][]model.QueryResultRow{
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a1"),
					model.NewQueryResultCol("limbName", "__missing__"),
					model.NewQueryResultCol("count()", 21),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a1"),
					model.NewQueryResultCol("limbName", "b12"),
					model.NewQueryResultCol("count()", 24),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a2"),
					model.NewQueryResultCol("limbName", "b21"),
					model.NewQueryResultCol("count()", 17),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a2"),
					model.NewQueryResultCol("limbName", "__missing__"),
					model.NewQueryResultCol("count()", 17),
				}},
			},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a1"),
					model.NewQueryResultCol("count()", 1036),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a2"),
					model.NewQueryResultCol("count()", 34),
				}},
			},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a1"),
					model.NewQueryResultCol("count()", 1036),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a2"),
					model.NewQueryResultCol("count()", 34),
				}},
			},
		},
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", 34290),
				model.NewQueryResultCol("aggr__2__key_0", "a1"),
				model.NewQueryResultCol("aggr__2__count", int64(1036)),
				model.NewQueryResultCol("aggr__2__order_1", 1036),
				model.NewQueryResultCol("aggr__2__8__count", 1036),
				model.NewQueryResultCol("aggr__2__8__5__parent_count", int64(1036)),
				model.NewQueryResultCol("aggr__2__8__5__key_0", "__missing__"),
				model.NewQueryResultCol("aggr__2__8__5__count", int64(21)),
				model.NewQueryResultCol("aggr__2__8__5__order_1", 21),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", 34290),
				model.NewQueryResultCol("aggr__2__key_0", "a1"),
				model.NewQueryResultCol("aggr__2__count", int64(1036)),
				model.NewQueryResultCol("aggr__2__order_1", 1036),
				model.NewQueryResultCol("aggr__2__8__count", 1036),
				model.NewQueryResultCol("aggr__2__8__5__parent_count", int64(1036)),
				model.NewQueryResultCol("aggr__2__8__5__key_0", "b12"),
				model.NewQueryResultCol("aggr__2__8__5__count", int64(24)),
				model.NewQueryResultCol("aggr__2__8__5__order_1", 24),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", 34290),
				model.NewQueryResultCol("aggr__2__key_0", "a2"),
				model.NewQueryResultCol("aggr__2__count", int64(34)),
				model.NewQueryResultCol("aggr__2__order_1", 34),
				model.NewQueryResultCol("aggr__2__8__count", 34),
				model.NewQueryResultCol("aggr__2__8__5__parent_count", 34),
				model.NewQueryResultCol("aggr__2__8__5__key_0", "b21"),
				model.NewQueryResultCol("aggr__2__8__5__count", int64(17)),
				model.NewQueryResultCol("aggr__2__8__5__order_1", 17),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", 34290),
				model.NewQueryResultCol("aggr__2__key_0", "a2"),
				model.NewQueryResultCol("aggr__2__count", int64(34)),
				model.NewQueryResultCol("aggr__2__order_1", 34),
				model.NewQueryResultCol("aggr__2__8__count", 34),
				model.NewQueryResultCol("aggr__2__8__5__parent_count", 34),
				model.NewQueryResultCol("aggr__2__8__5__key_0", "__missing__"),
				model.NewQueryResultCol("aggr__2__8__5__count", int64(17)),
				model.NewQueryResultCol("aggr__2__8__5__order_1", 17),
			}},
		},
		ExpectedSQLs: []string{
			`WITH cte_1 AS (` +
				`SELECT "surname" AS "cte_1_1", count() AS "cte_1_cnt" ` +
				`FROM "logs-generic-default" ` +
				`WHERE "surname" IS NOT NULL ` +
				`GROUP BY "surname" ` +
				`ORDER BY count() DESC, "surname" ` +
				`LIMIT 200) ` +
				`SELECT "surname", COALESCE("limbName",'__missing__'), count() ` +
				`FROM "logs-generic-default" INNER JOIN "cte_1" ON "surname" = "cte_1_1" ` +
				`WHERE "surname" IS NOT NULL ` +
				`GROUP BY "surname", COALESCE("limbName",'__missing__'), cte_1_cnt ` +
				`ORDER BY cte_1_cnt DESC, "surname", count() DESC, COALESCE("limbName",'__missing__') ` +
				`LIMIT 20 BY "surname"`,
			`WITH cte_1 AS (` +
				`SELECT "surname" AS "cte_1_1", count() AS "cte_1_cnt" ` +
				`FROM "logs-generic-default" ` +
				`WHERE "surname" IS NOT NULL ` +
				`GROUP BY "surname" ` +
				`ORDER BY count() DESC, "surname" ` +
				`LIMIT 200) ` +
				`SELECT "surname", count() ` +
				`FROM "logs-generic-default" INNER JOIN "cte_1" ON "surname" = "cte_1_1" ` +
				`WHERE "surname" IS NOT NULL ` +
				`GROUP BY "surname", cte_1_cnt ` +
				`ORDER BY cte_1_cnt DESC, "surname"`,
			`SELECT "surname", count() ` +
				`FROM "logs-generic-default" ` +
				`WHERE "surname" IS NOT NULL ` +
				`GROUP BY "surname" ` +
				`ORDER BY count() DESC, "surname" ` +
				`LIMIT 200`,
		},
		ExpectedPancakeSQL: `
			SELECT "aggr__2__parent_count", "aggr__2__key_0", "aggr__2__count",
			  "aggr__2__order_1", "aggr__2__8__count", "aggr__2__8__5__parent_count",
			  "aggr__2__8__5__key_0", "aggr__2__8__5__count", "aggr__2__8__5__order_1"
			FROM (
			  SELECT "aggr__2__parent_count", "aggr__2__key_0", "aggr__2__count",
				"aggr__2__order_1", "aggr__2__8__count", "aggr__2__8__5__parent_count",
				"aggr__2__8__5__key_0", "aggr__2__8__5__count", "aggr__2__8__5__order_1",
				dense_rank() OVER (ORDER BY "aggr__2__order_1" DESC, "aggr__2__key_0" ASC)
				AS "aggr__2__order_1_rank",
				dense_rank() OVER (PARTITION BY "aggr__2__key_0" ORDER BY
				"aggr__2__8__5__order_1" DESC, "aggr__2__8__5__key_0" ASC) AS
				"aggr__2__8__5__order_1_rank"
			  FROM (
				SELECT sum(count(*)) OVER () AS "aggr__2__parent_count",
				  "surname" AS "aggr__2__key_0",
				  sum(count(*)) OVER (PARTITION BY "aggr__2__key_0") AS "aggr__2__count",
				  sum(count()) OVER (PARTITION BY "aggr__2__key_0") AS "aggr__2__order_1",
				  sum(count(*)) OVER (PARTITION BY "aggr__2__key_0") AS "aggr__2__8__count",
				  sum(count(*)) OVER (PARTITION BY "aggr__2__key_0") AS
				  "aggr__2__8__5__parent_count",
				  COALESCE("limbName", '__missing__') AS "aggr__2__8__5__key_0",
				  count(*) AS "aggr__2__8__5__count", count() AS "aggr__2__8__5__order_1"
				FROM "logs-generic-default"
				GROUP BY "surname" AS "aggr__2__key_0",
				  COALESCE("limbName", '__missing__') AS "aggr__2__8__5__key_0"))
			WHERE ("aggr__2__order_1_rank"<=201 AND "aggr__2__8__5__order_1_rank"<=20)
			ORDER BY "aggr__2__order_1_rank" ASC, "aggr__2__8__5__order_1_rank" ASC`,
	},
	{ // [52]
		TestName: "2x terms with random_sampler in the middle",
		QueryRequestJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"2": {
					"aggs": {
						"8": {
							"random_sampler": {
								"probability": 1e-06,
								"seed": "1225474982"
							},
							"aggs": {
								"5": {
									"terms": {
										"field": "limbName",
										"missing": "__missing__",
										"size": 20
									}
								}
							}
						}
					},
					"terms": {
						"field": "surname",
						"shard_size": 1000,
						"size": 200
					}
				}
			},
			"fields": [],
			"runtime_mappings": {},
			"script_fields": {},
			"size": 0,
			"stored_fields": [
				"*"
			],
			"track_total_hits": false
		}`,
		ExpectedResponse: `
		{
			"completion_time_in_millis": 1720352002293,
			"expiration_time_in_millis": 1720352062445,
			"id": "FnpTUXdfTTZLUlBtQVo1YzBTVFBseEEcM19IaHdFWG5RN1d1eV9VaUcxenYwdzo0MTc0MA==",
			"is_partial": false,
			"is_running": false,
			"response": {
				"_shards": {
					"failed": 0,
					"skipped": 0,
					"successful": 1,
					"total": 1
				},
				"aggregations": {
					"2": {
						"buckets": [
							{
								"8": {
									"doc_count": 1036,
									"probability": 1e-06,
									"seed": "1225474982",
									"5": {
										"buckets": [
											{
												"doc_count": 21,
												"key": "__missing__"
											},
											{
												"doc_count": 24,
												"key": "b12"
											}
										],
										"doc_count_error_upper_bound": -1,
										"sum_other_doc_count": 991
									}
								},
								"doc_count": 1036,
								"key": "a1"
							},
							{
								"8": {
									"doc_count": 34,
									"probability": 1e-06,
									"seed": "1225474982",
									"5": {
										"buckets": [
											{
												"doc_count": 17,
												"key": "b21"
											},
											{
												"doc_count": 17,
												"key": "__missing__"
											}
										],
										"doc_count_error_upper_bound": 0,
										"sum_other_doc_count": 0
									}
								},
								"doc_count": 34,
								"key": "a2"
							}
						],
						"doc_count_error_upper_bound": -1,
						"sum_other_doc_count": 33220
					}
				},
				"hits": {
					"hits": [],
					"max_score": null,
					"total": {
						"relation": "eq",
						"value": 50427
					}
				},
				"timed_out": false,
				"took": 554
			},
			"start_time_in_millis": 1720352001739
		}`,
		ExpectedResults: [][]model.QueryResultRow{
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a1"),
					model.NewQueryResultCol("limbName", "__missing__"),
					model.NewQueryResultCol("count()", 21),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a1"),
					model.NewQueryResultCol("limbName", "b12"),
					model.NewQueryResultCol("count()", 24),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a2"),
					model.NewQueryResultCol("limbName", "b21"),
					model.NewQueryResultCol("count()", 17),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a2"),
					model.NewQueryResultCol("limbName", "__missing__"),
					model.NewQueryResultCol("count()", 17),
				}},
			},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a1"),
					model.NewQueryResultCol("count()", 1036),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a2"),
					model.NewQueryResultCol("count()", 34),
				}},
			},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a1"),
					model.NewQueryResultCol("count()", 1036),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("surname", "a2"),
					model.NewQueryResultCol("count()", 34),
				}},
			},
		},
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", 34290),
				model.NewQueryResultCol("aggr__2__key_0", "a1"),
				model.NewQueryResultCol("aggr__2__count", int64(1036)),
				model.NewQueryResultCol("aggr__2__order_1", 1036),
				model.NewQueryResultCol("aggr__2__8__count", 1036),
				model.NewQueryResultCol("aggr__2__8__5__parent_count", int64(1036)),
				model.NewQueryResultCol("aggr__2__8__5__key_0", "__missing__"),
				model.NewQueryResultCol("aggr__2__8__5__count", int64(21)),
				model.NewQueryResultCol("aggr__2__8__5__order_1", 21),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", 34290),
				model.NewQueryResultCol("aggr__2__key_0", "a1"),
				model.NewQueryResultCol("aggr__2__count", int64(1036)),
				model.NewQueryResultCol("aggr__2__order_1", 1036),
				model.NewQueryResultCol("aggr__2__8__count", 1036),
				model.NewQueryResultCol("aggr__2__8__5__parent_count", int64(1036)),
				model.NewQueryResultCol("aggr__2__8__5__key_0", "b12"),
				model.NewQueryResultCol("aggr__2__8__5__count", int64(24)),
				model.NewQueryResultCol("aggr__2__8__5__order_1", 24),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", 34290),
				model.NewQueryResultCol("aggr__2__key_0", "a2"),
				model.NewQueryResultCol("aggr__2__count", int64(34)),
				model.NewQueryResultCol("aggr__2__order_1", 34),
				model.NewQueryResultCol("aggr__2__8__count", 34),
				model.NewQueryResultCol("aggr__2__8__5__parent_count", 34),
				model.NewQueryResultCol("aggr__2__8__5__key_0", "b21"),
				model.NewQueryResultCol("aggr__2__8__5__count", int64(17)),
				model.NewQueryResultCol("aggr__2__8__5__order_1", 17),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", 34290),
				model.NewQueryResultCol("aggr__2__key_0", "a2"),
				model.NewQueryResultCol("aggr__2__count", int64(34)),
				model.NewQueryResultCol("aggr__2__order_1", 34),
				model.NewQueryResultCol("aggr__2__8__count", 34),
				model.NewQueryResultCol("aggr__2__8__5__parent_count", 34),
				model.NewQueryResultCol("aggr__2__8__5__key_0", "__missing__"),
				model.NewQueryResultCol("aggr__2__8__5__count", int64(17)),
				model.NewQueryResultCol("aggr__2__8__5__order_1", 17),
			}},
		},
		ExpectedSQLs: []string{
			`WITH cte_1 AS (` +
				`SELECT "surname" AS "cte_1_1", count() AS "cte_1_cnt" ` +
				`FROM "logs-generic-default" ` +
				`WHERE "surname" IS NOT NULL ` +
				`GROUP BY "surname" ` +
				`ORDER BY count() DESC, "surname" ` +
				`LIMIT 200) ` +
				`SELECT "surname", COALESCE("limbName",'__missing__'), count() ` +
				`FROM "logs-generic-default" INNER JOIN "cte_1" ON "surname" = "cte_1_1" ` +
				`WHERE "surname" IS NOT NULL ` +
				`GROUP BY "surname", COALESCE("limbName",'__missing__'), cte_1_cnt ` +
				`ORDER BY cte_1_cnt DESC, "surname", count() DESC, COALESCE("limbName",'__missing__') ` +
				`LIMIT 20 BY "surname"`,
			`WITH cte_1 AS (` +
				`SELECT "surname" AS "cte_1_1", count() AS "cte_1_cnt" ` +
				`FROM "logs-generic-default" ` +
				`WHERE "surname" IS NOT NULL ` +
				`GROUP BY "surname" ` +
				`ORDER BY count() DESC, "surname" ` +
				`LIMIT 200) ` +
				`SELECT "surname", count() ` +
				`FROM "logs-generic-default" INNER JOIN "cte_1" ON "surname" = "cte_1_1" ` +
				`WHERE "surname" IS NOT NULL ` +
				`GROUP BY "surname", cte_1_cnt ` +
				`ORDER BY cte_1_cnt DESC, "surname"`,
			`SELECT "surname", count() ` +
				`FROM "logs-generic-default" ` +
				`WHERE "surname" IS NOT NULL ` +
				`GROUP BY "surname" ` +
				`ORDER BY count() DESC, "surname" ` +
				`LIMIT 200`,
		},
		ExpectedPancakeSQL: `
			SELECT "aggr__2__parent_count", "aggr__2__key_0", "aggr__2__count",
			  "aggr__2__order_1", "aggr__2__8__count", "aggr__2__8__5__parent_count",
			  "aggr__2__8__5__key_0", "aggr__2__8__5__count", "aggr__2__8__5__order_1"
			FROM (
			  SELECT "aggr__2__parent_count", "aggr__2__key_0", "aggr__2__count",
				"aggr__2__order_1", "aggr__2__8__count", "aggr__2__8__5__parent_count",
				"aggr__2__8__5__key_0", "aggr__2__8__5__count", "aggr__2__8__5__order_1",
				dense_rank() OVER (ORDER BY "aggr__2__order_1" DESC, "aggr__2__key_0" ASC)
				AS "aggr__2__order_1_rank",
				dense_rank() OVER (PARTITION BY "aggr__2__key_0" ORDER BY
				"aggr__2__8__5__order_1" DESC, "aggr__2__8__5__key_0" ASC) AS
				"aggr__2__8__5__order_1_rank"
			  FROM (
				SELECT sum(count(*)) OVER () AS "aggr__2__parent_count",
				  "surname" AS "aggr__2__key_0",
				  sum(count(*)) OVER (PARTITION BY "aggr__2__key_0") AS "aggr__2__count",
				  sum(count()) OVER (PARTITION BY "aggr__2__key_0") AS "aggr__2__order_1",
				  sum(count(*)) OVER (PARTITION BY "aggr__2__key_0") AS "aggr__2__8__count",
				  sum(count(*)) OVER (PARTITION BY "aggr__2__key_0") AS
				  "aggr__2__8__5__parent_count",
				  COALESCE("limbName", '__missing__') AS "aggr__2__8__5__key_0",
				  count(*) AS "aggr__2__8__5__count", count() AS "aggr__2__8__5__order_1"
				FROM "logs-generic-default"
				GROUP BY "surname" AS "aggr__2__key_0",
				  COALESCE("limbName", '__missing__') AS "aggr__2__8__5__key_0"))
			WHERE ("aggr__2__order_1_rank"<=201 AND "aggr__2__8__5__order_1_rank"<=20)
			ORDER BY "aggr__2__order_1_rank" ASC, "aggr__2__8__5__order_1_rank" ASC`,
	},
}
