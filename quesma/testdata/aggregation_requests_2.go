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
						"min_doc_count": 1
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
						"min_doc_count": 1
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
						"min_doc_count": 1
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
						"min_doc_count": 1
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
						"min_doc_count": 1
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
						"min_doc_count": 1
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
						"min_doc_count": 1
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
				"day1": {
					"buckets": [
						{
							"key_as_string": "2024-06-09T22:00:00.000",
							"key": 1717970400000,
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
				}
			}
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__day1__key_0", int64(1717980400000/86400000)),
				model.NewQueryResultCol("aggr__day1__count", uint64(33)),
			}},
		},
		ExpectedAdditionalPancakeResults: [][]model.QueryResultRow{
			{{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__day2__key_0", int64(1717980400000/86400000)),
				model.NewQueryResultCol("aggr__day2__count", uint64(33)),
			}}},
			{{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__hour1__key_0", int64(1718031600000/3600000)),
				model.NewQueryResultCol("aggr__hour1__count", uint64(33)),
			}}},
			{{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__hour2__key_0", int64(1718024400000/3600000)),
				model.NewQueryResultCol("aggr__hour2__count", uint64(33)),
			}}},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("aggr__minute1__key_0", int64(1718033040000/60000)),
					model.NewQueryResultCol("aggr__minute1__count", uint64(9)),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("aggr__minute1__key_0", int64(1718033100000/60000)),
					model.NewQueryResultCol("aggr__minute1__count", uint64(24)),
				}},
			},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("aggr__minute2__key_0", int64(1718025840000/60000)),
					model.NewQueryResultCol("aggr__minute2__count", uint64(9)),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("aggr__minute2__key_0", int64(1718025900000/60000)),
					model.NewQueryResultCol("aggr__minute2__count", uint64(24)),
				}},
			},
			{{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__month1__key_0", int64(1717200000000)),
				model.NewQueryResultCol("aggr__month1__count", uint64(33)),
			}}},
			{{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__month2__key_0", int64(1717192800000)),
				model.NewQueryResultCol("aggr__month2__count", uint64(33)),
			}}},
			{{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__quarter1__key_0", int64(1711929600000)),
				model.NewQueryResultCol("aggr__quarter1__count", uint64(33)),
			}}},
			{{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__quarter2__key_0", int64(1711922400000)),
				model.NewQueryResultCol("aggr__quarter2__count", uint64(33)),
			}}},
			{{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__week1__key_0", int64(1717977600000)),
				model.NewQueryResultCol("aggr__week1__count", uint64(33)),
			}}},
			{{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__week2__key_0", int64(1717970400000)),
				model.NewQueryResultCol("aggr__week2__count", uint64(33)),
			}}},
			{{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__year1__key_0", int64(1704067200000)),
				model.NewQueryResultCol("aggr__year1__count", uint64(33)),
			}}},
			{{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__year2__key_0", int64(1704063600000)),
				model.NewQueryResultCol("aggr__year2__count", uint64(33)),
			}}},
		},
		ExpectedPancakeSQL: `
			SELECT toInt64((toUnixTimestamp64Milli("@timestamp")+timeZoneOffset(toTimezone(
              "@timestamp",'Europe/Warsaw'))*1000) / 86400000) AS "aggr__day1__key_0",
			  count(*) AS "aggr__day1__count"
			FROM ` + TableName + `
			GROUP BY toInt64((toUnixTimestamp64Milli("@timestamp")+timeZoneOffset(toTimezone(
              "@timestamp",'Europe/Warsaw'))*1000) / 86400000) AS "aggr__day1__key_0"
			ORDER BY "aggr__day1__key_0" ASC`,
		ExpectedAdditionalPancakeSQLs: []string{
			`SELECT toInt64(toUnixTimestamp64Milli("@timestamp") / 86400000) AS
			  "aggr__day2__key_0", count(*) AS "aggr__day2__count"
			FROM ` + TableName + `
			GROUP BY toInt64(toUnixTimestamp64Milli("@timestamp") / 86400000) AS
			  "aggr__day2__key_0"
			ORDER BY "aggr__day2__key_0" ASC`,
			`SELECT toInt64((toUnixTimestamp64Milli("@timestamp")+timeZoneOffset(toTimezone(
			"@timestamp",'Europe/Warsaw'))*1000) / 3600000) AS "aggr__hour1__key_0",
			count(*) AS "aggr__hour1__count"
			FROM ` + TableName + `
			GROUP BY toInt64((toUnixTimestamp64Milli("@timestamp")+timeZoneOffset(toTimezone(
			"@timestamp",'Europe/Warsaw'))*1000) / 3600000) AS "aggr__hour1__key_0"
			ORDER BY "aggr__hour1__key_0" ASC`,
			`SELECT toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000) AS
			  "aggr__hour2__key_0", count(*) AS "aggr__hour2__count"
			FROM ` + TableName + `
			GROUP BY toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000) AS
			  "aggr__hour2__key_0"
			ORDER BY "aggr__hour2__key_0" ASC`,
			`SELECT toInt64((toUnixTimestamp64Milli("@timestamp")+timeZoneOffset(toTimezone(
			"@timestamp",'Europe/Warsaw'))*1000) / 60000) AS "aggr__minute1__key_0",
			count(*) AS "aggr__minute1__count"
			FROM ` + TableName + `
			GROUP BY toInt64((toUnixTimestamp64Milli("@timestamp")+timeZoneOffset(toTimezone(
			"@timestamp",'Europe/Warsaw'))*1000) / 60000) AS "aggr__minute1__key_0"
			ORDER BY "aggr__minute1__key_0" ASC`,
			`SELECT toInt64(toUnixTimestamp64Milli("@timestamp") / 60000) AS
			  "aggr__minute2__key_0", count(*) AS "aggr__minute2__count"
			FROM ` + TableName + `
			GROUP BY toInt64(toUnixTimestamp64Milli("@timestamp") / 60000) AS
			  "aggr__minute2__key_0"
			ORDER BY "aggr__minute2__key_0" ASC`,
			`SELECT toInt64(toUnixTimestamp(toStartOfMonth(toTimezone("@timestamp",'Europe/Warsaw'))))*1000
    		  AS "aggr__month1__key_0", count(*) AS "aggr__month1__count"
			FROM ` + TableName + `
			GROUP BY toInt64(toUnixTimestamp(toStartOfMonth(toTimezone("@timestamp",'Europe/Warsaw'))))*1000
			  AS "aggr__month1__key_0"
			ORDER BY "aggr__month1__key_0" ASC`,
			`SELECT toInt64(toUnixTimestamp(toStartOfMonth(toTimezone("@timestamp",'UTC'))))*1000 AS
			  "aggr__month2__key_0", count(*) AS "aggr__month2__count"
			FROM ` + TableName + `
			GROUP BY toInt64(toUnixTimestamp(toStartOfMonth(toTimezone("@timestamp",'UTC'))))*1000 AS
			  "aggr__month2__key_0"
			ORDER BY "aggr__month2__key_0" ASC`,
			`SELECT toInt64(toUnixTimestamp(toStartOfQuarter(toTimezone("@timestamp",'Europe/Warsaw'))))*1000
    		  AS "aggr__quarter1__key_0", count(*) AS "aggr__quarter1__count"
			FROM ` + TableName + `
			GROUP BY toInt64(toUnixTimestamp(toStartOfQuarter(toTimezone("@timestamp",'Europe/Warsaw'))))*1000 AS
			  "aggr__quarter1__key_0"
			ORDER BY "aggr__quarter1__key_0" ASC`,
			`SELECT toInt64(toUnixTimestamp(toStartOfQuarter(toTimezone("@timestamp",'UTC'))))*1000 AS
			  "aggr__quarter2__key_0", count(*) AS "aggr__quarter2__count"
			FROM ` + TableName + `
			GROUP BY toInt64(toUnixTimestamp(toStartOfQuarter(toTimezone("@timestamp",'UTC'))))*1000 AS
			  "aggr__quarter2__key_0"
			ORDER BY "aggr__quarter2__key_0" ASC`,
			`SELECT  toInt64(toUnixTimestamp(toStartOfWeek(toTimezone("@timestamp",'Europe/Warsaw'))))*1000 AS
			  "aggr__week1__key_0", count(*) AS "aggr__week1__count"
			FROM ` + TableName + `
			GROUP BY toInt64(toUnixTimestamp(toStartOfWeek(toTimezone("@timestamp",'Europe/Warsaw'))))*1000
			  AS "aggr__week1__key_0"
			ORDER BY "aggr__week1__key_0" ASC`,
			`SELECT toInt64(toUnixTimestamp(toStartOfWeek(toTimezone("@timestamp",'UTC'))))*1000 AS
			  "aggr__week2__key_0", count(*) AS "aggr__week2__count"
			FROM ` + TableName + `
			GROUP BY toInt64(toUnixTimestamp(toStartOfWeek(toTimezone("@timestamp",'UTC'))))*1000 AS
			  "aggr__week2__key_0"
			ORDER BY "aggr__week2__key_0" ASC`,
			`SELECT toInt64(toUnixTimestamp(toStartOfYear(toTimezone("@timestamp",'Europe/Warsaw'))))*1000
    		  AS "aggr__year1__key_0", count(*) AS "aggr__year1__count"
			FROM ` + TableName + `
			GROUP BY toInt64(toUnixTimestamp(toStartOfYear(toTimezone("@timestamp",'Europe/Warsaw'))))*1000
			  AS "aggr__year1__key_0"
			ORDER BY "aggr__year1__key_0" ASC`,
			`SELECT toInt64(toUnixTimestamp(toStartOfYear(toTimezone("@timestamp",'UTC'))))*1000 AS
			  "aggr__year2__key_0", count(*) AS "aggr__year2__count"
			FROM ` + TableName + `
			GROUP BY toInt64(toUnixTimestamp(toStartOfYear(toTimezone("@timestamp",'UTC'))))*1000 AS
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
					"doc_count": 2786,
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
					]
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
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", 2786),
				model.NewQueryResultCol("aggr__2__key_0", "200"),
				model.NewQueryResultCol("aggr__2__count", 2570),
				model.NewQueryResultCol("metric__2__1_col_0", []time.Time{util.ParseTime("2024-04-21T06:11:13.619Z")}),
				model.NewQueryResultCol("metric__2__1_col_1", []time.Time{util.ParseTime("2024-04-21T12:21:13.414Z")}),
				model.NewQueryResultCol("metric__2__2_col_0", 10),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT sum(count(*)) OVER () AS "aggr__2__parent_count",
			  "response" AS "aggr__2__key_0", count(*) AS "aggr__2__count",
			  quantiles(0.010000)("timestamp") AS "metric__2__1_col_0",
			  quantiles(0.020000)("timestamp") AS "metric__2__1_col_1",
			  sumOrNull("count") AS "metric__2__2_col_0"
			FROM __quesma_table_name
			WHERE ("timestamp">=fromUnixTimestamp64Milli(1713401475845) AND "timestamp"<=fromUnixTimestamp64Milli(1714697475845))
			GROUP BY "response" AS "aggr__2__key_0"
			ORDER BY "aggr__2__count" DESC, "aggr__2__key_0" ASC
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
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", 34290),
				model.NewQueryResultCol("aggr__2__key_0", "a1"),
				model.NewQueryResultCol("aggr__2__count", int64(1036)),
				model.NewQueryResultCol("aggr__2__8__parent_count", 1036),
				model.NewQueryResultCol("aggr__2__8__key_0", "__missing__"),
				model.NewQueryResultCol("aggr__2__8__count", int64(21)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", 34290),
				model.NewQueryResultCol("aggr__2__key_0", "a1"),
				model.NewQueryResultCol("aggr__2__count", int64(1036)),
				model.NewQueryResultCol("aggr__2__8__parent_count", 1036),
				model.NewQueryResultCol("aggr__2__8__key_0", "b12"),
				model.NewQueryResultCol("aggr__2__8__count", int64(24)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", 34290),
				model.NewQueryResultCol("aggr__2__key_0", "a2"),
				model.NewQueryResultCol("aggr__2__count", int64(34)),
				model.NewQueryResultCol("aggr__2__8__parent_count", 34),
				model.NewQueryResultCol("aggr__2__8__key_0", "b21"),
				model.NewQueryResultCol("aggr__2__8__count", int64(17)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", 34290),
				model.NewQueryResultCol("aggr__2__key_0", "a2"),
				model.NewQueryResultCol("aggr__2__count", int64(34)),
				model.NewQueryResultCol("aggr__2__8__parent_count", 34),
				model.NewQueryResultCol("aggr__2__8__key_0", "__missing__"),
				model.NewQueryResultCol("aggr__2__8__count", int64(17)),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT "aggr__2__parent_count", "aggr__2__key_0", "aggr__2__count",
			  "aggr__2__8__parent_count", "aggr__2__8__key_0", "aggr__2__8__count"
			FROM (
			  SELECT "aggr__2__parent_count", "aggr__2__key_0", "aggr__2__count",
				"aggr__2__8__parent_count", "aggr__2__8__key_0", "aggr__2__8__count",
				dense_rank() OVER (ORDER BY "aggr__2__count" DESC, "aggr__2__key_0" ASC) AS
				"aggr__2__order_1_rank",
				dense_rank() OVER (PARTITION BY "aggr__2__key_0" ORDER BY
				"aggr__2__8__count" DESC, "aggr__2__8__key_0" ASC) AS
				"aggr__2__8__order_1_rank"
			  FROM (
				SELECT sum(count(*)) OVER () AS "aggr__2__parent_count",
				  "surname" AS "aggr__2__key_0",
				  sum(count(*)) OVER (PARTITION BY "aggr__2__key_0") AS "aggr__2__count",
				  sum(count(*)) OVER (PARTITION BY "aggr__2__key_0") AS
				  "aggr__2__8__parent_count",
				  COALESCE("limbName", '__missing__') AS "aggr__2__8__key_0",
				  count(*) AS "aggr__2__8__count"
				FROM __quesma_table_name
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
		ExpectedPancakeResults: []model.QueryResultRow{
			// nil in the middle
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", 34324),
				model.NewQueryResultCol("aggr__2__key_0", "a1"),
				model.NewQueryResultCol("aggr__2__count", int64(1036)),
				model.NewQueryResultCol("aggr__2__8__parent_count", 1036),
				model.NewQueryResultCol("aggr__2__8__key_0", "b11"),
				model.NewQueryResultCol("aggr__2__8__count", int64(21)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", 34324),
				model.NewQueryResultCol("aggr__2__key_0", "a1"),
				model.NewQueryResultCol("aggr__2__count", int64(1036)),
				model.NewQueryResultCol("aggr__2__8__parent_count", 1036),
				model.NewQueryResultCol("aggr__2__8__key_0", nil),
				model.NewQueryResultCol("aggr__2__8__count", int64(17)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", 34324),
				model.NewQueryResultCol("aggr__2__key_0", "a1"),
				model.NewQueryResultCol("aggr__2__count", int64(1036)),
				model.NewQueryResultCol("aggr__2__8__parent_count", 1036),
				model.NewQueryResultCol("aggr__2__8__key_0", "b12"),
				model.NewQueryResultCol("aggr__2__8__count", int64(24)),
			}},
			// nil at the beginningą
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", 34290),
				model.NewQueryResultCol("aggr__2__key_0", "a2"),
				model.NewQueryResultCol("aggr__2__count", int64(34)),
				model.NewQueryResultCol("aggr__2__8__parent_count", 34),
				model.NewQueryResultCol("aggr__2__8__key_0", nil),
				model.NewQueryResultCol("aggr__2__8__count", int64(57)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", 34290),
				model.NewQueryResultCol("aggr__2__key_0", "a2"),
				model.NewQueryResultCol("aggr__2__count", int64(34)),
				model.NewQueryResultCol("aggr__2__8__parent_count", 34),
				model.NewQueryResultCol("aggr__2__8__key_0", "b21"),
				model.NewQueryResultCol("aggr__2__8__count", int64(17)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", 34290),
				model.NewQueryResultCol("aggr__2__key_0", "a2"),
				model.NewQueryResultCol("aggr__2__count", int64(34)),
				model.NewQueryResultCol("aggr__2__8__parent_count", 34),
				model.NewQueryResultCol("aggr__2__8__key_0", "b22"),
				model.NewQueryResultCol("aggr__2__8__count", int64(17)),
			}},
			// nil at the end
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", 34290),
				model.NewQueryResultCol("aggr__2__key_0", "a3"),
				model.NewQueryResultCol("aggr__2__count_1", int64(34)),
				model.NewQueryResultCol("aggr__2__8__parent_count", 34),
				model.NewQueryResultCol("aggr__2__8__key_0", "b31"),
				model.NewQueryResultCol("aggr__2__8__count_1", int64(17)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", 34290),
				model.NewQueryResultCol("aggr__2__key_0", "a3"),
				model.NewQueryResultCol("aggr__2__count_1", int64(34)),
				model.NewQueryResultCol("aggr__2__8__parent_count", 34),
				model.NewQueryResultCol("aggr__2__8__key_0", "b32"),
				model.NewQueryResultCol("aggr__2__8__count_1", int64(17)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", 34290),
				model.NewQueryResultCol("aggr__2__key_0", "a3"),
				model.NewQueryResultCol("aggr__2__count_1", int64(34)),
				model.NewQueryResultCol("aggr__2__8__parent_count", 34),
				model.NewQueryResultCol("aggr__2__8__key_0", nil),
				model.NewQueryResultCol("aggr__2__8__count_1", int64(17)),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT "aggr__2__parent_count", "aggr__2__key_0", "aggr__2__count",
			  "aggr__2__8__parent_count", "aggr__2__8__key_0", "aggr__2__8__count"
			FROM (
			  SELECT "aggr__2__parent_count", "aggr__2__key_0", "aggr__2__count",
				"aggr__2__8__parent_count", "aggr__2__8__key_0", "aggr__2__8__count",
				dense_rank() OVER (ORDER BY "aggr__2__count" DESC, "aggr__2__key_0" ASC) AS
				"aggr__2__order_1_rank",
				dense_rank() OVER (PARTITION BY "aggr__2__key_0" ORDER BY
				"aggr__2__8__count" DESC, "aggr__2__8__key_0" ASC) AS
				"aggr__2__8__order_1_rank"
			  FROM (
				SELECT sum(count(*)) OVER () AS "aggr__2__parent_count",
				  "surname" AS "aggr__2__key_0",
				  sum(count(*)) OVER (PARTITION BY "aggr__2__key_0") AS "aggr__2__count",
				  sum(count(*)) OVER (PARTITION BY "aggr__2__key_0") AS
				  "aggr__2__8__parent_count", "limbName" AS "aggr__2__8__key_0",
				  count(*) AS "aggr__2__8__count"
				FROM __quesma_table_name
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
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", 34290),
				model.NewQueryResultCol("aggr__2__key_0", "miss"),
				model.NewQueryResultCol("aggr__2__count", int64(1036)),
				model.NewQueryResultCol("aggr__2__8__parent_count", 1036),
				model.NewQueryResultCol("aggr__2__8__key_0", "__missing__"),
				model.NewQueryResultCol("aggr__2__8__count", int64(21)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", 34290),
				model.NewQueryResultCol("aggr__2__key_0", "miss"),
				model.NewQueryResultCol("aggr__2__count", int64(1036)),
				model.NewQueryResultCol("aggr__2__8__parent_count", 1036),
				model.NewQueryResultCol("aggr__2__8__key_0", "b12"),
				model.NewQueryResultCol("aggr__2__8__count", int64(24)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", 34290),
				model.NewQueryResultCol("aggr__2__key_0", "a2"),
				model.NewQueryResultCol("aggr__2__count", int64(34)),
				model.NewQueryResultCol("aggr__2__8__parent_count", 34),
				model.NewQueryResultCol("aggr__2__8__key_0", "b21"),
				model.NewQueryResultCol("aggr__2__8__count", int64(17)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", 34290),
				model.NewQueryResultCol("aggr__2__key_0", "a2"),
				model.NewQueryResultCol("aggr__2__count", int64(34)),
				model.NewQueryResultCol("aggr__2__8__parent_count", 34),
				model.NewQueryResultCol("aggr__2__8__key_0", "__missing__"),
				model.NewQueryResultCol("aggr__2__8__count", int64(17)),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT "aggr__2__parent_count", "aggr__2__key_0", "aggr__2__count",
			  "aggr__2__8__parent_count", "aggr__2__8__key_0", "aggr__2__8__count"
			FROM (
			  SELECT "aggr__2__parent_count", "aggr__2__key_0", "aggr__2__count",
				"aggr__2__8__parent_count", "aggr__2__8__key_0", "aggr__2__8__count",
				dense_rank() OVER (ORDER BY "aggr__2__count" DESC, "aggr__2__key_0" ASC) AS
				"aggr__2__order_1_rank",
				dense_rank() OVER (PARTITION BY "aggr__2__key_0" ORDER BY
				"aggr__2__8__count" DESC, "aggr__2__8__key_0" ASC) AS
				"aggr__2__8__order_1_rank"
			  FROM (
				SELECT sum(count(*)) OVER () AS "aggr__2__parent_count",
				  COALESCE("surname", 'miss') AS "aggr__2__key_0",
				  sum(count(*)) OVER (PARTITION BY "aggr__2__key_0") AS "aggr__2__count",
				  sum(count(*)) OVER (PARTITION BY "aggr__2__key_0") AS
				  "aggr__2__8__parent_count",
				  COALESCE("limbName", '__missing__') AS "aggr__2__8__key_0",
				  count(*) AS "aggr__2__8__count"
				FROM __quesma_table_name
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
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", 34290),
				model.NewQueryResultCol("aggr__2__key_0", "a1"),
				model.NewQueryResultCol("aggr__2__count", int64(1036)),
				model.NewQueryResultCol("aggr__2__8__parent_count", 1036),
				model.NewQueryResultCol("aggr__2__8__key_0", "b11"),
				model.NewQueryResultCol("aggr__2__8__count", int64(21)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", 34290),
				model.NewQueryResultCol("aggr__2__key_0", "a1"),
				model.NewQueryResultCol("aggr__2__count", int64(1036)),
				model.NewQueryResultCol("aggr__2__8__parent_count", 1036),
				model.NewQueryResultCol("aggr__2__8__key_0", "b12"),
				model.NewQueryResultCol("aggr__2__8__count", int64(24)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", 34290),
				model.NewQueryResultCol("aggr__2__key_0", nil),
				model.NewQueryResultCol("aggr__2__count", int64(55)),
				model.NewQueryResultCol("aggr__2__8__parent_count", 34),
				model.NewQueryResultCol("aggr__2__8__key_0", "__missing__"),
				model.NewQueryResultCol("aggr__2__8__count", int64(21)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", 34290),
				model.NewQueryResultCol("aggr__2__key_0", nil),
				model.NewQueryResultCol("aggr__2__count", int64(55)),
				model.NewQueryResultCol("aggr__2__8__parent_count", 34),
				model.NewQueryResultCol("aggr__2__8__key_0", "lala"),
				model.NewQueryResultCol("aggr__2__8__count", int64(21)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", 34290),
				model.NewQueryResultCol("aggr__2__key_0", "a2"),
				model.NewQueryResultCol("aggr__2__count", int64(34)),
				model.NewQueryResultCol("aggr__2__8__parent_count", 34),
				model.NewQueryResultCol("aggr__2__8__key_0", "b21"),
				model.NewQueryResultCol("aggr__2__8__count", int64(17)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", 34290),
				model.NewQueryResultCol("aggr__2__key_0", "a2"),
				model.NewQueryResultCol("aggr__2__count", int64(34)),
				model.NewQueryResultCol("aggr__2__8__parent_count", 34290),
				model.NewQueryResultCol("aggr__2__8__key_0", nil),
				model.NewQueryResultCol("aggr__2__8__count", uint64(17)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", 34290),
				model.NewQueryResultCol("aggr__2__key_0", "a2"),
				model.NewQueryResultCol("aggr__2__count", uint64(34)),
				model.NewQueryResultCol("aggr__2__8__parent_count", 34),
				model.NewQueryResultCol("aggr__2__8__key_0", "b22"),
				model.NewQueryResultCol("aggr__2__8__count", int64(17)),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT "aggr__2__parent_count", "aggr__2__key_0", "aggr__2__count",
			  "aggr__2__8__parent_count", "aggr__2__8__key_0", "aggr__2__8__count"
			FROM (
			  SELECT "aggr__2__parent_count", "aggr__2__key_0", "aggr__2__count",
				"aggr__2__8__parent_count", "aggr__2__8__key_0", "aggr__2__8__count",
				dense_rank() OVER (ORDER BY "aggr__2__count" DESC, "aggr__2__key_0" ASC) AS
				"aggr__2__order_1_rank",
				dense_rank() OVER (PARTITION BY "aggr__2__key_0" ORDER BY
				"aggr__2__8__count" DESC, "aggr__2__8__key_0" ASC) AS
				"aggr__2__8__order_1_rank"
			  FROM (
				SELECT sum(count(*)) OVER () AS "aggr__2__parent_count",
				  "surname" AS "aggr__2__key_0",
				  sum(count(*)) OVER (PARTITION BY "aggr__2__key_0") AS "aggr__2__count",
				  sum(count(*)) OVER (PARTITION BY "aggr__2__key_0") AS
				  "aggr__2__8__parent_count", "limbName" AS "aggr__2__8__key_0",
				  count(*) AS "aggr__2__8__count"
				FROM __quesma_table_name
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
						"min_doc_count": 1
					},
					"aggs": {
						"3": {
							"date_histogram": {
								"field": "@timestamp",
								"fixed_interval": "40s",
								"min_doc_count": 1
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
				`SELECT count(*) ` +
					`FROM ` + QuotedTableName + ` ` +
					`WHERE ("message" iLIKE '%user%' ` +
					`AND ("@timestamp">=parseDateTime64BestEffort('2024-01-23T14:43:19.481Z') ` +
					`AND "@timestamp"<=parseDateTime64BestEffort('2024-01-23T14:58:19.481Z')))`,
				`SELECT "@timestamp" ` +
					`FROM ` + QuotedTableName + ` ` +
					`WHERE ("message" iLIKE '%user%' AND ("@timestamp">=parseDateTime64BestEffort('2024-01-23T14:43:19.481Z') ` +
					`AND "@timestamp"<=parseDateTime64BestEffort('2024-01-23T14:58:19.481Z'))) ` +
					`LIMIT 5`,
				`SELECT ` + timestampGroupByClause + `, count(*) ` +
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
				FROM ` + TableName + `
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
										"key": 5,
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
				model.NewQueryResultCol("aggr__2__3__key_0", 5),
				model.NewQueryResultCol("aggr__2__3__count", 1),
			}},
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
				SELECT floor("bytes"/100)*100 AS "aggr__2__key_0",
				  sum(count(*)) OVER (PARTITION BY "aggr__2__key_0") AS "aggr__2__count",
				  floor("bytes2"/5)*5 AS "aggr__2__3__key_0",
				  count(*) AS "aggr__2__3__count"
				FROM ` + TableName + `
				WHERE ("timestamp">=fromUnixTimestamp64Milli(1715348876077) AND "timestamp"<=fromUnixTimestamp64Milli(1715349776077))
				GROUP BY floor("bytes"/100)*100 AS "aggr__2__key_0",
				  floor("bytes2"/5)*5 AS "aggr__2__3__key_0"))
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
							"key": 9200.0,
							"3": {
								"buckets": []
							}
						},
						{
							"doc_count": 0,
							"key": 9300.0,
							"3": {
								"buckets": []
							}
						},
						{
							"doc_count": 0,
							"key": 9400.0,
							"3": {
								"buckets": []
							}
						},
						{
							"doc_count": 0,
							"key": 9500.0,
							"3": {
								"buckets": []
							}
						},
						{
							"doc_count": 0,
							"key": 9600.0,
							"3": {
								"buckets": []
							}
						},
						{
							"doc_count": 2,
							"key": 9700.0,
							"3": {
								"buckets": [
									{
										"key": 5,
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
				model.NewQueryResultCol("aggr__2__3__key_0", 5),
				model.NewQueryResultCol("aggr__2__3__count", 1),
			}},
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
				SELECT floor("bytes"/100)*100 AS "aggr__2__key_0",
				  sum(count(*)) OVER (PARTITION BY "aggr__2__key_0") AS "aggr__2__count",
				  floor("bytes2"/5)*5 AS "aggr__2__3__key_0",
				  count(*) AS "aggr__2__3__count"
				FROM ` + TableName + `
				WHERE ("timestamp">=fromUnixTimestamp64Milli(1715348876077) AND "timestamp"<=fromUnixTimestamp64Milli(1715349776077))
				GROUP BY floor("bytes"/100)*100 AS "aggr__2__key_0",
				  floor("bytes2"/5)*5 AS "aggr__2__3__key_0"))
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
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", 34290),
				model.NewQueryResultCol("aggr__2__key_0", "a1"),
				model.NewQueryResultCol("aggr__2__count", int64(1036)),
				model.NewQueryResultCol("aggr__2__8__count", 1036),
				model.NewQueryResultCol("aggr__2__8__5__parent_count", int64(1036)),
				model.NewQueryResultCol("aggr__2__8__5__key_0", "__missing__"),
				model.NewQueryResultCol("aggr__2__8__5__count", int64(21)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", 34290),
				model.NewQueryResultCol("aggr__2__key_0", "a1"),
				model.NewQueryResultCol("aggr__2__count", int64(1036)),
				model.NewQueryResultCol("aggr__2__8__count", 1036),
				model.NewQueryResultCol("aggr__2__8__5__parent_count", int64(1036)),
				model.NewQueryResultCol("aggr__2__8__5__key_0", "b12"),
				model.NewQueryResultCol("aggr__2__8__5__count", int64(24)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", 34290),
				model.NewQueryResultCol("aggr__2__key_0", "a2"),
				model.NewQueryResultCol("aggr__2__count", int64(34)),
				model.NewQueryResultCol("aggr__2__8__count", 34),
				model.NewQueryResultCol("aggr__2__8__5__parent_count", 34),
				model.NewQueryResultCol("aggr__2__8__5__key_0", "b21"),
				model.NewQueryResultCol("aggr__2__8__5__count", int64(17)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", 34290),
				model.NewQueryResultCol("aggr__2__key_0", "a2"),
				model.NewQueryResultCol("aggr__2__count", int64(34)),
				model.NewQueryResultCol("aggr__2__8__count", 34),
				model.NewQueryResultCol("aggr__2__8__5__parent_count", 34),
				model.NewQueryResultCol("aggr__2__8__5__key_0", "__missing__"),
				model.NewQueryResultCol("aggr__2__8__5__count", int64(17)),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT "aggr__2__parent_count", "aggr__2__key_0", "aggr__2__count",
			  "aggr__2__8__count", "aggr__2__8__5__parent_count", "aggr__2__8__5__key_0",
			  "aggr__2__8__5__count"
			FROM (
			  SELECT "aggr__2__parent_count", "aggr__2__key_0", "aggr__2__count",
				"aggr__2__8__count", "aggr__2__8__5__parent_count", "aggr__2__8__5__key_0",
				"aggr__2__8__5__count",
				dense_rank() OVER (ORDER BY "aggr__2__count" DESC, "aggr__2__key_0" ASC) AS
				"aggr__2__order_1_rank",
				dense_rank() OVER (PARTITION BY "aggr__2__key_0" ORDER BY
				"aggr__2__8__5__count" DESC, "aggr__2__8__5__key_0" ASC) AS
				"aggr__2__8__5__order_1_rank"
			  FROM (
				SELECT sum(count(*)) OVER () AS "aggr__2__parent_count",
				  "surname" AS "aggr__2__key_0",
				  sum(count(*)) OVER (PARTITION BY "aggr__2__key_0") AS "aggr__2__count",
				  sum(count(*)) OVER (PARTITION BY "aggr__2__key_0") AS "aggr__2__8__count",
				  sum(count(*)) OVER (PARTITION BY "aggr__2__key_0") AS
				  "aggr__2__8__5__parent_count",
				  COALESCE("limbName", '__missing__') AS "aggr__2__8__5__key_0",
				  count(*) AS "aggr__2__8__5__count"
				FROM __quesma_table_name
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
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", 34290),
				model.NewQueryResultCol("aggr__2__key_0", "a1"),
				model.NewQueryResultCol("aggr__2__count", int64(1036)),
				model.NewQueryResultCol("aggr__2__8__count", 1036),
				model.NewQueryResultCol("aggr__2__8__5__parent_count", int64(1036)),
				model.NewQueryResultCol("aggr__2__8__5__key_0", "__missing__"),
				model.NewQueryResultCol("aggr__2__8__5__count", int64(21)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", 34290),
				model.NewQueryResultCol("aggr__2__key_0", "a1"),
				model.NewQueryResultCol("aggr__2__count", int64(1036)),
				model.NewQueryResultCol("aggr__2__8__count", 1036),
				model.NewQueryResultCol("aggr__2__8__5__parent_count", int64(1036)),
				model.NewQueryResultCol("aggr__2__8__5__key_0", "b12"),
				model.NewQueryResultCol("aggr__2__8__5__count", int64(24)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", 34290),
				model.NewQueryResultCol("aggr__2__key_0", "a2"),
				model.NewQueryResultCol("aggr__2__count", int64(34)),
				model.NewQueryResultCol("aggr__2__8__count", 34),
				model.NewQueryResultCol("aggr__2__8__5__parent_count", 34),
				model.NewQueryResultCol("aggr__2__8__5__key_0", "b21"),
				model.NewQueryResultCol("aggr__2__8__5__count", int64(17)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", 34290),
				model.NewQueryResultCol("aggr__2__key_0", "a2"),
				model.NewQueryResultCol("aggr__2__count", int64(34)),
				model.NewQueryResultCol("aggr__2__8__count", 34),
				model.NewQueryResultCol("aggr__2__8__5__parent_count", 34),
				model.NewQueryResultCol("aggr__2__8__5__key_0", "__missing__"),
				model.NewQueryResultCol("aggr__2__8__5__count", int64(17)),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT "aggr__2__parent_count", "aggr__2__key_0", "aggr__2__count",
			  "aggr__2__8__count", "aggr__2__8__5__parent_count", "aggr__2__8__5__key_0",
			  "aggr__2__8__5__count"
			FROM (
			  SELECT "aggr__2__parent_count", "aggr__2__key_0", "aggr__2__count",
				"aggr__2__8__count", "aggr__2__8__5__parent_count", "aggr__2__8__5__key_0",
				"aggr__2__8__5__count",
				dense_rank() OVER (ORDER BY "aggr__2__count" DESC, "aggr__2__key_0" ASC) AS
				"aggr__2__order_1_rank",
				dense_rank() OVER (PARTITION BY "aggr__2__key_0" ORDER BY
				"aggr__2__8__5__count" DESC, "aggr__2__8__5__key_0" ASC) AS
				"aggr__2__8__5__order_1_rank"
			  FROM (
				SELECT sum(count(*)) OVER () AS "aggr__2__parent_count",
				  "surname" AS "aggr__2__key_0",
				  sum(count(*)) OVER (PARTITION BY "aggr__2__key_0") AS "aggr__2__count",
				  sum(count(*)) OVER (PARTITION BY "aggr__2__key_0") AS "aggr__2__8__count",
				  sum(count(*)) OVER (PARTITION BY "aggr__2__key_0") AS
				  "aggr__2__8__5__parent_count",
				  COALESCE("limbName", '__missing__') AS "aggr__2__8__5__key_0",
				  count(*) AS "aggr__2__8__5__count"
				FROM __quesma_table_name
				GROUP BY "surname" AS "aggr__2__key_0",
				  COALESCE("limbName", '__missing__') AS "aggr__2__8__5__key_0"))
			WHERE ("aggr__2__order_1_rank"<=201 AND "aggr__2__8__5__order_1_rank"<=20)
			ORDER BY "aggr__2__order_1_rank" ASC, "aggr__2__8__5__order_1_rank" ASC`,
	},
	{ // [53]
		TestName: "terms order by quantile, simplest - only one percentile",
		QueryRequestJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"0": {
					"aggs": {
						"1": {
							"aggs": {
								"2": {
									"percentiles": {
										"field": "docker.cpu.total.pct",
										"keyed": false,
										"percents": [
								  			75
										]
									}
								}
							},
							"terms": {
								"field": "container.name",
								"order": {
									"2.75": "desc"
								},
								"shard_size": 25,
								"size": 5
							}
						}
					},
					"date_histogram": {
						"field": "@timestamp",
						"fixed_interval": "12h",
						"min_doc_count": 1
					}
				}
			},
			"fields": [
				{
					"field": "@timestamp",
					"format": "date_time"
				},
				{
					"field": "docker.container.created",
					"format": "date_time"
				}
			],
			"query": {
				"bool": {
					"filter": [
						{
							"bool": {
								"minimum_should_match": 1,
								"should": [
									{
										"term": {
				  							"data_stream.dataset": {
												"value": "docker.cpu"
											}
										}
									}
								]
							}
						},
						{
							"range": {
								"@timestamp": {
									"format": "strict_date_optional_time",
									"gte": "2024-08-18T07:54:12.291Z",
									"lte": "2024-09-02T07:54:12.291Z"
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
			]
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
					"0": {
						"buckets": [
							{
								"key": 1706011200000,
								"key_as_string": "2024-01-23T12:00:00.000",
								"doc_count": 5,
								"1": {
									"buckets": [
										{
											"key": "x",
											"doc_count": 2,
											"2": {
												"values": [
													{
														"key": 75,
														"value": 349.95000000000005
													}
												]
											}
										},
 										{
											"key": "y",
											"doc_count": 1,
											"2": {
												"values": [
													{
														"key": 75,
														"value": 100.2
													}
												]
											}
										}
									],
									"sum_other_doc_count": 2
								}
							},
							{
								"key": 1706054400000,
								"key_as_string": "2024-01-24T00:00:00.000",
								"doc_count": 5,
								"1": {
									"buckets": [
										{
											"key": "a",
											"doc_count": 3,
											"2": {
												"values": [
													{
														"key": 75,
														"value": 5
													}
												]
											}
										}
									],
									"sum_other_doc_count": 2
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
						"value": 50427
					}
				},
				"timed_out": false,
				"took": 554
			},
			"start_time_in_millis": 1720352001739
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__key_0", int64(1706011200000/43200000)),
				model.NewQueryResultCol("aggr__0__count", int64(5)),
				model.NewQueryResultCol("aggr__0__1__parent_count", 5),
				model.NewQueryResultCol("aggr__0__1__key_0", "x"),
				model.NewQueryResultCol("aggr__0__1__count", int64(2)),
				model.NewQueryResultCol("metric__0__1__2_col_0", []float64{349.95000000000005}),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__key_0", int64(1706011200000/43200000)),
				model.NewQueryResultCol("aggr__0__count", int64(5)),
				model.NewQueryResultCol("aggr__0__1__parent_count", 5),
				model.NewQueryResultCol("aggr__0__1__key_0", "y"),
				model.NewQueryResultCol("aggr__0__1__count", int64(1)),
				model.NewQueryResultCol("metric__0__1__2_col_0", []float64{100.2}),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__key_0", int64(1706054400000/43200000)),
				model.NewQueryResultCol("aggr__0__count", int64(5)),
				model.NewQueryResultCol("aggr__0__1__parent_count", 5),
				model.NewQueryResultCol("aggr__0__1__key_0", "a"),
				model.NewQueryResultCol("aggr__0__1__count", int64(3)),
				model.NewQueryResultCol("metric__0__1__2_col_0", []float64{5}),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT "aggr__0__key_0", "aggr__0__count", "aggr__0__1__parent_count",
			  "aggr__0__1__key_0", "aggr__0__1__count", "metric__0__1__2_col_0"
			FROM (
			  SELECT "aggr__0__key_0", "aggr__0__count", "aggr__0__1__parent_count",
				"aggr__0__1__key_0", "aggr__0__1__count", "metric__0__1__2_col_0",
				dense_rank() OVER (ORDER BY "aggr__0__key_0" ASC) AS "aggr__0__order_1_rank"
				,
				dense_rank() OVER (PARTITION BY "aggr__0__key_0" ORDER BY
				"metric__0__1__2_col_0" DESC, "aggr__0__1__key_0" ASC) AS
				"aggr__0__1__order_1_rank"
			  FROM (
				SELECT toInt64(toUnixTimestamp64Milli("@timestamp") / 43200000) AS
				  "aggr__0__key_0",
				  sum(count(*)) OVER (PARTITION BY "aggr__0__key_0") AS "aggr__0__count",
				  sum(count(*)) OVER (PARTITION BY "aggr__0__key_0") AS
				  "aggr__0__1__parent_count", "container.name" AS "aggr__0__1__key_0",
				  count(*) AS "aggr__0__1__count",
				  quantiles(0.750000)("docker.cpu.total.pct") AS "metric__0__1__2_col_0"
				FROM __quesma_table_name
				WHERE ("data_stream.dataset"='docker.cpu' AND ("@timestamp">=
				  fromUnixTimestamp64Milli(1723967652291) AND "@timestamp"<=
				  fromUnixTimestamp64Milli(1725263652291)))
				GROUP BY toInt64(toUnixTimestamp64Milli("@timestamp") / 43200000) AS
				  "aggr__0__key_0", "container.name" AS "aggr__0__1__key_0"))
			WHERE "aggr__0__1__order_1_rank"<=6
			ORDER BY "aggr__0__order_1_rank" ASC, "aggr__0__1__order_1_rank" ASC`,
	},
	{ // [54]
		TestName: "terms order by quantile - more percentiles",
		QueryRequestJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"0": {
					"aggs": {
						"1": {
							"aggs": {
								"2": {
									"percentiles": {
										"field": "docker.cpu.total.pct",
										"keyed": false,
										"percents": [
								  			10, 75, 99
										]
									}
								}
							},
							"terms": {
								"field": "container.name",
								"order": {
									"2.75": "desc"
								},
								"shard_size": 25,
								"size": 5
							}
						}
					},
					"date_histogram": {
						"field": "@timestamp",
						"fixed_interval": "12h",
						"min_doc_count": 1
					}
				}
			},
			"fields": [
				{
					"field": "@timestamp",
					"format": "date_time"
				},
				{
					"field": "docker.container.created",
					"format": "date_time"
				}
			],
			"runtime_mappings": {},
			"script_fields": {},
			"size": 0,
			"stored_fields": [
				"*"
			]
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
					"0": {
						"buckets": [
							{
								"key": 1706011200000,
								"key_as_string": "2024-01-23T12:00:00.000",
								"doc_count": 5,
								"1": {
									"buckets": [
										{
											"key": "x",
											"doc_count": 2,
											"2": {
												"values": [
													{
														"key": 10,
														"value": 349.95000000000005
													},
													{
														"key": 75,
														"value": 10.1
													},
													{
														"key": 99,
														"value": 20.2
													}
												]
											}
										},
 										{
											"key": "y",
											"doc_count": 1,
											"2": {
												"values": [
													{
														"key": 10,
														"value": 100.2
													},
													{
														"key": 75,
														"value": 11.1
													},
													{
														"key": 99,
														"value": 21.2
													}
												]
											}
										}
									],
									"sum_other_doc_count": 2
								}
							},
							{
								"key": 1706054400000,
								"key_as_string": "2024-01-24T00:00:00.000",
								"doc_count": 5,
								"1": {
									"buckets": [
										{
											"key": "a",
											"doc_count": 3,
											"2": {
												"values": [
													{
														"key": 10,
														"value": 5
													},
													{
														"key": 75,
														"value": 12.1
													},
													{
														"key": 99,
														"value": 22.2
													}
												]
											}
										}
									],
									"sum_other_doc_count": 2
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
						"value": 50427
					}
				},
				"timed_out": false,
				"took": 554
			},
			"start_time_in_millis": 1720352001739
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__key_0", int64(1706011200000/43200000)),
				model.NewQueryResultCol("aggr__0__count", int64(5)),
				model.NewQueryResultCol("aggr__0__1__parent_count", 5),
				model.NewQueryResultCol("aggr__0__1__key_0", "x"),
				model.NewQueryResultCol("aggr__0__1__count", int64(2)),
				model.NewQueryResultCol("metric__0__1__2_col_0", []float64{349.95000000000005}),
				model.NewQueryResultCol("metric__0__1__2_col_1", []float64{10.1}),
				model.NewQueryResultCol("metric__0__1__2_col_2", []float64{20.2}),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__key_0", int64(1706011200000/43200000)),
				model.NewQueryResultCol("aggr__0__count", int64(5)),
				model.NewQueryResultCol("aggr__0__1__parent_count", 5),
				model.NewQueryResultCol("aggr__0__1__key_0", "y"),
				model.NewQueryResultCol("aggr__0__1__count", int64(1)),
				model.NewQueryResultCol("metric__0__1__2_col_0", []float64{100.2}),
				model.NewQueryResultCol("metric__0__1__2_col_1", []float64{11.1}),
				model.NewQueryResultCol("metric__0__1__2_col_2", []float64{21.2}),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__key_0", int64(1706054400000/43200000)),
				model.NewQueryResultCol("aggr__0__count", int64(5)),
				model.NewQueryResultCol("aggr__0__1__parent_count", 5),
				model.NewQueryResultCol("aggr__0__1__key_0", "a"),
				model.NewQueryResultCol("aggr__0__1__count", int64(3)),
				model.NewQueryResultCol("metric__0__1__2_col_0", []float64{5}),
				model.NewQueryResultCol("metric__0__1__2_col_1", []float64{12.1}),
				model.NewQueryResultCol("metric__0__1__2_col_2", []float64{22.2}),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT "aggr__0__key_0", "aggr__0__count", "aggr__0__1__parent_count",
			  "aggr__0__1__key_0", "aggr__0__1__count", "metric__0__1__2_col_0",
			  "metric__0__1__2_col_1", "metric__0__1__2_col_2"
			FROM (
			  SELECT "aggr__0__key_0", "aggr__0__count", "aggr__0__1__parent_count",
				"aggr__0__1__key_0", "aggr__0__1__count", "metric__0__1__2_col_0",
				"metric__0__1__2_col_1", "metric__0__1__2_col_2",
				dense_rank() OVER (ORDER BY "aggr__0__key_0" ASC) AS "aggr__0__order_1_rank"
				,
				dense_rank() OVER (PARTITION BY "aggr__0__key_0" ORDER BY
				"metric__0__1__2_col_1" DESC, "aggr__0__1__key_0" ASC) AS
				"aggr__0__1__order_1_rank"
			  FROM (
				SELECT toInt64(toUnixTimestamp64Milli("@timestamp") / 43200000) AS
				  "aggr__0__key_0",
				  sum(count(*)) OVER (PARTITION BY "aggr__0__key_0") AS "aggr__0__count",
				  sum(count(*)) OVER (PARTITION BY "aggr__0__key_0") AS
				  "aggr__0__1__parent_count", "container.name" AS "aggr__0__1__key_0",
				  count(*) AS "aggr__0__1__count",
				  quantiles(0.100000)("docker.cpu.total.pct") AS "metric__0__1__2_col_0",
				  quantiles(0.750000)("docker.cpu.total.pct") AS "metric__0__1__2_col_1",
				  quantiles(0.990000)("docker.cpu.total.pct") AS "metric__0__1__2_col_2"
				FROM __quesma_table_name
				GROUP BY toInt64(toUnixTimestamp64Milli("@timestamp") / 43200000) AS
				  "aggr__0__key_0", "container.name" AS "aggr__0__1__key_0"))
			WHERE "aggr__0__1__order_1_rank"<=6
			ORDER BY "aggr__0__order_1_rank" ASC, "aggr__0__1__order_1_rank" ASC`,
	},
	{ // [55]
		TestName: "terms order by percentile_ranks",
		QueryRequestJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"0": {
					"aggs": {
						"1": {
							"percentile_ranks": {
								"field": "DistanceKilometers",
								"values": [
									0, 50
								]
							}
						}
					},
					"terms": {
						"field": "Cancelled",
						"order": {
							"1.0": "desc"
						},
						"shard_size": 25,
						"size": 5
					}
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
			"start_time_in_millis": 1727114076973,
			"expiration_time_in_millis": 1727546076973,
			"completion_time_in_millis": 1727114076978,
			"response": {
				"took": 5,
				"timed_out": false,
				"_shards": {
					"total": 1,
					"successful": 1,
					"skipped": 0,
					"failed": 0
				},
				"hits": {
					"total": {
						"value": 212,
						"relation": "eq"
					},
					"max_score": null,
					"hits": []
				},
				"aggregations": {
					"0": {
						"doc_count_error_upper_bound": 0,
						"sum_other_doc_count": 0,
						"buckets": [
							{
								"1": {
									"values": {
										"0.0": 3.314917127071823,
										"50.0": 6.441097753551789
									}
								},
								"key": 0,
								"doc_count": 181
							},
							{
								"1": {
									"values": {
										"0.0": 3.225806451612903,
										"50.0": 9.813812484840025
									}
								},
								"key": 1,
								"doc_count": 31
							}
						]
					}
				}
			}
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__parent_count", 212),
				model.NewQueryResultCol("aggr__0__key_0", 0),
				model.NewQueryResultCol("aggr__0__count", int64(181)),
				model.NewQueryResultCol("metric__0__1_col_0", 3.314917127071823),
				model.NewQueryResultCol("metric__0__1_col_1", 6.441097753551789),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__0__parent_count", 212),
				model.NewQueryResultCol("aggr__0__key_0", 1),
				model.NewQueryResultCol("aggr__0__count", int64(31)),
				model.NewQueryResultCol("metric__0__1_col_0", 3.225806451612903),
				model.NewQueryResultCol("metric__0__1_col_1", 9.813812484840025),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT sum(count(*)) OVER () AS "aggr__0__parent_count",
			  "Cancelled" AS "aggr__0__key_0", count(*) AS "aggr__0__count",
			  countIf("DistanceKilometers"<=0)/count(*)*100 AS "metric__0__1_col_0",
			  countIf("DistanceKilometers"<=50)/count(*)*100 AS "metric__0__1_col_1"
			FROM __quesma_table_name
			GROUP BY "Cancelled" AS "aggr__0__key_0"
			ORDER BY "metric__0__1_col_0" DESC, "aggr__0__key_0" ASC
			LIMIT 6`,
	},
	{ // [56]
		TestName: "simple histogram with null values, no missing parameter",
		QueryRequestJson: `
		{
			"aggs": {
				"sample": {
					"aggs": {
						"histo": {
							"histogram": {
								"field": "taxful_total_price",
								"interval": 224.19300000000004
							}
						}
					},
					"sampler": {
						"shard_size": 5000
					}
				}
			},
			"runtime_mappings": {},
			"size": 0,
			"track_total_hits": true
		}`,
		ExpectedResponse: `
		{
			"completion_status": 200,
			"completion_time_in_millis": 0,
			"expiration_time_in_millis": 0,
			"id": "quesma_async_0191e0d2-589d-7dd9-8ac9-7f51fdf2f8af",
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
					"sample": {
						"doc_count": 1978,
						"histo": {
							"buckets": [
								{
									"doc_count": 1960,
									"key": 0
								},
								{
									"doc_count": 17,
									"key": 224.19300000000004
								}
							]
						}
					}
				},
				"hits": {
					"hits": [],
					"max_score": null,
					"total": {
						"relation": "eq",
						"value": 5934
					}
				},
				"timed_out": false,
				"took": 0
			},
			"start_time_in_millis": 0
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__sample__count", 1978),
				model.NewQueryResultCol("aggr__sample__histo__key_0", 0),
				model.NewQueryResultCol("aggr__sample__histo__count", 1960),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__sample__count", 1978),
				model.NewQueryResultCol("aggr__sample__histo__key_0", 224.19300000000004),
				model.NewQueryResultCol("aggr__sample__histo__count", 17),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__sample__count", 1978),
				model.NewQueryResultCol("aggr__sample__histo__key_0", nil),
				model.NewQueryResultCol("aggr__sample__histo__count", 1),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT sum(count(*)) OVER () AS "aggr__sample__count",
			  floor("taxful_total_price"/224.19300000000004)*224.19300000000004 AS
			  "aggr__sample__histo__key_0", count(*) AS "aggr__sample__histo__count"
			FROM (
			  SELECT "taxful_total_price"
			  FROM __quesma_table_name
			  LIMIT 20000)
			GROUP BY floor("taxful_total_price"/224.19300000000004)*224.19300000000004 AS
			  "aggr__sample__histo__key_0"
			ORDER BY "aggr__sample__histo__key_0" ASC`,
	},
	{ // [57]
		TestName: "histogram with null values, no missing parameter, and some subaggregation",
		QueryRequestJson: `
		{
			"aggs": {
				"histo": {
					"histogram": {
						"field": "taxful_total_price",
						"interval": 224.19300000000004
					},
					"aggs": {
						"0": {
							"terms": {
								"field": "type"
							}
						}
					}
				}
			},
			"runtime_mappings": {},
			"size": 0,
			"track_total_hits": true
		}`,
		ExpectedResponse: `
		{
			"completion_status": 200,
			"completion_time_in_millis": 0,
			"expiration_time_in_millis": 0,
			"id": "quesma_async_0191e0d2-589d-7dd9-8ac9-7f51fdf2f8af",
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
					"histo": {
						"buckets": [
							{
								"doc_count": 1960,
								"key": 0,
								"0": {
									"buckets": [
										{
											"doc_count": 42,
											"key": "order"
										},
										{
											"doc_count": 1,
											"key": "disorder"
										}
									],
									"doc_count_error_upper_bound": 0,
									"sum_other_doc_count": 1917
								}
							},
							{
								"doc_count": 17,
								"key": 224.19300000000004
							}
						]
					}
				},
				"hits": {
					"hits": [],
					"max_score": null,
					"total": {
						"relation": "eq",
						"value": 5934
					}
				},
				"timed_out": false,
				"took": 0
			},
			"start_time_in_millis": 0
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__histo__key_0", 0),
				model.NewQueryResultCol("aggr__histo__count", 1960),
				model.NewQueryResultCol("aggr__histo__0__parent_count", 1960),
				model.NewQueryResultCol("aggr__histo__0__key_0", "order"),
				model.NewQueryResultCol("aggr__histo__0__count", int64(42)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__histo__key_0", 0),
				model.NewQueryResultCol("aggr__histo__count", 1960),
				model.NewQueryResultCol("aggr__histo__0__parent_count", 1960),
				model.NewQueryResultCol("aggr__histo__0__key_0", "disorder"),
				model.NewQueryResultCol("aggr__histo__0__count", int64(1)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__histo__key_0", 224.19300000000004),
				model.NewQueryResultCol("aggr__histo__count", 17),
				model.NewQueryResultCol("aggr__histo__0__parent_count", 17),
				model.NewQueryResultCol("aggr__histo__0__key_0", nil),
				model.NewQueryResultCol("aggr__histo__0__count", int64(1)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__histo__key_0", nil),
				model.NewQueryResultCol("aggr__histo__count", 15),
				model.NewQueryResultCol("aggr__histo__0__parent_count", 15),
				model.NewQueryResultCol("aggr__histo__0__key_0", "a"),
				model.NewQueryResultCol("aggr__histo__0__count", int64(1)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__histo__key_0", nil),
				model.NewQueryResultCol("aggr__histo__count", 15),
				model.NewQueryResultCol("aggr__histo__0__parent_count", 15),
				model.NewQueryResultCol("aggr__histo__0__key_0", "b"),
				model.NewQueryResultCol("aggr__histo__0__count", int64(1)),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT "aggr__histo__key_0", "aggr__histo__count",
			  "aggr__histo__0__parent_count", "aggr__histo__0__key_0",
			  "aggr__histo__0__count"
			FROM (
			  SELECT "aggr__histo__key_0", "aggr__histo__count",
				"aggr__histo__0__parent_count", "aggr__histo__0__key_0",
				"aggr__histo__0__count",
				dense_rank() OVER (ORDER BY "aggr__histo__key_0" ASC) AS
				"aggr__histo__order_1_rank",
				dense_rank() OVER (PARTITION BY "aggr__histo__key_0" ORDER BY
				"aggr__histo__0__count" DESC, "aggr__histo__0__key_0" ASC) AS
				"aggr__histo__0__order_1_rank"
			  FROM (
				SELECT floor("taxful_total_price"/224.19300000000004)*224.19300000000004 AS
				  "aggr__histo__key_0",
				  sum(count(*)) OVER (PARTITION BY "aggr__histo__key_0") AS
				  "aggr__histo__count",
				  sum(count(*)) OVER (PARTITION BY "aggr__histo__key_0") AS
				  "aggr__histo__0__parent_count", "type" AS "aggr__histo__0__key_0",
				  count(*) AS "aggr__histo__0__count"
				FROM __quesma_table_name
				GROUP BY floor("taxful_total_price"/224.19300000000004)*224.19300000000004
				  AS "aggr__histo__key_0", "type" AS "aggr__histo__0__key_0"))
			WHERE "aggr__histo__0__order_1_rank"<=11
			ORDER BY "aggr__histo__order_1_rank" ASC, "aggr__histo__0__order_1_rank" ASC`,
	},
	{ // [58]
		TestName: "simple histogram with null values and missing parameter",
		QueryRequestJson: `
		{
			"aggs": {
				"sample": {
					"aggs": {
						"histo": {
							"histogram": {
								"field": "taxful_total_price",
								"interval": 224.19300000000004,
								"missing": 80
							}
						}
					},
					"sampler": {
						"shard_size": 5000
					}
				}
			},
			"runtime_mappings": {},
			"size": 0,
			"track_total_hits": true
		}`,
		ExpectedResponse: `
		{
			"completion_status": 200,
			"completion_time_in_millis": 0,
			"expiration_time_in_millis": 0,
			"id": "quesma_async_0191e0d2-589d-7dd9-8ac9-7f51fdf2f8af",
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
					"sample": {
						"doc_count": 1978,
						"histo": {
							"buckets": [
								{
									"doc_count": 1960,
									"key": 0
								},
								{
									"doc_count": 17,
									"key": 80
								},
								{
									"doc_count": 1,
									"key": 224.19300000000004
								}
							]
						}
					}
				},
				"hits": {
					"hits": [],
					"max_score": null,
					"total": {
						"relation": "eq",
						"value": 5934
					}
				},
				"timed_out": false,
				"took": 0
			},
			"start_time_in_millis": 0
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__sample__count", 1978),
				model.NewQueryResultCol("aggr__sample__histo__key_0", 0),
				model.NewQueryResultCol("aggr__sample__histo__count", 1960),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__sample__count", 1978),
				model.NewQueryResultCol("aggr__sample__histo__key_0", 80),
				model.NewQueryResultCol("aggr__sample__histo__count", 17),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__sample__count", 1978),
				model.NewQueryResultCol("aggr__sample__histo__key_0", 224.19300000000004),
				model.NewQueryResultCol("aggr__sample__histo__count", 1),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT sum(count(*)) OVER () AS "aggr__sample__count",
			  floor(COALESCE("taxful_total_price", 80)/224.19300000000004)*
			  224.19300000000004 AS "aggr__sample__histo__key_0",
			  count(*) AS "aggr__sample__histo__count"
			FROM (
			  SELECT "taxful_total_price"
			  FROM __quesma_table_name
			  LIMIT 20000)
			GROUP BY floor(COALESCE("taxful_total_price", 80)/224.19300000000004)*
			  224.19300000000004 AS "aggr__sample__histo__key_0"
			ORDER BY "aggr__sample__histo__key_0" ASC`,
	},
	{ // [59]
		TestName: "histogram with null values, missing parameter, and some subaggregation",
		QueryRequestJson: `
		{
			"aggs": {
				"histo": {
					"histogram": {
						"field": "taxful_total_price",
						"interval": 224.19300000000004,
						"missing": 800
					},
					"aggs": {
						"0": {
							"terms": {
								"field": "type"
							}
						}
					}
				}
			},
			"runtime_mappings": {},
			"size": 0,
			"track_total_hits": true
		}`,
		ExpectedResponse: `
		{
			"completion_status": 200,
			"completion_time_in_millis": 0,
			"expiration_time_in_millis": 0,
			"id": "quesma_async_0191e0d2-589d-7dd9-8ac9-7f51fdf2f8af",
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
					"histo": {
						"buckets": [
							{
								"doc_count": 1960,
								"key": 0,
								"0": {
									"buckets": [
										{
											"doc_count": 42,
											"key": "order"
										},
										{
											"doc_count": 1,
											"key": "disorder"
										}
									],
									"doc_count_error_upper_bound": 0,
									"sum_other_doc_count": 1917
								}
							},
							{
								"doc_count": 17,
								"key": 224.19300000000004
							},
							{
								"doc_count": 15,
								"key": 800,
								"0": {
									"buckets": [
										{
											"doc_count": 1,
											"key": "a"
										},
										{
											"doc_count": 1,
											"key": "b"
										}
									],
									"doc_count_error_upper_bound": 0,
									"sum_other_doc_count": 13
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
						"value": 5934
					}
				},
				"timed_out": false,
				"took": 0
			},
			"start_time_in_millis": 0
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__histo__key_0", 0),
				model.NewQueryResultCol("aggr__histo__count", 1960),
				model.NewQueryResultCol("aggr__histo__0__parent_count", 1960),
				model.NewQueryResultCol("aggr__histo__0__key_0", "order"),
				model.NewQueryResultCol("aggr__histo__0__count", int64(42)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__histo__key_0", 0),
				model.NewQueryResultCol("aggr__histo__count", 1960),
				model.NewQueryResultCol("aggr__histo__0__parent_count", 1960),
				model.NewQueryResultCol("aggr__histo__0__key_0", "disorder"),
				model.NewQueryResultCol("aggr__histo__0__count", int64(1)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__histo__key_0", 224.19300000000004),
				model.NewQueryResultCol("aggr__histo__count", 17),
				model.NewQueryResultCol("aggr__histo__0__parent_count", 17),
				model.NewQueryResultCol("aggr__histo__0__key_0", nil),
				model.NewQueryResultCol("aggr__histo__0__count", int64(1)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__histo__key_0", 800),
				model.NewQueryResultCol("aggr__histo__count", 15),
				model.NewQueryResultCol("aggr__histo__0__parent_count", 15),
				model.NewQueryResultCol("aggr__histo__0__key_0", "a"),
				model.NewQueryResultCol("aggr__histo__0__count", int64(1)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__histo__key_0", 800),
				model.NewQueryResultCol("aggr__histo__count", 15),
				model.NewQueryResultCol("aggr__histo__0__parent_count", 15),
				model.NewQueryResultCol("aggr__histo__0__key_0", "b"),
				model.NewQueryResultCol("aggr__histo__0__count", int64(1)),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT "aggr__histo__key_0", "aggr__histo__count",
			  "aggr__histo__0__parent_count", "aggr__histo__0__key_0",
			  "aggr__histo__0__count"
			FROM (
			  SELECT "aggr__histo__key_0", "aggr__histo__count",
				"aggr__histo__0__parent_count", "aggr__histo__0__key_0",
				"aggr__histo__0__count",
				dense_rank() OVER (ORDER BY "aggr__histo__key_0" ASC) AS
				"aggr__histo__order_1_rank",
				dense_rank() OVER (PARTITION BY "aggr__histo__key_0" ORDER BY
				"aggr__histo__0__count" DESC, "aggr__histo__0__key_0" ASC) AS
				"aggr__histo__0__order_1_rank"
			  FROM (
				SELECT floor(COALESCE("taxful_total_price", 800)/224.19300000000004)*
				  224.19300000000004 AS "aggr__histo__key_0",
				  sum(count(*)) OVER (PARTITION BY "aggr__histo__key_0") AS
				  "aggr__histo__count",
				  sum(count(*)) OVER (PARTITION BY "aggr__histo__key_0") AS
				  "aggr__histo__0__parent_count", "type" AS "aggr__histo__0__key_0",
				  count(*) AS "aggr__histo__0__count"
				FROM __quesma_table_name
				GROUP BY floor(COALESCE("taxful_total_price", 800)/224.19300000000004)*
				  224.19300000000004 AS "aggr__histo__key_0",
				  "type" AS "aggr__histo__0__key_0"))
			WHERE "aggr__histo__0__order_1_rank"<=11
			ORDER BY "aggr__histo__order_1_rank" ASC, "aggr__histo__0__order_1_rank" ASC`,
	},
	{ // [60]
		TestName: "simple date_histogram with null values, no missing parameter (DateTime)",
		QueryRequestJson: `
		{
			"aggs": {
				"sample": {
					"aggs": {
						"histo": {
							"date_histogram": {
								"field": "customer_birth_date"
							}
						}
					},
					"sampler": {
						"shard_size": 5000
					}
				}
			},
			"runtime_mappings": {},
			"size": 0,
			"track_total_hits": true
		}`,
		ExpectedResponse: `
		{
			"completion_status": 200,
			"completion_time_in_millis": 0,
			"expiration_time_in_millis": 0,
			"id": "quesma_async_0191e0d2-589d-7dd9-8ac9-7f51fdf2f8af",
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
					"sample": {
						"doc_count": 1978,
						"histo": {
							"buckets": [
								{
									"doc_count": 1960,
									"key": 1706021640000,
									"key_as_string": "2024-01-23T14:54:00.000"
								},
								{
									"doc_count": 17,
									"key": 1706021700000,
									"key_as_string": "2024-01-23T14:55:00.000"
								}
							]
						}
					}
				},
				"hits": {
					"hits": [],
					"max_score": null,
					"total": {
						"relation": "eq",
						"value": 5934
					}
				},
				"timed_out": false,
				"took": 0
			},
			"start_time_in_millis": 0
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__sample__count", 1978),
				model.NewQueryResultCol("aggr__sample__histo__key_0", int64(1706021640000/30000)),
				model.NewQueryResultCol("aggr__sample__histo__count", 1960),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__sample__count", 1978),
				model.NewQueryResultCol("aggr__sample__histo__key_0", int64(1706021700000/30000)),
				model.NewQueryResultCol("aggr__sample__histo__count", 17),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__sample__count", 1978),
				model.NewQueryResultCol("aggr__sample__histo__key_0", nil),
				model.NewQueryResultCol("aggr__sample__histo__count", 1),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT sum(count(*)) OVER () AS "aggr__sample__count",
			  toInt64(toUnixTimestamp("customer_birth_date") / 30) AS
			  "aggr__sample__histo__key_0", count(*) AS "aggr__sample__histo__count"
			FROM (
			  SELECT "customer_birth_date"
			  FROM __quesma_table_name
			  LIMIT 20000)
			GROUP BY toInt64(toUnixTimestamp("customer_birth_date") / 30) AS
			  "aggr__sample__histo__key_0"
			ORDER BY "aggr__sample__histo__key_0" ASC`,
	},
	{ // [61]
		TestName: "date_histogram with null values, no missing parameter, and some subaggregation",
		QueryRequestJson: `
		{
			"aggs": {
				"histo": {
					"date_histogram": {
						"field": "customer_birth_date_datetime64"
					},
					"aggs": {
						"0": {
							"terms": {
								"field": "type"
							}
						}
					}
				}
			},
			"runtime_mappings": {},
			"size": 0,
			"track_total_hits": true
		}`,
		ExpectedResponse: `
		{
			"completion_status": 200,
			"completion_time_in_millis": 0,
			"expiration_time_in_millis": 0,
			"id": "quesma_async_0191e0d2-589d-7dd9-8ac9-7f51fdf2f8af",
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
					"histo": {
						"buckets": [
							{
								"doc_count": 1960,
								"key": 1706021640000,
								"key_as_string": "2024-01-23T14:54:00.000",
								"0": {
									"buckets": [
										{
											"doc_count": 42,
											"key": "order"
										},
										{
											"doc_count": 1,
											"key": "disorder"
										}
									],
									"doc_count_error_upper_bound": 0,
									"sum_other_doc_count": 1917
								}
							},
							{
								"doc_count": 17,
								"key": 1706021670000,
								"key_as_string": "2024-01-23T14:54:30.000"
							}
						]
					}
				},
				"hits": {
					"hits": [],
					"max_score": null,
					"total": {
						"relation": "eq",
						"value": 5934
					}
				},
				"timed_out": false,
				"took": 0
			},
			"start_time_in_millis": 0
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__histo__key_0", int64(1706021640000/30000)),
				model.NewQueryResultCol("aggr__histo__count", 1960),
				model.NewQueryResultCol("aggr__histo__0__parent_count", 1960),
				model.NewQueryResultCol("aggr__histo__0__key_0", "order"),
				model.NewQueryResultCol("aggr__histo__0__count", int64(42)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__histo__key_0", int64(1706021640000/30000)),
				model.NewQueryResultCol("aggr__histo__count", 1960),
				model.NewQueryResultCol("aggr__histo__0__parent_count", 1960),
				model.NewQueryResultCol("aggr__histo__0__key_0", "disorder"),
				model.NewQueryResultCol("aggr__histo__0__count", int64(1)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__histo__key_0", int64(1706021670000/30000)),
				model.NewQueryResultCol("aggr__histo__count", 17),
				model.NewQueryResultCol("aggr__histo__0__parent_count", 17),
				model.NewQueryResultCol("aggr__histo__0__key_0", nil),
				model.NewQueryResultCol("aggr__histo__0__count", int64(1)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__histo__key_0", nil),
				model.NewQueryResultCol("aggr__histo__count", 15),
				model.NewQueryResultCol("aggr__histo__0__parent_count", 15),
				model.NewQueryResultCol("aggr__histo__0__key_0", "a"),
				model.NewQueryResultCol("aggr__histo__0__count", int64(1)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__histo__key_0", nil),
				model.NewQueryResultCol("aggr__histo__count", 15),
				model.NewQueryResultCol("aggr__histo__0__parent_count", 15),
				model.NewQueryResultCol("aggr__histo__0__key_0", "b"),
				model.NewQueryResultCol("aggr__histo__0__count", int64(1)),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT "aggr__histo__key_0", "aggr__histo__count",
			  "aggr__histo__0__parent_count", "aggr__histo__0__key_0",
			  "aggr__histo__0__count"
			FROM (
			  SELECT "aggr__histo__key_0", "aggr__histo__count",
				"aggr__histo__0__parent_count", "aggr__histo__0__key_0",
				"aggr__histo__0__count",
				dense_rank() OVER (ORDER BY "aggr__histo__key_0" ASC) AS
				"aggr__histo__order_1_rank",
				dense_rank() OVER (PARTITION BY "aggr__histo__key_0" ORDER BY
				"aggr__histo__0__count" DESC, "aggr__histo__0__key_0" ASC) AS
				"aggr__histo__0__order_1_rank"
			  FROM (
				SELECT toInt64(toUnixTimestamp64Milli("customer_birth_date_datetime64") / 30000) AS
				  "aggr__histo__key_0",
				  sum(count(*)) OVER (PARTITION BY "aggr__histo__key_0") AS
				  "aggr__histo__count",
				  sum(count(*)) OVER (PARTITION BY "aggr__histo__key_0") AS
				  "aggr__histo__0__parent_count", "type" AS "aggr__histo__0__key_0",
				  count(*) AS "aggr__histo__0__count"
				FROM __quesma_table_name
				GROUP BY toInt64(toUnixTimestamp64Milli("customer_birth_date_datetime64") / 30000) AS
				  "aggr__histo__key_0", "type" AS "aggr__histo__0__key_0"))
			WHERE "aggr__histo__0__order_1_rank"<=11
			ORDER BY "aggr__histo__order_1_rank" ASC, "aggr__histo__0__order_1_rank" ASC`,
	},
	{ // [62]
		TestName: "date_histogram with null values, missing parameter (DateTime, not DateTime64), and some subaggregation",
		QueryRequestJson: `
		{
			"aggs": {
				"histo": {
					"date_histogram": {
						"field": "customer_birth_date",
						"missing": "2024-01-23T14:56:00"
					},
					"aggs": {
						"0": {
							"terms": {
								"field": "type"
							}
						}
					}
				}
			},
			"runtime_mappings": {},
			"size": 0,
			"track_total_hits": true
		}`,
		ExpectedResponse: `
		{
			"completion_status": 200,
			"completion_time_in_millis": 0,
			"expiration_time_in_millis": 0,
			"id": "quesma_async_0191e0d2-589d-7dd9-8ac9-7f51fdf2f8af",
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
					"histo": {
						"buckets": [
							{
								"doc_count": 1960,
								"key": 1706021640000,
								"key_as_string": "2024-01-23T14:54:00.000",
								"0": {
									"buckets": [
										{
											"doc_count": 42,
											"key": "order"
										},
										{
											"doc_count": 1,
											"key": "disorder"
										}
									],
									"doc_count_error_upper_bound": 0,
									"sum_other_doc_count": 1917
								}
							},
							{
								"doc_count": 17,
								"key": 1706021700000,
								"key_as_string": "2024-01-23T14:55:00.000"
							},
							{
								"doc_count": 15,
								"key": 1706021760000,
								"key_as_string": "2024-01-23T14:56:00.000",
								"0": {
									"buckets": [
										{
											"doc_count": 1,
											"key": "a"
										},
										{
											"doc_count": 1,
											"key": "b"
										}
									],
									"doc_count_error_upper_bound": 0,
									"sum_other_doc_count": 13
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
						"value": 5934
					}
				},
				"timed_out": false,
				"took": 0
			},
			"start_time_in_millis": 0
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__histo__key_0", int64(1706021640000/30000)),
				model.NewQueryResultCol("aggr__histo__count", 1960),
				model.NewQueryResultCol("aggr__histo__0__parent_count", 1960),
				model.NewQueryResultCol("aggr__histo__0__key_0", "order"),
				model.NewQueryResultCol("aggr__histo__0__count", int64(42)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__histo__key_0", int64(1706021640000/30000)),
				model.NewQueryResultCol("aggr__histo__count", 1960),
				model.NewQueryResultCol("aggr__histo__0__parent_count", 1960),
				model.NewQueryResultCol("aggr__histo__0__key_0", "disorder"),
				model.NewQueryResultCol("aggr__histo__0__count", int64(1)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__histo__key_0", int64(1706021700000/30000)),
				model.NewQueryResultCol("aggr__histo__count", 17),
				model.NewQueryResultCol("aggr__histo__0__parent_count", 17),
				model.NewQueryResultCol("aggr__histo__0__key_0", nil),
				model.NewQueryResultCol("aggr__histo__0__count", int64(1)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__histo__key_0", int64(1706021760000/30000)),
				model.NewQueryResultCol("aggr__histo__count", 15),
				model.NewQueryResultCol("aggr__histo__0__parent_count", 15),
				model.NewQueryResultCol("aggr__histo__0__key_0", "a"),
				model.NewQueryResultCol("aggr__histo__0__count", int64(1)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__histo__key_0", int64(1706021760000/30000)),
				model.NewQueryResultCol("aggr__histo__count", 15),
				model.NewQueryResultCol("aggr__histo__0__parent_count", 15),
				model.NewQueryResultCol("aggr__histo__0__key_0", "b"),
				model.NewQueryResultCol("aggr__histo__0__count", int64(1)),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT "aggr__histo__key_0", "aggr__histo__count",
			  "aggr__histo__0__parent_count", "aggr__histo__0__key_0",
			  "aggr__histo__0__count"
			FROM (
			  SELECT "aggr__histo__key_0", "aggr__histo__count",
				"aggr__histo__0__parent_count", "aggr__histo__0__key_0",
				"aggr__histo__0__count",
				dense_rank() OVER (ORDER BY "aggr__histo__key_0" ASC) AS
				"aggr__histo__order_1_rank",
				dense_rank() OVER (PARTITION BY "aggr__histo__key_0" ORDER BY
				"aggr__histo__0__count" DESC, "aggr__histo__0__key_0" ASC) AS
				"aggr__histo__0__order_1_rank"
			  FROM (
				SELECT toInt64(toUnixTimestamp(COALESCE("customer_birth_date",
				  fromUnixTimestamp(1706021760))) / 30) AS "aggr__histo__key_0",
				  sum(count(*)) OVER (PARTITION BY "aggr__histo__key_0") AS
				  "aggr__histo__count",
				  sum(count(*)) OVER (PARTITION BY "aggr__histo__key_0") AS
				  "aggr__histo__0__parent_count", "type" AS "aggr__histo__0__key_0",
				  count(*) AS "aggr__histo__0__count"
				FROM __quesma_table_name
				GROUP BY toInt64(toUnixTimestamp(COALESCE("customer_birth_date",
				  fromUnixTimestamp(1706021760))) / 30) AS "aggr__histo__key_0",
				  "type" AS "aggr__histo__0__key_0"))
			WHERE "aggr__histo__0__order_1_rank"<=11
			ORDER BY "aggr__histo__order_1_rank" ASC, "aggr__histo__0__order_1_rank" ASC`,
	},
	{ // [63]
		TestName: "date_histogram with missing, different formats, and types (DateTime/DateTime64)",
		QueryRequestJson: `
		{
			"aggs": {
				"histo1": {
					"date_histogram": {
						"field": "customer_birth_date",
						"fixed_interval": "90000ms",
						"missing": "2024-02-02T13"
					}
				},
				"histo2": {
					"date_histogram": {
						"field": "customer_birth_date",
						"fixed_interval": "90000ms",
						"missing": "2024-02-02T13:00:00"
					}
				},
				"histo3": {
					"date_histogram": {
						"field": "customer_birth_date_datetime64",
						"fixed_interval": "90000ms",
						"missing": "2024-02-02T13:00:00.000"
					}
				},
				"histo4": {
					"date_histogram": {
						"field": "customer_birth_date_datetime64",
						"fixed_interval": "90000ms",
						"missing": "2024-02-02T13:00:00+07:00"
					}
				},
				"histo5": {
					"date_histogram": {
						"field": "customer_birth_date",
						"fixed_interval": "90000ms",
						"missing": "2024-02-02T13:00:00.000+07:00"
					}
				}
			},
			"runtime_mappings": {},
			"size": 0,
			"track_total_hits": true
		}`,
		ExpectedResponse: `
		{
			"took": 0,
			"timed_out": false,
			"_shards": {
				"total": 1,
				"successful": 1,
				"skipped": 0,
				"failed": 0
			},
			"hits": {
				"total": {
					"value": 4675,
					"relation": "eq"
				},
				"max_score": null,
				"hits": []
			},
			"aggregations": {
				"histo1": {
					"buckets": [
						{
							"key_as_string": "2024-02-02T13:00:00.000",
							"key": 1706878800000,
							"doc_count": 4675
						}
					]
				},
				"histo2": {
					"buckets": [
						{
							"key_as_string": "2024-02-02T13:00:00.000",
							"key": 1706878800000,
							"doc_count": 4675
						}
					]
				},
				"histo3": {
					"buckets": [
						{
							"key_as_string": "2024-02-02T13:00:00.000",
							"key": 1706878800000,
							"doc_count": 4675
						}
					]
				},
				"histo4": {
					"buckets": [
						{
							"key_as_string": "2024-02-02T06:00:00.000",
							"key": 1706853600000,
							"doc_count": 4675
						}
					]
				},
				"histo5": {
					"buckets": [
						{
							"key_as_string": "2024-02-02T06:00:00.000",
							"key": 1706853600000,
							"doc_count": 4675
						}
					]
				}
			}
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__histo1__key_0", int64(1706878800000/90000)),
				model.NewQueryResultCol("aggr__histo1__count", int64(4675)),
			}},
		},
		ExpectedAdditionalPancakeResults: [][]model.QueryResultRow{
			{{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__histo2__key_0", int64(1706878800000/90000)),
				model.NewQueryResultCol("aggr__histo2__count", int64(4675)),
			}}},
			{{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__histo3__key_0", int64(1706878800000/90000)),
				model.NewQueryResultCol("aggr__histo3__count", int64(4675)),
			}}},
			{{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__histo4__key_0", int64(1706853600000/90000)),
				model.NewQueryResultCol("aggr__histo4__count", int64(4675)),
			}}},
			{{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__histo5__key_0", int64(1706853600000/90000)),
				model.NewQueryResultCol("aggr__histo5__count", int64(4675)),
			}}},
		},
		ExpectedPancakeSQL: `
			SELECT toInt64(toUnixTimestamp(COALESCE("customer_birth_date",
			  fromUnixTimestamp(1706878800))) / 90) AS "aggr__histo1__key_0",
			  count(*) AS "aggr__histo1__count"
			FROM __quesma_table_name
			GROUP BY toInt64(toUnixTimestamp(COALESCE("customer_birth_date",
			  fromUnixTimestamp(1706878800))) / 90) AS "aggr__histo1__key_0"
			ORDER BY "aggr__histo1__key_0" ASC`,
		ExpectedAdditionalPancakeSQLs: []string{
			`SELECT toInt64(toUnixTimestamp(COALESCE("customer_birth_date",
			  fromUnixTimestamp(1706878800))) / 90) AS "aggr__histo2__key_0",
			  count(*) AS "aggr__histo2__count"
			FROM __quesma_table_name
			GROUP BY toInt64(toUnixTimestamp(COALESCE("customer_birth_date",
			  fromUnixTimestamp(1706878800))) / 90) AS "aggr__histo2__key_0"
			ORDER BY "aggr__histo2__key_0" ASC`,
			`SELECT toInt64(toUnixTimestamp64Milli(COALESCE("customer_birth_date_datetime64",
              fromUnixTimestamp64Milli(1706878800000))) / 90000) AS "aggr__histo3__key_0",
			  count(*) AS "aggr__histo3__count"
			FROM __quesma_table_name
			GROUP BY toInt64(toUnixTimestamp64Milli(COALESCE("customer_birth_date_datetime64",
              fromUnixTimestamp64Milli(1706878800000))) / 90000) AS "aggr__histo3__key_0"
			ORDER BY "aggr__histo3__key_0" ASC`,
			`SELECT toInt64(toUnixTimestamp64Milli(COALESCE("customer_birth_date_datetime64",
			  fromUnixTimestamp64Milli(1706853600000))) / 90000) AS "aggr__histo4__key_0",
			  count(*) AS "aggr__histo4__count"
			FROM __quesma_table_name
			GROUP BY toInt64(toUnixTimestamp64Milli(COALESCE("customer_birth_date_datetime64",
			  fromUnixTimestamp64Milli(1706853600000))) / 90000) AS "aggr__histo4__key_0"
			ORDER BY "aggr__histo4__key_0" ASC`,
			`SELECT toInt64(toUnixTimestamp(COALESCE("customer_birth_date",
			  fromUnixTimestamp(1706853600))) / 90) AS "aggr__histo5__key_0",
			  count(*) AS "aggr__histo5__count"
			FROM __quesma_table_name
			GROUP BY toInt64(toUnixTimestamp(COALESCE("customer_birth_date",
			  fromUnixTimestamp(1706853600))) / 90) AS "aggr__histo5__key_0"
			ORDER BY "aggr__histo5__key_0" ASC`,
		},
	},
	{ // [64]
		TestName: "histogram, min_doc_count=0, int keys when interval=1",
		QueryRequestJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"interval-2": {
					"histogram": {
						"field": "total_quantity",
						"interval": 2,
						"min_doc_count": 0
					}
				},
				"interval-1": {
					"histogram": {
						"field": "total_quantity",
						"interval": 1,
						"min_doc_count": 0
					}
				},
				"interval-0.5": {
					"histogram": {
						"field": "total_quantity",
						"interval": 0.5,
						"min_doc_count": 0
					}
				},
				"interval-0": {
					"histogram": {
						"field": "total_quantity",
						"interval": 0,
						"min_doc_count": 0
					}
				}
			},
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
			"took": 0,
			"timed_out": false,
			"_shards": {
				"total": 1,
				"successful": 1,
				"skipped": 0,
				"failed": 0
			},
			"hits": {
				"total": {
					"value": 4675,
					"relation": "eq"
				},
				"max_score": null,
				"hits": []
			},
			"aggregations": {
				"interval-2": {
					"buckets": [
						{
							"doc_count": 87,
							"key": 0
						},
						{
							"doc_count": 0,
							"key": 2
						},
						{
							"doc_count": 411,
							"key": 4
						}
					]
				},
				"interval-1": {
					"buckets": [
						{
							"doc_count": 87,
							"key": 0
						},
						{
							"doc_count": 0,
							"key": 1
						},
						{
							"doc_count": 411,
							"key": 2
						}
					]
				},
				"interval-0.5": {
					"buckets": [
						{
							"doc_count": 87,
							"key": 0
						},
						{
							"doc_count": 0,
							"key": 0.5
						},
						{
							"doc_count": 411,
							"key": 1
						}
					]
				},
				"interval-0": {
					"buckets": []
				}
			}
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__interval-0__key_0", nil),
				model.NewQueryResultCol("aggr__interval-0__count", int64(4675)),
			}},
		},
		ExpectedAdditionalPancakeResults: [][]model.QueryResultRow{
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("aggr__interval-0.5__key_0", 0.0),
					model.NewQueryResultCol("aggr__interval-0.5__count", int64(87)),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("aggr__interval-0.5__key_0", 1.0),
					model.NewQueryResultCol("aggr__interval-0.5__count", int64(411)),
				}},
			},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("aggr__interval-1__key_0", 0),
					model.NewQueryResultCol("aggr__interval-1__count", int64(87)),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("aggr__interval-1__key_0", int64(2)),
					model.NewQueryResultCol("aggr__interval-1__count", int64(411)),
				}},
			},
			{
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("aggr__interval-2__key_0", 0.0),
					model.NewQueryResultCol("aggr__interval-2__count", int64(87)),
				}},
				{Cols: []model.QueryResultCol{
					model.NewQueryResultCol("aggr__interval-2__key_0", 4.0),
					model.NewQueryResultCol("aggr__interval-2__count", int64(411)),
				}},
			},
		},
		ExpectedPancakeSQL: `
			SELECT floor("total_quantity"/0)*0 AS "aggr__interval-0__key_0",
			  count(*) AS "aggr__interval-0__count"
			FROM __quesma_table_name
			GROUP BY floor("total_quantity"/0)*0 AS "aggr__interval-0__key_0"
			ORDER BY "aggr__interval-0__key_0" ASC`,
		ExpectedAdditionalPancakeSQLs: []string{
			`SELECT floor("total_quantity"/0.5)*0.5 AS "aggr__interval-0.5__key_0",
			  count(*) AS "aggr__interval-0.5__count"
			FROM __quesma_table_name
			GROUP BY floor("total_quantity"/0.5)*0.5 AS "aggr__interval-0.5__key_0"
			ORDER BY "aggr__interval-0.5__key_0" ASC`,
			`SELECT "total_quantity" AS "aggr__interval-1__key_0",
			  count(*) AS "aggr__interval-1__count"
			FROM __quesma_table_name
			GROUP BY "total_quantity" AS "aggr__interval-1__key_0"
			ORDER BY "aggr__interval-1__key_0" ASC`,
			`SELECT floor("total_quantity"/2)*2 AS "aggr__interval-2__key_0",
			  count(*) AS "aggr__interval-2__count"
			FROM __quesma_table_name
			GROUP BY floor("total_quantity"/2)*2 AS "aggr__interval-2__key_0"
			ORDER BY "aggr__interval-2__key_0" ASC`,
		},
	},
	{ // [65]
		TestName: "simplest composite: 1 terms",
		QueryRequestJson: `
		{
			"size": 0,
			"aggs": {
				"my_buckets": {
					"composite": {
						"sources": [
							{
								"product": {
									"terms": {
										"field": "product"
									}
								}
							}
						]
					}
				}
			}
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
				"my_buckets": {
					"after_key": {
						"product": 45.118141174316406
					},
					"buckets": [
						{
							"doc_count": 601,
							"key": {
								"product": 0.0
							}
						},
						{
							"doc_count": 12,
							"key": {
								"product": 20.101646423339844
							}
						},
						{
							"doc_count": 1,
							"key": {
								"product": 29.588184356689453
							}
						},
						{
							"doc_count": 2,
							"key": {
								"product": 31.64774513244629
							}
						},
						{
							"doc_count": 2,
							"key": {
								"product": 36.98516845703125
							}
						},
						{
							"doc_count": 1,
							"key": {
								"product": 40.57283401489258
							}
						},
						{
							"doc_count": 2,
							"key": {
								"product": 41.956443786621094
							}
						},
						{
							"doc_count": 1,
							"key": {
								"product": 43.53862762451172
							}
						},
						{
							"doc_count": 3,
							"key": {
								"product": 44.48069763183594
							}
						},
						{
							"doc_count": 8,
							"key": {
								"product": 45.118141174316406
							}
						}
					]
				}
			},
			"hits": {
				"hits": [],
				"max_score": null,
				"total": {
					"relation": "gte",
					"value": 10000
				}
			},
			"timed_out": false,
			"took": 6
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__my_buckets__key_0", 0.0),
				model.NewQueryResultCol("aggr__my_buckets__count", int64(601)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__my_buckets__key_0", 20.101646423339844),
				model.NewQueryResultCol("aggr__my_buckets__count", int64(12)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__my_buckets__key_0", 29.588184356689453),
				model.NewQueryResultCol("aggr__my_buckets__count", int64(1)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__my_buckets__key_0", 31.64774513244629),
				model.NewQueryResultCol("aggr__my_buckets__count", int64(2)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__my_buckets__key_0", 36.98516845703125),
				model.NewQueryResultCol("aggr__my_buckets__count", int64(2)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__my_buckets__key_0", 40.57283401489258),
				model.NewQueryResultCol("aggr__my_buckets__count", int64(1)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__my_buckets__key_0", 41.956443786621094),
				model.NewQueryResultCol("aggr__my_buckets__count", int64(2)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__my_buckets__key_0", 43.53862762451172),
				model.NewQueryResultCol("aggr__my_buckets__count", int64(1)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__my_buckets__key_0", 44.48069763183594),
				model.NewQueryResultCol("aggr__my_buckets__count", int64(3)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__my_buckets__key_0", 45.118141174316406),
				model.NewQueryResultCol("aggr__my_buckets__count", int64(8)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__my_buckets__key_0", 1234),
				model.NewQueryResultCol("aggr__my_buckets__count", int64(8)),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT "product" AS "aggr__my_buckets__key_0",
              count(*) AS "aggr__my_buckets__count"
            FROM __quesma_table_name
            GROUP BY "product" AS "aggr__my_buckets__key_0"
            ORDER BY "aggr__my_buckets__count" DESC, "aggr__my_buckets__key_0" ASC
            LIMIT 11`,
	},
	{ // [66]
		TestName: "simplest composite: 1 histogram (with size)",
		QueryRequestJson: `
		{
			"size": 0,
			"aggs": {
				"my_buckets": {
					"composite": {
						"size": 3,
						"sources": [
							{
								"histo": {
									"histogram": {
										"field": "price",
										"interval": 5
									}
								}
							}
						]
					}
				}
			}
		}`,
		ExpectedResponse: `
		{
			"took": 6,
			"timed_out": false,
			"_shards": {
				"total": 1,
				"successful": 1,
				"skipped": 0,
				"failed": 0
			},
			"hits": {
				"total": {
					"value": 2727,
					"relation": "eq"
				},
				"max_score": null,
				"hits": []
			},
			"aggregations": {
				"my_buckets": {
					"after_key": {
						"histo": 40
					},
					"buckets": [
						{
							"key": {
								"histo": 0
							},
							"doc_count": 121
						},
						{
							"key": {
								"histo": 20
							},
							"doc_count": 3
						},
						{
							"key": {
								"histo": 40
							},
							"doc_count": 4
						}
					]
				}
			}
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__my_buckets__key_0", 0),
				model.NewQueryResultCol("aggr__my_buckets__count", 121),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__my_buckets__key_0", 20),
				model.NewQueryResultCol("aggr__my_buckets__count", 3),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__my_buckets__key_0", 40),
				model.NewQueryResultCol("aggr__my_buckets__count", 4),
			}},
		}, // ZLE bo musze tylko 3 (dodac limit)
		ExpectedPancakeSQL: `
			SELECT floor("price"/5)*5 AS "aggr__my_buckets__key_0",
			  count(*) AS "aggr__my_buckets__count"
			FROM __quesma_table_name
			GROUP BY floor("price"/5)*5 AS "aggr__my_buckets__key_0"
			ORDER BY "aggr__my_buckets__key_0" ASC`,
	},
	{ // [67]
		TestName: "simplest composite: 1 date_histogram",
		QueryRequestJson: `
		{
			"size": 0,
			"aggs": {
				"my_buckets": {
					"composite": {
						"sources": [
							{
								"date": {
									"date_histogram": {
										"field": "timestamp",
										"calendar_interval": "1d"
									}
								}
							}
						]
					}
				}
			}
		}`,
		ExpectedResponse: `
		{
		
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__histo__key_0", int64(1706021640000/30000)),
				model.NewQueryResultCol("aggr__histo__count", 1960),
				model.NewQueryResultCol("aggr__histo__0__parent_count", 1960),
				model.NewQueryResultCol("aggr__histo__0__key_0", "order"),
				model.NewQueryResultCol("aggr__histo__0__count", int64(42)),
			}},
		},
		ExpectedPancakeSQL: `
			`,
	},
	{ // [68]
		TestName: "simplest composite: 1 geotile_grid",
		QueryRequestJson: `
		{
			"size": 0,
			"aggs": {
				"my_buckets": {
					"composite": {
						"sources": [
							{
								"tile": {
									"geotile_grid": {
										"field": "location",
										"precision": 8
									}
								}
							}
						]
					}
				}
			}
		}`,
		ExpectedResponse: `
		{
		
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__histo__key_0", int64(1706021640000/30000)),
				model.NewQueryResultCol("aggr__histo__count", 1960),
				model.NewQueryResultCol("aggr__histo__0__parent_count", 1960),
				model.NewQueryResultCol("aggr__histo__0__key_0", "order"),
				model.NewQueryResultCol("aggr__histo__0__count", int64(42)),
			}},
		},
		ExpectedPancakeSQL: `
			`,
	},
	{ // [69]
		TestName: "composite: 2 sources + 1 subaggregation + size parameter",
		QueryRequestJson: `
		{
			"size": 0,
			"aggs": {
				"my_buckets": {
					"composite": {
						"size": 3,
						"sources": [
							{
								"date": {
									"date_histogram": {
										"field": "timestamp",
										"calendar_interval": "1d",
										"order": "desc"
									}
								}
							},
          					{
								"product": {
									"terms": {
										"field": "product"
									}
								}
							}
						]
					},
					"aggregations": {
        				"the_avg": {
          					"avg": {
								"field": "price"
							}
        				}
      				}
				}
			}
		}`,
		ExpectedResponse: `
		{
		
		}`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__histo__key_0", int64(1706021640000/30000)),
				model.NewQueryResultCol("aggr__histo__count", 1960),
				model.NewQueryResultCol("aggr__histo__0__key_0", "order"),
				model.NewQueryResultCol("aggr__histo__0__count", int64(42)),
			}},
		},
		ExpectedPancakeSQL: `
			`,
	},
}
