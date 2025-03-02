// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package testdata

import "github.com/QuesmaOrg/quesma/platform/model"

var GrafanaAggregationTests = []AggregationTestCase{
	{ // [0]
		TestName: "format: epoch_millis",
		QueryRequestJson: `
		{
			"aggs": {
				"2": {
					"date_histogram": {
						"field": "@timestamp",
						"fixed_interval": "2000ms",
						"min_doc_count": 0,
						"extended_bounds": {
							"min": 1740930494000,
							"max": 1740930500000
						},
						"format": "epoch_millis"
					}
				}
			},
			"size": 0
		}`,
		ExpectedResponse: `
		{
			"aggregations": {
                "2": {
                    "buckets": [
                        {
                            "doc_count": 0,
                            "key": 1740930494000,
                            "key_as_string": "1740930494000"
                        },
                        {
                            "doc_count": 4,
                            "key": 1740930496000,
                            "key_as_string": "1740930496000"
                        },
                        {
                            "doc_count": 0,
                            "key": 1740930498000,
                            "key_as_string": "1740930498000"
                        },
                        {
                            "doc_count": 1,
                            "key": 1740930500000,
                            "key_as_string": "1740930500000"
                        }
                    ]
                }
            },
            "hits": {
                "hits": [],
                "max_score": null,
                "total": {
                    "relation": "eq",
                    "value": 7
                }
            },
            "status": 200,
            "timed_out": false,
            "took": 30
        }`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__key_0", int64(1740930496000/2000)),
				model.NewQueryResultCol("aggr__2__count", int64(4)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__key_0", int64(1740930500000/2000)),
				model.NewQueryResultCol("aggr__2__count", int64(1)),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT toInt64(toUnixTimestamp64Milli("@timestamp") / 2000) AS "aggr__2__key_0",
			  count(*) AS "aggr__2__count"
			FROM __quesma_table_name
			GROUP BY toInt64(toUnixTimestamp64Milli("@timestamp") / 2000) AS
			  "aggr__2__key_0"
			ORDER BY "aggr__2__key_0" ASC`,
	},
	{ // [1]
		TestName: "1x terms with min_doc_count, need to erase some rows with count < min_doc_count",
		QueryRequestJson: `
		{
			"aggs": {
				"2": {
					"terms": {
						"field": "extension.keyword",
						"size": 4,
						"min_doc_count": 40,
						"order": {
							"_key": "desc"
						}
					}
				}
			},
			"size": 0
		}`,
		ExpectedResponse: `
		{
            "aggregations": {
                "2": {
					"doc_count_error_upper_bound": 0,
					"sum_other_doc_count": 195,
					"buckets": [
						{
							"key": "zip",
							"doc_count": 41
						}
					]
				}
			},
            "hits": {
                "hits": [],
                "max_score": null,
                "total": {
                    "relation": "eq",
                    "value": 234
                }
            },
            "status": 200,
            "timed_out": false,
            "took": 1
        }`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", int64(236)),
				model.NewQueryResultCol("aggr__2__key_0", "zip"),
				model.NewQueryResultCol("aggr__2__count", int64(41)),
				model.NewQueryResultCol("aggr__2__order_1", int64(1)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", int64(236)),
				model.NewQueryResultCol("aggr__2__key_0", "tar"),
				model.NewQueryResultCol("aggr__2__count", int64(31)),
				model.NewQueryResultCol("aggr__2__order_1", int64(0)),
			}},
		},
		ExpectedPancakeSQL: `
		  	SELECT sum(count(*)) OVER () AS "aggr__2__parent_count",
			  "extension" AS "aggr__2__key_0", count(*) AS "aggr__2__count",
			  count(*)>=40 AS "aggr__2__order_1"
			FROM __quesma_table_name
			GROUP BY "extension" AS "aggr__2__key_0"
			ORDER BY "aggr__2__order_1" DESC, "aggr__2__key_0" DESC
			LIMIT 5`,
	},
	{ // [2]
		TestName: "2x terms with min_doc_count",
		QueryRequestJson: `
		{
			"aggs": {
				"2": {
					"aggs": {
						"3": {
							"terms": {
								"field": "message"
							}	
						}
					},
					"terms": {
						"field": "extension.keyword",
						"size": 4,
						"min_doc_count": 30,
						"order": {
							"_key": "desc"
						}
					}
				}
			},
			"size": 0
		}`,
		ExpectedResponse: `
		{
            "aggregations": {
                "2": {
					"doc_count_error_upper_bound": 0,
					"sum_other_doc_count": 164,
					"buckets": [
						{
							"3": {
								"doc_count_error_upper_bound": 0,
								"sum_other_doc_count": 9,
								"buckets": [
									{
										"key": 0,
										"doc_count": 18
									},
									{
										"key": 6680,
										"doc_count": 4
									}
								]
							},
							"key": "zip",
							"doc_count": 31
						},
						{
							"3": {
								"doc_count_error_upper_bound": 0,
								"sum_other_doc_count": 14,
								"buckets": [
									{
										"key": 0,
										"doc_count": 25
									},
									{
										"key": 1873,
										"doc_count": 2
									}
								]
							},
							"key": "tar",
							"doc_count": 41
						}
					]
				}
			},
            "hits": {
                "hits": [],
                "max_score": null,
                "total": {
                    "relation": "eq",
                    "value": 234
                }
            },
            "status": 200,
            "timed_out": false,
            "took": 1
        }`,
		ExpectedPancakeResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", int64(236)),
				model.NewQueryResultCol("aggr__2__key_0", "zip"),
				model.NewQueryResultCol("aggr__2__count", int64(31)),
				model.NewQueryResultCol("aggr__2__order_1", int64(0)),
				model.NewQueryResultCol("aggr__2__3__parent_count", int64(31)),
				model.NewQueryResultCol("aggr__2__3__key_0", 0),
				model.NewQueryResultCol("aggr__2__3__count", int64(18)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", int64(236)),
				model.NewQueryResultCol("aggr__2__key_0", "zip"),
				model.NewQueryResultCol("aggr__2__count", int64(31)),
				model.NewQueryResultCol("aggr__2__order_1", int64(1)),
				model.NewQueryResultCol("aggr__2__3__parent_count", int64(31)),
				model.NewQueryResultCol("aggr__2__3__key_0", 6680),
				model.NewQueryResultCol("aggr__2__3__count", int64(4)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", int64(236)),
				model.NewQueryResultCol("aggr__2__key_0", "tar"),
				model.NewQueryResultCol("aggr__2__count", int64(41)),
				model.NewQueryResultCol("aggr__2__order_1", int64(1)),
				model.NewQueryResultCol("aggr__2__3__parent_count", int64(41)),
				model.NewQueryResultCol("aggr__2__3__key_0", 0),
				model.NewQueryResultCol("aggr__2__3__count", int64(25)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__parent_count", int64(236)),
				model.NewQueryResultCol("aggr__2__key_0", "tar"),
				model.NewQueryResultCol("aggr__2__count", int64(41)),
				model.NewQueryResultCol("aggr__2__order_1", int64(1)),
				model.NewQueryResultCol("aggr__2__3__parent_count", int64(41)),
				model.NewQueryResultCol("aggr__2__3__key_0", 1873),
				model.NewQueryResultCol("aggr__2__3__count", int64(2)),
			}},
		},
		ExpectedPancakeSQL: `
		  	SELECT "aggr__2__parent_count", "aggr__2__key_0", "aggr__2__count",
			  "aggr__2__order_1", "aggr__2__3__parent_count", "aggr__2__3__key_0",
			  "aggr__2__3__count"
			FROM (
			  SELECT "aggr__2__parent_count", "aggr__2__key_0", "aggr__2__count",
				"aggr__2__order_1", "aggr__2__3__parent_count", "aggr__2__3__key_0",
				"aggr__2__3__count",
				dense_rank() OVER (ORDER BY "aggr__2__order_1" DESC, "aggr__2__key_0" DESC)
				AS "aggr__2__order_1_rank",
				dense_rank() OVER (PARTITION BY "aggr__2__key_0" ORDER BY
				"aggr__2__3__count" DESC, "aggr__2__3__key_0" ASC) AS
				"aggr__2__3__order_1_rank"
			  FROM (
				SELECT sum(count(*)) OVER () AS "aggr__2__parent_count",
				  "extension" AS "aggr__2__key_0",
				  sum(count(*)) OVER (PARTITION BY "aggr__2__key_0") AS "aggr__2__count",
				  sum(count(*)>=30) OVER (PARTITION BY "aggr__2__key_0") AS
				  "aggr__2__order_1",
				  sum(count(*)) OVER (PARTITION BY "aggr__2__key_0") AS
				  "aggr__2__3__parent_count", "message" AS "aggr__2__3__key_0",
				  count(*) AS "aggr__2__3__count"
				FROM __quesma_table_name
				GROUP BY "extension" AS "aggr__2__key_0", "message" AS "aggr__2__3__key_0"))
			WHERE ("aggr__2__order_1_rank"<=5 AND "aggr__2__3__order_1_rank"<=11)
			ORDER BY "aggr__2__order_1_rank" ASC, "aggr__2__3__order_1_rank" ASC`,
	},
}
