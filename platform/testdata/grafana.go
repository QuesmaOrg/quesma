// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package testdata

import "github.com/QuesmaOrg/quesma/platform/model"

var GrafanaAggregationTests = []AggregationTestCase{
	{ // [0]
		TestName: "simple max/min aggregation as 2 siblings",
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
}
