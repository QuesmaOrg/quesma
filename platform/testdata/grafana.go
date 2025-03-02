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
						"fixed_interval":"2000ms",
						"min_doc_count":0,
						"extended_bounds": {
							"min":1740928804319,
							"max":1740932404319
						},
						"format":"epoch_millis"
					}
				}
			},
			"size":0
		}`,
		ExpectedResponse: `
		{
			aggregations": {
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
                        },
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
				model.NewQueryResultCol("aggr__sampler__count", int64(4675)),
				model.NewQueryResultCol("aggr__sampler__eventRate__key_0", int64(1726358400000)),
				model.NewQueryResultCol("aggr__sampler__eventRate__count", int64(442)),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__sampler__count", int64(4675)),
				model.NewQueryResultCol("aggr__sampler__eventRate__key_0", int64(1728777600000)),
				model.NewQueryResultCol("aggr__sampler__eventRate__count", int64(1)),
			}},
		},
		ExpectedPancakeSQL: `
			SELECT sum(count(*)) OVER () AS "aggr__sampler__count",
			  toInt64(toUnixTimestamp(toStartOfWeek(toTimezone("order_date", 'UTC'))))*1000
			  AS "aggr__sampler__eventRate__key_0",
			  count(*) AS "aggr__sampler__eventRate__count"
			FROM (
			  SELECT "order_date"
			  FROM __quesma_table_name
			  LIMIT 20000)
			GROUP BY toInt64(toUnixTimestamp(toStartOfWeek(toTimezone("order_date", 'UTC')))
			  )*1000 AS "aggr__sampler__eventRate__key_0"
			ORDER BY "aggr__sampler__eventRate__key_0" ASC`,
	},
}
