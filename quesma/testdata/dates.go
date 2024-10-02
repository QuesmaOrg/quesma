// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package testdata

import "quesma/model"

var AggregationTestsWithDates = []AggregationTestCase{
	{ // [0]
		TestName: "simple max/min aggregation as 2 siblings",
		QueryRequestJson: `
		{
  "aggs": {
    "sampler": {
      "aggs": {
        "eventRate": {
          "date_histogram": {
            "extended_bounds": {
              "max": 1727859403270,
              "min": 1727858503270
            },
            "field": "order_date",
            "calendar_interval": "1w",
            "min_doc_count": 0
          }
        }
      },
      "random_sampler": {
        "probability": 0.000001,
        "seed": "1292529172"
      }
    }
  },
  "size": 0,
  "track_total_hits": false
}`,
		ExpectedResponse: `
		{
			"completion_time_in_millis": 1707486436398,
			"expiration_time_in_millis": 1707486496397,
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
					"sampler": {
						"doc_count": 4675,
						"eventRate": {
							"buckets": [
								{
									"doc_count": 442,
									"key": 1726358400000,
									"key_as_string": "2024-09-15T00:00:00.000"
								},
								{
									"doc_count": 0,
									"key": 1726963200000,
									"key_as_string": "2024-09-22T00:00:00.000"
								},
								{
									"doc_count": 0,
									"key": 1727568000000,
									"key_as_string": "2024-09-29T00:00:00.000"
								},
								{
									"doc_count": 0,
									"key": 1728172800000,
									"key_as_string": "2024-10-06T00:00:00.000"
								},
								{
									"doc_count": 1,
									"key": 1728777600000,
									"key_as_string": "2024-10-13T00:00:00.000"
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
						"value": 2200
					}
				},
				"timed_out": false,
				"took": 1
			},
			"start_time_in_millis": 1707486436397
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
