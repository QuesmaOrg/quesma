// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package invalid_search_requests

import "quesma/testdata"

var InvalidBucketAggregationTestsSimple = []testdata.AggregationTestCase{
	{
		TestName: "Kibana 8.15, Metrics: Aggregation: Rate, invalid Unit (10)", //reason [eaggs] > reason
		QueryRequestJson: `
		{
			"aggs": {
				"0": {
					"date_histogram": {
						"field": "order_date"
					}
				}
			}
		}`,
		ExpectedResponse: `
		{
			"error": {
				"caused_by": {
					"reason": "Unsupported unit 10",
					"type": "illegal_argument_exception"
				},
				"reason": "[1:59] [rate] failed to parse field [unit]",
				"root_cause": [
					{
						"reason": "[1:59] [rate] failed to parse field [unit]",
						"type": "x_content_parse_exception"
					}
				],
				"type": "x_content_parse_exception"
			},
			"status": 400
		} (400 status code)`,
	},
}
