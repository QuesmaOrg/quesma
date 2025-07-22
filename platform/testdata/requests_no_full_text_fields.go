// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package testdata

import "github.com/QuesmaOrg/quesma/platform/model"

// TestsSearchNoFullTextFields - test cases for search queries for a table with no full text fields
// Query generator do not check for full text fields, so it can generate queries with full text search
var TestsSearchNoFullTextFields = []SearchTestCase{
	{
		Name: "((quick AND fox) OR (brown AND fox) OR fox) AND NOT news",
		QueryJson: `
		{
			"query": {
				"bool": {
					"filter": [
						{
							"bool": {
								"filter": [
									{
										"bool": {
											"minimum_should_match": 1,
											"should": [
												{
													"bool": {
														"filter": [
															{
																"multi_match": {
																	"lenient": true,
																	"query": "quick",
																	"type": "best_fields"
																}
															},
															{
																"multi_match": {
																	"lenient": true,
																	"query": "fox",
																	"type": "best_fields"
																}
															}
														]
													}
												},
												{
													"bool": {
														"filter": [
															{
																"multi_match": {
																	"lenient": true,
																	"query": "brown",
																	"type": "best_fields"
																}
															},
															{
																"multi_match": {
																	"lenient": true,
																	"query": "fox",
																	"type": "best_fields"
																}
															}
														]
													}
												},
												{
													"multi_match": {
														"lenient": true,
														"query": "fox",
														"type": "best_fields"
													}
												}
											]
										}
									},
									{
										"bool": {
											"must_not": {
												"multi_match": {
													"lenient": true,
													"query": "news",
													"type": "best_fields"
												}
											}
										}
									}
								]
							}
						},
						{
							"range": {
								"timestamp": {
									"format": "strict_date_optional_time",
									"gte": "2024-03-26T09:56:02.241Z",
									"lte": "2024-04-10T08:56:02.241Z"
								}
							}
						}
					],
					"must": [],
					"must_not": [],
					"should": []
				}
			}
		}`,
		WantedSql: []string{
			"(((((`__quesma_fulltext_field_name` iLIKE '%quick%' AND `__quesma_fulltext_field_name` iLIKE '%fox%') OR (`__quesma_fulltext_field_name` iLIKE '%brown%' AND `__quesma_fulltext_field_name` iLIKE '%fox%')) OR `__quesma_fulltext_field_name` iLIKE '%fox%') AND NOT (`__quesma_fulltext_field_name` iLIKE '%news%')) AND (`timestamp`>='2024-03-26T09:56:02.241Z' AND `timestamp`<='2024-04-10T08:56:02.241Z'))",
		},
		WantedQueryType: model.ListAllFields,
		//WantedQuery: []model.Query{
		//	justSimplestWhere(`(((((false AND false) OR (false AND false)) OR false) AND NOT (false)) AND ("timestamp">='2024-03-26T09:56:02.241Z' AND "timestamp"<='2024-04-10T08:56:02.241Z'))`),
		//},
		WantedRegexes: []string{}, // empty, as not important so far. Can be filled later if needed
	},
}
