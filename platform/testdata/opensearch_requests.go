// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package testdata

import (
	"github.com/QuesmaOrg/quesma/platform/model"
)

var OpensearchSearchTests = []SearchTestCase{
	{
		Name: "Basic Explorer request",
		QueryJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"2": {
					"date_histogram": {
						"field": "-@timestamp",
						"fixed_interval": "30s",
						"min_doc_count": 1,
						"time_zone": "Europe/Warsaw"
					}
				}
			},
			"docvalue_fields": [
				{
					"field": "-@timestamp",
					"format": "date_time"
				}
			],
			"highlight": {
				"fields": {
					"*": {}
				},
				"fragment_size": 2147483647,
				"post_tags": [
					"@/opensearch-dashboards-highlighted-field@"
				],
				"pre_tags": [
					"@opensearch-dashboards-highlighted-field@"
				]
			},
			"query": {
				"bool": {
					"filter": [
						{
							"match_all": {}
						},
						{
							"range": {
								"-@timestamp": {
									"format": "strict_date_optional_time",
									"gte": "2024-04-04T13:18:18.149Z",
									"lte": "2024-04-04T13:33:18.149Z"
								}
							}
						}
					],
					"must": [],
					"must_not": [],
					"should": []
				}
			},
			"script_fields": {},
			"size": 500,
			"sort": [
				{
					"-@timestamp": {
						"order": "desc",
						"unmapped_type": "boolean"
					}
				}
			],
			"stored_fields": [
				"*"
			],
			"version": true,
			"track_total_hits": true
		}`,
		WantedSql: []string{
			`("__timestamp">=__quesma_from_unixtime64mili(1712236698149) AND "__timestamp"<=__quesma_from_unixtime64mili(1712237598149))`,
		},
		WantedQueryType: model.ListAllFields,
		WantedQueries: []string{
			`SELECT "__bytes", "__timestamp", "message_____"
			FROM __quesma_table_name
			WHERE ("__timestamp">=fromUnixTimestamp64Milli(1712236698149) AND "__timestamp"<=fromUnixTimestamp64Milli(1712237598149))
			ORDER BY "__timestamp" DESC LIMIT 500`,
			`SELECT sum(count(*)) OVER () AS "metric____quesma_total_count_col_0",
			  toInt64((toUnixTimestamp64Milli("__timestamp")+timeZoneOffset(toTimezone(
			  "__timestamp", 'Europe/Warsaw'))*1000) / 30000) AS "aggr__2__key_0",
			  count(*) AS "aggr__2__count"
			FROM __quesma_table_name
			WHERE ("__timestamp">=fromUnixTimestamp64Milli(1712236698149) AND "__timestamp"<=fromUnixTimestamp64Milli(1712237598149))
			GROUP BY toInt64((toUnixTimestamp64Milli("__timestamp")+timeZoneOffset(
			  toTimezone("__timestamp", 'Europe/Warsaw'))*1000) / 30000) AS "aggr__2__key_0"
			ORDER BY "aggr__2__key_0" ASC`,
		},
	},
	{
		Name: "Basic Explorer request, but without SELECT *",
		QueryJson: `
		{
			"_source": {
				"excludes": []
			},
			"aggs": {
				"2": {
					"date_histogram": {
						"field": "-@timestamp",
						"fixed_interval": "30s",
						"min_doc_count": 1
					}
				}
			},
			"docvalue_fields": [
				{
					"field": "-@timestamp",
					"format": "date_time"
				}
			],
			"highlight": {
				"fields": {
					"*": {}
				},
				"fragment_size": 2147483647,
				"post_tags": [
					"@/opensearch-dashboards-highlighted-field@"
				],
				"pre_tags": [
					"@opensearch-dashboards-highlighted-field@"
				]
			},
			"query": {
				"bool": {
					"filter": [
						{
							"match_all": {}
						},
						{
							"range": {
								"-@timestamp": {
									"format": "strict_date_optional_time",
									"gte": "2024-04-04T13:18:18.149Z",
									"lte": "2024-04-04T13:33:18.149Z"
								}
							}
						}
					],
					"must": [],
					"must_not": [],
					"should": []
				}
			},
			"script_fields": {},
			"size": 0,
			"sort": [
				{
					"-@timestamp": {
						"order": "desc",
						"unmapped_type": "boolean"
					}
				}
			],
			"stored_fields": [
				"*"
			],
			"version": true,
			"track_total_hits": true
		}`,
		WantedSql: []string{
			`("__timestamp">=__quesma_from_unixtime64mili(1712236698149) AND "__timestamp"<=__quesma_from_unixtime64mili(1712237598149))`,
		},
		WantedQueryType: model.Normal,
		WantedQueries: []string{
			`SELECT sum(count(*)) OVER () AS "metric____quesma_total_count_col_0",
			  toInt64(toUnixTimestamp64Milli("__timestamp") / 30000) AS "aggr__2__key_0",
       		  count(*) AS "aggr__2__count"
			FROM __quesma_table_name
			WHERE ("__timestamp">=fromUnixTimestamp64Milli(1712236698149) AND "__timestamp"<=fromUnixTimestamp64Milli(1712237598149))
			GROUP BY toInt64(toUnixTimestamp64Milli("__timestamp") / 30000) AS "aggr__2__key_0"
			ORDER BY "aggr__2__key_0" ASC`,
		},
	},
}
