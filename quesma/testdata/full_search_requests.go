// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package testdata

import (
	"fmt"
	"quesma/model"
)

func selectCnt(limit int) string {
	return fmt.Sprintf(`SELECT count(*) FROM (SELECT 1 FROM %s LIMIT %d)`, TableName, limit)
}
func selectTotalCnt() string {
	return fmt.Sprintf("SELECT count(*) FROM %s", TableName)
}
func selectStar(limit int) string {
	return fmt.Sprintf("SELECT \"message\" FROM %s LIMIT %d", TableName, limit)
}

func resultCount(cnt int) []model.QueryResultRow {
	return []model.QueryResultRow{{
		Cols: []model.QueryResultCol{model.NewQueryResultCol("count()", uint64(cnt))},
	}}
}

func resultSelect(cnt int) []model.QueryResultRow {
	result := make([]model.QueryResultRow, cnt)
	for i := range cnt {
		result[i] = model.QueryResultRow{
			Cols: []model.QueryResultCol{model.NewQueryResultCol("message", "example")},
		}
	}
	return result
}

const IndexName = `"` + TableName + `"`

var FullSearchRequests = []FullSearchTestCase{

	// SearchQueryType == Normal

	{ // [0]
		Name: "We can't deduct hits count from the rows list, we should send count(*) LIMIT 1 request",
		QueryRequestJson: `
		{
			"runtime_mappings": {},
			"size": 0,
			"track_total_hits": 1
		}`,
		ExpectedResponse: `
		{
			"_shards": {
				"total": 1,
				"successful": 1,
				"skipped": 0,
				"failed": 0
			},
			"hits": {
				"total": {
					"value": 1,
					"relation": "gte"
				},
				"max_score": null,
				"hits": []
			}
		}`,
		ExpectedSQLs:       []string{selectCnt(1)},
		ExpectedSQLResults: [][]model.QueryResultRow{resultCount(1)},
	},
	{ // [1]
		Name: "We can deduct hits count from the rows list, we shouldn't any count(*) request",
		QueryRequestJson: `
		{
			"runtime_mappings": {},
			"size": 1,
			"track_total_hits": 1
		}`,
		ExpectedResponse: `
		{
			"_shards": {
				"total": 1,
				"successful": 1,
				"skipped": 0,
				"failed": 0
			},
			"hits": {
				"total": {
					"value": 1,
					"relation": "gte"
				},
				"max_score": null,
				"hits": [
					{
						"_index": ` + IndexName + `,
						"_id": "1",
						"_score": 0.0,
						"_source": {
							"message": "example"
						},
						"fields": {
							"message": ["example"]
						}
					}
				]
			}
		}`,
		ExpectedSQLs:       []string{selectStar(1)},
		ExpectedSQLResults: [][]model.QueryResultRow{resultSelect(1)},
	},
	{ // [2]
		Name: "We can deduct hits count from the rows list, we shouldn't any count(*) request, we should return gte 1",
		// TODO: Not sure if we should return 1 gte or 2 gte
		QueryRequestJson: `
		{
			"runtime_mappings": {},
			"size": 2,
			"track_total_hits": 1
		}`,
		ExpectedResponse: `
		{
			"_shards": {
				"total": 1,
				"successful": 1,
				"skipped": 0,
				"failed": 0
			},
			"hits": {
				"total": {
					"value": 2,
					"relation": "gte"
				},
				"max_score": null,
				"hits": [
					{
						"_index": ` + IndexName + `,
						"_id": "1",
						"_score": 0.0,
						"_source": {
							"message": "example"
						},
						"fields": {
							"message": ["example"]
						}
					},
					{
						"_index": ` + IndexName + `,
						"_id": "2",
						"_score": 0.0,
						"_source": {
							"message": "example"
						},
						"fields": {
							"message": ["example"]
						}
					}
				]
			}
		}`,
		ExpectedSQLs:       []string{selectStar(2)},
		ExpectedSQLResults: [][]model.QueryResultRow{resultSelect(2)},
	},
	{ // [3] here our LIMIT 2 request returns 1 row
		Name: "We can deduct hits count from the rows list, we shouldn't any count(*) request, we should return eq 1",
		QueryRequestJson: `
		{
			"runtime_mappings": {},
			"size": 2,
			"track_total_hits": 1
		}`,
		ExpectedResponse: `
		{
			"_shards": {
				"total": 1,
				"successful": 1,
				"skipped": 0,
				"failed": 0
			},
			"hits": {
				"total": {
					"value": 2,
					"relation": "gte"
				},
				"max_score": null,
				"hits": [
					{
						"_index": ` + IndexName + `,
						"_id": "1",
						"_score": 0.0,
						"_source": {
							"message": "example"
						},
						"fields": {
							"message": ["example"]
						}
					},
					{
						"_index": ` + IndexName + `,
						"_id": "2",
						"_score": 0.0,
						"_source": {
							"message": "example"
						},
						"fields": {
							"message": ["example"]
						}
					}
				]
			}
		}`,
		ExpectedSQLs:       []string{selectStar(2)},
		ExpectedSQLResults: [][]model.QueryResultRow{resultSelect(2)},
	},
	{ // [4]
		Name: "track_total_hits: false",
		QueryRequestJson: `
		{
			"runtime_mappings": {},
			"size": 2,
			"track_total_hits": false
		}`,
		ExpectedResponse: `
		{
			"_shards": {
				"total": 1,
				"successful": 1,
				"skipped": 0,
				"failed": 0
			},
			"hits": {
				"total": {
					"value": 2,
					"relation": "gte"
				},
				"max_score": null,
				"hits": [
					{
						"_index": ` + IndexName + `,
						"_id": "1",
						"_score": 0.0,
						"_source": {
							"message": "example"
						},
						"fields": {
							"message": ["example"]
						}
					},
					{
						"_index": ` + IndexName + `,
						"_id": "2",
						"_score": 0.0,
						"_source": {
							"message": "example"
						},
						"fields": {
							"message": ["example"]
						}
					}
				]
			}
		}`,
		ExpectedSQLs:       []string{selectStar(2)},
		ExpectedSQLResults: [][]model.QueryResultRow{resultSelect(2)},
	},
	{ // [5]
		Name: "track_total_hits: true, size >= count(*)",
		QueryRequestJson: `
		{
			"runtime_mappings": {},
			"size": 2,
			"track_total_hits": true
		}`,
		ExpectedResponse: `
		{
			"_shards": {
				"total": 1,
				"successful": 1,
				"skipped": 0,
				"failed": 0
			},
			"hits": {
				"total": {
					"value": 2,
					"relation": "eq"
				},
				"max_score": null,
				"hits": [
					{
						"_index": ` + IndexName + `,
						"_id": "1",
						"_score": 0.0,
						"_source": {
							"message": "example"
						},
						"fields": {
							"message": ["example"]
						}
					},
					{
						"_index": ` + IndexName + `,
						"_id": "2",
						"_score": 0.0,
						"_source": {
							"message": "example"
						},
						"fields": {
							"message": ["example"]
						}
					}
				]
			}
		}`,
		ExpectedSQLs:       []string{selectStar(2), selectTotalCnt()},
		ExpectedSQLResults: [][]model.QueryResultRow{resultSelect(2), resultCount(2)},
	},
	{ // [6]
		Name: "track_total_hits: true, size < count(*)",
		QueryRequestJson: `
		{
			"runtime_mappings": {},
			"size": 1,
			"track_total_hits": true
		}`,
		ExpectedResponse: `
		{
			"_shards": {
				"total": 1,
				"successful": 1,
				"skipped": 0,
				"failed": 0
			},
			"hits": {
				"total": {
					"value": 123,
					"relation": "eq"
				},
				"max_score": null,
				"hits": [
					{
						"_index": ` + IndexName + `,
						"_id": "1",
						"_score": 0.0,
						"_source": {
							"message": "example"
						},
						"fields": {
							"message": ["example"]
						}
					}
				]
			}
		}`,
		ExpectedSQLs:       []string{selectStar(1), selectTotalCnt()},
		ExpectedSQLResults: [][]model.QueryResultRow{resultSelect(1), resultCount(123)},
	},

	// SearchQueryType == ...

}
