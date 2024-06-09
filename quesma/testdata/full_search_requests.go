package testdata

import "fmt"

func selectCnt(limit int) string {
	return fmt.Sprintf("SELECT count() FROM (SELECT 1 FROM %s LIMIT %d )", QuotedTableName, limit)
}
func selectTotalCnt() string {
	return fmt.Sprintf("SELECT count() FROM %s", QuotedTableName)
}
func selectStar(limit int) string {
	return fmt.Sprintf("SELECT \"message\" FROM %s LIMIT %d", QuotedTableName, limit)
}

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
			"took": 12,
			"timed_out": false,
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
		ExpectedSQLs: []string{selectCnt(1)},
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
			"took": 10,
			"timed_out": false,
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
				"max_score": 1,
				"hits": [
					{
						"_index": ".ds-logs-generic-default-2024.05.30-000001",
						"_id": "5d9yyo8B5yxeSrtV-A2A",
						"_score": 1,
						"_source": {
							"severity": "info",
							"@timestamp": "2024-05-30T17:01:23.44Z",
							"host.name": "cassandra",
							"source": "centos",
							"service.name": "auth",
							"message": "User password reset"
							}
						}
					}
				]
			}
		}`,
		ExpectedSQLs: []string{selectStar(1)},
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
		ExpectedSQLs: []string{selectStar(2)},
	},
	{ // [3] here our LIMIT 2 request returns 1 row
		Name: "We can deduct hits count from the rows list, we shouldn't any count(*) request, we should return eq 1",
		QueryRequestJson: `
		{
			"runtime_mappings": {},
			"size": 2,
			"track_total_hits": 1
		}`,
		ExpectedSQLs: []string{selectStar(2)},
	},
	{ // [4]
		Name: "track_total_hits: false",
		QueryRequestJson: `
		{
			"runtime_mappings": {},
			"size": 2,
			"track_total_hits": false
		}`,
		ExpectedSQLs: []string{selectStar(2)},
	},
	{ // [5]
		Name: "track_total_hits: true, size >= count(*)",
		QueryRequestJson: `
		{
			"runtime_mappings": {},
			"size": 2,
			"track_total_hits": true
		}`,
		ExpectedSQLs: []string{selectStar(2), selectTotalCnt()},
	},
	{ // [6]
		Name: "track_total_hits: true, size < count(*)",
		QueryRequestJson: `
		{
			"runtime_mappings": {},
			"size": 1,
			"track_total_hits": true
		}`,
		ExpectedSQLs: []string{selectStar(1), selectTotalCnt()},
	},

	// SearchQueryType == ...

}
