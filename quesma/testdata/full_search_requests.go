package testdata

import "fmt"

func selectCnt(limit int) string {
	return fmt.Sprintf("SELECT COUNT(*) FROM %s LIMIT %d", QuotedTableName, limit)
}
func selectStar(limit int) string {
	return fmt.Sprintf("SELECT * FROM %s LIMIT %d", QuotedTableName, limit)
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
		// TODO make hits smaller
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
						},
						{
						"_index": ".ds-logs-generic-default-2024.05.30-000001",
						"_id": "8N9yyo8B5yxeSrtV_w0C",
						"_score": 1,
						"_source": {
							"severity": "debug",
							"@timestamp": "2024-05-30T17:01:25.118Z",
							"host.name": "hestia",
							"source": "coreos",
							"service.name": "service",
							"message": "User password changed"
						}
					}
				]
			}
		}`,
		ExpectedSQLs: []string{selectStar(1)},
	},
	{ // [2]
		Name: "We can deduct hits count from the rows list, we shouldn't any count(*) request, we should return gte 1",
		QueryRequestJson: `
		{
			"runtime_mappings": {},
			"size": 2,
			"track_total_hits": 1
		}`,
	},
	{ // [3] here our LIMIT 2 request returns 1 row
		Name: "We can deduct hits count from the rows list, we shouldn't any count(*) request, we should return eq 1",
		QueryRequestJson: `
		{
			"runtime_mappings": {},
			"size": 2,
			"track_total_hits": 1
		}`,
	},
	{ // [4]
		Name: "track_total_hits: false",
		QueryRequestJson: `
		{
			"runtime_mappings": {},
			"size": 2,
			"track_total_hits": false
		}`,
	},
	{ // [5]
		Name: "track_total_hits: true, size >= count(*)",
		QueryRequestJson: `
		{
			"runtime_mappings": {},
			"size": 2,
			"track_total_hits": true
		}`,
	},
	{ // [6]
		Name: "track_total_hits: true, size < count(*)",
		QueryRequestJson: `
		{
			"runtime_mappings": {},
			"size": 1,
			"track_total_hits": true
		}`,
	},

	// SearchQueryType == ...

}
