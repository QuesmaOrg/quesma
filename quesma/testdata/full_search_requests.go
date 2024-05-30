package testdata

var FullSearchRequests = []FullSearchTestCase{
	{ // [0]
		Name: "We can't deduct hits count from the rows list, we should send count(*) LIMIT 1 request",
		QueryRequestJson: `
		{
			"runtime_mappings": {},
			"size": 0,
			"track_total_hits": 1
		}`,
	},
	{ // [1]
		Name: "We can deduct hits count from the rows list, we shouldn't any count(*) request",
		QueryRequestJson: `
		{
			"runtime_mappings": {},
			"size": 1,
			"track_total_hits": 1
		}`,
	},
	{ // [2]
		Name: "We can deduct hits count from the rows list, we shouldn't any count(*) request ver 2",
		QueryRequestJson: `
		{
			"runtime_mappings": {},
			"size": 2,
			"track_total_hits": 1
		}`,
	},
}
