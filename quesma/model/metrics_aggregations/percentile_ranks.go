package metrics_aggregations

import (
	"fmt"
	"mitmproxy/quesma/model"
	"strings"
)

type PercentileRanks struct{}

func (query PercentileRanks) IsBucketAggregation() bool {
	return false
}

func (query PercentileRanks) TranslateSqlResponseToJson(rows []model.QueryResultRow, level int) []model.JsonMap {
	valueMap := make(map[string]float64)
	for _, percentileRank := range rows[0].Cols[level:] {
		// percentileRank.ColName looks like this [...]<=X,[...]. We're extracting X.
		// It always needs to have .Y or .YZ at the end, so 1 or 2 digits after the dot, and dot is mandatory.
		// Also can't be .00, needs to be .0
		beg := strings.Index(percentileRank.ColName, "<=")
		end := strings.Index(percentileRank.ColName[beg:], ",")
		cutValue := percentileRank.ColName[beg+2 : beg+end]

		dot := strings.Index(cutValue, ".")
		if dot == -1 {
			cutValue += ".0"
		} else if end-dot >= len(".00") && cutValue[dot:dot+3] == ".00" {
			cutValue = cutValue[:dot+2]
		} else {
			cutValue = cutValue[:dot+3]
		}
		fmt.Println("counterPercentile", percentileRank.ColName, percentileRank.Value.(float64), cutValue)
		valueMap[cutValue] = percentileRank.Value.(float64)
	}
	return []model.JsonMap{{
		"values": valueMap,
	}}
}

func (query PercentileRanks) String() string {
	return "percentile_ranks"
}
