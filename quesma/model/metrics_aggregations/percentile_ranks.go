package metrics_aggregations

import (
	"context"
	"mitmproxy/quesma/logger"
	"mitmproxy/quesma/model"
	"strings"
)

type PercentileRanks struct {
	ctx context.Context
}

func NewPercentileRanks(ctx context.Context) PercentileRanks {
	return PercentileRanks{ctx: ctx}
}

func (query PercentileRanks) IsBucketAggregation() bool {
	return false
}

func (query PercentileRanks) TranslateSqlResponseToJson(rows []model.QueryResultRow, level int) []model.JsonMap {
	valueMap := make(map[string]float64)
	for _, percentileRank := range rows[0].Cols[level:] {
		// percentileRank.ColName looks like this [...]<=X,[...]. We're extracting X.
		// It always needs to have .Y or .YZ at the end, so 1 or 2 digits after the dot, and dot is mandatory.
		// Also, can't be .00, needs to be .0
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
		if value, ok := percentileRank.Value.(float64); ok {
			valueMap[cutValue] = value
		} else {
			logger.WarnWithCtx(query.ctx).Msgf("failed to convert percentile rank value to float64, type: %T, value: %v",
				percentileRank.Value, percentileRank.Value)
		}
	}
	return []model.JsonMap{{
		"values": valueMap,
	}}
}

func (query PercentileRanks) String() string {
	return "percentile_ranks"
}
