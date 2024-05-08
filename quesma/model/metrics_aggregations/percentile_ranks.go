package metrics_aggregations

import (
	"context"
	"mitmproxy/quesma/logger"
	"mitmproxy/quesma/model"
	"strconv"
	"strings"
)

type PercentileRanks struct {
	ctx context.Context
	// defines what response should look like
	// https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-metrics-percentile-rank-aggregation.html#_keyed_response_5
	Keyed bool
}

func NewPercentileRanks(ctx context.Context, keyed bool) PercentileRanks {
	return PercentileRanks{ctx: ctx, Keyed: keyed}
}

func (query PercentileRanks) IsBucketAggregation() bool {
	return false
}

func (query PercentileRanks) TranslateSqlResponseToJson(rows []model.QueryResultRow, level int) []model.JsonMap {
	if len(rows) == 0 {
		logger.WarnWithCtx(query.ctx).Msg("no rows in percentile ranks response")
		return make([]model.JsonMap, 0)
	}
	// I duplicate a lot of code in this if/else below,
	// but I think it's worth it, as this function might get called a lot of times for a single query.
	// And because of complete separation in if/else, I guess it might (should) be slightly faster (?)
	if query.Keyed {
		valueMap := make(model.JsonMap)
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
				logger.WarnWithCtx(query.ctx).Msgf("failed to convert percentile rank value to float64, type: %T, value: %v. Skipping",
					percentileRank.Value, percentileRank.Value)
			}
		}
		return []model.JsonMap{{
			"values": valueMap,
		}}
	} else {
		buckets := make([]model.JsonMap, 0)
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
			cutValueFloat, _ := strconv.ParseFloat(cutValue, 64)
			if value, ok := percentileRank.Value.(float64); ok {
				buckets = append(buckets, model.JsonMap{
					"key":   cutValueFloat,
					"value": value,
				})
			} else {
				logger.WarnWithCtx(query.ctx).Msgf("failed to convert percentile rank value to float64, type: %T, value: %v. Skipping",
					percentileRank.Value, percentileRank.Value)
			}
		}
		return []model.JsonMap{{
			"values": buckets,
		}}
	}
}

func (query PercentileRanks) String() string {
	return "percentile_ranks"
}
