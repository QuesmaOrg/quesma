package metrics_aggregations

import (
	"context"
	"mitmproxy/quesma/logger"
	"mitmproxy/quesma/model"
	"strings"
)

type Stats struct {
	ctx context.Context
}

func NewStats(ctx context.Context) Stats {
	return Stats{ctx: ctx}
}

func (query Stats) IsBucketAggregation() bool {
	return false
}

func (query Stats) TranslateSqlResponseToJson(rows []model.QueryResultRow, level int) []model.JsonMap {
	var resultMap model.JsonMap
	if len(rows) > 0 {
		resultMap = make(model.JsonMap)
		for _, v := range rows[0].Cols[level:] {
			// v.ColName = e.g. avgOrNull(...). We need to extract only 'avg'.
			// first: avgOrNull(..) -> avgOrNull
			firstLeftBracketIndex := strings.Index(v.ColName, "(")
			if firstLeftBracketIndex == -1 {
				logger.Error().Msgf("invalid column name in stats aggregation: %s. Skipping", v.ColName)
				continue
			}
			fullName := v.ColName[:firstLeftBracketIndex]
			// second: if ends with OrNull, then avgOrNull -> avg
			withoutOrNull, _ := strings.CutSuffix(fullName, "OrNull")
			resultMap[withoutOrNull] = v.Value
		}
	}
	return []model.JsonMap{resultMap}
}

func (query Stats) CalculateResultIfMissing(model.QueryResultRow, []model.QueryResultRow) model.QueryResultRow {
	return model.QueryResultRow{}
}

func (query Stats) String() string {
	return "stats"
}
