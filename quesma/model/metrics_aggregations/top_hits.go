package metrics_aggregations

import (
	"context"
	"fmt"
	"mitmproxy/quesma/logger"
	"mitmproxy/quesma/model"
	"mitmproxy/quesma/schema"
	"strconv"
	"strings"
)

type TopHits struct {
	ctx context.Context
}

func NewTopHits(ctx context.Context) TopHits {
	return TopHits{ctx: ctx}
}

func (query TopHits) IsBucketAggregation() bool {
	return false
}

// TODO implement correct
func (query TopHits) TranslateSqlResponseToJson(rows []model.QueryResultRow, level int) []model.JsonMap {
	var topElems []any
	if len(rows) > 0 && level >= len(rows[0].Cols)-1 {
		// values are [level, len(row.Cols) - 1]
		logger.WarnWithCtx(query.ctx).Msgf(
			"no columns returned for top_hits aggregation, level: %d, len(rows[0].Cols): %d, len(rows): %d",
			level, len(rows[0].Cols), len(rows),
		)
	}
	for _, row := range rows {
		if len(row.Cols) == 0 {
			logger.WarnWithCtx(query.ctx).Msg("no columns returned for top_hits aggregation, skipping")
			continue
		}

		valuesForHits := row.Cols
		sourceMap := model.JsonMap{}

		for _, col := range valuesForHits {
			var withoutQuotes string
			if unquoted, err := strconv.Unquote(col.ColName); err == nil {
				withoutQuotes = unquoted
			} else {
				withoutQuotes = col.ColName
			}
			colName, _ := strings.CutPrefix(withoutQuotes, `windowed_`)
			fmt.Println(colName, col.ColName, col.Value, col.ColType.Name, schema.TypePoint.Name)
			if col.ColType.Name == schema.TypePoint.Name {
				colName := ""
				hits := make(model.JsonMap)
				// TODO merge both into one field
				if strings.Contains(col.ColName, "::lon") {
					hits["lon"] = col.ExtractValue(query.ctx)
					colName = strings.TrimSuffix(col.ColName, "::lon")
				}
				if strings.Contains(col.ColName, "::lat") {
					hits["lat"] = col.ExtractValue(query.ctx)
					colName = strings.TrimSuffix(col.ColName, "::lat")
				}
				sourceMap[colName] = hits

			} else {
				sourceMap[col.ColName] = col.ExtractValue(query.ctx)
			}
		}

		elem := model.JsonMap{
			"_source": sourceMap,
		}
		topElems = append(topElems, elem)
	}
	return []model.JsonMap{{
		"hits": topElems,
	}}
}

func (query TopHits) String() string {
	return "top_hits"
}

func (query TopHits) PostprocessResults(rowsFromDB []model.QueryResultRow) []model.QueryResultRow {
	return rowsFromDB
}
