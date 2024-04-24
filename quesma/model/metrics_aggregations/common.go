package metrics_aggregations

import (
	"context"
	"mitmproxy/quesma/logger"
	"mitmproxy/quesma/model"
)

func metricsTranslateSqlResponseToJson(ctx context.Context, rows []model.QueryResultRow, level int) []model.JsonMap {
	var value any = nil
	if len(rows) > 0 {
		value = rows[0].Cols[level].Value
	} else {
		logger.WarnWithCtx(ctx).Msg("no value returned for metrics aggregation")
	}
	return []model.JsonMap{{
		"value": value,
	}}
}
