package metrics_aggregations

import (
	"context"
	"mitmproxy/quesma/clickhouse"
	"mitmproxy/quesma/logger"
	"mitmproxy/quesma/model"
)

func metricsTranslateSqlResponseToJson(ctx context.Context, rows []model.QueryResultRow, level int) []model.JsonMap {
	var value any = nil
	if len(rows) > 0 {
		if len(rows[0].Cols) > 0 {
			value = rows[0].Cols[len(rows[0].Cols)-1].Value
		} else {
			logger.WarnWithCtx(ctx).Msg("no columns returned for metrics aggregation")
		}
	} else {
		logger.WarnWithCtx(ctx).Msg("no rows returned for metrics aggregation")
	}
	return []model.JsonMap{{
		"value": value,
	}}
}

func metricsTranslateSqlResponseToJsonWithFieldTypeCheck(
	ctx context.Context, rows []model.QueryResultRow, level int, fieldType clickhouse.DateTimeType) []model.JsonMap {
	if fieldType == clickhouse.Invalid {
		// if it's not a date, we do just a normal response
		return metricsTranslateSqlResponseToJson(ctx, rows, level)
	}
	
}
