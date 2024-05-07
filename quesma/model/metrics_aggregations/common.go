package metrics_aggregations

import (
	"context"
	"fmt"
	"mitmproxy/quesma/clickhouse"
	"mitmproxy/quesma/logger"
	"mitmproxy/quesma/model"
	"time"
)

func metricsTranslateSqlResponseToJson(ctx context.Context, rows []model.QueryResultRow, level int) []model.JsonMap {
	var value any = nil
	if resultRowsAreFine(ctx, rows) {
		value = rows[0].Cols[len(rows[0].Cols)-1].Value
	}
	return []model.JsonMap{{
		"value": value,
	}}
}

// same as metricsTranslateSqlResponseToJson for all types except DateTimeType
// with DateTimes, we need to return 2 values, instead of 1.
func metricsTranslateSqlResponseToJsonWithFieldTypeCheck(
	ctx context.Context, rows []model.QueryResultRow, level int, fieldType clickhouse.DateTimeType) []model.JsonMap {
	fmt.Println(fieldType)
	if fieldType == clickhouse.Invalid {
		// if it's not a date, we do just a normal response
		return metricsTranslateSqlResponseToJson(ctx, rows, level)
	}

	var value, valueAsString any = nil, nil
	if resultRowsAreFine(ctx, rows) {
		valueAsAny := rows[0].Cols[len(rows[0].Cols)-1].Value
		if valueAsTime, isString := valueAsAny.(time.Time); isString {
			value = valueAsTime.UnixMilli()
			valueAsString = valueAsTime.Format(time.RFC3339Nano)
		} else {
			logger.WarnWithCtx(ctx).Msg("could not parse date")
		}
	}
	response := model.JsonMap{
		"value": value,
	}
	if value != nil {
		response["value_as_string"] = valueAsString
	}
	return []model.JsonMap{response}
}

func resultRowsAreFine(ctx context.Context, rows []model.QueryResultRow) bool {
	if len(rows) == 0 {
		logger.WarnWithCtx(ctx).Msg("no rows returned for metrics aggregation")
		return false
	}
	if len(rows[0].Cols) == 0 {
		logger.WarnWithCtx(ctx).Msg("no columns returned for metrics aggregation")
		return false
	}
	return true
}
