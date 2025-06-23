// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package metrics_aggregations

import (
	"context"
	"github.com/QuesmaOrg/quesma/platform/clickhouse"
	"github.com/QuesmaOrg/quesma/platform/logger"
	"github.com/QuesmaOrg/quesma/platform/model"
	"time"
)

func metricsTranslateSqlResponseToJson(ctx context.Context, rows []model.QueryResultRow) model.JsonMap {
	var value any = nil
	if resultRowsAreNonEmpty(ctx, rows) {
		value = rows[0].Cols[len(rows[0].Cols)-1].Value
	}
	return model.JsonMap{
		"value": value,
	}
}

func metricsTranslateSqlResponseToJsonZeroDefault(ctx context.Context, rows []model.QueryResultRow) model.JsonMap {
	var value any = int64(0)
	if resultRowsAreNonEmpty(ctx, rows) {
		value = rows[0].Cols[len(rows[0].Cols)-1].Value
	}
	return model.JsonMap{
		"value": value,
	}
}

// metricsTranslateSqlResponseToJsonWithFieldTypeCheck is the same as metricsTranslateSqlResponseToJson for all types except DateTimes.
// With DateTimes, we need to return 2 values, instead of 1, that's the difference.
func metricsTranslateSqlResponseToJsonWithFieldTypeCheck(
	ctx context.Context, rows []model.QueryResultRow, fieldType clickhouse.DateTimeType) model.JsonMap {
	if fieldType == clickhouse.Invalid {
		// if it's not a date, we do just a normal response
		return metricsTranslateSqlResponseToJson(ctx, rows)
	}

	var value, valueAsString any = nil, nil
	if resultRowsAreNonEmpty(ctx, rows) {
		valueAsAny := rows[0].Cols[len(rows[0].Cols)-1].Value
		if valueAsTime, ok := valueAsAny.(time.Time); ok {
			value = valueAsTime.UnixMilli()
			valueAsString = valueAsTime.Format("2006-01-02T15:04:05.000")
		} else {
			logger.WarnWithCtx(ctx).Msgf("could not parse date %v", valueAsAny)
		}
	}
	response := model.JsonMap{
		"value": value,
	}
	if value != nil {
		response["value_as_string"] = valueAsString
	}
	return response
}

func resultRowsAreNonEmpty(ctx context.Context, rows []model.QueryResultRow) bool {
	if len(rows) == 0 {
		return false
	}
	if len(rows[0].Cols) == 0 {
		logger.WarnWithCtx(ctx).Msg("no columns returned for metrics aggregation")
		return false
	}
	return true
}
