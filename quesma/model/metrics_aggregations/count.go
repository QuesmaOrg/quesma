// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package metrics_aggregations

import (
	"context"
	"quesma/logger"
	"quesma/model"
)

type Count struct {
	ctx context.Context
}

func NewCount(ctx context.Context) Count {
	return Count{ctx: ctx}
}

func (query Count) IsBucketAggregation() bool {
	return false
}

func (query Count) TranslateSqlResponseToJson(rows []model.QueryResultRow, level int) model.JsonMap {
	if len(rows) == 0 {
		logger.WarnWithCtx(query.ctx).Msg("no rows returned for count aggregation")
		return make(model.JsonMap, 0)
	}
	if len(rows) > 1 {
		logger.WarnWithCtx(query.ctx).Msg("More than one row returned for count aggregation")
	}
	return model.JsonMap{"doc_count": rows[0].Cols[level].Value}
}

func (query Count) String() string {
	return "count"
}

func (query Count) PostprocessResults(rowsFromDB []model.QueryResultRow) []model.QueryResultRow {
	return rowsFromDB
}

func (query Count) MetricsAggregation() {}

func (query Count) ColumnsNr() int {
	return 1
}
