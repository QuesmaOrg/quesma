// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package bucket_aggregations

import (
	"context"
	"fmt"
	"quesma/logger"
	"quesma/model"
	"time"
)

type AutoDateHistogram struct {
	ctx       context.Context
	field     model.Expr // name of the field, e.g. timestamp
	bucketsNr int
	key       int64
}

func NewAutoDateHistogram(ctx context.Context, field model.Expr, bucketsNr int) *AutoDateHistogram {
	return &AutoDateHistogram{ctx: ctx, field: field, bucketsNr: bucketsNr}
}

func (query *AutoDateHistogram) AggregationType() model.AggregationType {
	return model.BucketAggregation
}

func (query *AutoDateHistogram) TranslateSqlResponseToJson(rows []model.QueryResultRow) model.JsonMap {
	fmt.Println(rows)
	if len(rows) == 0 {
		logger.WarnWithCtx(query.ctx).Msgf("no rows returned for %s", query.String())
		return make(model.JsonMap, 0)
	}
	return model.JsonMap{
		"buckets": []model.JsonMap{{
			"key":           query.key,
			"key_as_string": time.UnixMilli(query.key).Format("2006-01-02T15:04:05.000-07:00"),
			"doc_count":     rows[0].LastColValue(),
		}},
		"interval": "100y",
	}
}

func (query *AutoDateHistogram) String() string {
	return fmt.Sprintf("auto_date_histogram(field: %v, bucketsNr: %d)", model.AsString(query.field), query.bucketsNr)
}

func (query *AutoDateHistogram) GetField() model.Expr {
	return query.field
}

func (query *AutoDateHistogram) SetKey(key int64) {
	query.key = key
}
