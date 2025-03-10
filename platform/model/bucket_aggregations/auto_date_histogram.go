// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package bucket_aggregations

import (
	"context"
	"fmt"
	"github.com/QuesmaOrg/quesma/platform/logger"
	"github.com/QuesmaOrg/quesma/platform/model"
	"time"
)

// TODO: only bucketsNr=1 is supported for now. Implement other cases.
type AutoDateHistogram struct {
	ctx       context.Context
	field     model.ColumnRef // name of the field, e.g. timestamp
	bucketsNr int
	key       int64
}

// NewAutoDateHistogram creates a new AutoDateHistogram aggregation, during parsing.
// Key is set later, during pancake transformation.
func NewAutoDateHistogram(ctx context.Context, field model.ColumnRef, bucketsNr int) *AutoDateHistogram {
	return &AutoDateHistogram{ctx: ctx, field: field, bucketsNr: bucketsNr}
}

func (query *AutoDateHistogram) AggregationType() model.AggregationType {
	return model.BucketAggregation
}

func (query *AutoDateHistogram) TranslateSqlResponseToJson(rows []model.QueryResultRow) model.JsonMap {
	if len(rows) == 0 {
		logger.WarnWithCtx(query.ctx).Msgf("no rows returned for %s", query.String())
		return make(model.JsonMap, 0)
	}
	if len(rows) != 1 {
		logger.WarnWithCtx(query.ctx).Msgf("unexpected (!= 1) number of rows returned for %s: %d.", query.String(), len(rows))
	}
	return model.JsonMap{
		"buckets": []model.JsonMap{{
			"key":           query.key,
			"key_as_string": time.UnixMilli(query.key).UTC().Format("2006-01-02T15:04:05.000"),
			"doc_count":     rows[0].LastColValue(),
		}},
		"interval": "100y", // seems working for bucketsNr=1 case. Will have to be changed for other cases.
	}
}

func (query *AutoDateHistogram) String() string {
	return fmt.Sprintf("auto_date_histogram(field: %v, bucketsNr: %d)", model.AsString(query.field), query.bucketsNr)
}

func (query *AutoDateHistogram) GetField() model.ColumnRef {
	return query.field
}

func (query *AutoDateHistogram) SetKey(key int64) {
	query.key = key
}
