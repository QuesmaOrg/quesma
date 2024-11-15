// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package bucket_aggregations

import (
	"context"
	"fmt"
	"quesma/logger"
	"quesma/model"
)

type (
	Composite struct {
		ctx              context.Context
		size             int
		baseAggregations []*BaseAggregation
	}
	BaseAggregation struct {
		name        string
		aggregation model.QueryType
	}
)

func NewComposite(ctx context.Context, size int, baseAggregations []*BaseAggregation) *Composite {
	return &Composite{ctx: ctx, size: size, baseAggregations: baseAggregations}
}

func NewBaseAggregation(name string, aggregation model.QueryType) *BaseAggregation {
	return &BaseAggregation{name: name, aggregation: aggregation}
}

func (query *Composite) AggregationType() model.AggregationType {
	return model.BucketAggregation
}

func (query *Composite) TranslateSqlResponseToJson(rows []model.QueryResultRow) model.JsonMap {
	minimumExpectedColNr := len(query.baseAggregations) + 1 // +1 for doc_count. Can be more, if this Composite has parent aggregations, but never fewer.
	if len(rows) > 0 && len(rows[0].Cols) < minimumExpectedColNr {
		logger.ErrorWithCtx(query.ctx).Msgf(
			"unexpected number of columns in terms aggregation response, len: %d, expected (at least): %d, rows[0]: %v", len(rows[0].Cols), minimumExpectedColNr, rows[0])
	}
	var buckets []model.JsonMap
	for _, row := range rows {
		startIndex := len(row.Cols) - len(query.baseAggregations) - 1
		if startIndex < 0 {
			logger.WarnWithCtx(query.ctx).Msgf("startIndex < 0 - too few columns. row: %+v", row)
			startIndex = 0
		}
		keyColumns := row.Cols[startIndex : len(row.Cols)-1] // last col isn't a key, it's doc_count
		key := make(model.JsonMap, len(keyColumns))
		for i, col := range keyColumns {
			key[query.baseAggregations[i].name] = col.Value
		}

		bucket := model.JsonMap{
			"key":       key,
			"doc_count": query.docCount(&row),
		}
		buckets = append(buckets, bucket)
	}

	response := model.JsonMap{
		"buckets": buckets,
	}
	if len(rows) > 0 {
		response["after_key"] = buckets[len(buckets)-1]["key"]
	}
	return response
}

func (query *Composite) String() string {
	return fmt.Sprintf("composite(size: %d)", query.size)
}

func (query *Composite) docCount(row *model.QueryResultRow) any {
	return row.Cols[len(row.Cols)-1].Value
}
