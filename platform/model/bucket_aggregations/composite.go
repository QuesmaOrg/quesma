// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package bucket_aggregations

import (
	"context"
	"fmt"
	"github.com/QuesmaOrg/quesma/platform/logger"
	"github.com/QuesmaOrg/quesma/platform/model"
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
	minimumExpectedColNr := query.expectedBaseAggrColumnsNr() + 1 // +1 for doc_count. Can be more, if this Composite has parent aggregations, but never fewer.
	if len(rows) > 0 && len(rows[0].Cols) < minimumExpectedColNr {
		logger.ErrorWithCtx(query.ctx).Msgf("too few columns in composite aggregation response, len: %d, expected (at least): %d, rows[0]: %v", len(rows[0].Cols), minimumExpectedColNr, rows[0])
	}

	buckets := make([]model.JsonMap, 0, len(rows))
	for _, row := range rows {
		colIdx := 0
		key := make(model.JsonMap, len(query.baseAggregations))
		for _, baseAggr := range query.baseAggregations {
			col := row.Cols[colIdx]
			if dateHistogram, ok := baseAggr.aggregation.(*DateHistogram); ok {
				if originalKey, ok := col.Value.(int64); ok {
					key[baseAggr.name] = dateHistogram.calculateResponseKey(originalKey)
				} else {
					logger.ErrorWithCtx(query.ctx).Msgf("unexpected value in date_histogram key column: %v", col.Value)
				}
				colIdx += 1
			} else if geotileGrid, ok := baseAggr.aggregation.(GeoTileGrid); ok {
				key[baseAggr.name] = geotileGrid.calcKey(row.Cols[colIdx:])
				colIdx += 3
			} else {
				key[baseAggr.name] = col.Value
				colIdx += 1
			}
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
	if len(buckets) > 0 {
		response["after_key"] = buckets[len(buckets)-1]["key"]
	}
	return response
}

func (query *Composite) String() string {
	return fmt.Sprintf("composite(size: %d, base aggregations: %v)", query.size, query.baseAggregations)
}

func (query *Composite) docCount(row *model.QueryResultRow) any {
	return row.Cols[len(row.Cols)-1].Value
}

func (query *Composite) expectedBaseAggrColumnsNr() (columnsNr int) {
	for _, baseAggr := range query.baseAggregations {
		if _, ok := baseAggr.aggregation.(GeoTileGrid); ok {
			columnsNr += 3
		} else {
			columnsNr += 1
		}
	}
	return
}
