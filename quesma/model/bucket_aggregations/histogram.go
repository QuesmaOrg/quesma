// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package bucket_aggregations

import (
	"context"
	"github.com/QuesmaOrg/quesma/quesma/logger"
	"github.com/QuesmaOrg/quesma/quesma/model"
	"github.com/QuesmaOrg/quesma/quesma/util"
)

type Histogram struct {
	ctx         context.Context
	interval    float64
	minDocCount int
}

func NewHistogram(ctx context.Context, interval float64, minDocCount int) *Histogram {
	return &Histogram{ctx: ctx, interval: interval, minDocCount: minDocCount}
}

func (query *Histogram) AggregationType() model.AggregationType {
	return model.BucketAggregation
}

func (query *Histogram) TranslateSqlResponseToJson(rows []model.QueryResultRow) model.JsonMap {
	if len(rows) > 0 && len(rows[0].Cols) < 2 {
		logger.ErrorWithCtx(query.ctx).Msgf(
			"unexpected number of columns in histogram aggregation response, len(rows[0].Cols): "+
				"%d", len(rows[0].Cols),
		)
	}

	if query.minDocCount == 0 {
		rows = query.NewRowsTransformer().Transform(query.ctx, rows)
	}

	var response []model.JsonMap
	for _, row := range rows {
		response = append(response, model.JsonMap{
			"key":       row.Cols[len(row.Cols)-2].Value,
			"doc_count": row.Cols[len(row.Cols)-1].Value,
		})
	}
	return model.JsonMap{
		"buckets": response,
	}
}

func (query *Histogram) String() string {
	return "histogram"
}

func (query *Histogram) SetMinDocCountToZero() {
	query.minDocCount = 0
}

func (query *Histogram) NewRowsTransformer() model.QueryRowsTransformer {
	return &HistogramRowsTransformer{
		interval:    query.interval,
		MinDocCount: query.minDocCount,
	}
}

type HistogramRowsTransformer struct {
	interval    float64
	MinDocCount int
}

// if minDocCount == 0, and we have buckets e.g. [key, value1], [key+2*interval, value2], we need to insert [key+1*interval, 0]
// CAUTION: a different kind of postprocessing is needed for MinDocCount > 1, but I haven't seen any query with that yet, so not implementing it now.
func (query *HistogramRowsTransformer) Transform(ctx context.Context, rowsFromDB []model.QueryResultRow) []model.QueryResultRow {
	if query.MinDocCount != 0 || len(rowsFromDB) < 2 {
		// we only add empty rows, when
		// a) MinDocCount == 0
		// b) we have > 1 rows, with < 2 rows we can't add anything in between
		return rowsFromDB
	}
	postprocessedRows := make([]model.QueryResultRow, 0, len(rowsFromDB))
	postprocessedRows = append(postprocessedRows, rowsFromDB[0])

	getKey := query.getKeyFloat64
	if query.interval == 1.0 {
		getKey = query.getKeyUnknownType
	}

	for i := 1; i < len(rowsFromDB); i++ {
		if len(rowsFromDB[i-1].Cols) < 2 || len(rowsFromDB[i].Cols) < 2 {
			logger.ErrorWithCtx(ctx).Msgf(
				"unexpected number of columns in histogram aggregation response (< 2),"+
					"rowsFromDB[%d]: %+v, rowsFromDB[%d]: %+v. Skipping those rows in postprocessing",
				i-1, rowsFromDB[i-1], i, rowsFromDB[i],
			)
		}
		lastKey, okLast := getKey(rowsFromDB[i-1])
		currentKey, okCurrent := getKey(rowsFromDB[i])
		if okLast && okCurrent {
			// we need to add rows in between
			for midKey := lastKey + query.interval; util.IsSmaller(midKey, currentKey); midKey += query.interval {
				midRow := rowsFromDB[i-1].Copy()
				midRow.Cols[len(midRow.Cols)-2].Value = midKey
				midRow.Cols[len(midRow.Cols)-1].Value = 0
				postprocessedRows = append(postprocessedRows, midRow)
			}
		}
		postprocessedRows = append(postprocessedRows, rowsFromDB[i])
	}
	return postprocessedRows
}

// we're sure key is either float64, or in rare cases nil
func (query *HistogramRowsTransformer) getKeyFloat64(row model.QueryResultRow) (float64, bool) {
	switch val := row.Cols[len(row.Cols)-2].Value.(type) {
	case float64:
		return val, true
	default:
		return -1, false
	}
}

// we don't know the type
func (query *HistogramRowsTransformer) getKeyUnknownType(row model.QueryResultRow) (float64, bool) {
	val := row.Cols[len(row.Cols)-2].Value
	if valAsFloat, ok := util.ExtractNumeric64Maybe(val); ok {
		return valAsFloat, true
	}
	return -1, false
}
