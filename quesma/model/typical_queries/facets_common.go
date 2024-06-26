// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package typical_queries

import (
	"context"
	"quesma/logger"
	"quesma/model"
)

func facetsTranslateSqlResponseToJson(ctx context.Context, rows []model.QueryResultRow) model.JsonMap {
	const maxFacets = 10 // facets show only top 10 values
	bucketsNr := min(len(rows), maxFacets)
	buckets := make([]model.JsonMap, 0, bucketsNr)
	returnedRowsNr := 0
	var sampleCount uint64

	// Let's make the following branching only for tests' sake. In production, we always have uint64,
	// but go-sqlmock can only return int64, so let's keep it like this for now.
	// Normally, only 'uint64' case would be needed.

	// Not checking for cast errors here, they may be a lot of them, and error should never happen.
	// One of the better place to allow panic, I think.
	if bucketsNr > 0 {
		switch rows[0].Cols[model.ResultColDocCountIndex].Value.(type) {
		case int64:
			for i, row := range rows[:bucketsNr] {
				buckets = append(buckets, make(model.JsonMap))
				for _, col := range row.Cols {
					buckets[i][col.ColName] = col.Value
				}
				returnedRowsNr += int(row.Cols[model.ResultColDocCountIndex].Value.(int64))
			}
			for _, row := range rows {
				sampleCount += uint64(row.Cols[model.ResultColDocCountIndex].Value.(int64))
			}
		case uint64:
			for i, row := range rows[:bucketsNr] {
				buckets = append(buckets, make(model.JsonMap))
				for _, col := range row.Cols {
					buckets[i][col.ColName] = col.Value
				}
				returnedRowsNr += int(row.Cols[model.ResultColDocCountIndex].Value.(uint64))
			}
			for _, row := range rows {
				sampleCount += row.Cols[model.ResultColDocCountIndex].Value.(uint64)
			}
		default:
			logger.WarnWithCtx(ctx).Msgf("unknown type for facets doc_count: %T, value: %v",
				rows[0].Cols[model.ResultColDocCountIndex].Value, rows[0].Cols[model.ResultColDocCountIndex].Value)
		}
	}

	return model.JsonMap{
		"sample": model.JsonMap{
			"doc_count": int(sampleCount),
			"sample_count": model.JsonMap{
				"value": int(sampleCount),
			},
			"top_values": model.JsonMap{
				"buckets":                     buckets,
				"sum_other_doc_count":         int(sampleCount) - returnedRowsNr,
				"doc_count_error_upper_bound": 0,
			},
		},
	}
}
