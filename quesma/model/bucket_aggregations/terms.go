// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package bucket_aggregations

import (
	"context"
	"quesma/logger"
	"quesma/model"
	"quesma/util"
)

type Terms struct {
	ctx         context.Context
	significant bool // true <=> significant_terms, false <=> terms
	OrderByExpr model.Expr
}

func NewTerms(ctx context.Context, significant bool, orderByExpr model.Expr) Terms {
	return Terms{ctx: ctx, significant: significant, OrderByExpr: orderByExpr}
}

func (query Terms) AggregationType() model.AggregationType {
	return model.BucketAggregation
}

func (query Terms) TranslateSqlResponseToJson(rows []model.QueryResultRow) model.JsonMap {
	if len(rows) > 0 && len(rows[0].Cols) < 2 {
		logger.ErrorWithCtx(query.ctx).Msgf(
			"unexpected number of columns in terms aggregation response, len: %d, rows[0]: %v", len(rows[0].Cols), rows[0])
	}
	if len(rows) == 0 {
		return model.JsonMap{}
	}

	var response []model.JsonMap
	for _, row := range rows {
		docCount := query.docCount(row)
		bucket := model.JsonMap{
			"key":       query.key(row),
			"doc_count": docCount,
		}
		if query.significant {
			bucket["score"] = docCount
			bucket["bg_count"] = docCount
		}
		response = append(response, bucket)
	}

	if !query.significant {
		parentCountAsInt, _ := util.ExtractInt64(query.parentCount(rows[0]))
		sumOtherDocCount := int(parentCountAsInt) - query.sumDocCounts(rows)
		return model.JsonMap{
			"sum_other_doc_count":         sumOtherDocCount,
			"doc_count_error_upper_bound": 0,
			"buckets":                     response,
		}
	} else {
		parentDocCount, _ := util.ExtractInt64(query.parentCount(rows[0]))
		return model.JsonMap{
			"buckets":   response,
			"doc_count": parentDocCount,
			"bg_count":  parentDocCount,
		}
	}
}

func (query Terms) String() string {
	if !query.significant {
		return "terms"
	}
	return "significant_terms"
}

func (query Terms) sumDocCounts(rows []model.QueryResultRow) int {
	sum := 0
	if len(rows) > 0 {
		switch query.docCount(rows[0]).(type) {
		case int64:
			for _, row := range rows {
				sum += int(query.docCount(row).(int64))
			}
		case uint64:
			for _, row := range rows {
				sum += int(query.docCount(row).(uint64))
			}
		default:
			logger.WarnWithCtx(query.ctx).Msgf("unknown type for terms doc_count: %T, value: %v",
				query.docCount(rows[0]), query.docCount(rows[0]))
		}
	}
	return sum
}

func (query Terms) docCount(row model.QueryResultRow) any {
	return row.Cols[len(row.Cols)-1].Value
}

func (query Terms) key(row model.QueryResultRow) any {
	return row.Cols[len(row.Cols)-2].Value
}

func (query Terms) parentCount(row model.QueryResultRow) any {
	return row.Cols[len(row.Cols)-3].Value
}
