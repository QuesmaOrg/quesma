// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package bucket_aggregations

import (
	"context"
	"quesma/logger"
	"quesma/model"
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

func (query Terms) TranslateSqlResponseToJson(rows []model.QueryResultRow, level int) model.JsonMap {
	if len(rows) > 0 && len(rows[0].Cols) < 2 {
		logger.ErrorWithCtx(query.ctx).Msgf(
			"unexpected number of columns in terms aggregation response, len: %d, rows[0]: %v", len(rows[0].Cols), rows[0])
	}
	if len(rows) == 0 {
		return model.JsonMap{}
	}

	var response []model.JsonMap
	for _, row := range rows {
		docCount := row.Cols[len(row.Cols)-1].Value
		bucket := model.JsonMap{
			"key":       row.Cols[len(row.Cols)-2].Value,
			"doc_count": docCount,
		}
		if query.significant {
			bucket["score"] = docCount
			bucket["bg_count"] = docCount
		}
		response = append(response, bucket)
	}
	return model.JsonMap{
		"doc_count_error_upper_bound": 0,
		"buckets":                     response,
	}
}

func (query Terms) String() string {
	if !query.significant {
		return "terms"
	}
	return "significant_terms"
}
