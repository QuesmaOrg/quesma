// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package bucket_aggregations

import (
	"context"
	"github.com/QuesmaOrg/quesma/platform/logger"
	"github.com/QuesmaOrg/quesma/platform/model"
)

type FilterAgg struct {
	ctx         context.Context
	WhereClause model.Expr
}

func NewFilterAgg(ctx context.Context, whereClause model.Expr) FilterAgg {
	return FilterAgg{ctx: ctx, WhereClause: whereClause}
}

func (query FilterAgg) AggregationType() model.AggregationType {
	return model.BucketAggregation
}

func (query FilterAgg) TranslateSqlResponseToJson(rows []model.QueryResultRow) model.JsonMap {
	var docCount any = 0
	if len(rows) == 0 {
		logger.WarnWithCtx(query.ctx).Msg("no rows returned for filter aggregation")
	} else if len(rows[0].Cols) == 0 {
		logger.WarnWithCtx(query.ctx).Msg("no columns returned for filter aggregation")
	} else {
		docCount = rows[0].Cols[0].Value
	}
	return model.JsonMap{"doc_count": docCount}
}

func (query FilterAgg) String() string {
	return "count"
}

func (query FilterAgg) DoesNotHaveGroupBy() bool {
	return true
}

func (query FilterAgg) CombinatorGroups() (result []CombinatorGroup) {
	return []CombinatorGroup{{
		idx:         0,
		Prefix:      "",
		Key:         "",
		WhereClause: query.WhereClause,
	}}
}

func (query FilterAgg) CombinatorTranslateSqlResponseToJson(subGroup CombinatorGroup, rows []model.QueryResultRow) model.JsonMap {
	return query.TranslateSqlResponseToJson(rows)
}

func (query FilterAgg) CombinatorSplit() []model.QueryType {
	return []model.QueryType{query}
}
