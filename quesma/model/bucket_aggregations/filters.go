// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package bucket_aggregations

import (
	"context"
	"fmt"
	"github.com/QuesmaOrg/quesma/quesma/logger"
	"github.com/QuesmaOrg/quesma/quesma/model"
)

type Filters struct {
	ctx     context.Context
	Filters []Filter
}

func NewFiltersEmpty(ctx context.Context) Filters {
	return Filters{ctx: ctx}
}

func NewFilters(ctx context.Context, filters []Filter) Filters {
	return Filters{ctx: ctx, Filters: filters}
}

func (query Filters) NewFiltersSingleFilter(filterIdx int) Filters {
	if filterIdx < 0 || filterIdx >= len(query.Filters) {
		logger.ErrorWithCtx(query.ctx).Msgf("invalid index %d for filters aggregation", filterIdx)
		return NewFiltersEmpty(query.ctx)
	}
	return NewFilters(query.ctx, []Filter{query.Filters[filterIdx]})
}

type Filter struct {
	Name string
	Sql  model.SimpleQuery
}

func NewFilter(name string, sql model.SimpleQuery) Filter {
	return Filter{Name: name, Sql: sql}
}

func (query Filters) AggregationType() model.AggregationType {
	return model.BucketAggregation
}

func (query Filters) TranslateSqlResponseToJson(rows []model.QueryResultRow) model.JsonMap {
	var value any = 0.0
	if len(rows) > 0 {
		if len(rows[0].Cols) > 0 {
			value = rows[0].Cols[len(rows[0].Cols)-1].Value
		} else {
			logger.ErrorWithCtx(query.ctx).Msgf("unexpected number of columns in filters aggregation response, len(rows[0].Cols): %d", len(rows[0].Cols))
		}
	}
	return model.JsonMap{
		"doc_count": value,
	}
}

func (query Filters) String() string {
	return "filters"
}

func (query Filters) DoesNotHaveGroupBy() bool {
	return true
}

func (query Filters) CombinatorGroups() (result []CombinatorGroup) {
	for filterIdx, filter := range query.Filters {
		prefix := fmt.Sprintf("filter_%d__", filterIdx)
		if len(query.Filters) == 1 {
			prefix = ""
		}
		result = append(result, CombinatorGroup{
			idx:         filterIdx,
			Prefix:      prefix,
			Key:         filter.Name,
			WhereClause: filter.Sql.WhereClause,
		})
	}
	return
}

func (query Filters) CombinatorTranslateSqlResponseToJson(subGroup CombinatorGroup, rows []model.QueryResultRow) model.JsonMap {
	return query.TranslateSqlResponseToJson(rows)
}

func (query Filters) CombinatorSplit() []model.QueryType {
	result := make([]model.QueryType, 0, len(query.Filters))
	for i := range query.Filters {
		result = append(result, query.NewFiltersSingleFilter(i))
	}
	return result
}
