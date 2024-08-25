// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package bucket_aggregations

import (
	"context"
	"fmt"
	"quesma/logger"
	"quesma/model"
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

func (query Filters) TranslateSqlResponseToJson(rows []model.QueryResultRow, level int) model.JsonMap {
	var value any = nil
	if len(rows) > 0 {
		if len(rows[0].Cols) > 0 {
			value = rows[0].Cols[len(rows[0].Cols)-1].Value
		} else {
			logger.ErrorWithCtx(query.ctx).Msgf("unexpected number of columns in filters aggregation response, len(rows[0].Cols): %d, level: %d", len(rows[0].Cols), level)
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

func (query Filters) SubGroups() (result []SubGroup) {
	for filterIdx, filter := range query.Filters {
		result = append(result, SubGroup{
			idx:         filterIdx,
			Prefix:      fmt.Sprintf("filter_%d__", filterIdx),
			Key:         filter.Name,
			WhereClause: filter.Sql.WhereClause,
		})
	}
	return
}

func (query Filters) SubGroupTranslateSqlResponseToJson(subGroup SubGroup, rows []model.QueryResultRow) model.JsonMap {
	return query.TranslateSqlResponseToJson(rows, 0)
}
