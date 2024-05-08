package metrics_aggregations

import (
	"context"
	"mitmproxy/quesma/model"
)

type Cardinality struct {
	ctx context.Context
}

func NewCardinality(ctx context.Context) Cardinality {
	return Cardinality{ctx: ctx}
}

func (query Cardinality) IsBucketAggregation() bool {
	return false
}

func (query Cardinality) TranslateSqlResponseToJson(rows []model.QueryResultRow, level int) []model.JsonMap {
	return metricsTranslateSqlResponseToJson(query.ctx, rows, level)
}

func (query Cardinality) CalculateResultIfMissing(model.QueryResultRow, []model.QueryResultRow) model.QueryResultRow {
	return model.QueryResultRow{}
}

func (query Cardinality) String() string {
	return "cardinality"
}
