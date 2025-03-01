// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package metrics_aggregations

import (
	"context"
	"github.com/QuesmaOrg/quesma/platform/model"
)

type Cardinality struct {
	ctx context.Context
}

func NewCardinality(ctx context.Context) Cardinality {
	return Cardinality{ctx: ctx}
}

func (query Cardinality) AggregationType() model.AggregationType {
	return model.MetricsAggregation
}

func (query Cardinality) TranslateSqlResponseToJson(rows []model.QueryResultRow) model.JsonMap {
	var value any = 0
	if resultRowsAreNonEmpty(query.ctx, rows) {
		value = rows[0].Cols[len(rows[0].Cols)-1].Value
	}
	return model.JsonMap{
		"value": value,
	}
}

func (query Cardinality) String() string {
	return "cardinality"
}
