// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package typical_queries

import (
	"context"
	"quesma/model"
)

type Count struct {
	ctx context.Context
}

func NewCount(ctx context.Context) Count {
	return Count{ctx: ctx}
}

func (query Count) AggregationType() model.AggregationType {
	return model.TypicalAggregation
}

func (query Count) TranslateSqlResponseToJson(rows []model.QueryResultRow, level int) model.JsonMap {
	return make(model.JsonMap, 0)
}

func (query Count) String() string {
	return "count (non-aggregation)"
}
