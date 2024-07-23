// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package typical_queries

import (
	"context"
	"quesma/model"
)

// Facets There's no such aggregation in Elastic. It's a special type of a simple bucket aggregation request.
// that so far we handle differently than via standard handling, for optimization purposes.
//
// It's aggs part looks like this:
//
//	"aggs": {
//		"sample": {
//			"aggs": {
//				"sample_count": { "value_count": { "field": "some-field-name" } },
//				"top_values": { "terms": { "field": "some-field-name", "size": 10 } }
//			},
//			"sampler": { "shard_size": 5000 }
//		}
//	}
type Facets struct {
	ctx context.Context
}

func NewFacets(ctx context.Context) Facets {
	return Facets{ctx: ctx}
}

func (query Facets) AggregationType() model.AggregationType {
	return model.TypicalAggregation
}

func (query Facets) TranslateSqlResponseToJson(rows []model.QueryResultRow, level int) model.JsonMap {
	return facetsTranslateSqlResponseToJson(query.ctx, rows)
}

func (query Facets) String() string {
	return "facets"
}


