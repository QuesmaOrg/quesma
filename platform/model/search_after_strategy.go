// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package model

import (
	"context"
	"github.com/QuesmaOrg/quesma/platform/schema"
)

type (
	SearchAfterStrategy interface {
		// ValidateAndParse validates the 'searchAfter', which is what came from the request's search_after field.
		ValidateAndParse(query *Query, indexSchema schema.Schema) error
		TransformQuery(query *Query) (*Query, error)
		TransformHit(ctx context.Context, hit *SearchHit, pkFieldName *string, sortFieldNames []string,
			rows []QueryResultRow) *SearchHit
	}
	SearchAfterStrategyType int
)

const (
	BasicAndFast SearchAfterStrategyType = iota
	Bulletproof
	JustDiscardTheParameter
	DefaultSearchAfterStrategy = Bulletproof
)

func (s SearchAfterStrategyType) String() string {
	return []string{"BasicAndFast", "Bulletproof", "JustDiscardTheParameter"}[s]
}
