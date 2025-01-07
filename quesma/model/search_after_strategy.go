package model

import (
	"context"
	"quesma/schema"
)

type (
	SearchAfterStrategy interface {
		// ValidateAndParse validates the 'searchAfter', which is what came from the request's search_after field.
		ValidateAndParse(query *Query, indexSchema schema.Schema) (searchAfterParamParsed []Expr, err error)
		TransformQuery(query *Query, searchAfterParameterParsed []Expr) (*Query, error)
		TransformHit(ctx context.Context, hit *SearchHit, pkFieldName *string, sortFieldNames []string,
			rows []QueryResultRow, lastNRowsSameSortValues int) (hitTransformed *SearchHit, lastNRowsSameSortValuesNew int)
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
