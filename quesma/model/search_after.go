// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package model

import (
	"fmt"
	"quesma/util"
)

type SearchAfterStrategy interface {
	Validate(searchAfter any) error
	ApplyStrategyAndTransformQuery(query *Query, searchAfter any) *Query
	DoSomethingWithHitsResult(hitsResult *SearchHits) // TODO change name
}

func SearchAfterStrategyFactory(strategyName string, timestampField ColumnRef) SearchAfterStrategy {
	switch strategyName {
	case "basic_and_fast":
		return NewSearchAfterStrategyBasicAndFast(timestampField)
	default:
		return SearchAfterStrategyBasicAndFast{}
	}
}

// ----------------------------------------------------------
// | First, simple strategy: BasicAndFast (default for now) |
// ----------------------------------------------------------

type SearchAfterStrategyBasicAndFastParamType int64

const EmptySearchAfter = SearchAfterStrategyBasicAndFastParamType(-1)

type SearchAfterStrategyBasicAndFast struct {
	timestampField ColumnRef
}

func NewSearchAfterStrategyBasicAndFast(timestampField ColumnRef) SearchAfterStrategyBasicAndFast {
	return SearchAfterStrategyBasicAndFast{timestampField: timestampField}
}

func (s SearchAfterStrategyBasicAndFast) Validate(searchAfter any) error {
	_, err := s.validateAndParse(searchAfter)
	return err
}

func (s SearchAfterStrategyBasicAndFast) ApplyStrategyAndTransformQuery(query *Query, searchAfterRaw any) *Query {
	searchAfterTs, _ := s.validateAndParse(searchAfterRaw) // we validate during parsing and error there. No need to check here.
	if searchAfterTs == EmptySearchAfter {
		return query
	}
	timestampRangeClause := NewInfixExpr(s.timestampField, "<=", NewFunction("fromUnixTimestamp64Milli", NewLiteral(searchAfterTs)))
	query.SelectCommand.WhereClause = Or([]Expr{query.SelectCommand.WhereClause, timestampRangeClause})
	return query
}

func (s SearchAfterStrategyBasicAndFast) DoSomethingWithHitsResult(*SearchHits) {
	// no-op
}

// Validate validates the SearchAfter. 'sa' is what came from the request's search_after field.
func (s SearchAfterStrategyBasicAndFast) validateAndParse(searchAfter any) (SearchAfterStrategyBasicAndFastParamType, error) {
	if searchAfter == nil {
		return EmptySearchAfter, nil
	}

	asArray, ok := searchAfter.([]any)
	if !ok {
		return EmptySearchAfter, fmt.Errorf("search_after must be an array")
	}

	if len(asArray) > 1 {
		return EmptySearchAfter, fmt.Errorf("for basic_and_fast strategy, search_after must have at most one element")
	}
	if shouldBeTimestamp, ok := util.ExtractNumeric64Maybe(asArray[0]); ok {
		if shouldBeTimestamp >= 0 && util.IsFloat64AnInt64(shouldBeTimestamp) {
			return SearchAfterStrategyBasicAndFastParamType(int64(shouldBeTimestamp)), nil
		}
		return EmptySearchAfter, fmt.Errorf("for basic_and_fast strategy, search_after must be a unix timestamp in milliseconds")
	}
	return EmptySearchAfter, fmt.Errorf("for basic_and_fast strategy, search_after must be an integer")
}
