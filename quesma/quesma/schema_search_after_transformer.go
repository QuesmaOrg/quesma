// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package quesma

import (
	"fmt"
	"github.com/k0kubun/pp"
	"quesma/logger"
	"quesma/model"
	"quesma/schema"
	"quesma/util"
)

type (
	searchAfterStrategy interface {
		// Validate validates the 'searchAfter', which is what came from the request's search_after field.
		validateAndParse(query *model.Query, indexSchema schema.Schema) (searchAfterParameterParsed []model.Expr, err error)
		transform(query *model.Query, searchAfterParameterParsed []model.Expr) (*model.Query, error)
		doSomethingWithHitsResult(hitsResult *model.SearchHits) // TODO change name
	}
	searchAfterStrategyType int
)

// , sortFields []model.OrderByExpr, searchAfter any
func searchAfterStrategyFactory(strategy searchAfterStrategyType) searchAfterStrategy {
	switch strategy {
	case bulletproof:
		return searchAfterStrategyBulletproof{}
	case justDiscardTheParameter:
		return searchAfterStrategyJustDiscardTheParameter{}
	case basicAndFast:
		return searchAfterStrategyBasicAndFast{}
	default:
		logger.Error().Msgf("Unknown search_after strategy: %d. Using Bulletproof.", strategy)
		return searchAfterStrategyBulletproof{}
	}
}

const (
	basicAndFast searchAfterStrategyType = iota // default for a second
	bulletproof
	justDiscardTheParameter
	defaultSearchAfterStrategy = basicAndFast
)

func (s searchAfterStrategyType) String() string {
	return []string{"BasicAndFast", "Bulletproof", "JustDiscardTheParameter"}[s]
}

// ---------------------------------------------------------------------------------
// | Bulletproof, but might be a bit slower for gigantic datasets                    |
// ---------------------------------------------------------------------------------

// sortFields  []model.OrderByExpr
//	pkHashes    []string // md5 for now, should be improved to shorten hashes lengths
//	searchAfter any

type searchAfterStrategyBulletproof struct{}

func (s searchAfterStrategyBulletproof) validateAndParse(query *model.Query, indexSchema schema.Schema) (searchAfterParameterParsed []model.Expr, err error) {
	logger.Debug().Msgf("searchAfter: %v", query.SearchAfter)
	if query.SearchAfter == nil {
		return nil, nil
	}

	asArray, ok := query.SearchAfter.([]any)
	if !ok {
		return nil, fmt.Errorf("search_after must be an array, got: %v", query.SearchAfter)
	}

	if len(asArray) != len(query.SelectCommand.OrderBy) {
		return nil, fmt.Errorf("len(search_after) != len(sortFields), search_after: %v, sortFields: %v", asArray, query.SelectCommand.OrderBy)
	}

	return nil, nil
	/*

		var timestampMs int64
		if shouldBeTimestamp, ok := util.ExtractNumeric64Maybe(asArray[0]); ok {
			if shouldBeTimestamp >= 0 && util.IsFloat64AnInt64(shouldBeTimestamp) {
				timestampMs = int64(shouldBeTimestamp)
			} else {
				return empty, fmt.Errorf("for Bulletproof strategy, search_after[0] must be a unix timestamp in milliseconds")
			}
		} else {
			return empty, fmt.Errorf("for Bulletproof strategy, search_after must be an integer")
		}

		return searchAfterParsedBulletproof{timestampMs: timestampMs, pkHashes: make([]string, 0)}, nil // TODO add parsing pk hashes

	*/
}

func (s searchAfterStrategyBulletproof) transform(query *model.Query, searchAfterParsed []model.Expr) (*model.Query, error) {
	//timestampRangeClause := NewInfixExpr(s.timestampField, "<=", NewFunction("fromUnixTimestamp64Milli", NewLiteral(searchAfter.timestampMs))) // TODO fix this for Hydrolix...
	logger.Debug().Msgf("search_after: %v, query before: %v", query.SearchAfter, model.AsString(query.SelectCommand))
	//query.SelectCommand.WhereClause = And([]Expr{query.SelectCommand.WhereClause, timestampRangeClause})
	//query.SelectCommand.Limit += len(s.pkHashes)
	logger.Debug().Msgf("query after: %v", model.AsString(query.SelectCommand))
	return query, nil
}

func (s searchAfterStrategyBulletproof) doSomethingWithHitsResult(*model.SearchHits) {
	// no-op
}

// -------------------------------------------------------------------------------------------------------------------------------
// | JustDiscardTheParameter: probably only good for tests or when you don't need this functionality and want better performance |
// -------------------------------------------------------------------------------------------------------------------------------

type searchAfterStrategyJustDiscardTheParameter struct{}

func (s searchAfterStrategyJustDiscardTheParameter) validateAndParse(*model.Query, schema.Schema) (searchAfterParameterParsed []model.Expr, err error) {
	return nil, nil
}

func (s searchAfterStrategyJustDiscardTheParameter) transform(query *model.Query, _ []model.Expr) (*model.Query, error) {
	return query, nil
}

func (s searchAfterStrategyJustDiscardTheParameter) doSomethingWithHitsResult(*model.SearchHits) {
	// no-op
}

// -------------------------------------------------------------------
// | First, simple strategy: BasicAndFast (default for just a second |
// -------------------------------------------------------------------

type searchAfterStrategyBasicAndFast struct{}

func (s searchAfterStrategyBasicAndFast) validateAndParse(query *model.Query, indexSchema schema.Schema) (searchAfterParsed []model.Expr, err error) {
	if query.SearchAfter == nil {
		return nil, nil
	}

	asArray, ok := query.SearchAfter.([]any)
	if !ok {
		return nil, fmt.Errorf("search_after must be an array, got: %v", query.SearchAfter)
	}
	if len(asArray) != len(query.SelectCommand.OrderBy) {
		return nil, fmt.Errorf("len(search_after) != len(sortFields), search_after: %v, sortFields: %v", asArray, query.SelectCommand.OrderBy)
	}

	fmt.Println("searchAfterParsed")
	n := len(asArray)
	searchAfterParsed = make([]model.Expr, n)
	for i, searchAfterValue := range asArray {
		column, ok := query.SelectCommand.OrderBy[i].Expr.(model.ColumnRef)
		if !ok {
			return nil, fmt.Errorf("for basic_and_fast strategy, order by must be a column reference")
		}
		field, resolved := indexSchema.ResolveField(column.ColumnName)
		if !resolved {
			return nil, fmt.Errorf("could not resolve field: %v", model.AsString(query.SelectCommand.OrderBy[i].Expr))
		}
		// TODO: fix error msgs below
		if field.Type.Name == "date" {
			if number, isNumber := util.ExtractNumeric64Maybe(searchAfterValue); isNumber {
				if number >= 0 && util.IsFloat64AnInt64(number) {
					searchAfterParsed[i] = model.NewFunction("fromUnixTimestamp64Milli", model.NewLiteral(int64(number)))
				} else {
					return nil, fmt.Errorf("for basic_and_fast strategy, search_after must be a unix timestamp in milliseconds")
				}
			} else {
				return nil, fmt.Errorf("for basic_and_fast strategy, search_after must be an integer")
			}
		} else {
			searchAfterParsed[i] = model.NewLiteral(searchAfterValue)
		}
	}

	return searchAfterParsed, nil
}

func (s searchAfterStrategyBasicAndFast) transform(query *model.Query, searchAfterParsed []model.Expr) (*model.Query, error) {
	// If all order by's would be DESC, we would add to the where clause:
	// tuple(sortField1, sortField2, ...) > tuple(searchAfter1, searchAfter2, ...)
	// But because some fields might be ASC, we need to swap those values in the tuple.
	fmt.Println("searchAfterParsed", searchAfterParsed)
	tupleLen := len(searchAfterParsed)
	lhs := model.NewTupleExpr(make([]model.Expr, tupleLen))
	rhs := model.NewTupleExpr(make([]model.Expr, tupleLen))
	for i, searchAfterValue := range searchAfterParsed {
		lhs.Exprs[i] = searchAfterValue
		rhs.Exprs[i] = query.SelectCommand.OrderBy[i].Expr
		if query.SelectCommand.OrderBy[i].Direction == model.AscOrder {
			lhs.Exprs[i], rhs.Exprs[i] = rhs.Exprs[i], lhs.Exprs[i] // swap
		}
	}

	newWhereClause := model.NewInfixExpr(lhs, ">", rhs)
	pp.Println("newWhereClause", newWhereClause, query.SelectCommand.WhereClause)
	// logger.Debug().Msgf("search_after_ts: %d, query before: %v", searchAfterTs, AsString(query.SelectCommand))
	query.SelectCommand.WhereClause = model.And([]model.Expr{query.SelectCommand.WhereClause, newWhereClause})
	// logger.Debug().Msgf("query after search_after transformation: %v", AsString(query.SelectCommand))
	return query, nil
}

func (s searchAfterStrategyBasicAndFast) doSomethingWithHitsResult(*model.SearchHits) {
	// no-op
}

func (s *SchemaCheckPass) applySearchAfterParameter(indexSchema schema.Schema, query *model.Query) (*model.Query, error) {
	searchAfterParsed, err := s.searchAfterStrategy.validateAndParse(query, indexSchema)
	if err != nil {
		return nil, err
	}
	if searchAfterParsed == nil {
		return query, nil
	}

	return s.searchAfterStrategy.transform(query, searchAfterParsed)
}
