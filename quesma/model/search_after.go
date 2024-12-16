// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package model

import (
	"fmt"
	"quesma/logger"
	"quesma/util"
)

// TODO I'll think how to structure this file (ideally each strategy in separate file) and in which module to put it
// after I finish the implementation of most complex strategy. It'll save me some thinking for now.

type (
	SearchAfterStrategy interface {
		// Validate validates the 'searchAfter', which is what came from the request's search_after field.
		Validate(searchAfter any) error
		ApplyStrategyAndTransformQuery(query *Query, searchAfter any) *Query
		DoSomethingWithHitsResult(hitsResult *SearchHits) // TODO change name
	}
	SearchAfterStrategyType int
)

func SearchAfterStrategyFactory(strategy SearchAfterStrategyType, timestampField ColumnRef) SearchAfterStrategy {
	switch strategy {
	case Bulletproof:
		return SearchAfterStrategyBulletproof{timestampField}
	case JustDiscardTheParameter:
		return SearchAfterStrategyJustDiscardTheParameter{}
	case BasicAndFast:
		return SearchAfterStrategyBasicAndFast{timestampField}
	default:
		logger.Error().Msgf("Unknown search_after strategy: %d. Using Bulletproof.", strategy)
		return SearchAfterStrategyBulletproof{timestampField}
	}
}

const (
	BasicAndFast SearchAfterStrategyType = iota // default for a second
	Bulletproof
	JustDiscardTheParameter
	emptySearchAfterTs = int64(-1)
)

func (s SearchAfterStrategyType) String() string {
	return []string{"BasicAndFast", "Bulletproof", "JustDiscardTheParameter"}[s]
}

// ---------------------------------------------------------------------------------
// | Bulletproof, but might be a bit slower for gigantic datasets                    |
// ---------------------------------------------------------------------------------

type (
	SearchAfterStrategyBulletproof struct {
		timestampField ColumnRef
	}
	searchAfterParsedBulletproof struct {
		timestampMs int64
		pkHashes    []string // md5 for now, should be improved to shorten hashes lengths
	}
)

// Validate validates the 'searchAfter', which is what came from the request's search_after field.
func (s SearchAfterStrategyBulletproof) Validate(searchAfter any) error {
	logger.Debug().Msgf("searchAfter: %v", searchAfter)
	_, err := s.validateAndParse(searchAfter)
	return err
}

func (s SearchAfterStrategyBulletproof) validateAndParse(searchAfter any) (searchAfterParsedBulletproof, error) {
	empty := searchAfterParsedBulletproof{timestampMs: emptySearchAfterTs}
	logger.Debug().Msgf("searchAfter: %v", searchAfter)
	if searchAfter == nil {
		return empty, nil
	}

	asArray, ok := searchAfter.([]any)
	if !ok {
		return empty, fmt.Errorf("search_after must be an array")
	}
	if len(asArray) == 0 {
		return empty, fmt.Errorf("for Bulletproof strategy, search_after must have at most one element")
	}

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
}

func (s SearchAfterStrategyBulletproof) ApplyStrategyAndTransformQuery(query *Query, searchAfterRaw any) *Query {
	searchAfter, _ := s.validateAndParse(searchAfterRaw) // we validate during parsing and error there. No need to check here.
	if searchAfter.timestampMs == emptySearchAfterTs {
		return query
	}
	timestampRangeClause := NewInfixExpr(s.timestampField, "<=", NewFunction("fromUnixTimestamp64Milli", NewLiteral(searchAfter.timestampMs))) // TODO fix this for Hydrolix...
	logger.Debug().Msgf("search_after: %v, query before: %v", searchAfter, AsString(query.SelectCommand))
	query.SelectCommand.WhereClause = And([]Expr{query.SelectCommand.WhereClause, timestampRangeClause})
	query.SelectCommand.Limit += len(searchAfter.pkHashes)
	logger.Debug().Msgf("query after: %v", AsString(query.SelectCommand))
	return query
}

func (s SearchAfterStrategyBulletproof) DoSomethingWithHitsResult(*SearchHits) {
	// no-op
}

// -------------------------------------------------------------------------------------------------------------------------------
// | JustDiscardTheParameter: probably only good for tests or when you don't need this functionality and want better performance |
// -------------------------------------------------------------------------------------------------------------------------------

type SearchAfterStrategyJustDiscardTheParameter struct{}

func (s SearchAfterStrategyJustDiscardTheParameter) Validate(any) error {
	return nil
}

func (s SearchAfterStrategyJustDiscardTheParameter) ApplyStrategyAndTransformQuery(query *Query, _ any) *Query {
	return query
}

func (s SearchAfterStrategyJustDiscardTheParameter) DoSomethingWithHitsResult(*SearchHits) {
	// no-op
}

// -------------------------------------------------------------------
// | First, simple strategy: BasicAndFast (default for just a second |
// -------------------------------------------------------------------

type SearchAfterStrategyBasicAndFast struct {
	timestampField ColumnRef
}

func (s SearchAfterStrategyBasicAndFast) Validate(searchAfter any) error {
	_, err := s.validateAndParse(searchAfter)
	return err
}

func (s SearchAfterStrategyBasicAndFast) validateAndParse(searchAfter any) (timestampMs int64, err error) {
	if searchAfter == nil {
		return emptySearchAfterTs, nil
	}

	asArray, ok := searchAfter.([]any)
	if !ok {
		return emptySearchAfterTs, fmt.Errorf("search_after must be an array")
	}

	if len(asArray) != 1 {
		return emptySearchAfterTs, fmt.Errorf("for basic_and_fast strategy, search_after must have exactly one element")
	}
	if shouldBeTimestamp, ok := util.ExtractNumeric64Maybe(asArray[0]); ok {
		if shouldBeTimestamp >= 0 && util.IsFloat64AnInt64(shouldBeTimestamp) {
			return int64(shouldBeTimestamp), nil
		}
		return emptySearchAfterTs, fmt.Errorf("for basic_and_fast strategy, search_after must be a unix timestamp in milliseconds")
	}
	return emptySearchAfterTs, fmt.Errorf("for basic_and_fast strategy, search_after must be an integer")
}

func (s SearchAfterStrategyBasicAndFast) ApplyStrategyAndTransformQuery(query *Query, searchAfterRaw any) *Query {
	searchAfterTs, _ := s.validateAndParse(searchAfterRaw) // we validate during parsing and error there. No need to check here.
	if searchAfterTs == emptySearchAfterTs {
		return query
	}
	timestampRangeClause := NewInfixExpr(s.timestampField, "<", NewFunction("fromUnixTimestamp64Milli", NewLiteral(searchAfterTs)))
	// logger.Debug().Msgf("search_after_ts: %d, query before: %v", searchAfterTs, AsString(query.SelectCommand))
	query.SelectCommand.WhereClause = And([]Expr{query.SelectCommand.WhereClause, timestampRangeClause})
	// logger.Debug().Msgf("query after search_after transformation: %v", AsString(query.SelectCommand))
	return query
}

func (s SearchAfterStrategyBasicAndFast) DoSomethingWithHitsResult(*SearchHits) {
	// no-op
}
