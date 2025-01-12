// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package quesma

import (
	"context"
	"fmt"
	"quesma/logger"
	"quesma/model"
	"quesma/schema"
	"quesma/util"
)

func searchAfterStrategyFactory(strategy model.SearchAfterStrategyType) model.SearchAfterStrategy {
	switch strategy {
	case model.Bulletproof:
		return searchAfterStrategyBulletproof{}
	case model.JustDiscardTheParameter:
		return searchAfterStrategyJustDiscardTheParameter{}
	case model.BasicAndFast:
		return searchAfterStrategyBasicAndFast{}
	default:
		logger.Error().Msgf("Unknown search_after strategy: %d. Using default (basicAndFast).", strategy)
		return searchAfterStrategyBasicAndFast{}
	}
}

// ---------------------------------------------------------------------------------
// | Bulletproof, but might be a bit slower for gigantic datasets                  |
// ---------------------------------------------------------------------------------

// sortFields  []model.OrderByExpr
//	pkHashes    []string // md5 for now, should be improved to shorten hashes lengths
//	searchAfter any

type searchAfterStrategyBulletproof struct {
}

// ValidateAndParse validates the 'searchAfter', which is what came from the request's search_after field.
func (s searchAfterStrategyBulletproof) ValidateAndParse(query *model.Query, indexSchema schema.Schema) (searchAfterParamsParsed []model.Expr, err error) {
	sortParamsParsed, err := validateAndParseCommonOnlySortParams(query, indexSchema)
	if err != nil {
		return sortParamsParsed, err
	}

	searchAfter, isArr := query.SearchAfter.([]any)
	if !isArr {
		return make([]model.Expr, 0), nil
	}

	searchAfterParamsParsed = make([]model.Expr, 0, len(searchAfter))
	searchAfterParamsParsed = append(searchAfterParamsParsed, sortParamsParsed...)
	sortParamsNr := len(sortParamsParsed)
	for i := sortParamsNr; i < len(searchAfter); i++ {
		searchAfterParamsParsed = append(searchAfterParamsParsed, model.NewLiteral(util.SingleQuoteIfString(searchAfter[i])))
	}

	return searchAfterParamsParsed, nil
}

func (s searchAfterStrategyBulletproof) TransformQuery(query *model.Query, searchAfterParsed []model.Expr) (*model.Query, error) {
	// If all order by's would be DESC, we would add to the where clause:
	// tuple(sortField1, sortField2, ...) > tuple(searchAfter1, searchAfter2, ...)
	// OR (tuple(sortField1, sortField2, ...) == tuple(searchAfter1, searchAfter2, ...)
	//   AND primary_key NOT IN (searchAfterPrimaryKey1, searchAfterPrimaryKey2, ...))

	// Because some fields might be ASC, we need to swap sortField_i with searchAfter_i
	sortFieldsNr := len(query.SelectCommand.OrderBy)
	fmt.Println("searchAfterParsed", searchAfterParsed, sortFieldsNr)
	lhs := model.NewTupleExpr(make([]model.Expr, sortFieldsNr)...)
	rhs := model.NewTupleExpr(make([]model.Expr, sortFieldsNr)...)
	for i, searchAfterValue := range searchAfterParsed {
		lhs.Exprs[i] = searchAfterValue
		rhs.Exprs[i] = query.SelectCommand.OrderBy[i].Expr
		if query.SelectCommand.OrderBy[i].Direction == model.AscOrder {
			lhs.Exprs[i], rhs.Exprs[i] = rhs.Exprs[i], lhs.Exprs[i] // swap
		}
	}

	newWhereClause1 := model.NewInfixExpr(lhs, ">", rhs)
	pkField := query.Schema.GetPrimaryKey()
	if len(searchAfterParsed) != sortFieldsNr || pkField == nil {
		// It means we have 0 primary keys -> we just imitate basicAndFast strategy
		if len(searchAfterParsed) != 0 {
			query.SelectCommand.WhereClause = model.And([]model.Expr{query.SelectCommand.WhereClause, newWhereClause1})
		}
		return query, nil
	}

	fmt.Println("pkField", pkField, "searchAfterParsed", searchAfterParsed, "sortFieldsNr", sortFieldsNr, query.SelectCommand.OrderBy)
	notInTuple := model.NewTupleExpr(searchAfterParsed[sortFieldsNr:]...)
	newWhereClause2_1 := model.NewInfixExpr(lhs, "=", rhs)
	newWhereClause2_2 := model.NewInfixExpr(model.NewColumnRef(pkField.AsString()), "NOT IN", notInTuple)

	newWhereClauseFull := model.Or([]model.Expr{newWhereClause1, model.And([]model.Expr{newWhereClause2_1, newWhereClause2_2})})
	query.SelectCommand.WhereClause = model.And([]model.Expr{query.SelectCommand.WhereClause, newWhereClauseFull})
	return query, nil
}

func (s searchAfterStrategyBulletproof) TransformHit(ctx context.Context, hit *model.SearchHit, pkFieldName *string, sortFieldNames []string,
	rows []model.QueryResultRow, lastNRowsSameSortValues int) (hitTransformed *model.SearchHit, lastNRowsSameSortValuesNew int) {

	hitTransformed, lastNRowsSameSortValuesNew = hit, 1 // default values, when returning early
	if pkFieldName == nil {
		return
	}
	if len(rows) == 0 { // sanity check
		logger.Warn().Msg("searchAfterStrategyBulletproof.TransformHit: rows is empty")
		return
	}

	// find the primary key column
	pkColIdx := -1
	for i, col := range rows[0].Cols {
		if col.ColName == *pkFieldName {
			pkColIdx = i
			break
		}
	}
	if pkColIdx == -1 {
		logger.Warn().Msgf("searchAfterStrategyBulletproof.TransformHit: primary key column %s not found in rows", *pkFieldName)
		return
	}

	hitTransformed.Sort = append(hitTransformed.Sort, rows[len(rows)-1].Cols[pkColIdx].Value)

	// if current_row != last_row (we check only 'sortFieldNames' columns), we have only one "result" row added above
	if len(rows) == 1 || rows[len(rows)-1].SameSubsetOfColumns(&rows[len(rows)-2], sortFieldNames) {
		return hitTransformed, 1
	}

	// else we have lastNRowsSameSortValues+1 "result" rows
	for i, cnt := len(rows)-2, 0; cnt < lastNRowsSameSortValues; i, cnt = i-1, cnt+1 {
		hit.Sort = append(hit.Sort, rows[i].Cols[pkColIdx].Value)
	}
	return hitTransformed, lastNRowsSameSortValues + 1
}

// -------------------------------------------------------------------------------------------------------------------------------
// | JustDiscardTheParameter: probably only good for tests or when you don't need this functionality and want better performance |
// -------------------------------------------------------------------------------------------------------------------------------

type searchAfterStrategyJustDiscardTheParameter struct{}

// ValidateAndParse validates the 'searchAfter', which is what came from the request's search_after field.
func (s searchAfterStrategyJustDiscardTheParameter) ValidateAndParse(*model.Query, schema.Schema) (searchAfterParamParsed []model.Expr, err error) {
	return nil, nil
}

func (s searchAfterStrategyJustDiscardTheParameter) TransformQuery(query *model.Query, _ []model.Expr) (*model.Query, error) {
	return query, nil
}

func (s searchAfterStrategyJustDiscardTheParameter) TransformHit(ctx context.Context, hit *model.SearchHit, pkFieldName *string, sortFieldNames []string,
	rows []model.QueryResultRow, lastNRowsSameSortValues int) (hitTransformed *model.SearchHit, lastNRowsSameSortValuesNew int) {
	return hit, 0 // lastNRowsSameSortValues doesn't matter
}

// ----------------------------------------------------------------------------------
// | First, simple strategy: BasicAndFast (default until Bulletproof is implemented |
// ----------------------------------------------------------------------------------

type searchAfterStrategyBasicAndFast struct{}

// ValidateAndParse validates the 'searchAfter', which is what came from the request's search_after field.
func (s searchAfterStrategyBasicAndFast) ValidateAndParse(query *model.Query, indexSchema schema.Schema) (searchAfterParamParsed []model.Expr, err error) {
	return validateAndParseCommonOnlySortParams(query, indexSchema)
}

func (s searchAfterStrategyBasicAndFast) TransformQuery(query *model.Query, searchAfterParsed []model.Expr) (*model.Query, error) {
	// If all order by's would be DESC, we would add to the where clause:
	// tuple(sortField1, sortField2, ...) > tuple(searchAfter1, searchAfter2, ...)
	// But because some fields might be ASC, we need to swap sortField_i with searchAfter_i
	fmt.Println("searchAfterParsed", searchAfterParsed)
	sortFieldsNr := len(searchAfterParsed)
	lhs := model.NewTupleExpr(make([]model.Expr, sortFieldsNr)...)
	rhs := model.NewTupleExpr(make([]model.Expr, sortFieldsNr)...)
	for i, searchAfterValue := range searchAfterParsed {
		lhs.Exprs[i] = searchAfterValue
		rhs.Exprs[i] = query.SelectCommand.OrderBy[i].Expr
		if query.SelectCommand.OrderBy[i].Direction == model.AscOrder {
			lhs.Exprs[i], rhs.Exprs[i] = rhs.Exprs[i], lhs.Exprs[i] // swap
		}
	}

	newWhereClause := model.NewInfixExpr(lhs, ">", rhs)
	query.SelectCommand.WhereClause = model.And([]model.Expr{query.SelectCommand.WhereClause, newWhereClause})
	return query, nil
}

func (s searchAfterStrategyBasicAndFast) TransformHit(ctx context.Context, hit *model.SearchHit, pkFieldName *string, sortFieldNames []string,
	rows []model.QueryResultRow, lastNRowsSameSortValues int) (hitTransformed *model.SearchHit, lastNRowsSameSortValuesNew int) {
	return hit, 0 // lastNRowsSameSortValues doesn't matter
}

func validateAndParseCommonOnlySortParams(query *model.Query, indexSchema schema.Schema) (searchAfterParamParsed []model.Expr, err error) {
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

	sortFieldsNr := len(asArray)
	searchAfterParamParsed = make([]model.Expr, sortFieldsNr)
	for i, searchAfterValue := range asArray {
		column, ok := query.SelectCommand.OrderBy[i].Expr.(model.ColumnRef)
		if !ok {
			return nil, fmt.Errorf("for basicAndFast strategy, order by must be a column reference")
		}

		field, resolved := indexSchema.ResolveField(column.ColumnName)
		if !resolved {
			return nil, fmt.Errorf("could not resolve field: %v", model.AsString(query.SelectCommand.OrderBy[i].Expr))
		}

		if field.Type.Name == "date" || field.Type.Name == "timestamp" {
			if number, isNumber := util.ExtractNumeric64Maybe(searchAfterValue); isNumber {
				if number >= 0 && util.IsFloat64AnInt64(number) {
					// this param will always be timestamp in milliseconds, as we create it like this while rendering hits
					searchAfterParamParsed[i] = model.NewFunction("fromUnixTimestamp64Milli", model.NewLiteral(int64(number)))
				} else {
					return nil, fmt.Errorf("for basicAndFast strategy, search_after must be a unix timestamp in milliseconds")
				}
			} else {
				return nil, fmt.Errorf("for basicAndFast strategy, search_after must be a number")
			}
		} else {
			searchAfterParamParsed[i] = model.NewLiteral(util.SingleQuoteIfString(searchAfterValue))
		}
	}

	return searchAfterParamParsed, nil
}

func (s *SchemaCheckPass) applySearchAfterParameter(indexSchema schema.Schema, query *model.Query) (*model.Query, error) {
	searchAfterParsed, err := s.searchAfterStrategy.ValidateAndParse(query, indexSchema)
	if err != nil {
		return nil, err
	}
	if searchAfterParsed == nil {
		return query, nil
	}

	return s.searchAfterStrategy.TransformQuery(query, searchAfterParsed)
}
