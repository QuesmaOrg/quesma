package frontend_connectors

import (
	"context"
	"fmt"
	"github.com/QuesmaOrg/quesma/platform/logger"
	"github.com/QuesmaOrg/quesma/platform/model"
	"github.com/QuesmaOrg/quesma/platform/schema"
	"github.com/QuesmaOrg/quesma/platform/util"
	"github.com/k0kubun/pp"
)

func SearchAfterStrategyFactory(strategy model.SearchAfterStrategyType) model.SearchAfterStrategy {
	switch strategy {
	case model.Bulletproof:
		return &searchAfterStrategyBulletproof{}
	case model.JustDiscardTheParameter:
		return &searchAfterStrategyJustDiscardTheParameter{}
	case model.BasicAndFast:
		return &searchAfterStrategyBasicAndFast{}
	default:
		logger.Error().Msgf("Unknown search_after strategy: %d. Using default (basicAndFast).", strategy)
		return &searchAfterStrategyBulletproof{}
	}
}

// ---------------------------------------------------------------------------------
// | Bulletproof, but might be a bit slower for gigantic datasets                  |
// ---------------------------------------------------------------------------------

type searchAfterStrategyBulletproof struct {
	sortParamsRaw           []any
	sortParams              []model.Expr // before ValidateAndParse: nil; after non-error ValidateAndParse: non-nil array
	primaryKeys             []any        // before ValidateAndParse: nil; after non-error ValidateAndParse: non-nil array
	lastNRowsSameSortValues int          // 0 for first row, else >= 1 meaning how many last rows have the same sort values
}

// ValidateAndParse validates the 'searchAfter', which is what came from the request's search_after field.
// add comment
func (s *searchAfterStrategyBulletproof) ValidateAndParse(query *model.Query, indexSchema schema.Schema) error {
	var err error
	s.sortParams, s.primaryKeys, err = validateAndParseCommon(query, indexSchema)
	if err != nil || s.sortParams == nil {
		return err
	}

	s.sortParamsRaw = make([]any, len(s.sortParams))
	for i, param := range query.SearchAfter.([]any)[:len(s.sortParams)] { // we're sure it's []any because of validateAndParseCommon above
		s.sortParamsRaw[i] = param
	}
	pp.Println("parse", s)
	return nil
}

func (s *searchAfterStrategyBulletproof) TransformQuery(query *model.Query) (*model.Query, error) {
	fmt.Println("GOT searchAfterParsed", s.sortParams, s.primaryKeys)
	if s.sortParams == nil {
		return query, nil
	}

	// If all order by's would be DESC, we would add to the where clause:
	// tuple(sortField1, sortField2, ...) > tuple(searchAfter1, searchAfter2, ...)
	// OR (tuple(sortField1, sortField2, ...) == tuple(searchAfter1, searchAfter2, ...)
	//   AND primary_key NOT IN (searchAfterPrimaryKey1, searchAfterPrimaryKey2, ...))
	//
	// Because some fields might be ASC, we need to swap sortField_i with searchAfter_i
	sortFieldsNr := len(s.sortParams)
	fmt.Println("searchAfterParsed", s.sortParams, sortFieldsNr)
	lhs := model.NewTupleExpr(make([]model.Expr, sortFieldsNr)...)
	rhs := model.NewTupleExpr(make([]model.Expr, sortFieldsNr)...)
	for i, sortParam := range s.sortParams {
		lhs.Exprs[i] = sortParam
		rhs.Exprs[i] = query.SelectCommand.OrderBy[i].Expr
		if query.SelectCommand.OrderBy[i].Direction == model.AscOrder {
			lhs.Exprs[i], rhs.Exprs[i] = rhs.Exprs[i], lhs.Exprs[i] // swap
		}
	}

	newWhereClause1 := model.NewInfixExpr(lhs, ">", rhs)
	pkField := query.Schema.GetPrimaryKey()
	if len(s.primaryKeys) == 0 || pkField == nil {
		// It means we have 0 primary keys -> we just imitate basicAndFast strategy
		if len(s.sortParams) > 0 {
			query.SelectCommand.WhereClause = model.And([]model.Expr{query.SelectCommand.WhereClause, newWhereClause1})
		}
		return query, nil
	}

	fmt.Println("pkField", pkField, "searchAfterParsed", s.sortParams, "sortFieldsNr", sortFieldsNr, query.SelectCommand.OrderBy)
	pks := make([]model.Expr, 0, len(s.primaryKeys))
	for _, pk := range s.primaryKeys {
		pks = append(pks, model.NewLiteralSingleQuoteIfString(pk))
	}
	notInTuple := model.NewTupleExpr(pks...)
	newWhereClause2_1 := model.NewInfixExpr(lhs, "=", rhs)
	newWhereClause2_2 := model.NewInfixExpr(model.NewColumnRef(pkField.AsString()), "NOT IN", notInTuple)

	newWhereClauseFull := model.Or([]model.Expr{newWhereClause1, model.And([]model.Expr{newWhereClause2_1, newWhereClause2_2})})
	query.SelectCommand.WhereClause = model.And([]model.Expr{query.SelectCommand.WhereClause, newWhereClauseFull})
	return query, nil
}

func (s *searchAfterStrategyBulletproof) TransformHit(ctx context.Context, hit *model.SearchHit, pkFieldName *string,
	sortFieldNames []string, rows []model.QueryResultRow) *model.SearchHit {

	pp.Println("KK transformHit", s)

	if pkFieldName == nil {
		return hit
	}
	if len(rows) == 0 { // sanity check
		logger.Warn().Msg("searchAfterStrategyBulletproof.TransformHit: rows is empty")
		return hit
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
		return hit
	}

	fmt.Println("pkColIdx", pkColIdx, "pkFieldName", *pkFieldName)

	hit.Sort = append(hit.Sort, rows[len(rows)-1].Cols[pkColIdx].Value)

	fmt.Println("KK transformHit 2, rows len:", len(rows))

	addPreQueryPKs := func() {
		for _, pk := range s.primaryKeys {
			hit.Sort = append(hit.Sort, pk)
		}
	}
	if len(rows) == 1 {
		// here we can't compare the last two rows, but need to compare
		// one row with what we've received in search_after param
		s.lastNRowsSameSortValues = 1
		if s.sortParamsRaw != nil { // TODO add && rows[0].SameSubsetOfColumnsRaw(s.sortParamsRaw, sortFieldNames) {
			// rows[0] has the same sort values
			addPreQueryPKs()
			s.lastNRowsSameSortValues += len(s.primaryKeys)
		}
		pp.Println("AB", s, rows[0], sortFieldNames)
		return hit
	}

	// if current_row != last_row (we check only 'sortFieldNames' columns), we have only one "result" row added above
	if !rows[len(rows)-1].SameSubsetOfColumns(&rows[len(rows)-2], sortFieldNames) {
		fmt.Println("cols different")
		s.lastNRowsSameSortValues = 1
		return hit
	}

	// else we have lastNRowsSameSortValues+1 "result" rows
	for i, cnt := len(rows)-2, 0; i >= 0 && cnt < s.lastNRowsSameSortValues; i, cnt = i-1, cnt+1 {
		fmt.Println("adding", rows[i].Cols[pkColIdx].Value)
		hit.Sort = append(hit.Sort, rows[i].Cols[pkColIdx].Value)
	}
	if len(hit.Sort) <= s.lastNRowsSameSortValues {
		addPreQueryPKs()
	}
	s.lastNRowsSameSortValues += 1
	return hit
}

// -------------------------------------------------------------------------------------------------------------------------------
// | JustDiscardTheParameter: probably only good for tests or when you don't need this functionality and want better performance |
// -------------------------------------------------------------------------------------------------------------------------------

type searchAfterStrategyJustDiscardTheParameter struct{}

func (s *searchAfterStrategyJustDiscardTheParameter) ValidateAndParse(query *model.Query, indexSchema schema.Schema) error {
	return nil
}

func (s *searchAfterStrategyJustDiscardTheParameter) TransformQuery(query *model.Query) (*model.Query, error) {
	return query, nil
}

func (s *searchAfterStrategyJustDiscardTheParameter) TransformHit(ctx context.Context, hit *model.SearchHit, pkFieldName *string,
	sortFieldNames []string, rows []model.QueryResultRow) *model.SearchHit {
	return hit
}

// ----------------------------------------------------------------------------------
// | First, simple strategy: BasicAndFast (default until Bulletproof is implemented |
// ----------------------------------------------------------------------------------

type searchAfterStrategyBasicAndFast struct {
	sortParams []model.Expr // before ValidateAndParse: nil; after non-error ValidateAndParse: non-nil arry
}

// ValidateAndParse validates the 'searchAfter', which is what came from the request's search_after field.
func (s *searchAfterStrategyBasicAndFast) ValidateAndParse(query *model.Query, indexSchema schema.Schema) error {
	sortParams, otherParams, err := validateAndParseCommon(query, indexSchema)
	fmt.Println("validateAndParse", sortParams, otherParams)
	if err != nil {
		return err
	}
	if len(otherParams) != 0 {
		return fmt.Errorf("BasicAndFast should have only sort params. search_after: %v, sortFields: %v", query.SearchAfter, query.SelectCommand.OrderBy)
	}

	s.sortParams = sortParams
	pp.Println("1", s)
	return nil
}

func (s *searchAfterStrategyBasicAndFast) TransformQuery(query *model.Query) (*model.Query, error) {
	fmt.Println("searchAfterParsed", s.sortParams)
	pp.Println(s)
	if len(s.sortParams) == 0 {
		return query, nil
	}

	// If all order by's would be DESC, we would add to the where clause:
	// tuple(sortField1, sortField2, ...) > tuple(searchAfter1, searchAfter2, ...)
	// But because some fields might be ASC, we need to swap sortField_i with searchAfter_i
	sortFieldsNr := len(s.sortParams)
	lhs := model.NewTupleExpr(make([]model.Expr, sortFieldsNr)...)
	rhs := model.NewTupleExpr(make([]model.Expr, sortFieldsNr)...)
	for i, searchAfterValue := range s.sortParams {
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

func (s *searchAfterStrategyBasicAndFast) TransformHit(ctx context.Context, hit *model.SearchHit, pkFieldName *string,
	sortFieldNames []string, rows []model.QueryResultRow) *model.SearchHit {
	return hit
}

func validateAndParseCommon(query *model.Query, indexSchema schema.Schema) (sortParams []model.Expr, otherParams []any, err error) {
	if query.SearchAfter == nil {
		return nil, nil, nil
	}

	asArray, ok := query.SearchAfter.([]any)
	if !ok {
		return nil, nil, fmt.Errorf("search_after must be an array, got: %v", query.SearchAfter)
	}

	sortFieldsNr := len(query.SelectCommand.OrderBy)
	allFieldsNr := len(asArray)
	fmt.Println("all", allFieldsNr, "sort", sortFieldsNr)
	if allFieldsNr < sortFieldsNr {
		return nil, nil, fmt.Errorf("len(search_after) < len(sortFields), search_after: %v, sortFields: %v", asArray, query.SelectCommand.OrderBy)
	}

	sortParams = make([]model.Expr, 0, sortFieldsNr)
	otherParams = make([]any, 0, allFieldsNr-sortFieldsNr)
	for i, searchAfterValue := range asArray {
		if i >= sortFieldsNr {
			otherParams = append(otherParams, searchAfterValue)
			continue
		}

		column, ok := query.SelectCommand.OrderBy[i].Expr.(model.ColumnRef)
		if !ok {
			return nil, nil, fmt.Errorf("for basicAndFast strategy, order by must be a column reference")
		}

		field, resolved := indexSchema.ResolveField(column.ColumnName)
		if !resolved {
			return nil, nil, fmt.Errorf("could not resolve field: %v", model.AsString(query.SelectCommand.OrderBy[i].Expr))
		}

		if field.Type.Name == "date" || field.Type.Name == "timestamp" {
			if number, isNumber := util.ExtractNumeric64Maybe(searchAfterValue); isNumber {
				if number >= 0 && util.IsFloat64AnInt64(number) {
					// this param will always be timestamp in milliseconds, as we create it like this while rendering hits
					sortParams = append(sortParams, model.NewFunction("fromUnixTimestamp64Milli", model.NewLiteral(int64(number))))
				} else {
					return nil, nil, fmt.Errorf("for basicAndFast strategy, search_after must be a unix timestamp in milliseconds")
				}
			} else {
				return nil, nil, fmt.Errorf("for basicAndFast strategy, search_after must be a number")
			}
		} else {
			sortParams = append(sortParams, model.NewLiteral(util.SingleQuoteIfString(searchAfterValue)))
		}
	}

	return sortParams, otherParams, nil
}
