// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package bucket_aggregations

import (
	"context"
	"fmt"
	"github.com/QuesmaOrg/quesma/platform/logger"
	"github.com/QuesmaOrg/quesma/platform/model"
	"github.com/QuesmaOrg/quesma/platform/util"
	"github.com/QuesmaOrg/quesma/platform/util/regex"
	"reflect"
)

// TODO when adding include/exclude, check escaping of ' and \ in those fields
type Terms struct {
	ctx         context.Context
	significant bool // true <=> significant_terms, false <=> terms
	minDocCount int
	// include is either:
	//   - single value: then for strings, it can be a regex.
	//   - array: then field must match exactly one of the values (never a regex)
	// Nil if missing in request.
	include any
	// exclude is either:
	//   - single value: then for strings, it can be a regex.
	//   - array: then field must match exactly one of the values (never a regex)
	// Nil if missing in request.
	exclude any
}

func NewTerms(ctx context.Context, significant bool, minDocCount int, include, exclude any) Terms {
	return Terms{ctx: ctx, significant: significant, minDocCount: minDocCount, include: include, exclude: exclude}
}

func (query Terms) AggregationType() model.AggregationType {
	return model.BucketAggregation
}

func (query Terms) TranslateSqlResponseToJson(rows []model.QueryResultRow) model.JsonMap {
	if len(rows) > 0 && len(rows[0].Cols) < 2 {
		logger.ErrorWithCtx(query.ctx).Msgf(
			"unexpected number of columns in terms aggregation response, len: %d, rows[0]: %v", len(rows[0].Cols), rows[0])
	}
	if len(rows) == 0 {
		return model.JsonMap{"buckets": []model.JsonMap{}}
	}

	if query.minDocCount > 1 {
		rows = query.NewRowsTransformer().Transform(query.ctx, rows)
	}

	buckets := make([]model.JsonMap, 0, len(rows))
	for _, row := range rows {
		docCount := query.docCount(row)
		bucket := model.JsonMap{
			"doc_count": docCount,
		}
		if query.significant {
			bucket["score"] = docCount
			bucket["bg_count"] = docCount
		}

		// response for bool keys is different
		key := query.key(row)
		if boolPtr, isBoolPtr := key.(*bool); isBoolPtr {
			key = *boolPtr
		}
		if keyAsBool, ok := key.(bool); ok {
			bucket["key"] = util.BoolToInt(keyAsBool)
			bucket["key_as_string"] = util.BoolToString(keyAsBool)
		} else {
			bucket["key"] = key
		}

		buckets = append(buckets, bucket)
	}

	if !query.significant {
		parentCountAsInt, _ := util.ExtractInt64(query.parentCount(rows[0]))
		sumOtherDocCount := int(parentCountAsInt) - query.sumDocCounts(rows)
		return model.JsonMap{
			"sum_other_doc_count":         sumOtherDocCount,
			"doc_count_error_upper_bound": 0,
			"buckets":                     buckets,
		}
	} else {
		parentDocCount, _ := util.ExtractInt64(query.parentCount(rows[0]))
		return model.JsonMap{
			"buckets":   buckets,
			"doc_count": parentDocCount,
			"bg_count":  parentDocCount,
		}
	}
}

func (query Terms) String() string {
	if !query.significant {
		return "terms"
	}
	return "significant_terms"
}

func (query Terms) IsSignificant() bool {
	return query.significant
}

func (query Terms) sumDocCounts(rows []model.QueryResultRow) int {
	sum := 0
	if len(rows) > 0 {
		switch query.docCount(rows[0]).(type) {
		case int64:
			for _, row := range rows {
				sum += int(query.docCount(row).(int64))
			}
		case uint64:
			for _, row := range rows {
				sum += int(query.docCount(row).(uint64))
			}
		default:
			logger.WarnWithCtx(query.ctx).Msgf("unknown type for terms doc_count: %T, value: %v",
				query.docCount(rows[0]), query.docCount(rows[0]))
		}
	}
	return sum
}

func (query Terms) docCount(row model.QueryResultRow) any {
	return row.Cols[len(row.Cols)-1].Value
}

func (query Terms) key(row model.QueryResultRow) any {
	return row.Cols[len(row.Cols)-2].Value
}

func (query Terms) parentCount(row model.QueryResultRow) any {
	return row.Cols[len(row.Cols)-3].Value
}

func (query Terms) UpdateFieldForIncludeAndExclude(field model.Expr) (updatedField model.Expr, didWeUpdateField bool) {
	// We'll use here everywhere Clickhouse 'if' function: if(condition, then, else)
	// In our case field becomes: if(condition that field is not excluded, field, NULL)
	ifOrNull := func(condition model.Expr) model.FunctionExpr {
		return model.NewFunction("if", condition, field, model.NullExpr)
	}

	hasExclude := query.exclude != nil
	excludeArr, excludeIsArray := query.exclude.([]any)
	switch {
	case hasExclude && excludeIsArray:
		if len(excludeArr) == 0 {
			return field, false
		}

		// Select expr will be: if(field NOT IN (excludeArr[0], excludeArr[1], ...), field, NULL)
		exprs := make([]model.Expr, 0, len(excludeArr))
		for _, excludeVal := range excludeArr {
			exprs = append(exprs, model.NewLiteralSingleQuoteString(excludeVal))
		}
		return ifOrNull(model.NewInfixExpr(field, "NOT IN", model.NewTupleExpr(exprs...))), true
	case hasExclude:
		switch exclude := query.exclude.(type) {
		case string: // hard case, might be regex
			funcName, patternExpr := regex.ToClickhouseExpr(exclude)
			return ifOrNull(model.NewInfixExpr(field, "NOT "+funcName, patternExpr)), true
		default: // easy case, never regex
			return ifOrNull(model.NewInfixExpr(field, "!=", model.NewLiteral(query.exclude))), true
		}

	default:
		return field, false // TODO implement similar support for 'include' in next PR
	}
}

// TODO make part of QueryType interface and implement for all aggregations
// TODO add bad requests to tests
// Doing so will ensure we see 100% of what we're interested in in our logs (now we see ~95%)
func CheckParamsTerms(ctx context.Context, paramsRaw any) error {
	eitherRequired := map[string]string{"field": "string", "script": "map"}
	optionalParams := map[string]string{
		"size":                      "float64|string", // TODO should be int|string, will be fixed
		"shard_size":                "float64",        // TODO should be int, will be fixed
		"order":                     "order",          // TODO add order type
		"min_doc_count":             "float64",        // TODO should be int, will be fixed
		"shard_min_doc_count":       "float64",        // TODO should be int, will be fixed
		"show_term_doc_count_error": "bool",
		"exclude":                   "not-checking-type-now-complicated",
		"include":                   "not-checking-type-now-complicated",
		"collect_mode":              "string",
		"execution_hint":            "string",
		"missing":                   "string",
		"value_type":                "string",
	}
	logIfYouSeeThemParams := []string{
		"shard_size", "shard_min_doc_count", "show_term_doc_count_error",
		"collect_mode", "execution_hint", "value_type",
	}

	params, ok := paramsRaw.(model.JsonMap)
	if !ok {
		return fmt.Errorf("params is not a map, but %+v", paramsRaw)
	}

	// check if required are present
	nrOfRequired := 0
	for paramName := range eitherRequired {
		if _, exists := params[paramName]; exists {
			nrOfRequired++
		}
	}
	if nrOfRequired != 1 {
		return fmt.Errorf("expected exactly one of %v in Terms params %v", eitherRequired, params)
	}
	if field, exists := params["field"]; exists {
		if _, isString := field.(string); !isString {
			return fmt.Errorf("field is not a string, but %T", field)
		}
	} else {
		_, hasInclude := params["include"]
		_, hasExclude := params["exclude"]
		_, hasMissing := params["missing"]
		if hasInclude || hasExclude || hasMissing {
			return fmt.Errorf("field is missing, but include/exclude/missing are present in Terms params %v", params)
		}
		// TODO check script's type as well
	}

	// check if only required/optional are present
	for paramName := range params {
		if _, isRequired := eitherRequired[paramName]; !isRequired {
			wantedType, isOptional := optionalParams[paramName]
			if !isOptional {
				return fmt.Errorf("unexpected parameter %s found in Terms params %v", paramName, params)
			}
			if wantedType == "not-checking-type-now-complicated" || wantedType == "order" || wantedType == "float64|string" {
				continue // TODO: add that later
			}
			if reflect.TypeOf(params[paramName]).Name() != wantedType { // TODO I'll make a small rewrite to not use reflect here
				return fmt.Errorf("optional parameter %s is not of type %s, but %T", paramName, wantedType, params[paramName])
			}
		}
	}

	// log if you see them
	for _, warnParam := range logIfYouSeeThemParams {
		if _, exists := params[warnParam]; exists {
			logger.WarnWithCtxAndThrottling(ctx, "terms", warnParam, "we didn't expect %s in Terms params %v", warnParam, params)
		}
	}

	return nil
}

func (query Terms) NewRowsTransformer() model.QueryRowsTransformer {
	return &TermsRowsTransformer{minDocCount: int64(query.minDocCount)}
}

type TermsRowsTransformer struct {
	minDocCount int64
}

func (qt TermsRowsTransformer) Transform(ctx context.Context, rowsFromDB []model.QueryResultRow) []model.QueryResultRow {
	postprocessedRows := make([]model.QueryResultRow, 0, len(rowsFromDB))
	for _, row := range rowsFromDB {
		docCount, err := util.ExtractInt64(row.LastColValue())
		if err != nil {
			logger.ErrorWithCtx(ctx).Msgf("unexpected type for terms doc_count: %T, value: %v. Returning empty rows.",
				row.LastColValue(), row.LastColValue())
			return []model.QueryResultRow{}
		}
		if docCount >= qt.minDocCount {
			postprocessedRows = append(postprocessedRows, row)
		}
	}
	return postprocessedRows
}
