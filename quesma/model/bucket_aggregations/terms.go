// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package bucket_aggregations

import (
	"context"
	"fmt"
	"quesma/logger"
	"quesma/model"
	"quesma/util"
	"quesma/util/regex"
	"reflect"
)

type Terms struct {
	ctx         context.Context
	significant bool // true <=> significant_terms, false <=> terms
	// Either:
	//   - single value: then for strings, it can be a regex.
	//   - array: then field must match exactly one of the values (never a regex)
	// Nil if missing in request.
	include any
	// Either:
	//   - single value: then for strings, it can be a regex.
	//   - array: then field must match exactly one of the values (never a regex)
	// Nil if missing in request.
	exclude any
}

func NewTerms(ctx context.Context, significant bool, include, exclude any) Terms {
	return Terms{ctx: ctx, significant: significant, include: include, exclude: exclude}
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
		return model.JsonMap{}
	}

	var response []model.JsonMap
	for _, row := range rows {
		docCount := query.docCount(row)
		bucket := model.JsonMap{
			"key":       query.key(row),
			"doc_count": docCount,
		}
		if query.significant {
			bucket["score"] = docCount
			bucket["bg_count"] = docCount
		}
		response = append(response, bucket)
	}

	if !query.significant {
		parentCountAsInt, _ := util.ExtractInt64(query.parentCount(rows[0]))
		sumOtherDocCount := int(parentCountAsInt) - query.sumDocCounts(rows)
		return model.JsonMap{
			"sum_other_doc_count":         sumOtherDocCount,
			"doc_count_error_upper_bound": 0,
			"buckets":                     response,
		}
	} else {
		parentDocCount, _ := util.ExtractInt64(query.parentCount(rows[0]))
		return model.JsonMap{
			"buckets":   response,
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

func (query Terms) UpdateFieldForIncludeAndExclude(field model.Expr) (updatedField model.Expr, didWeUpdate bool) {
	// We'll use here everywhere Clickhouse 'if' function: if(condition, then, else)
	// In our case: if(condition that field is not excluded, field, NULL)
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
			exprs = append(exprs, model.NewLiteral(excludeVal))
		}
		return ifOrNull(model.NewInfixExpr(field, "NOT IN", model.NewTupleExpr(exprs...))), true
	case hasExclude:
		switch exclude := query.exclude.(type) {
		case string: // hard case, might be regex
			funcName, patternExpr := regex.ToClickhouseExpr(exclude)
			return ifOrNull(model.NewInfixExpr(field, funcName, patternExpr)), true
		default: // easy case, never regex
			return ifOrNull(model.NewInfixExpr(field, "!=", model.NewLiteral(query.exclude))), true
		}

	default:
		return field, false // TODO implement support for include this in next PR
	}
}

// TODO make part of QueryType interface and implement for all aggregations
// TODO add bad requests to tests
// Doing so will ensure we see 100% of what we're interested in in our logs (now we see ~95%)
func CheckParamsTerms(ctx context.Context, paramsRaw any) error {
	requiredParams := map[string]string{"field": "string"}
	optionalParams := map[string]string{
		"size":                      "float64", // TODO should be int, will be fixed
		"shard_size":                "float64", // TODO should be int, will be fixed
		"order":                     "order",   // TODO add order type
		"min_doc_count":             "float64", // TODO should be int, will be fixed
		"shard_min_doc_count":       "float64", // TODO should be int, will be fixed
		"show_term_doc_count_error": "bool",
		"exclude":                   "not-checking-type-rn-complicated",
		"include":                   "not-checking-type-rn-complicated",
		"collect_mode":              "string",
		"execution_hint":            "string",
		"missing":                   "string",
		"value_type":                "string",
	}
	logIfYouSeeThemParams := []string{
		"shard_size", "min_doc_count", "shard_min_doc_count",
		"show_term_doc_count_error", "collect_mode", "execution_hint", "value_type",
	}

	params, ok := paramsRaw.(model.JsonMap)
	if !ok {
		return fmt.Errorf("params is not a map, but %+v", paramsRaw)
	}

	// check if required are present
	for paramName, paramType := range requiredParams {
		paramVal, exists := params[paramName]
		if !exists {
			return fmt.Errorf("required parameter %s not found in Terms params", paramName)
		}
		if reflect.TypeOf(paramVal).Name() != paramType { // TODO I'll make a small rewrite to not use reflect here
			return fmt.Errorf("required parameter %s is not of type %s, but %T", paramName, paramType, paramVal)
		}
	}

	// check if only required/optional are present
	for paramName := range params {
		if _, isRequired := requiredParams[paramName]; !isRequired {
			wantedType, isOptional := optionalParams[paramName]
			if !isOptional {
				return fmt.Errorf("unexpected parameter %s found in Terms params %v", paramName, params)
			}
			if wantedType == "not-checking-type-rn-complicated" || wantedType == "order" {
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
