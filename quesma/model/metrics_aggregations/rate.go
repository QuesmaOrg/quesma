// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package metrics_aggregations

import (
	"context"
	"fmt"
	"quesma/logger"
	"quesma/model"
	"quesma/util"
	"reflect"
	"strings"
)

type (
	Rate struct {
		ctx        context.Context
		unit       RateUnit
		multiplier float64
	}
	RateUnit int
	RateMode int
)

const (
	second RateUnit = iota
	minute
	hour
	day
	week
	month
	quarter
	year
)
const (
	sum RateMode = iota
	valueCount
)

// NewRate creates a new Rate aggregation, during parsing.
// 'multiplier' is set later, during pancake transformation.
func NewRate(ctx context.Context, unit string) *Rate {
	return &Rate{ctx: ctx, unit: newRateUnit(ctx, unit)}
}

func (query *Rate) AggregationType() model.AggregationType {
	return model.MetricsAggregation
}

func (query *Rate) TranslateSqlResponseToJson(rows []model.QueryResultRow) model.JsonMap {
	if len(rows) != 1 || len(rows[0].Cols) != 1 {
		logger.ErrorWithCtx(query.ctx).Msgf("unexpected number of rows or columns returned for %s: %d, %d.", query.String(), len(rows), len(rows[0].Cols))
		return model.JsonMap{"value": nil}
	}

	parentVal, ok := util.ExtractNumeric64Maybe(rows[0].Cols[0].Value)
	if !ok {
		logger.WarnWithCtx(query.ctx).Msgf("cannot extract numeric value from %v, %T", rows[0].Cols[0], rows[0].Cols[0].Value)
		return model.JsonMap{"value": nil}
	}
	return model.JsonMap{"value": parentVal * query.multiplier}
}

func (query *Rate) CalcAndSetMultiplier(parentIntervalInMs int64) {
	if parentIntervalInMs == 0 {
		logger.ErrorWithCtx(query.ctx).Msgf("parent interval is 0, cannot calculate rate multiplier")
		return
	}

	rateInMs := query.unit.ToMilliseconds(query.ctx)
	// unit month/quarter/year is special, only compatible with month/quarter/year calendar intervals
	if query.unit == month || query.unit == quarter || query.unit == year {
		oneMonthInMs := int64(30 * 24 * 60 * 60 * 1000)
		if parentIntervalInMs < oneMonthInMs {
			logger.WarnWithCtx(query.ctx).Msgf("parent interval (%d ms) is not compatible with rate unit %s", parentIntervalInMs, query.unit)
			return
		}
		if query.unit == year {
			rateInMs = 360 * 24 * 60 * 60 * 1000 // round to 360 days, so year/month = 12, year/quarter = 3, as should be
		}
	}

	if rateInMs%parentIntervalInMs == 0 {
		query.multiplier = float64(rateInMs / parentIntervalInMs)
	} else {
		query.multiplier = float64(rateInMs) / float64(parentIntervalInMs)
	}
}

func (query *Rate) String() string {
	return fmt.Sprintf("rate(unit: %s)", query.unit)
}

func newRateUnit(ctx context.Context, unit string) RateUnit {
	switch strings.ToLower(unit) {
	case "second":
		return second
	case "minute":
		return minute
	case "hour":
		return hour
	case "day":
		return day
	case "week":
		return week
	case "month":
		return month
	case "quarter":
		return quarter
	case "year":
		return year
	default:
		// theoretically unreachable, as this is checked during parsing
		logger.ErrorWithCtx(ctx).Msgf("invalid rate unit: %s", unit)
		return second
	}
}

func (u RateUnit) String() string {
	switch u {
	case second:
		return "second"
	case minute:
		return "minute"
	case hour:
		return "hour"
	case day:
		return "day"
	case week:
		return "week"
	case month:
		return "month"
	case quarter:
		return "quarter"
	case year:
		return "year"
	default:
		// theoretically unreachable
		return "invalid"
	}
}

func (u RateUnit) ToMilliseconds(ctx context.Context) int64 {
	switch u {
	case second:
		return 1000
	case minute:
		return 60 * 1000
	case hour:
		return 60 * 60 * 1000
	case day:
		return 24 * 60 * 60 * 1000
	case week:
		return 7 * 24 * 60 * 60 * 1000
	case month:
		return 30 * 24 * 60 * 60 * 1000
	case quarter:
		return 3 * 30 * 24 * 60 * 60 * 1000
	case year:
		return 365 * 24 * 60 * 60 * 1000
	default:
		logger.ErrorWithCtx(ctx).Msgf("invalid rate unit: %s", u)
		return 0
	}
}

// mode: sum or value_count (sum default)

// TODO make part of QueryType interface and implement for all aggregations
// TODO add bad requests to tests
// Doing so will ensure we see 100% of what we're interested in in our logs (now we see ~95%)
func CheckParamsRate(ctx context.Context, paramsRaw any) error {
	requiredParams := map[string]string{
		"unit": "string",
	}
	optionalParams := map[string]string{
		"field": "string",
		"mode":  "string",
	}

	params, ok := paramsRaw.(model.JsonMap)
	if !ok {
		return fmt.Errorf("params is not a map, but %+v", paramsRaw)
	}

	// check if required are present
	for paramName, paramType := range requiredParams {
		paramVal, exists := params[paramName]
		if !exists {
			return fmt.Errorf("required parameter %s not found in params", paramName)
		}
		if reflect.TypeOf(paramVal).Name() != paramType { // TODO I'll make a small rewrite to not use reflect here
			return fmt.Errorf("required parameter %s is not of type %s, but %T", paramName, paramType, paramVal)
		}
	}
	// TODO additional check for unit

	// check if only required/optional are present
	for paramName := range params {
		if _, isRequired := requiredParams[paramName]; !isRequired {
			wantedType, isOptional := optionalParams[paramName]
			if !isOptional {
				return fmt.Errorf("unexpected parameter %s found in IP Range params %v", paramName, params)
			}
			if reflect.TypeOf(params[paramName]).Name() != wantedType { // TODO I'll make a small rewrite to not use reflect here
				return fmt.Errorf("optional parameter %s is not of type %s, but %T", paramName, wantedType, params[paramName])
			}
		}
	}
	// TODO additional check for field (resolve) + mode (one of 2 values)

	return nil
}
