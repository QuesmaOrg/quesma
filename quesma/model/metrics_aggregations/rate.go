// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package metrics_aggregations

import (
	"context"
	"fmt"
	"github.com/QuesmaOrg/quesma/quesma/logger"
	"github.com/QuesmaOrg/quesma/quesma/model"
	"github.com/QuesmaOrg/quesma/quesma/util"
	"github.com/k0kubun/pp"
	"reflect"
	"strings"
	"time"
)

type (
	Rate struct {
		ctx                context.Context
		unit               RateUnit
		multiplier         float64
		parentIntervalInMs int64
		fieldPresent       bool
	}
	RateUnit int
	RateMode string
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
	invalidRateUnit
)

const (
	RateModeSum        RateMode = "sum"
	RateModeValueCount RateMode = "value_count"
	RateModeInvalid    RateMode = "invalid"
)

// NewRate creates a new Rate aggregation, during parsing.
// 'multiplier' and 'parentIntervalInMs' are set later, during pancake transformation.
func NewRate(ctx context.Context, unit string, fieldPresent bool) *Rate {
	return &Rate{ctx: ctx, unit: newRateUnit(ctx, unit), fieldPresent: fieldPresent}
}

func (query *Rate) AggregationType() model.AggregationType {
	return model.MetricsAggregation
}

func (query *Rate) TranslateSqlResponseToJson(rows []model.QueryResultRow) model.JsonMap {
	fmt.Println(rows)
	// rows[0] is either: val (1 column)
	// or parent date_histogram's key, val (2 columns)
	if len(rows) != 1 || (len(rows[0].Cols) != 1 && len(rows[0].Cols) != 2) {
		logger.ErrorWithCtx(query.ctx).Msgf("unexpected number of rows or columns returned for %s: %+v.", query.String(), rows)
		return model.JsonMap{"value": nil}
	}

	fmt.Println(rows)
	parentVal, ok := util.ExtractNumeric64Maybe(rows[0].LastColValue())
	if !ok {
		logger.WarnWithCtx(query.ctx).Msgf("cannot extract numeric value from %v, %T", rows[0].Cols[0], rows[0].Cols[0].Value)
		return model.JsonMap{"value": nil}
	}

	fix := 1.0
	thirtyDaysInMs := int64(30 * 24 * 60 * 60 * 1000)
	pp.Println(query)
	//
	needToCountDaysNr := query.parentIntervalInMs%thirtyDaysInMs == 0 &&
		(query.unit == second || query.unit == minute || query.unit == hour || query.unit == day || query.unit == week)
	weHaveParentDateHistogramKey := len(rows[0].Cols) == 2
	if needToCountDaysNr && weHaveParentDateHistogramKey {
		// we need to count days of every month, as it can be 28, 29, 30 or 31...
		// for our average to be correct (in Elastic it always is)
		fmt.Println("parentIntervalInMs", query.parentIntervalInMs/thirtyDaysInMs)
		someTime := time.UnixMilli(rows[0].Cols[0].Value.(int64)).Add(48 * time.Hour)
		fmt.Println("someTime1", someTime)
		// someTime.Day() is in [28, 31] U {1}. I want it to be 2, so I'm sure I'm in the right month for all timezones.
		for someTime.Day() == 1 || someTime.Day() > 25 {
			someTime = someTime.Add(24 * time.Hour)
		}
		fmt.Println("someTime2", someTime)
		actualDays := 0
		currentDays := query.parentIntervalInMs / thirtyDaysInMs * 30 // e.g. 90 for 3 months date_histogram
		currentDaysConst := currentDays
		for currentDays > 0 {
			actualDays += util.DaysInMonth(someTime)
			currentDays -= 30
			someTime = someTime.AddDate(0, -1, 0)
		}
		fix = float64(currentDaysConst) / float64(actualDays)
		fmt.Println("actualDays", actualDays, "currentDays", currentDays, "fix", fix)
	}
	fmt.Println(query.multiplier)

	return model.JsonMap{"value": fix * parentVal * query.multiplier}
}

func (query *Rate) CalcAndSetMultiplier(parentIntervalInMs int64) {
	query.parentIntervalInMs = parentIntervalInMs
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

func (query *Rate) FieldPresent() bool {
	return query.fieldPresent
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
		logger.WarnWithCtxAndThrottling(ctx, "rate", "unit", "invalid rate unit: %s", unit)
		return invalidRateUnit
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
	case invalidRateUnit:
		return "invalid"
	default:
		// theoretically unreachable
		return "invalid, but not invalidRateUnit"
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

func NewRateMode(ctx context.Context, mode string) RateMode {
	switch mode {
	case "sum", "":
		return RateModeSum
	case "value_count":
		return RateModeValueCount
	default:
		logger.WarnWithCtxAndThrottling(ctx, "rate", "mode", "invalid rate mode: %s", mode)
		return RateModeInvalid
	}
}

func (m RateMode) String() string {
	switch m {
	case RateModeSum:
		return "sum"
	case RateModeValueCount:
		return "value_count"
	case RateModeInvalid:
		return "invalid"
	default:
		return "invalid, but not RateModeInvalid"
	}
}

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
	if newRateUnit(ctx, params["unit"].(string)) == invalidRateUnit {
		return fmt.Errorf("invalid rate unit: %v", params["unit"])
	}

	// check if only required/optional are present, and if present - that they have correct types
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
	if mode, exists := params["mode"]; exists && NewRateMode(ctx, mode.(string)) == RateModeInvalid {
		return fmt.Errorf("invalid rate mode: %v", params["mode"])
	}

	return nil
}
