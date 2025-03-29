// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package metrics_aggregations

import (
	"context"
	"fmt"
	"github.com/QuesmaOrg/quesma/platform/logger"
	"github.com/QuesmaOrg/quesma/platform/model"
	"github.com/QuesmaOrg/quesma/platform/util"
	"reflect"
	"strings"
	"time"
)

type (
	Rate struct {
		ctx            context.Context
		unit           RateUnit
		multiplier     float64
		parentInterval time.Duration
		fieldPresent   bool
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
)

const (
	RateModeSum        RateMode = "sum"
	RateModeValueCount RateMode = "value_count"
	RateModeInvalid    RateMode = "invalid"
)

// NewRate creates a new Rate aggregation, during parsing.
// 'multiplier' and 'parentIntervalInMs' are set later, during pancake transformation.
func NewRate(ctx context.Context, unit string, fieldPresent bool) (*Rate, error) {
	rateUnit, err := newRateUnit(ctx, unit)
	rate := &Rate{ctx: ctx, unit: rateUnit, fieldPresent: fieldPresent}
	if err != nil {
		rate.unit = second
	}
	return rate, err
}

func (query *Rate) AggregationType() model.AggregationType {
	return model.MetricsAggregation
}

func (query *Rate) TranslateSqlResponseToJson(rows []model.QueryResultRow) model.JsonMap {
	// rows[0] is either: val (1 column)
	// or parent date_histogram's key, val (2 columns)
	if len(rows) != 1 || (len(rows[0].Cols) != 1 && len(rows[0].Cols) != 2) {
		logger.ErrorWithCtx(query.ctx).Msgf("unexpected number of rows or columns returned for %s: %+v.", query.String(), rows)
		return model.JsonMap{"value": nil}
	}

	parentVal, ok := util.ExtractNumeric64Maybe(rows[0].LastColValue())
	if !ok {
		logger.WarnWithCtx(query.ctx).Msgf("cannot extract numeric value from %v, %T", rows[0].Cols[0], rows[0].Cols[0].Value)
		return model.JsonMap{"value": nil}
	}

	var (
		fix               = 1.0 // e.g. 90/88 if there are 88 days in 3 months, but our calculations are based on 90 days
		thirtyDays        = 30 * util.Day()
		needToCountDaysNr = query.parentInterval.Milliseconds()%thirtyDays.Milliseconds() == 0 &&
			(query.unit == second || query.unit == minute || query.unit == hour || query.unit == day || query.unit == week)
		weHaveParentDateHistogramKey = len(rows[0].Cols) == 2
	)

	if needToCountDaysNr && weHaveParentDateHistogramKey {
		// Calculating 'fix':
		// We need to count days of every month, as it can be 28, 29, 30 or 31...
		// So that our average is correct (in Elastic it always is)
		parentDateHistogramKey, ok := rows[0].Cols[0].Value.(int64)
		if !ok {
			logger.WarnWithCtx(query.ctx).Msgf("cannot extract parent date_histogram key from %v, %T", rows[0].Cols[0], rows[0].Cols[0].Value)
			return model.JsonMap{"value": nil}
		}

		someTime := time.UnixMilli(parentDateHistogramKey).Add(48 * time.Hour)
		// someTime.Day() is in [28, 31] U {1}. I want it to be >= 2, so I'm sure I'm in the right month for all timezones.
		for someTime.Day() == 1 || someTime.Day() > 25 {
			someTime = someTime.Add(24 * time.Hour)
		}

		actualDays := 0
		currentDays := query.parentInterval.Milliseconds() / thirtyDays.Milliseconds() * 30 // e.g. 90 for 3 months date_histogram
		currentDaysConst := currentDays
		for currentDays > 0 {
			actualDays += util.DaysInMonth(someTime)
			currentDays -= 30
			someTime = someTime.AddDate(0, -1, 0)
		}
		fix = float64(currentDaysConst) / float64(actualDays)
	}

	return model.JsonMap{"value": fix * parentVal * query.multiplier}
}

func (query *Rate) CalcAndSetMultiplier(parentInterval time.Duration) {
	query.parentInterval = parentInterval
	if parentInterval.Milliseconds() == 0 {
		logger.ErrorWithCtx(query.ctx).Msgf("parent interval is 0, cannot calculate rate multiplier")
		return
	}

	rate := query.unit.ToDuration(query.ctx)
	// unit month/quarter/year is special, only compatible with month/quarter/year calendar intervals
	if query.unit == month || query.unit == quarter || query.unit == year {
		oneMonth := 30 * util.Day()
		if parentInterval < oneMonth {
			logger.WarnWithCtx(query.ctx).Msgf("parent interval (%d ms) is not compatible with rate unit %s", parentInterval, query.unit.String(query.ctx))
			return
		}
		if query.unit == year {
			rate = 360 * util.Day() // round to 360 days, so year/month = 12, year/quarter = 3, as should be
		}
	}

	if rate.Milliseconds()%parentInterval.Milliseconds() == 0 {
		query.multiplier = float64(rate.Milliseconds() / parentInterval.Milliseconds())
	} else {
		query.multiplier = float64(rate.Milliseconds()) / float64(parentInterval.Milliseconds())
	}
}

func (query *Rate) String() string {
	return fmt.Sprintf("rate(unit: %s)", query.unit.String(query.ctx))
}

func (query *Rate) FieldPresent() bool {
	return query.fieldPresent
}

func newRateUnit(ctx context.Context, unit string) (RateUnit, error) {
	switch strings.ToLower(unit) {
	case "second":
		return second, nil
	case "minute":
		return minute, nil
	case "hour":
		return hour, nil
	case "day":
		return day, nil
	case "week":
		return week, nil
	case "month":
		return month, nil
	case "quarter":
		return quarter, nil
	case "year":
		return year, nil
	default:
		logger.WarnWithCtxAndThrottling(ctx, "rate", "unit", "invalid rate unit: %s", unit)
		return second, fmt.Errorf("invalid rate unit: %s", unit)
	}
}

func (u RateUnit) String(ctx context.Context) string {
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
		logger.WarnWithCtxAndThrottling(ctx, "rate", "unit", "invalid rate unit: %d", u)
		return "invalid"
	}
}

func (u RateUnit) ToDuration(ctx context.Context) time.Duration {
	switch u {
	case second:
		return time.Second
	case minute:
		return time.Minute
	case hour:
		return time.Hour
	case day:
		return util.Day()
	case week:
		return 7 * util.Day()
	case month:
		return 30 * util.Day()
	case quarter:
		return 90 * util.Day()
	case year:
		return 365 * util.Day()
	default:
		logger.ErrorWithCtx(ctx).Msgf("invalid rate unit: %s", u.String(ctx))
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
	if _, err := newRateUnit(ctx, params["unit"].(string)); err != nil {
		return fmt.Errorf("invalid rate unit: %v", params["unit"])
	}

	// check if only required/optional are present, and if present - that they have correct types
	for paramName := range params {
		if _, isRequired := requiredParams[paramName]; !isRequired {
			wantedType, isOptional := optionalParams[paramName]
			if !isOptional {
				return fmt.Errorf("unexpected parameter %s found in Rate params %v", paramName, params)
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
