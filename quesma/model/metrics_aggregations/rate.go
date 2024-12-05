// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package metrics_aggregations

import (
	"context"
	"fmt"
	"quesma/logger"
	"quesma/model"
	"quesma/util"
	"strings"
)

type (
	Rate struct {
		ctx        context.Context
		unit       RateUnit
		multiplier float64
	}
	RateUnit int
)

const (
	Second RateUnit = iota
	Minute
	Hour
	Day
	Week
	Month
	Quarter
	Year
	Invalid
)

// NewRate creates a new Rate aggregation, during parsing.
// Multiplier is set later, during pancake transformation.
func NewRate(ctx context.Context, unit string) *Rate {
	return &Rate{ctx: ctx, unit: NewRateUnit(unit)}
}

func (query *Rate) AggregationType() model.AggregationType {
	return model.MetricsAggregation
}

func (query *Rate) TranslateSqlResponseToJson(rows []model.QueryResultRow) model.JsonMap {
	if len(rows) != 1 || len(rows[0].Cols) != 1 {
		logger.WarnWithCtx(query.ctx).Msgf("unexpected number of rows or columns returned for %s: %d, %d.", query.String(), len(rows), len(rows[0].Cols))
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
	if query.unit == Month || query.unit == Quarter || query.unit == Year {
		oneMonthInMs := int64(30 * 24 * 60 * 60 * 1000)
		if parentIntervalInMs < oneMonthInMs {
			logger.WarnWithCtx(query.ctx).Msgf("parent interval (%d ms) is not compatible with rate unit %s", parentIntervalInMs, query.unit)
			return
		}
		if query.unit == Year {
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

func NewRateUnit(unit string) RateUnit {
	switch strings.ToLower(unit) {
	case "second":
		return Second
	case "minute":
		return Minute
	case "hour":
		return Hour
	case "day":
		return Day
	case "week":
		return Week
	case "month":
		return Month
	case "quarter":
		return Quarter
	case "year":
		return Year
	default:
		return Invalid
	}
}

func (u RateUnit) String() string {
	switch u {
	case Second:
		return "second"
	case Minute:
		return "minute"
	case Hour:
		return "hour"
	case Day:
		return "day"
	case Week:
		return "week"
	case Month:
		return "month"
	case Quarter:
		return "quarter"
	case Year:
		return "year"
	default:
		return "invalid"
	}
}

func (u RateUnit) ToMilliseconds(ctx context.Context) int64 {
	switch u {
	case Second:
		return 1000
	case Minute:
		return 60 * 1000
	case Hour:
		return 60 * 60 * 1000
	case Day:
		return 24 * 60 * 60 * 1000
	case Week:
		return 7 * 24 * 60 * 60 * 1000
	case Month:
		return 30 * 24 * 60 * 60 * 1000
	case Quarter:
		return 3 * 30 * 24 * 60 * 60 * 1000
	case Year:
		return 365 * 24 * 60 * 60 * 1000
	default:
		logger.ErrorWithCtx(ctx).Msgf("invalid rate unit: %s", u)
		return 0
	}
}
