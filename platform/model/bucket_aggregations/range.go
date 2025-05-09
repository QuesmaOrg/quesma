// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package bucket_aggregations

import (
	"context"
	"fmt"
	"github.com/QuesmaOrg/quesma/platform/logger"
	"github.com/QuesmaOrg/quesma/platform/model"
	"math"
	"strconv"
	"strings"
)

var IntervalInfiniteRange = math.NaN()

type Interval struct {
	Begin float64
	End   float64
}

func NewInterval(begin, end float64) Interval {
	return Interval{begin, end}
}

// String returns key part of the response, e.g. "1.0-2.0", or "*-6.55"
func (interval Interval) String() string {
	return interval.floatToString(interval.Begin) + "-" + interval.floatToString(interval.End)
}

func (interval Interval) ToWhereClause(field model.Expr) model.Expr { // returns a condition for the interval, just like we want it in SQL's WHERE
	var sqlLeft, sqlRight model.Expr
	if !interval.IsOpeningBoundInfinite() {
		sqlLeft = model.NewInfixExpr(field, ">=", model.NewLiteral(strconv.FormatFloat(interval.Begin, 'f', -1, 64)))
	}
	if !interval.IsClosingBoundInfinite() {
		sqlRight = model.NewInfixExpr(field, "<", model.NewLiteral(strconv.FormatFloat(interval.End, 'f', -1, 64)))
	}
	switch {
	case sqlLeft != nil && sqlRight != nil:
		return model.NewInfixExpr(sqlLeft, "AND", sqlRight)
	case sqlLeft != nil:
		return sqlLeft
	case sqlRight != nil:
		return sqlRight
	default:
		return model.NewInfixExpr(field, "IS", model.NotNullExpr)
	}
}

func (interval Interval) IsOpeningBoundInfinite() bool {
	return math.IsNaN(interval.Begin)
}
func (interval Interval) IsClosingBoundInfinite() bool {
	return math.IsNaN(interval.End)
}

// floatToString converts float to string in a proper format (1 -> 1.0, 5.4 -> 5.4, 1.234 -> 1.234)
// If it's NaN (unbounded), it returns "*"
func (interval Interval) floatToString(number float64) string {
	if math.IsNaN(number) {
		return "*"
	}
	asString := fmt.Sprintf("%f", number)
	// fmt.Println("as1", asString)
	dotIdx := strings.IndexRune(asString, '.')
	if dotIdx == -1 {
		return asString + ".0"
	}
	asString = strings.TrimRight(asString, "0")
	// fmt.Println("as2", asString)
	if dotIdx == len(asString)-1 {
		return asString + "0"
	} else {
		return asString
	}
}

type Range struct {
	ctx       context.Context
	Expr      model.Expr
	Intervals []Interval
	// defines what response should look like
	// https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-range-aggregation.html#_keyed_response_4
	Keyed bool
}

func NewRange(ctx context.Context, expr model.Expr, intervals []Interval, keyed bool) Range {
	return Range{ctx, expr, intervals, keyed}
}

func (query Range) AggregationType() model.AggregationType {
	return model.BucketAggregation
}

func (query Range) TranslateSqlResponseToJson(rows []model.QueryResultRow) model.JsonMap {
	if len(rows) != 1 {
		logger.ErrorWithCtx(query.ctx).Msgf("unexpected %d of rows in range aggregation response. Expected 1.", len(rows))
		return model.JsonMap{}
	}
	startIteration := len(rows[0].Cols) - 1 - len(query.Intervals)
	endIteration := len(rows[0].Cols) - 1
	if startIteration >= endIteration || startIteration < 0 {
		logger.ErrorWithCtx(query.ctx).Msgf(
			"unexpected column nr in aggregation response, startIteration: %d, endIteration: %d", startIteration, endIteration)
		return model.JsonMap{}
	}
	if query.Keyed {
		var response = make(model.JsonMap)
		for i, col := range rows[0].Cols[startIteration:endIteration] {
			responseForInterval := query.responseForInterval(query.Intervals[i], col.Value)
			response[query.Intervals[i].String()] = responseForInterval
		}
		return response
	} else {
		var response []model.JsonMap
		for i, col := range rows[0].Cols[startIteration:endIteration] {
			responseForInterval := query.responseForInterval(query.Intervals[i], col.Value)
			responseForInterval["key"] = query.Intervals[i].String()
			response = append(response, responseForInterval)
		}
		return model.JsonMap{
			"buckets": response,
		}
	}
}

func (query Range) String() string {
	return "range, intervals: " + fmt.Sprintf("%v", query.Intervals)
}

func (query Range) responseForInterval(interval Interval, value any) model.JsonMap {
	response := model.JsonMap{}
	if value != nil {
		// occasionally we may not have count (e.g. top_hits) and it's ok
		response["doc_count"] = value
	}
	if !interval.IsOpeningBoundInfinite() {
		response["from"] = interval.Begin
	}
	if !interval.IsClosingBoundInfinite() {
		response["to"] = interval.End
	}
	return response
}

func (query Range) DoesNotHaveGroupBy() bool {
	return true
}

func (query Range) CombinatorGroups() (result []CombinatorGroup) {
	for intervalIdx, interval := range query.Intervals {
		prefix := fmt.Sprintf("range_%d__", intervalIdx)
		if len(query.Intervals) == 1 {
			prefix = ""
		}
		result = append(result, CombinatorGroup{
			idx:         intervalIdx,
			Prefix:      prefix,
			Key:         interval.String(),
			WhereClause: interval.ToWhereClause(query.Expr),
		})
	}
	return
}

func (query Range) CombinatorTranslateSqlResponseToJson(subGroup CombinatorGroup, rows []model.QueryResultRow) model.JsonMap {
	interval := query.Intervals[subGroup.idx]
	var count any
	if len(rows[0].Cols) > 0 {
		// occasionally we may not have count (e.g. top_hits) and it's ok
		count = rows[0].Cols[len(rows[0].Cols)-1].Value
	}
	return query.responseForInterval(interval, count)
}

func (query Range) CombinatorSplit() []model.QueryType {
	result := make([]model.QueryType, 0, len(query.Intervals))
	for _, interval := range query.Intervals {
		result = append(result, NewRange(query.ctx, query.Expr, []Interval{interval}, query.Keyed))
	}
	return result
}
