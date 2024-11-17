// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package bucket_aggregations

import (
	"context"
	"fmt"
	"quesma/logger"
	"quesma/model"
	"time"
)

const UnboundedInterval = "*"

// DateTimeInterval represents a date range. Both Begin and End are either:
// 1) in Clickhouse's proper format, e.g. toStartOfDay(subDate(now(), INTERVAL 3 week))
// 2) * (UnboundedInterval), which means no bound
type DateTimeInterval struct {
	Begin string
	End   string
}

func NewDateTimeInterval(begin, end string) DateTimeInterval {
	return DateTimeInterval{
		Begin: begin,
		End:   end,
	}
}

// BeginTimestampToSQL returns SQL select for the begin timestamp, and a boolean indicating if the select is needed
// We query Clickhouse for this timestamp, as it's defined in Clickhouse's format, e.g. now()-1d.
// It's only 1 more field to our SELECT query, so it shouldn't be a performance issue.
func (interval DateTimeInterval) BeginTimestampToSQL() (sqlSelect model.Expr, selectNeeded bool) {
	if interval.Begin != UnboundedInterval {
		return model.NewFunction("toInt64", model.NewFunction("toUnixTimestamp", model.NewLiteral(interval.Begin))), true
	}
	return nil, false
}

// EndTimestampToSQL returns SQL select for the end timestamp, and a boolean indicating if the select is needed
// We query Clickhouse for this timestamp, as it's defined in Clickhouse's format, e.g. now()-1d.
// It's only 1 more field to our SELECT query, so it isn't a performance issue.
func (interval DateTimeInterval) EndTimestampToSQL() (sqlSelect model.Expr, selectNeeded bool) {
	if interval.End != UnboundedInterval {
		return model.NewFunction("toInt64", model.NewFunction("toUnixTimestamp", model.NewLiteral(interval.End))), true
	}
	return nil, false
}

func (interval DateTimeInterval) ToWhereClause(field model.Expr) model.Expr {
	begin, isBegin := interval.BeginTimestampToSQL()
	end, isEnd := interval.EndTimestampToSQL()

	if isBegin {
		begin = model.NewInfixExpr(field, ">=", begin)
	}
	if isEnd {
		end = model.NewInfixExpr(field, "<", end)
	}

	if isBegin && isEnd {
		return model.NewInfixExpr(begin, "AND", end)
	} else if isBegin {
		return begin
	} else if isEnd {
		return end
	} else {
		return model.TrueExpr
	}
}

type DateRange struct {
	ctx             context.Context
	Field           model.Expr
	Format          string
	Intervals       []DateTimeInterval
	SelectColumnsNr int // how many columns we add to the query because of date_range aggregation, e.g. SELECT x,y,z -> 3
}

func NewDateRange(ctx context.Context, field model.Expr, format string, intervals []DateTimeInterval, selectColumnsNr int) DateRange {
	return DateRange{ctx: ctx, Field: field, Format: format, Intervals: intervals, SelectColumnsNr: selectColumnsNr}
}

func (query DateRange) AggregationType() model.AggregationType {
	return model.BucketAggregation
}

func (query DateRange) TranslateSqlResponseToJson(rows []model.QueryResultRow) model.JsonMap {
	if len(rows) != 1 {
		logger.ErrorWithCtx(query.ctx).Msgf("unexpected number of rows in date_range aggregation response, len: %d", len(rows))
		return nil
	}

	response := make([]model.JsonMap, 0)
	startIteration := len(rows[0].Cols) - 1 - query.SelectColumnsNr
	if startIteration < 0 || startIteration >= len(rows[0].Cols) {
		logger.ErrorWithCtx(query.ctx).Msgf(
			"unexpected column nr in aggregation response, startIteration: %d, len(rows[0].Cols): %d",
			startIteration, len(rows[0].Cols),
		)
		return nil
	}
	for intervalIdx, columnIdx := 0, startIteration; intervalIdx < len(query.Intervals); intervalIdx++ {
		responseForInterval, nextColumnIdx := query.responseForInterval(&rows[0], intervalIdx, columnIdx)
		response = append(response, responseForInterval)
		columnIdx = nextColumnIdx
	}
	return model.JsonMap{
		"buckets": response,
	}
}

func (query DateRange) String() string {
	return "date_range, intervals: " + fmt.Sprintf("%v", query.Intervals)
}

func (query DateRange) responseForInterval(row *model.QueryResultRow, intervalIdx, columnIdx int) (
	response model.JsonMap, nextColumnIdx int) {
	response = model.JsonMap{
		"doc_count": row.Cols[columnIdx].Value,
	}
	columnIdx++

	var from, to int64
	var fromString, toString string
	if query.Intervals[intervalIdx].Begin == UnboundedInterval {
		fromString = UnboundedInterval
	} else {
		if columnIdx >= len(row.Cols) {
			logger.ErrorWithCtx(query.ctx).Msgf("trying to read column after columns length, query: %v, row: %v", query, row)
			return nil, columnIdx
		}
		from = query.parseTimestamp(row.Cols[columnIdx].Value)
		fromString = timestampToString(from)
		response["from"] = from * 1000
		response["from_as_string"] = fromString
		columnIdx++
	}

	if query.Intervals[intervalIdx].End == UnboundedInterval {
		toString = UnboundedInterval
	} else {
		if columnIdx >= len(row.Cols) {
			logger.ErrorWithCtx(query.ctx).Msgf("trying to read column after columns length, query: %v, row: %v", query, row)
			return nil, columnIdx
		}
		to = query.parseTimestamp(row.Cols[columnIdx].Value)
		toString = timestampToString(to)
		response["to"] = to * 1000
		response["to_as_string"] = toString
		columnIdx++
	}

	response["key"] = fromString + "-" + toString
	return response, columnIdx
}

// timestampToString converts timestamp to string in format "2006-01-02T15:04:05.000", which is good for Clickhouse's response
func timestampToString(unixTimestampInSeconds int64) string {
	return time.Unix(unixTimestampInSeconds, 0).UTC().Format("2006-01-02T15:04:05.000")
}

// parseTimestamp converts timestamp to int64. I have no idea why, but same function toInt64(...) once returns int64, and once uint64.
func (query DateRange) parseTimestamp(timestamp any) int64 {
	if maybeUint64, ok := timestamp.(uint64); ok {
		return int64(maybeUint64)
	}
	return timestamp.(int64)
}

func (query DateRange) DoesNotHaveGroupBy() bool {
	return true
}

func (query DateRange) CombinatorGroups() (result []CombinatorGroup) {
	for intervalIdx, interval := range query.Intervals {
		prefix := fmt.Sprintf("range_%d__", intervalIdx)
		if len(query.Intervals) == 1 {
			prefix = ""
		}
		result = append(result, CombinatorGroup{
			idx:         intervalIdx,
			Prefix:      prefix,
			Key:         prefix, // TODO: we need translate date to real time
			WhereClause: interval.ToWhereClause(query.Field),
		})
	}
	return
}

func (query DateRange) CombinatorTranslateSqlResponseToJson(subGroup CombinatorGroup, rows []model.QueryResultRow) model.JsonMap {
	if len(rows) == 0 || len(rows[0].Cols) == 0 {
		panic(fmt.Sprintf("need at least one row and column in date_range aggregation response, rows: %d, cols: %d", len(rows), len(rows[0].Cols)))
	}
	count := rows[0].Cols[len(rows[0].Cols)-1].Value
	response := model.JsonMap{
		"key":       subGroup.Key,
		"doc_count": count,
	}

	// TODO: we need translate relative to real time
	interval := query.Intervals[subGroup.idx]
	if interval.Begin != UnboundedInterval {
		response["from"] = interval.Begin
		response["from_as_string"] = interval.Begin
	}
	if interval.End != UnboundedInterval {
		response["to"] = interval.End
		response["to_as_string"] = interval.End
	}

	return response
}

func (query DateRange) CombinatorSplit() []model.QueryType {
	result := make([]model.QueryType, 0, len(query.Intervals))
	for _, interval := range query.Intervals {
		result = append(result, NewDateRange(query.ctx, query.Field, query.Format, []DateTimeInterval{interval}, query.SelectColumnsNr))
	}
	return result
}
