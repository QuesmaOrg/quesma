// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package bucket_aggregations

import (
	"context"
	"fmt"
	"github.com/QuesmaOrg/quesma/platform/logger"
	"github.com/QuesmaOrg/quesma/platform/model"
	"github.com/k0kubun/pp"
	"time"
)

var UnboundedInterval model.Expr = nil

const UnboundedIntervalString = "*"

// DateTimeInterval represents a date range. Both Begin and End are either:
// 1) in Clickhouse's proper format, e.g. toStartOfDay(subDate(now(), INTERVAL 3 week))
// 2) * (UnboundedInterval), which means no bound
type DateTimeInterval struct {
	begin model.Expr
	end   model.Expr
}

func NewDateTimeInterval(begin, end model.Expr) DateTimeInterval {
	return DateTimeInterval{
		begin: begin,
		end:   end,
	}
}

func (interval DateTimeInterval) ToWhereClause(field model.Expr) model.Expr {
	var begin, end model.Expr
	isBegin := interval.begin != UnboundedInterval
	isEnd := interval.end != UnboundedInterval
	if isBegin {
		begin = model.NewInfixExpr(field, ">=", interval.begin)
	}
	if isEnd {
		end = model.NewInfixExpr(field, "<", interval.end)
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

// TODO support time_zone
type DateRange struct {
	ctx             context.Context
	field           model.Expr
	format          string
	intervals       []DateTimeInterval
	selectColumnsNr int // how many columns we add to the query because of date_range aggregation, e.g. SELECT x,y,z -> 3
}

func NewDateRange(ctx context.Context, field model.Expr, format string, intervals []DateTimeInterval, selectColumnsNr int) DateRange {
	return DateRange{ctx: ctx, field: field, format: format, intervals: intervals, selectColumnsNr: selectColumnsNr}
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
	startIteration := len(rows[0].Cols) - 1 - query.selectColumnsNr
	if startIteration < 0 || startIteration >= len(rows[0].Cols) {
		logger.ErrorWithCtx(query.ctx).Msgf(
			"unexpected column nr in aggregation response, startIteration: %d, len(rows[0].Cols): %d",
			startIteration, len(rows[0].Cols),
		)
		return nil
	}
	for intervalIdx, columnIdx := 0, startIteration; intervalIdx < len(query.intervals); intervalIdx++ {
		responseForInterval, nextColumnIdx := query.responseForInterval(&rows[0], intervalIdx, columnIdx)
		fmt.Println("responseForInterval", responseForInterval)
		response = append(response, responseForInterval)
		columnIdx = nextColumnIdx
	}
	return model.JsonMap{
		"buckets": response,
	}
}

func (query DateRange) String() string {
	return "date_range, intervals: " + fmt.Sprintf("%v", query.intervals)
}

func (query DateRange) responseForInterval(row *model.QueryResultRow, intervalIdx, columnIdx int) (
	response model.JsonMap, nextColumnIdx int) {
	response = model.JsonMap{
		"doc_count": row.Cols[columnIdx].Value,
	}
	columnIdx++

	var from, to int64
	var fromString, toString string
	if query.intervals[intervalIdx].begin == UnboundedInterval {
		fromString = UnboundedIntervalString
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

	if query.intervals[intervalIdx].end == UnboundedInterval {
		toString = UnboundedIntervalString
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
	for intervalIdx, interval := range query.intervals {
		prefix := fmt.Sprintf("range_%d__", intervalIdx)
		if len(query.intervals) == 1 {
			prefix = ""
		}
		result = append(result, CombinatorGroup{
			idx:         intervalIdx,
			Prefix:      prefix,
			Key:         prefix, // TODO: we need translate date to real time
			WhereClause: interval.ToWhereClause(query.field),
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
	interval := query.intervals[subGroup.idx]
	pp.Println(interval, model.AsString(interval.begin), model.AsString(interval.end))
	if interval.begin != UnboundedInterval {
		response["from"] = model.AsString(interval.begin)
		response["from_as_string"] = interval.begin
	}
	if interval.end != UnboundedInterval {
		response["to"] = interval.end
		response["to_as_string"] = interval.end
	}

	return response
}

func (query DateRange) CombinatorSplit() []model.QueryType {
	result := make([]model.QueryType, 0, len(query.intervals))
	for _, interval := range query.intervals {
		result = append(result, NewDateRange(query.ctx, query.field, query.format, []DateTimeInterval{interval}, query.selectColumnsNr))
	}
	return result
}
