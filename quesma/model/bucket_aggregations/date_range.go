package bucket_aggregations

import (
	"context"
	"fmt"
	"mitmproxy/quesma/logger"
	"mitmproxy/quesma/model"
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

// String returns key part of the response, e.g. "1.0-2.0", or "*-6.55"
func (interval DateTimeInterval) String() string {
	return "dupa" //interval.floatToString(interval.Begin) + "-" + interval.floatToString(interval.End)
}

// ToSQLSelectQuery returns count(...) where ... is a condition for the interval, just like we want it in SQL's SELECT
// from elastic docs: Note that this aggregation includes the from value and excludes the to value for each range.
func (interval DateTimeInterval) ToSQLSelectQuery(quotedFieldName string) string {
	if interval.Begin != UnboundedInterval && interval.End != UnboundedInterval {
		return fmt.Sprintf("count(if(%s >= %s AND %s < %s, 1, NULL))",
			quotedFieldName, interval.Begin, quotedFieldName, interval.End)
	} else if interval.Begin != UnboundedInterval {
		return fmt.Sprintf("count(if(%s >= %s, 1, NULL))", quotedFieldName, interval.Begin)
	} else if interval.End != UnboundedInterval {
		return fmt.Sprintf("count(if(%s < %s, 1, NULL))", quotedFieldName, interval.End)
	}
	return "count()"
}

// ToWhereClause returns a condition for the interval, just like we want it in SQL's WHERE
func (interval DateTimeInterval) ToWhereClause(quotedFieldName string) string {
	var sqlLeft, sqlRight string
	if interval.Begin != UnboundedInterval {
		sqlLeft = quotedFieldName + " >= " + interval.Begin
	}
	if interval.End != UnboundedInterval {
		sqlRight = quotedFieldName + " < " + interval.End
	}
	switch {
	case sqlLeft != "" && sqlRight != "":
		return sqlLeft + " AND " + sqlRight
	case sqlLeft != "":
		return sqlLeft
	case sqlRight != "":
		return sqlRight
	default:
		return quotedFieldName + " IS NOT NULL"
	}
}

// BeginTimestampToSQL returns SQL select for the begin timestamp, and a boolean indicating if the select is needed
// We query Clickhouse for this timestamp, as it's defined in Clickhouse's format, e.g. now()-1d.
// It's only 1 more field to our SELECT query, so it shouldn't be a performance issue.
func (interval DateTimeInterval) BeginTimestampToSQL() (sqlSelect string, selectNeeded bool) {
	if interval.Begin != UnboundedInterval {
		return "toInt64(toUnixTimestamp(" + interval.Begin + "))", true
	}
	return "", false
}

// EndTimestampToSQL returns SQL select for the end timestamp, and a boolean indicating if the select is needed
// We query Clickhouse for this timestamp, as it's defined in Clickhouse's format, e.g. now()-1d.
// It's only 1 more field to our SELECT query, so it isn't a performance issue.
func (interval DateTimeInterval) EndTimestampToSQL() (sqlSelect string, selectNeeded bool) {
	if interval.End != UnboundedInterval {
		return "toInt64(toUnixTimestamp(" + interval.End + "))", true
	}
	return "", false
}

type DateRange struct {
	ctx             context.Context
	Format          string
	QuotedFieldName string
	Intervals       []DateTimeInterval
	SelectColumnsNr int // how many columns we add to the query because of date_range aggregation, e.g. SELECT x,y,z -> 3
}

func NewDateRange(ctx context.Context, quotedFieldName string, format string, intervals []DateTimeInterval, selectColumnsNr int) DateRange {
	return DateRange{ctx: ctx, QuotedFieldName: quotedFieldName, Format: format, Intervals: intervals, SelectColumnsNr: selectColumnsNr}
}

func (query DateRange) IsBucketAggregation() bool {
	return true
}

func (query DateRange) TranslateSqlResponseToJson(rows []model.QueryResultRow, level int) []model.JsonMap {
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
	return response
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
