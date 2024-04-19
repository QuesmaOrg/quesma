package bucket_aggregations

import (
	"fmt"
	"math"
	"mitmproxy/quesma/model"
	"strconv"
	"strings"
)

const keyedDefaultValue = false

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

// ToSQLSelectQuery returns count(...) where ... is a condition for the interval, just like we want it in SQL's SELECT
func (interval Interval) ToSQLSelectQuery(quotedFieldName string) string {
	var sqlLeft, sqlRight, sql string
	if !interval.IsOpeningBoundInfinite() {
		sqlLeft = quotedFieldName + ">=" + strconv.FormatFloat(interval.Begin, 'f', -1, 64)
	}
	if !interval.IsClosingBoundInfinite() {
		sqlRight = quotedFieldName + "<" + strconv.FormatFloat(interval.End, 'f', -1, 64)
	}
	switch {
	case sqlLeft != "" && sqlRight != "":
		sql = sqlLeft + " AND " + sqlRight
	case sqlLeft != "":
		sql = sqlLeft
	case sqlRight != "":
		sql = sqlRight
	default:
		return "count()"
	}
	return "count(if(" + sql + ", 1, NULL))"
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
	QuotedFieldName string
	Intervals       []Interval
	// defines what response should look like
	// https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-range-aggregation.html#_keyed_response_4
	Keyed bool
}

func NewRange(quotedFieldName string, intervals []Interval, keyed bool) Range {
	return Range{quotedFieldName, intervals, keyed}
}

func NewRangeWithDefaultKeyed(quotedFieldName string, intervals []Interval) Range {
	return Range{quotedFieldName, intervals, keyedDefaultValue}
}

func (query Range) IsBucketAggregation() bool {
	return true
}

func (query Range) TranslateSqlResponseToJson(rows []model.QueryResultRow, level int) []model.JsonMap {
	// fmt.Println("AI is awesome", query, rows[0].Cols)
	startIteration := len(rows[0].Cols) - 1 - len(query.Intervals)
	endIteration := len(rows[0].Cols) - 1
	if query.Keyed {
		var response = make(model.JsonMap)
		for i, col := range rows[0].Cols[startIteration:endIteration] {
			responseForInterval := query.responseForInterval(query.Intervals[i], col.Value)
			response[query.Intervals[i].String()] = responseForInterval
		}
		return []model.JsonMap{response}
	} else {
		var response []model.JsonMap
		for i, col := range rows[0].Cols[startIteration:endIteration] {
			responseForInterval := query.responseForInterval(query.Intervals[i], col.Value)
			responseForInterval["key"] = query.Intervals[i].String()
			response = append(response, responseForInterval)
		}
		return response
	}
}

func (query Range) String() string {
	return "range, intervals: " + fmt.Sprintf("%v", query.Intervals)
}

func (query Range) responseForInterval(interval Interval, value any) model.JsonMap {
	response := model.JsonMap{
		"doc_count": value,
	}
	if !interval.IsOpeningBoundInfinite() {
		response["from"] = interval.Begin
	}
	if !interval.IsClosingBoundInfinite() {
		response["to"] = interval.End
	}
	return response
}
