// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package clickhouse

import (
	"bytes"
	"fmt"
	"github.com/QuesmaOrg/quesma/quesma/model"
	"github.com/goccy/go-json"
	"strings"
	"time"
)

// Code doesn't need to be pretty, 99.9% it's just for our purposes
// Parses type from SHOW COLUMNS FROM "table"
func parseTypeFromShowColumns(typ, name string) (Type, string) {
	// i1, i2, i3 indices of results of strings.Index
	// returns if i1 is closer to the beginning than i2 and i3
	isClosest := func(i1, i2, i3 int) bool {
		return i1 != -1 && (i1 < i2 || i2 == -1) && (i1 < i3 || i3 == -1)
	}

	// '(' -> indentationLvl++
	// ')' -> indentationLvl--
	// We want a comma when indentationLvl == 0, so we have a new field
	indexCommaSameIndentationLvl := func(s string) int {
		lvl := 0
		for i, c := range s {
			if c == ',' && lvl == 0 {
				return i
			} else if c == '(' {
				lvl++
			} else if c == ')' {
				lvl--
			}
			if lvl < 0 {
				return -1
			}
		}
		return -1
	}

	// s - type string
	var parseTypeRec func(s, colName string) (Type, string)
	parseTypeRec = func(s, colName string) (Type, string) {
		cols := make([]*Column, 0)
		finish := func() (Type, string) {
			if len(cols) == 1 {
				return cols[0].Type, colName
			} else {
				return MultiValueType{Name: "Tuple", Cols: cols}, colName
			}
		}
		for {
			if len(s) == 0 {
				return finish()
			}

			iSpace := strings.Index(s, " ")
			iComma := indexCommaSameIndentationLvl(s)
			iLeft := strings.Index(s, "(")
			iRight := strings.Index(s, ")")
			if iSpace == -1 && iComma == -1 && iLeft == -1 && iRight == -1 {
				cols = append(cols, &Column{Name: colName, Type: NewBaseType(s)})
				return finish()
			}

			name := ""
			if iSpace != -1 {
				name = s[:iSpace]
			} else if iLeft != -1 {
				name = s[:iLeft]
			}
			if isClosest(iLeft, iComma, iRight) { // '(' is closest
				if name == "Array" {
					baseType, _ := parseTypeRec(s[iLeft+1:], "")
					return CompoundType{Name: "Array", BaseType: baseType}, name
				} else {
					colType, _ := parseTypeRec(s[iLeft+1:], name)
					cols = append(cols, &Column{Name: "Tuple", Type: colType})
					if iComma != -1 {
						s = s[iComma+2:]
					} else {
						return finish() // check if needed to assign typ = typCasted
					}
				}
			} else {
				end := iComma
				if isClosest(iRight, iComma, -1) {
					end = iRight
				}
				cols = append(cols, &Column{Name: name, Type: NewBaseType(s[iSpace+1 : end])}) // TODO inspect type
			}
			if isClosest(iComma, iLeft, iRight) { // ',' closest (same ind lvl)
				// TODO
				// m[name] = nil
				s = s[iComma+2:]
			} else if isClosest(iRight, iLeft, iComma) { // ')' closest
				return finish() // TODO
			}
		}
	}
	return parseTypeRec(typ, name)
}

func PrettyJson(jsonStr string) string {
	var prettyJSON bytes.Buffer
	if err := json.Indent(&prettyJSON, []byte(jsonStr), "", "    "); err != nil {
		return fmt.Sprintf("PrettyJson err: %v\n", err)
	}
	return prettyJSON.String()
}

// TimestampGroupBy returns string to be used in the select part of Clickhouse query, when grouping by timestamp interval.
// e.g.
// - timestampGroupBy("@timestamp", DateTime64, 30 seconds) --> toInt64(toUnixTimestamp64Milli(`@timestamp`)/30000)
// - timestampGroupBy("@timestamp", DateTime, 30 seconds)   --> toInt64(toUnixTimestamp(`@timestamp`)/30)
func TimestampGroupBy(fullTimestampExpr model.Expr, timestampField model.ColumnRef, groupByInterval time.Duration) model.Expr {
	toUnixTsFunc := model.NewInfixExpr(
		model.NewFunction(model.ToUnixTimestampMs, fullTimestampExpr),
		" / ", // TODO nasty hack to make our string-based tests pass. Operator should not contain spaces obviously
		model.NewMillisecondsLiteral(timestampField, groupByInterval.Milliseconds()),
	)
	return model.NewFunction("toInt64", toUnixTsFunc)
}

func TimestampGroupByWithTimezone(fullTimestampExpr model.Expr, timestampField model.ColumnRef, groupByInterval time.Duration, timezone string) model.Expr {

	// If no timezone, or timezone is default (UTC), we just return TimestampGroupBy(...)
	if timezone == "" {
		return TimestampGroupBy(fullTimestampExpr, timestampField, groupByInterval)
	}

	var offset model.Expr = model.NewInfixExpr(
		model.NewFunction(
			"timeZoneOffset",
			model.NewFunction(
				"toTimezone",
				fullTimestampExpr, model.NewLiteralSingleQuoteString(timezone),
			),
		),
		"*",
		model.NewMillisecondsLiteral(timestampField, 1000),
	)

	unixTsWithOffset := model.NewInfixExpr(
		model.NewFunction(model.ToUnixTimestampMs, fullTimestampExpr),
		"+",
		offset,
	)

	groupByExpr := model.NewInfixExpr(
		model.NewParenExpr(unixTsWithOffset),
		" / ", // TODO nasty hack to make our string-based tests pass. Operator should not contain spaces obviously
		model.NewMillisecondsLiteral(timestampField, groupByInterval.Milliseconds()),
	)

	return model.NewFunction("toInt64", groupByExpr)
}
