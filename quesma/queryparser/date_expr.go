// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package queryparser

import (
	"errors"
	"fmt"
	"quesma/model"
	"strconv"
	"strings"
	"time"
)

type timeUnit string

type DateMathInterval struct {
	amount int
	unit   timeUnit
}

type DateMathExpression struct {
	intervals []DateMathInterval
	rounding  timeUnit
}

func ParseDateMathExpression(input string) (*DateMathExpression, error) {

	result := &DateMathExpression{}
	result.intervals = []DateMathInterval{}

	result.rounding = ""

	const NOW_LENGTH = 3
	const OPERATOR_ADD = '+'
	const OPERATOR_SUB = '-'
	const ROUNDING = '/'

	const now = "now"

	expr := input

	if strings.HasPrefix(expr, now) {
		expr = expr[NOW_LENGTH:]
	} else {
		return nil, fmt.Errorf("invalid date math expression: 'now' keyword expected")
	}

	var number string
	var rounding bool
	for index := 0; index < len(expr); index++ {

		letter := expr[index]

		switch letter {

		case OPERATOR_ADD:
			number = string(letter)
		case OPERATOR_SUB:
			number = string(letter)

		case ROUNDING:
			rounding = true

		case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':

			number = number + string(letter)

		case 'm', 's', 'h', 'd', 'w', 'M', 'y', 'Y':

			if rounding {
				result.rounding = timeUnit(letter)

				if len(expr[index:]) > 1 {
					return nil, fmt.Errorf("garbage at the end of expression: %s", expr[1:])
				}

			} else {

				if len(number) == 0 {
					return nil, fmt.Errorf("number expected in date math expression %s", expr)
				} else {
					val, err := strconv.Atoi(number)
					if err != nil {
						return nil, fmt.Errorf("invalid number in date math expression %s", number)
					}

					result.intervals = append(result.intervals, DateMathInterval{amount: val, unit: timeUnit(expr[index])})
					number = ""
				}

			}

		default:
			return nil, fmt.Errorf("invalid character in date math expression '%s' expr: %s", string(expr[index]), input)
		}
	}

	return result, nil

}

type DateMathExpressionRenderer interface {
	RenderSQL(expression *DateMathExpression) (model.Expr, error)
}

const DateMathExpressionFormatLiteral = "literal"
const DateMathExpressionFormatClickhouse = "clickhouse_intervals"
const DateMathExpressionFormatLiteralTest = "test"

func DateMathExpressionRendererFactory(format string) DateMathExpressionRenderer {
	switch format {
	case "":
		return &DateMathAsClickhouseIntervals{}
	case DateMathExpressionFormatClickhouse:
		return &DateMathAsClickhouseIntervals{}
	case DateMathExpressionFormatLiteral:
		return &DateMathExpressionAsLiteral{now: time.Now()}
	case DateMathExpressionFormatLiteralTest:
		return &DateMathExpressionAsLiteral{now: time.Date(2024, 5, 17, 12, 1, 2, 3, time.UTC)}
	default:
		return nil
	}
}

type DateMathAsClickhouseIntervals struct{}

func (b *DateMathAsClickhouseIntervals) RenderSQL(expression *DateMathExpression) (model.Expr, error) {

	result := model.NewFunction("now")

	for _, interval := range expression.intervals {

		if interval.amount == 0 {
			continue
		}

		amount := interval.amount

		var op string
		if amount < 0 {
			op = "subDate"
			amount = -amount
		} else {
			op = "addDate"
		}

		unit, err := b.parseTimeUnit(interval.unit)
		if err != nil {
			return nil, fmt.Errorf("invalid time unit: %s", interval.unit)
		}

		result = model.NewFunction(op, result, model.NewLiteral(fmt.Sprintf("INTERVAL %d %s", amount, unit)))
	}

	var roundingFunction = map[string]string{
		"d": "toStartOfDay",
		"w": "toStartOfWeek",
		"M": "toStartOfMonth",
		"Y": "toStartOfYear",
	}

	if expression.rounding != "" {

		if function, ok := roundingFunction[string(expression.rounding)]; ok {
			result = model.NewFunction(function, result)
		} else {
			return nil, fmt.Errorf("invalid rounding unit: %s", expression.rounding)
		}

	}

	return result, nil
}

func (b *DateMathAsClickhouseIntervals) parseTimeUnit(timeUnit timeUnit) (string, error) {
	switch timeUnit {
	case "m":
		return "minute", nil
	case "s":
		return "second", nil
	case "h", "H":
		return "hour", nil
	case "d":
		return "day", nil
	case "w":
		return "week", nil
	case "M":
		return "month", nil
	case "y":
		return "year", nil
	}
	return "", errors.New("unsupported time unit")
}

type DateMathExpressionAsLiteral struct {
	now time.Time
}

func (b *DateMathExpressionAsLiteral) RenderSQL(expression *DateMathExpression) (model.Expr, error) {

	const format = "2006-01-02 15:04:05"

	result := b.now

	for _, interval := range expression.intervals {

		if interval.amount == 0 {
			continue
		}

		amount := interval.amount

		switch interval.unit {
		case "m":
			result = result.Add(time.Minute * time.Duration(amount))

		case "s":
			result = result.Add(time.Duration(amount) * time.Second)

		case "h", "H":
			result = result.Add(time.Duration(amount) * time.Hour)

		case "d":
			result = result.AddDate(0, 0, amount)

		case "w":
			result = result.AddDate(0, 0, amount*7)

		case "M":
			result = result.AddDate(0, amount, 0)

		case "Y", "y":
			result = result.AddDate(amount, 0, 0)

		default:
			return nil, fmt.Errorf("unsupported time unit: %s", interval.unit)
		}

	}

	switch expression.rounding {
	case "":
		// do nothing
	case "d":
		result = time.Date(result.Year(), result.Month(), result.Day(), 0, 0, 0, 0, result.Location())
	case "w":
		weekday := int(result.Weekday())
		result = result.AddDate(0, 0, -weekday)
		result = time.Date(result.Year(), result.Month(), result.Day(), 0, 0, 0, 0, result.Location())
	case "M":
		result = time.Date(result.Year(), result.Month(), 1, 0, 0, 0, 0, result.Location())
	case "Y":
		result = time.Date(result.Year(), 1, 1, 0, 0, 0, 0, result.Location())

	default:
		return nil, fmt.Errorf("unsupported rounding unit: %s", expression.rounding)
	}

	fmt.Println("result", result)
	return model.NewLiteralSingleQuoteString(result.Format(format)), nil
}
