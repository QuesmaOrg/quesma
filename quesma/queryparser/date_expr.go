package queryparser

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

type timeUnit string

var timeUnits = []timeUnit{"m", "s", "h", "d", "w", "M", "y"}

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
			number = string(expr[index])
		case OPERATOR_SUB:
			number = string(expr[index])

		case ROUNDING:
			rounding = true

		case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':

			number = number + string(expr[index])

		case 'm', 's', 'h', 'd', 'w', 'M', 'y', 'Y':

			if rounding {
				result.rounding = timeUnit(expr[index])

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
	RenderSQL(expression *DateMathExpression) (string, error)
}

type DateMathAsClickhouseIntervals struct{}

func (b *DateMathAsClickhouseIntervals) RenderSQL(expression *DateMathExpression) (string, error) {

	var result string

	result = "now()"

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
			return "", fmt.Errorf("invalid time unit: %s", interval.unit)
		}

		result = fmt.Sprintf("%s(%s, INTERVAL %d %s)", op, result, amount, unit)
	}

	const defaultRounding = 'd'
	var roundingFunction = map[string]string{
		"d": "toStartOfDay",
		"w": "toStartOfWeek",
		"M": "toStartOfMonth",
		"Y": "toStartOfYear",
	}

	if expression.rounding != "" {

		fn := roundingFunction["d"]
		if function, ok := roundingFunction[string(expression.rounding)]; ok {
			fn = function
		}
		result = fmt.Sprintf("%s(%s)", fn, result)

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
