package queryparser

import (
	"errors"
	"mitmproxy/quesma/logger"
	"unicode"
)

type timeUnit string

type interval struct {
	amount int
	unit   timeUnit
}

type dateMathExpression struct {
	intervals []interval
	rounding  timeUnit
}

func parseTimeUnit(timeUnit string) (string, error) {
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

func tokenizeDateMathExpr(expr string) []string {
	tokens := make([]string, 0)
	const NOW_LENGTH = 3
	const OPERATOR_ADD = '+'
	const OPERATOR_SUB = '-'
	for index := 0; index < len(expr); index++ {
		// This is now keyword
		if expr[index] == 'n' {
			if len(expr) < NOW_LENGTH {
				return tokens
			}
			index = index + NOW_LENGTH
			token := expr[:index]
			if token != "now" {
				return tokens
			}
			tokens = append(tokens, token)
		}
		if index < len(expr) && (expr[index] == OPERATOR_ADD || expr[index] == OPERATOR_SUB) {
			token := expr[index : index+1]
			tokens = append(tokens, token)
			index = index + 1
		} else {
			logger.Error().Msgf("operator expected in date math expression '%s'", expr)
			return tokens
		}
		var number string
		for ; index < len(expr)-1; index++ {
			if !unicode.IsDigit(rune(expr[index])) {
				break
			}
			if unicode.IsDigit(rune(expr[index])) {
				number = number + string(expr[index])
			}
		}
		// Check if number has been tokenized
		// correctly and if not, return tokens
		if len(number) == 0 {
			logger.Error().Msgf("number expected in date math expression '%s'", expr)
			return tokens
		}
		tokens = append(tokens, number)
		token := expr[index]
		tokens = append(tokens, string(token))
	}
	return tokens
}

func parseDateMathExpr(expr string) dateMathExpression {
	return dateMathExpression{tokens: tokenizeDateMathExpr(expr), rounding: "d"}
}

type mathExpressionBuilder interface {
	build(expression dateMathExpression) string
}

type clickhouseDateMathExpressionBuilder struct{}

func (b *clickhouseDateMathExpressionBuilder) build(expression dateMathExpression) string {
	return b.build1(expression.tokens)
}

func (builder *clickhouseDateMathExpressionBuilder) build1(tokens []string) string {
	const NEXT_OP_DISTANCE = 3
	const TIME_UNIT_DISTANCE = 2
	const TIME_AMOUNT_DISTANCE = 1
	if len(tokens) == 0 {
		return ""
	}
	tokenIndex := 0
	currentExpr := tokens[tokenIndex]
	switch currentExpr {
	case "now":
		currentExpr = "now()"
	default:
		logger.Error().Msg("unsupported date math argument")
	}
	tokenIndex = tokenIndex + 1
	for tokenIndex+TIME_UNIT_DISTANCE < len(tokens) {
		op := tokens[tokenIndex]
		switch op {
		case "+":
			op = "addDate"
		case "-":
			op = "subDate"
		}
		timeUnit, err := parseTimeUnit(tokens[tokenIndex+TIME_UNIT_DISTANCE])
		if err != nil {
			logger.Error().Msg(err.Error())
			return ""
		}
		timeAmount := tokens[tokenIndex+TIME_AMOUNT_DISTANCE]
		currentExpr = op + "(" + currentExpr + "," + " INTERVAL " + timeAmount + " " + timeUnit + ")"
		tokenIndex = tokenIndex + NEXT_OP_DISTANCE
	}
	return currentExpr
}
