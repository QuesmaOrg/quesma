package lucene

import (
	"fmt"
	"mitmproxy/quesma/logger"
	"slices"
	"strconv"
	"strings"
)

// value is a part of an expression, representing what we query for (expression without fields for which we query).
// e.g. for expression "abc", value is "abc", for expression "title:abc", value is also "abc",
// and for expression "title:(abc OR (def AND ghi))", value is "(abc OR (def AND ghi))".

var wildcards = map[rune]rune{
	'*': '%',
	'?': '_',
}

var specialCharacters = []rune{'+', '-', '!', '(', ')', '{', '}', '[', ']', '^', '"', '~', '*', '?', ':', '\\'} // they can be escaped in query string

type value interface {
	toSQL(fieldName string) string
}

type termValue struct {
	term string
}

func newTermValue(term string) termValue {
	return termValue{term: term}
}

func (v termValue) toSQL(fieldName string) string {
	termAsStringToClickhouse, wildcardsExist := v.transformSpecialCharacters()

	if alreadyQuoted(v.term) {
		termAsStringToClickhouse = termAsStringToClickhouse[1 : len(termAsStringToClickhouse)-1]
	}
	if wildcardsExist {
		return strconv.Quote(fieldName) + " ILIKE '" + termAsStringToClickhouse + "'"
	} else {
		return strconv.Quote(fieldName) + " = '" + termAsStringToClickhouse + "'"
	}
}

// transformSpecialCharacters transforms special characters in term to their SQL equivalents.
// - Removes escaping, so \[special character] -> [special character]
// - * and ? are transformed to % and _
func (v termValue) transformSpecialCharacters() (termFinal string, wildcardsExist bool) {
	strAsRunes := []rune(v.term)
	var returnTerm strings.Builder
	for i := 0; i < len(strAsRunes); i++ {
		curRune := strAsRunes[i]
		replacement, isWildcard := wildcards[curRune]
		if isWildcard {
			wildcardsExist = true
			returnTerm.WriteRune(replacement)
			continue
		}

		if i == len(strAsRunes)-1 {
			returnTerm.WriteRune(curRune)
			continue
		}

		nextRune := strAsRunes[i+1]
		if curRune == escapeCharacter && slices.Contains(specialCharacters, nextRune) {
			returnTerm.WriteRune(nextRune)
			i++
		} else {
			returnTerm.WriteRune(curRune)
		}
	}
	return returnTerm.String(), wildcardsExist
}

type rangeValue struct {
	lowerBound          any  // unbounded (nil) means no lower bound
	upperBound          any  // unbounded (nil) means no upper bound
	lowerBoundInclusive bool // true <=> "gte", false <=> "gt"
	upperBoundInclusive bool // true <=> "lte", false <=> "lt"
}

// value of rangeValue's lowerBound/upperBound in case of unbounded range
var unbounded any = nil

func newRangeValue(lowerBound any, lowerBoundInclusive bool, upperBound any, upperBoundInclusive bool) rangeValue {
	return rangeValue{lowerBound: lowerBound, upperBound: upperBound, lowerBoundInclusive: lowerBoundInclusive, upperBoundInclusive: upperBoundInclusive}
}

func newRangeValueGte(lowerBound any) rangeValue {
	return newRangeValue(lowerBound, true, unbounded, false)
}

func newRangeValueGt(lowerBound any) rangeValue {
	return newRangeValue(lowerBound, false, unbounded, false)
}

func newRangeValueLte(upperBound any) rangeValue {
	return newRangeValue(unbounded, false, upperBound, true)
}

func newRangeValueLt(upperBound any) rangeValue {
	return newRangeValue(unbounded, false, upperBound, false)
}

// totallyUnbounded returns true <=> the range is [* TO *] (always true unless field is null)
func (v rangeValue) totallyUnbounded() bool {
	return v.lowerBound == unbounded && v.upperBound == unbounded
}

func (v rangeValue) toSQL(fieldName string) string {
	if v.totallyUnbounded() {
		return strconv.Quote(fieldName) + " IS NOT NULL"
	}

	var left, right, operator string
	if v.lowerBound != unbounded {
		if v.lowerBoundInclusive {
			operator = ">="
		} else {
			operator = ">"
		}
		left = fmt.Sprintf("%s %s '%v'", strconv.Quote(fieldName), operator, v.lowerBound)
	}
	if v.upperBound != unbounded {
		if v.upperBoundInclusive {
			operator = "<="
		} else {
			operator = "<"
		}
		right = fmt.Sprintf("%s %s '%v'", strconv.Quote(fieldName), operator, v.upperBound)
	}
	if left != "" && right != "" {
		return "(" + left + " AND " + right + ")"
	}
	return left + right
}

type andValue struct {
	left  value
	right value
}

func newAndValue(left, right value) andValue {
	return andValue{left: left, right: right}
}

func (v andValue) toSQL(fieldName string) string {
	return "(" + v.left.toSQL(fieldName) + " AND " + v.right.toSQL(fieldName) + ")"
}

type orValue struct {
	left  value
	right value
}

func newOrValue(left, right value) orValue {
	return orValue{left: left, right: right}
}

func (v orValue) toSQL(fieldName string) string {
	return "(" + v.left.toSQL(fieldName) + " OR " + v.right.toSQL(fieldName) + ")"
}

type notValue struct {
	value value
}

func newNotValue(value value) notValue {
	return notValue{value: value}
}

func (v notValue) toSQL(fieldName string) string {
	return "NOT (" + v.value.toSQL(fieldName) + ")"
}

type invalidValue struct {
}

func newInvalidValue() invalidValue {
	return invalidValue{}
}

func (v invalidValue) toSQL(fieldName string) string {
	return "false"
}

// buildValue builds a value from p.tokens
// stack is a stack of previous values from the last ( (opening parenthesis)
// parenthesisLevel == 0 <=> we're not inside parenthesis, 1 otherwise
func (p *luceneParser) buildValue(stack []value, parenthesisLevel int) value {
	for {
		if len(p.tokens) == 0 {
			logger.Error().Msgf("invalid Lucene expression, missing value, stack: %v, parenthesisLevel: %v", stack, parenthesisLevel)
			return newInvalidValue()
		}

		tok := p.tokens[0]
		p.tokens = p.tokens[1:]

		// let's add the default OR separator, unless last token wasn't already an operator
		var addOrSeparator bool
		if _, currentTokenIsRightParenthesis := tok.(rightParenthesisToken); !currentTokenIsRightParenthesis && len(stack) > 0 {
			switch stack[len(stack)-1].(type) {
			case andValue, orValue, notValue:
				addOrSeparator = false
			default:
				addOrSeparator = true
			}
		}

		switch currentToken := tok.(type) {
		case leftParenthesisToken:
			stack = append(stack, p.buildValue([]value{}, 1))
		case rightParenthesisToken:
			if parenthesisLevel == 0 {
				logger.Error().Msgf("invalid expression, unexpected right parenthesis, tokens: %v", p.tokens)
				return newInvalidValue()
			}
			if len(stack) == 0 {
				logger.Error().Msgf("invalid expression, can't have ) with an empty stack, tokens: %v", p.tokens)
				return newInvalidValue()
			}
			for len(stack) > 1 {
				stack = orLastTwoValues(stack)
			}
			return stack[0]
		case andToken:
			addOrSeparator = false
			if len(stack) == 0 {
				logger.Error().Msgf("invalid expression, can't have AND with an empty stack, tokens: %v", p.tokens)
				return newInvalidValue()
			}
			stack = append(stack, p.buildValue([]value{}, 0))
			and := newAndValue(stack[len(stack)-2], stack[len(stack)-1])
			stack = stack[:len(stack)-2]
			stack = append(stack, and)
		case orToken:
			addOrSeparator = false
			if len(stack) == 0 {
				logger.Error().Msgf("invalid expression, can't have OR with an empty stack, tokens: %v", p.tokens)
				return newInvalidValue()
			}
			stack = append(stack, p.buildValue([]value{}, 0))
			stack = orLastTwoValues(stack)
		case notToken:
			addOrSeparator = false
			stack = append(stack, newNotValue(p.buildValue([]value{}, 0)))
		case termToken:
			stack = append(stack, newTermValue(currentToken.term))
		case rangeToken:
			stack = append(stack, currentToken.rangeValue)
		default:
			logger.Error().Msgf("invalid expression, unexpected token %v, tokens: %v", currentToken, p.tokens)
			return newInvalidValue()
		}

		if addOrSeparator {
			stack = orLastTwoValues(stack)
		}

		if parenthesisLevel == 0 {
			return stack[0]
		}
	}
}

// orLastTwoValues pops the last two values from the stack, ORs them and pushes the result back to the stack.
func orLastTwoValues(stack []value) []value {
	or := newOrValue(stack[len(stack)-2], stack[len(stack)-1])
	stack = stack[:len(stack)-2]
	return append(stack, or)
}

// alreadyQuoted returns true <=> len(s) >= 2 && s is already quoted (e.g. "abc")
func alreadyQuoted(s string) bool {
	return len(s) >= 2 && s[0] == '"' && s[len(s)-1] == '"'
}
