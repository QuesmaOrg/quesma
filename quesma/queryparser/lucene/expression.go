package lucene

import (
	"mitmproxy/quesma/logger"
	"strings"
)

// expression is an interface representing a fully parsed (part of) a Lucene query.
// leafExpression is a smallest part of a query that can be translated into SQL,
// e.g. "title:abc", or "abc", or "title:(abc OR def)".
// Expression can be only a part of the query, as e.g. for a query: "title:abc AND text:def",
// there are two leftExpressions: "title:abc" and "text:def".
//
// During parsing, we only keep one expression, because we're combining leafExpression into
// a tree of expressions. We keep the lastExpression to combine it with the next one.
// E.g. "title:abc AND text:def" is parsed into andExpression(title:abc, text:def)".

type expression interface {
	toSQL() string
}

type andExpression struct {
	left  expression
	right expression
}

func newAndExpression(left, right expression) andExpression {
	return andExpression{left: left, right: right}
}

func (e andExpression) toSQL() string {
	return "(" + e.left.toSQL() + " AND " + e.right.toSQL() + ")"
}

type orExpression struct {
	left  expression
	right expression
}

func newOrExpression(left, right expression) orExpression {
	return orExpression{left: left, right: right}
}

func (e orExpression) toSQL() string {
	return "(" + e.left.toSQL() + " OR " + e.right.toSQL() + ")"
}

type notExpression struct {
	expr expression
}

func newNotExpression(expr expression) notExpression {
	return notExpression{expr: expr}
}

func (e notExpression) toSQL() string {
	return "NOT (" + e.expr.toSQL() + ")"
}

type leafExpression struct {
	fieldNames []string // empty string in query means default field(s). They'll be present here.
	value      value
}

func newLeafExpression(fieldNames []string, value value) leafExpression {
	return leafExpression{fieldNames: fieldNames, value: value}
}

func (e leafExpression) toSQL() string {
	if len(e.fieldNames) == 0 {
		return "false"
	}

	var sql strings.Builder
	for i, fieldName := range e.fieldNames {
		if i > 0 {
			sql.WriteString(" OR ")
		}
		sql.WriteString(e.value.toSQL(fieldName))
	}
	if len(e.fieldNames) == 1 {
		return sql.String()
	}
	return "(" + sql.String() + ")"
}

type invalidExpression struct {
}

func newInvalidExpression() invalidExpression {
	return invalidExpression{}
}

func (e invalidExpression) toSQL() string {
	return "false"
}

// buildExpression builds an expression tree from p.tokens
// Called only when p.tokens is not empty.
func (p *luceneParser) buildExpression(addDefaultOperator bool) expression {
	tok := p.tokens[0]
	p.tokens = p.tokens[1:]
	var currentExpression expression
	switch currentToken := tok.(type) {
	case fieldNameToken:
		if len(p.tokens) <= 1 {
			logger.Error().Msgf("invalid expression, missing value, tokens: %v", p.tokens)
			p.tokens = p.tokens[:0]
			return newInvalidExpression()
		}
		if _, isNextTokenSeparator := p.tokens[0].(separatorToken); !isNextTokenSeparator {
			logger.Error().Msgf("invalid expression, missing separator, tokens: %v", p.tokens)
			return newInvalidExpression()
		}
		p.tokens = p.tokens[1:]
		currentExpression = newLeafExpression(
			[]string{currentToken.fieldName},
			p.buildValue([]value{}, 0),
		)
	case separatorToken:
		currentExpression = newLeafExpression(
			p.defaultFieldNames,
			p.buildValue([]value{}, 0),
		)
	case termToken:
		currentExpression = newLeafExpression(
			p.defaultFieldNames,
			newTermValue(currentToken.term),
		)
	case andToken:
		return newAndExpression(p.lastExpression, p.buildExpression(false))
	case orToken:
		return newOrExpression(p.lastExpression, p.buildExpression(false))
	case notToken:
		currentExpression = newNotExpression(p.buildExpression(false))
	case leftParenthesisToken:
		return newLeafExpression(
			p.defaultFieldNames,
			p.buildValue([]value{}, 1),
		)
	default:
		logger.Error().Msgf("buildExpression: invalid expression, unexpected token: %#v, tokens: %v", currentToken, p.tokens)
		return newInvalidExpression()
	}
	if !addDefaultOperator || p.lastExpression == nil {
		return currentExpression
	}
	switch currentExpression.(type) {
	case notExpression:
		return newAndExpression(p.lastExpression, currentExpression)
	default:
		return newOrExpression(p.lastExpression, currentExpression)
	}
}
