package lucene

import (
	"mitmproxy/quesma/logger"
	wc "mitmproxy/quesma/queryparser/where_clause"
	"strings"
)

// expression is an interface representing a fully parsed (part of) Lucene query.
// leafExpression is a smallest part of a query that can be translated into SQL,
// e.g. "title:abc", or "abc", or "title:(abc OR def)".
// Expression can be only a part of the query, as e.g. for a query: "title:abc AND text:def",
// there are two leftExpressions: "title:abc" and "text:def".
//
// During parsing, we only keep one expression, because we're combining leafExpressions into
// a tree of expressions. We keep the lastExpression to combine it with the next one.
// E.g. "title:abc AND text:def" is parsed into andExpression(title:abc, text:def)".

type expression interface {
	toSQL() string
	toStatement() wc.Statement
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

func (e andExpression) toStatement() wc.Statement {
	return wc.NewInfixOp(e.left.toStatement(), "AND", e.right.toStatement())
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

func (e orExpression) toStatement() wc.Statement {
	return wc.NewInfixOp(e.left.toStatement(), "OR", e.right.toStatement())
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

func (e notExpression) toStatement() wc.Statement {
	return wc.NewPrefixOp("NOT", []wc.Statement{e.expr.toStatement()})
}

type leafExpression struct {
	fieldNames []string // empty string in query means default field(s). They'll be present here.
	value      value
}

func newLeafExpression(fieldNames []string, value value) leafExpression {
	return leafExpression{fieldNames: fieldNames, value: value}
}

func (e leafExpression) toStatement() wc.Statement {
	if len(e.fieldNames) == 0 {
		return wc.NewLiteral("false")
	}

	var newStatement wc.Statement
	if len(e.fieldNames) > 0 {
		newStatement = e.value.toStatement(e.fieldNames[0])
		for _, fieldName := range e.fieldNames[1:] {
			newStatement = wc.NewInfixOp(newStatement, "OR", e.value.toStatement(fieldName))
		}
	}
	if len(e.fieldNames) == 1 {
		return e.value.toStatement(e.fieldNames[0])
	}
	return newStatement
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

func (e invalidExpression) toStatement() wc.Statement {
	return wc.NewLiteral("false")
}

var invalidExpressionInstance = newInvalidExpression()

// buildExpression builds an expression tree from p.tokens
// Called only when p.tokens is not empty.
func (p *luceneParser) buildExpression(addDefaultOperator bool) wc.Statement {
	tok := p.tokens[0]
	p.tokens = p.tokens[1:]
	var currentStatement wc.Statement
	switch currentToken := tok.(type) {
	case fieldNameToken:
		if len(p.tokens) <= 1 {
			logger.Error().Msgf("invalid expression, missing value, tokens: %v", p.tokens)
			p.tokens = p.tokens[:0]
			return invalidExpressionInstance.toStatement()
		}
		if _, isNextTokenSeparator := p.tokens[0].(separatorToken); !isNextTokenSeparator {
			logger.Error().Msgf("invalid expression, missing separator, tokens: %v", p.tokens)
			return invalidExpressionInstance.toStatement()
		}
		p.tokens = p.tokens[1:]
		currentStatement = newLeafExpression(
			[]string{currentToken.fieldName},
			p.buildValue([]value{}, 0),
		).toStatement()
	case separatorToken:
		currentStatement = newLeafExpression(
			p.defaultFieldNames,
			p.buildValue([]value{}, 0),
		).toStatement()
	case termToken:
		currentStatement = newLeafExpression(
			p.defaultFieldNames,
			newTermValue(currentToken.term),
		).toStatement()
	case andToken:
		return wc.NewInfixOp(p.WhereStatement, "AND", p.buildExpression(false))
	case orToken:
		return wc.NewInfixOp(p.WhereStatement, "OR", p.buildExpression(false))
	case notToken:
		latterExp := p.buildExpression(false)
		currentStatement = wc.NewPrefixOp("NOT", []wc.Statement{latterExp})
	case leftParenthesisToken:
		currentStatement = newLeafExpression(
			p.defaultFieldNames,
			p.buildValue([]value{}, 1),
		).toStatement()
	default:
		logger.Error().Msgf("buildExpression: invalid expression, unexpected token: %#v, tokens: %v", currentToken, p.tokens)
		return invalidExpressionInstance.toStatement()
	}
	if !addDefaultOperator || p.WhereStatement == nil {
		return currentStatement
	}
	switch stmt := currentStatement.(type) {
	case *wc.PrefixOp:
		if stmt.Op == "NOT" {
			return wc.NewInfixOp(p.WhereStatement, "AND", currentStatement)
		} else {
			return wc.NewInfixOp(p.WhereStatement, "OR", currentStatement)
		}
	default:
		return wc.NewInfixOp(p.WhereStatement, "OR", currentStatement)
	}
}
