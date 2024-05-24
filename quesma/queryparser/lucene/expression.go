package lucene

import (
	"mitmproxy/quesma/logger"
	wc "mitmproxy/quesma/queryparser/where_clause"
)

func (p *luceneParser) BuildWhereStatement() wc.Statement {
	for len(p.tokens) > 0 {
		p.WhereStatement = p.buildWhereStatement(true)
	}
	if p.WhereStatement == nil {
		p.WhereStatement = wc.NewLiteral("true")
	}
	return p.WhereStatement
}

// LeafStatement is a smallest part of a query that can be translated into SQL,
// e.g. "title:abc", or "abc", or "title:(abc OR def)".
func newLeafStatement(fieldNames []string, value value) wc.Statement {
	if len(fieldNames) == 0 {
		return wc.NewLiteral("false")
	}

	var newStatement wc.Statement
	if len(fieldNames) > 0 {
		newStatement = value.toStatement(fieldNames[0])
		for _, fieldName := range fieldNames[1:] {
			newStatement = wc.NewInfixOp(newStatement, "OR", value.toStatement(fieldName))
		}
	}
	if len(fieldNames) == 1 {
		return value.toStatement(fieldNames[0])
	}
	return newStatement
}

var invalidStatement = wc.NewLiteral("false")

// buildWhereStatement builds a WHERE statement from the tokens.
// During parsing, we only keep one expression, because we're combining leafExpressions into
// a tree of expressions. We keep the lastExpression to combine it with the next one.
// E.g. "title:abc AND text:def" is parsed into andExpression(title:abc, text:def)".
func (p *luceneParser) buildWhereStatement(addDefaultOperator bool) wc.Statement {
	tok := p.tokens[0]
	p.tokens = p.tokens[1:]
	var currentStatement wc.Statement
	switch currentToken := tok.(type) {
	case fieldNameToken:
		if len(p.tokens) <= 1 {
			logger.Error().Msgf("invalid expression, missing value, tokens: %v", p.tokens)
			p.tokens = p.tokens[:0]
			return invalidStatement
		}
		if _, isNextTokenSeparator := p.tokens[0].(separatorToken); !isNextTokenSeparator {
			logger.Error().Msgf("invalid expression, missing separator, tokens: %v", p.tokens)
			return invalidStatement
		}
		p.tokens = p.tokens[1:]
		currentStatement = newLeafStatement([]string{currentToken.fieldName}, p.buildValue([]value{}, 0))
	case separatorToken:
		currentStatement = newLeafStatement(
			p.defaultFieldNames,
			p.buildValue([]value{}, 0),
		)
	case termToken:
		currentStatement = newLeafStatement(p.defaultFieldNames, newTermValue(currentToken.term))
	case andToken:
		return wc.NewInfixOp(p.WhereStatement, "AND", p.buildWhereStatement(false))
	case orToken:
		return wc.NewInfixOp(p.WhereStatement, "OR", p.buildWhereStatement(false))
	case notToken:
		latterExp := p.buildWhereStatement(false)
		currentStatement = wc.NewPrefixOp("NOT", []wc.Statement{latterExp})
	case leftParenthesisToken:
		currentStatement = newLeafStatement(p.defaultFieldNames, p.buildValue([]value{}, 1))
	default:
		logger.Error().Msgf("buildExpression: invalid expression, unexpected token: %#v, tokens: %v", currentToken, p.tokens)
		return invalidStatement
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
