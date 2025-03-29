// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package lucene

import (
	"github.com/QuesmaOrg/quesma/platform/logger"
	"github.com/QuesmaOrg/quesma/platform/model"
)

var invalidStatement = model.FalseExpr

func (p *luceneParser) BuildWhereStatement() model.Expr {
	for len(p.tokens) > 0 {
		p.WhereStatement = p.buildWhereStatement(true)
	}
	if p.WhereStatement == nil {
		return model.TrueExpr
	}
	return p.WhereStatement
}

// LeafStatement is the smallest part of a query that can be translated into SQL,
// e.g. "title:abc", or "abc", or "title:(abc OR def)".
func newLeafStatement(fieldNames []string, value value) model.Expr {
	if len(fieldNames) == 0 {
		return invalidStatement
	}

	var newStatement model.Expr
	if len(fieldNames) > 0 {
		newStatement = value.toExpression(fieldNames[0])
		for _, fieldName := range fieldNames[1:] {
			newStatement = model.NewInfixExpr(newStatement, "OR", value.toExpression(fieldName))
		}
	}
	if len(fieldNames) == 1 {
		return value.toExpression(fieldNames[0])
	}
	return newStatement
}

// buildWhereStatement builds a WHERE statement from the tokens.
// During parsing, we only keep one expression, because we're combining leafExpressions into
// a tree of expressions. We keep the lastExpression to combine it with the next one.
// E.g. "title:abc AND text:def" is parsed into andExpression(title:abc, text:def)".
func (p *luceneParser) buildWhereStatement(addDefaultOperator bool) model.Expr {
	if len(p.tokens) == 0 {
		return invalidStatement
	}

	tok := p.tokens[0]
	p.tokens = p.tokens[1:]
	var currentStatement model.Expr

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
		if name, resolved := p.currentSchema.ResolveField(currentToken.fieldName); resolved {
			currentStatement = newLeafStatement([]string{name.InternalPropertyName.AsString()}, p.buildValue([]value{}, 0))
		} else {
			currentStatement = newLeafStatement([]string{currentToken.fieldName}, p.buildValue([]value{}, 0))
		}
	case separatorToken:
		currentStatement = newLeafStatement(
			p.defaultFieldNames,
			p.buildValue([]value{}, 0),
		)
	case termToken:
		currentStatement = newLeafStatement(p.defaultFieldNames, newTermValue(currentToken.term))
	case andToken:
		return model.NewInfixExpr(p.WhereStatement, "AND", p.buildWhereStatement(false))
	case orToken:
		return model.NewInfixExpr(p.WhereStatement, "OR", p.buildWhereStatement(false))
	case notToken:
		latterExp := p.buildWhereStatement(false)
		currentStatement = model.NewPrefixExpr("NOT", []model.Expr{latterExp})
	case existsToken:
		fieldName, ok := p.buildValue([]value{}, 0).(termValue)
		if !ok {
			logger.Error().Msgf("buildExpression: invalid expression, unexpected token: %#v, tokens: %v", currentToken, p.tokens)
			return invalidStatement
		}
		currentStatement = model.NewInfixExpr(model.NewColumnRef(fieldName.term), " IS NOT ", model.NullExpr)
	case leftParenthesisToken:
		currentStatement = model.NewParenExpr(p.buildWhereStatement(false))
	case rightParenthesisToken:
		if p.WhereStatement == nil {
			return invalidStatement
		}
		return p.WhereStatement
	default:
		logger.Error().Msgf("buildExpression: invalid expression, unexpected token: %#v, tokens: %v", currentToken, p.tokens)
		return invalidStatement
	}

	if !addDefaultOperator || p.WhereStatement == nil {
		return currentStatement
	}

	switch stmt := currentStatement.(type) {
	case model.PrefixExpr:
		if stmt.Op == "NOT" {
			return model.NewInfixExpr(p.WhereStatement, "AND", currentStatement)
		} else {
			return model.NewInfixExpr(p.WhereStatement, "OR", currentStatement)
		}
	default:
		return model.NewInfixExpr(p.WhereStatement, "OR", currentStatement)
	}
}
