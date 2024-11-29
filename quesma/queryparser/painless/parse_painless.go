// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package painless

import (
	"errors"
	"github.com/antlr4-go/antlr/v4"
	"quesma/logger"
	"quesma/model"
	painless_antlr "quesma/queryparser/painless/antlr"
)

func ParsePainlessScriptToExpr(s string) model.Expr {
	// TODO: add a real parser here
	if s == "emit(doc['timestamp'].value.getHour());" {
		return model.NewFunction(model.DateHourFunction, model.NewColumnRef(model.TimestampFieldName))
	}

	// harmless default
	return model.NewLiteral("NULL")
}

func NewParsePainlessScriptToExpr(s string) (model.Expr, error) {
	ast, err := parse(s)

	if err != nil { // pass NULL if we can't parse it
		logger.Error().Err(err).Msgf("failed to parse painless script '%s'", s)
		return model.NewLiteral("NULL"), err
	}

	visitor := NewPainlessTransformer()

	resultExpr := ast.Accept(visitor).(model.Expr)

	if len(visitor.Errors) > 0 {
		return model.NewLiteral("NULL"), errors.Join(visitor.Errors...)
	}

	return resultExpr, nil
}

func parse(painlessScript string) (painless_antlr.IDeclarationContext, error) {

	errorListener := &PainlessErrorListener{}

	input := antlr.NewInputStream(painlessScript)
	lexer := painless_antlr.NewPainlessLexer(input)
	lexer.RemoveErrorListeners()
	lexer.AddErrorListener(errorListener)

	stream := antlr.NewCommonTokenStream(lexer, 0)
	parser := painless_antlr.NewPainlessParser(stream)
	parser.RemoveErrorListeners()
	parser.AddErrorListener(errorListener)

	ast := parser.Declaration()
	if len(errorListener.Errors) > 0 {
		return nil, errors.Join(errorListener.Errors...)
	}

	return ast, nil
}
