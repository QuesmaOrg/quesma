// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package painless

import (
	"errors"
	"fmt"
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

func ParsePainlessV2ScriptToExpr(s string) (model.Expr, error) {
	ast, err := parse(s)

	if err != nil { // pass NULL if we can't parse it
		logger.Error().Err(err).Msgf("failed to parse painless script '%s'", s)
		return model.NewLiteral("NULL"), err
	}

	visitor := NewPainlessTransformer()

	result := ast.Accept(visitor)

	if len(visitor.Errors) > 0 {
		return model.NewLiteral("NULL"), errors.Join(visitor.Errors...)
	}

	if resultExpr, ok := result.(model.Expr); ok {
		return resultExpr, nil
	} else {
		return model.NewLiteral("NULL"), fmt.Errorf("unexpected result type '%v'", result)
	}
}

func parse(painlessScript string) (painless_antlr.IStatementContext, error) {

	errorListener := &PainlessErrorListener{}

	input := antlr.NewInputStream(painlessScript)
	lexer := painless_antlr.NewPainlessLexer(input)
	lexer.RemoveErrorListeners()
	lexer.AddErrorListener(errorListener)

	stream := antlr.NewCommonTokenStream(lexer, 0)
	parser := painless_antlr.NewPainlessParser(stream)
	parser.RemoveErrorListeners()
	parser.AddErrorListener(errorListener)

	ast := parser.Statement()

	fmt.Println("JM", ast.ToStringTree(nil, parser))

	if len(errorListener.Errors) > 0 {
		return nil, errors.Join(errorListener.Errors...)
	}

	return ast, nil
}
