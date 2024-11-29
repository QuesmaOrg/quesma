// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package painless

import (
	"github.com/antlr4-go/antlr/v4"
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

func parse(painlessScript string) (painless_antlr.IExpressionContext, error) {

	input := antlr.NewInputStream(painlessScript)
	lexer := painless_antlr.NewPainlessLexer(input)
	stream := antlr.NewCommonTokenStream(lexer, 0)

	parser := painless_antlr.NewPainlessParser(stream)
	ast := parser.Expression()

	return ast, nil
}
