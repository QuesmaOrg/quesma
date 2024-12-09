// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package model

// This function is used to skip the transformation of the expression it takes two arguments (expression, transformation_name)
const skipTransformFunctionName = "__quesma_skip_transform"

func NewSkipTransformation(expr Expr, transformationName string) Expr {
	return NewFunction(skipTransformFunctionName, expr, NewLiteral(transformationName))
}

func ShouldSkipTransformation(query Expr, transformationName string) (Expr, bool) {

	var shouldSkip bool

	visitor := &BaseExprVisitor{}

	visitor.OverrideVisitFunction = func(b *BaseExprVisitor, e FunctionExpr) interface{} {
		if e.Name == skipTransformFunctionName {
			if len(e.Args) == 2 {
				if literal, ok := e.Args[1].(LiteralExpr); ok {
					if literal.Value == transformationName {
						shouldSkip = true
						return e.Args[0]
					}
				}
			}
		}

		args := b.VisitChildren(e.Args)
		return NewFunction(e.Name, args...)
	}

	res := query.Accept(visitor)

	if res, ok := res.(Expr); ok {
		return res, shouldSkip
	}

	return nil, false
}
