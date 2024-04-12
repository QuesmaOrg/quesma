package eql

import (
	"fmt"
	"mitmproxy/quesma/eql/transform"
)

type Transformer struct {
}

func NewTransformer() *Transformer {
	return &Transformer{}
}

// It transforms EQL query to Clickhouse query (where clause).
//
// Transformation is done in several steps:
// 1. Parse EQL query
// 2. Convert EQL to Exp model
// 3. Replace operators/functions with clickhouse operators/functions
// 4. Replace the field names with clickhouse field names
// 5. Render the expression as WHERE clause

func (t *Transformer) TransformQuery(query string) (string, error) {

	// 1. parse EQL
	p := NewEQL()
	ast, err := p.Parse(query)
	if err != nil {
		return "", err
	}

	if !p.IsSupported(ast) {
		return "", fmt.Errorf("unsupported query type") // TODO proper error message
	}

	// 2. Convert EQL to Exp model
	eql2ExpTransformer := transform.NewEQLParseTreeToExpTransformer()
	var exp transform.Exp
	exp = ast.Accept(eql2ExpTransformer).(transform.Exp)
	if len(eql2ExpTransformer.Errors) > 0 {
		return "", fmt.Errorf("eql2exp conversion errors: count=%d, %v", len(eql2ExpTransformer.Errors), eql2ExpTransformer.Errors)
	}

	// exp can be null if query is empty
	// we return empty as well
	if exp == nil {
		return "", nil
	}

	// 3. Replace operators with clickhouse operators
	transOp := &transform.InfixOpTransformer{}
	exp = exp.Accept(transOp).(transform.Exp)

	if len(transOp.Errors) > 0 {
		return "", fmt.Errorf("transforming opertators failed: errors: count=%d message: %v", len(transOp.Errors), transOp.Errors)
	}

	// 4. TODO Add "functions" transformer

	// 5. TODO Add "field names" transformer

	// 6. Render the expression as WHERE clause
	// TODO errors while rendering ?
	// TODO add configuration for renderer
	renderer := &transform.Renderer{}
	whereClause := exp.Accept(renderer).(string)

	return whereClause, nil
}
