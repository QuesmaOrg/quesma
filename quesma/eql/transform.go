package eql

import (
	"fmt"
	"mitmproxy/quesma/eql/transform"
)

type Transformer struct {
	FieldNameTranslator func(*transform.Symbol) (*transform.Symbol, error)
}

func NewTransformer() *Transformer {
	return &Transformer{
		// by default it's AS IS translation
		FieldNameTranslator: func(s *transform.Symbol) (*transform.Symbol, error) {
			return s, nil
		},
	}
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
	transOp := &transform.ClickhouseTransformer{}
	exp = exp.Accept(transOp).(transform.Exp)

	if len(transOp.Errors) > 0 {
		return "", fmt.Errorf("transforming opertators failed: errors: count=%d message: %v", len(transOp.Errors), transOp.Errors)
	}

	transFieldName := &transform.FieldNameTransformer{
		Translate: t.FieldNameTranslator,
	}
	exp = exp.Accept(transFieldName).(transform.Exp)
	if len(transFieldName.Errors) > 0 {
		return "", fmt.Errorf("transforming field names failed: errors: count=%d message: %v", len(transFieldName.Errors), transFieldName.Errors)
	}

	// 6. Render the expression as WHERE clause
	// TODO errors while rendering ?
	// TODO add configuration for renderer
	renderer := &transform.Renderer{}
	whereClause := exp.Accept(renderer).(string)

	return whereClause, nil
}
