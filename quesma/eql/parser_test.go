// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package eql

import (
	"github.com/QuesmaOrg/quesma/quesma/eql/parser"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestEQL_ParseNoErrors(t *testing.T) {

	eqlQueries := []string{
		`any where true`,
		`process where process_name == "foo.exe"`,
		`process where process_name == "foo.exe" and process_path == "c:\\windows"`,
		`process where process_name == "cmd.exe" and process_path == "C:\\Windows\\System32\\cmd.exe" or process_command_line == "cmd.exe"`,
		`sequence [ process where foo == 1] [ process where bar == 2]`,
		`sample by foo [ bar where true ]`,
		"any where true | head 3",
		"process where ?notexistsing == true",
	}

	for _, eqlQuery := range eqlQueries {

		t.Run(eqlQuery, func(t *testing.T) {
			p := NewEQL()

			_, err := p.Parse(eqlQuery)

			assert.NoErrorf(t, err, "Error parsing %v", p.Errors)
		})
	}
}

func TestEQL_IsSupported(t *testing.T) {

	tests := []struct {
		query     string
		supported bool
	}{
		{"simple where true", true},
		{"process where true | head 3 ", false},
		{"sequence [ simple where true] [ simple where true]", false},
		{"sample by foo [ bar where true ]", false},
	}

	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			p := NewEQL()

			ast, err := p.Parse(tt.query)

			assert.NoError(t, err)
			assert.NotNil(t, ast)
			assert.Equal(t, tt.supported, p.IsSupported(ast))
		})
	}
}

type CategoryVisitor struct {
	parser.BaseEQLVisitor
	category string
}

func (v *CategoryVisitor) VisitQuery(ctx *parser.QueryContext) interface{} {
	ctx.SimpleQuery().Accept(v)
	return nil
}
func (v *CategoryVisitor) VisitSimpleQuery(ctx *parser.SimpleQueryContext) interface{} {
	ctx.Category().Accept(v)
	return nil
}

func (v *CategoryVisitor) VisitCategory(ctx *parser.CategoryContext) interface{} {

	switch {
	case ctx.ID() != nil:
		v.category = ctx.ID().GetText()
	case ctx.STRING() != nil:
		v.category = ctx.STRING().GetText()
	case ctx.ANY() != nil:
		v.category = ctx.ANY().GetText()
	default:
		v.category = "unknown"
	}
	return nil
}

func TestEQL_category(t *testing.T) {

	tests := []struct {
		query,
		category string
	}{

		{`any where true`, `any`},
		{`process where true`, `process`},
		{`"process" where true`, `"process"`},
		{`"""process foo""" where true`, `"""process foo"""`},
	}

	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			p := NewEQL()

			ast, err := p.Parse(tt.query)
			assert.NoError(t, err)
			assert.NotNil(t, ast.SimpleQuery())
			assert.Equal(t, 1, ast.SimpleQuery().Category().GetChildCount())
			v := &CategoryVisitor{}
			ast.Accept(v)
			assert.Equal(t, tt.category, v.category)
		})
	}
}
