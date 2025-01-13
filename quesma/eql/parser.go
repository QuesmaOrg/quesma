// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package eql

import (
	"fmt"
	"github.com/QuesmaOrg/quesma/quesma/eql/parser"
	"github.com/antlr4-go/antlr/v4"
)

//  antlr -Dlanguage=Go  -package parser quesma/eql/parser/EQL.g4

type CustomSyntaxError struct {
	line, column int
	msg          string
}

func (c *CustomSyntaxError) Error() string {
	return fmt.Sprintf("line %d:%d %s", c.line, c.column, c.msg)
}

type CustomErrorListener struct {
	Errors []error
}

func (c *CustomErrorListener) SyntaxError(recognizer antlr.Recognizer, offendingSymbol interface{}, line, column int, msg string, e antlr.RecognitionException) {
	c.Errors = append(c.Errors, &CustomSyntaxError{
		line:   line,
		column: column,
		msg:    msg,
	})
}

func (c *CustomErrorListener) ReportAmbiguity(recognizer antlr.Parser, dfa *antlr.DFA, startIndex, stopIndex int, exact bool, ambigAlts *antlr.BitSet, configs *antlr.ATNConfigSet) {
	c.Errors = append(c.Errors, fmt.Errorf("ReportAmbiguity NOT implemented"))
}
func (c *CustomErrorListener) ReportAttemptingFullContext(recognizer antlr.Parser, dfa *antlr.DFA, startIndex, stopIndex int, conflictingAlts *antlr.BitSet, configs *antlr.ATNConfigSet) {
	c.Errors = append(c.Errors, fmt.Errorf("ReportAttemptingFullContext NOT implemented"))
}
func (c *CustomErrorListener) ReportContextSensitivity(recognizer antlr.Parser, dfa *antlr.DFA, startIndex, stopIndex, prediction int, configs *antlr.ATNConfigSet) {
	c.Errors = append(c.Errors, fmt.Errorf("ReportContextSensitivity NOT implemented"))
}

type EQL struct {
	Errors []error
}

func NewEQL() *EQL {
	return &EQL{}
}

func (s *EQL) Parse(query string) (parser.IQueryContext, error) {

	errorListener := &CustomErrorListener{}

	input := antlr.NewInputStream(query)

	lexer := parser.NewEQLLexer(input)
	lexer.RemoveErrorListeners()
	lexer.AddErrorListener(errorListener)

	stream := antlr.NewCommonTokenStream(lexer, 0)

	eqlParser := parser.NewEQLParser(stream)
	eqlParser.RemoveErrorListeners()
	eqlParser.AddErrorListener(errorListener)

	ast := eqlParser.Query()

	s.Errors = errorListener.Errors

	if len(s.Errors) > 0 {
		return nil, fmt.Errorf("parse error: count=%d: %v", len(s.Errors), s.Errors) // FIXME multierror here
	}

	return ast, nil
}

func (s *EQL) IsSupported(ast parser.IQueryContext) bool {
	return ast.SimpleQuery() != nil && len(ast.AllPipe()) == 0
}
