package eql

import (
	"fmt"
	"github.com/antlr4-go/antlr/v4"
	"mitmproxy/quesma/eql/parser"
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
	*antlr.DefaultErrorListener // Embed default which ensures we fit the interface
	Errors                      []error
}

func (c *CustomErrorListener) SyntaxError(recognizer antlr.Recognizer, offendingSymbol interface{}, line, column int, msg string, e antlr.RecognitionException) {
	c.Errors = append(c.Errors, &CustomSyntaxError{
		line:   line,
		column: column,
		msg:    msg,
	})
}

type EQL struct {
	Errors []error
}

func NewEQL() *EQL {
	return &EQL{}
}

func (s *EQL) Parse(query string) (parser.IQueryContext, error) {

	input := antlr.NewInputStream(query)
	lexer := parser.NewEQLLexer(input)
	stream := antlr.NewCommonTokenStream(lexer, 0)
	eqlParser := parser.NewEQLParser(stream)
	errorListener := &CustomErrorListener{}

	eqlParser.AddErrorListener(errorListener)
	ast := eqlParser.Query()

	s.Errors = errorListener.Errors

	if len(errorListener.Errors) > 0 {
		return nil, fmt.Errorf("parse error: count=%d", len(s.Errors))
	}

	return ast, nil
}

func (s *EQL) IsSupported(ast parser.IQueryContext) bool {
	return ast.SimpleQuery() != nil
}
