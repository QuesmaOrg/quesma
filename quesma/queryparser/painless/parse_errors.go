// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package painless

import (
	"fmt"
	"github.com/antlr4-go/antlr/v4"
)

type PainlessSyntaxError struct {
	line, column int
	msg          string
}

func (c *PainlessSyntaxError) Error() string {
	return fmt.Sprintf("line %d:%d %s", c.line, c.column, c.msg)
}

type PainlessErrorListener struct {
	Errors []error
}

func (c *PainlessErrorListener) SyntaxError(recognizer antlr.Recognizer, offendingSymbol interface{}, line, column int, msg string, e antlr.RecognitionException) {
	c.Errors = append(c.Errors, &PainlessSyntaxError{
		line:   line,
		column: column,
		msg:    msg,
	})
}

func (c *PainlessErrorListener) ReportAmbiguity(recognizer antlr.Parser, dfa *antlr.DFA, startIndex, stopIndex int, exact bool, ambigAlts *antlr.BitSet, configs *antlr.ATNConfigSet) {
	c.Errors = append(c.Errors, fmt.Errorf("ReportAmbiguity NOT implemented"))
}
func (c *PainlessErrorListener) ReportAttemptingFullContext(recognizer antlr.Parser, dfa *antlr.DFA, startIndex, stopIndex int, conflictingAlts *antlr.BitSet, configs *antlr.ATNConfigSet) {
	c.Errors = append(c.Errors, fmt.Errorf("ReportAttemptingFullContext NOT implemented"))
}
func (c *PainlessErrorListener) ReportContextSensitivity(recognizer antlr.Parser, dfa *antlr.DFA, startIndex, stopIndex, prediction int, configs *antlr.ATNConfigSet) {
	c.Errors = append(c.Errors, fmt.Errorf("ReportContextSensitivity NOT implemented"))
}
