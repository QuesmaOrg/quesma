// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package painful

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"unicode"
	"unicode/utf8"
)

var g = &grammar{
	rules: []*rule{
		{
			name: "Expr",
			pos:  position{line: 8, col: 1, offset: 123},
			expr: &choiceExpr{
				pos: position{line: 8, col: 9, offset: 131},
				alternatives: []any{
					&labeledExpr{
						pos:   position{line: 8, col: 9, offset: 131},
						label: "expr",
						expr: &ruleRefExpr{
							pos:  position{line: 8, col: 14, offset: 136},
							name: "OpExpr",
						},
					},
					&labeledExpr{
						pos:   position{line: 8, col: 23, offset: 145},
						label: "expr",
						expr: &ruleRefExpr{
							pos:  position{line: 8, col: 28, offset: 150},
							name: "MethodCall",
						},
					},
					&labeledExpr{
						pos:   position{line: 8, col: 41, offset: 163},
						label: "expr",
						expr: &ruleRefExpr{
							pos:  position{line: 8, col: 46, offset: 168},
							name: "Accessor",
						},
					},
					&labeledExpr{
						pos:   position{line: 8, col: 57, offset: 179},
						label: "expr",
						expr: &ruleRefExpr{
							pos:  position{line: 8, col: 62, offset: 184},
							name: "Doc",
						},
					},
					&labeledExpr{
						pos:   position{line: 8, col: 68, offset: 190},
						label: "expr",
						expr: &ruleRefExpr{
							pos:  position{line: 8, col: 73, offset: 195},
							name: "Emit",
						},
					},
					&actionExpr{
						pos: position{line: 8, col: 81, offset: 203},
						run: (*parser).callonExpr12,
						expr: &labeledExpr{
							pos:   position{line: 8, col: 81, offset: 203},
							label: "expr",
							expr: &ruleRefExpr{
								pos:  position{line: 8, col: 86, offset: 208},
								name: "String",
							},
						},
					},
				},
			},
			leader:        true,
			leftRecursive: true,
		},
		{
			name: "Emit",
			pos:  position{line: 12, col: 1, offset: 242},
			expr: &actionExpr{
				pos: position{line: 12, col: 8, offset: 249},
				run: (*parser).callonEmit1,
				expr: &seqExpr{
					pos: position{line: 12, col: 8, offset: 249},
					exprs: []any{
						&litMatcher{
							pos:        position{line: 12, col: 8, offset: 249},
							val:        "emit",
							ignoreCase: false,
							want:       "\"emit\"",
						},
						&litMatcher{
							pos:        position{line: 12, col: 15, offset: 256},
							val:        "(",
							ignoreCase: false,
							want:       "\"(\"",
						},
						&ruleRefExpr{
							pos:  position{line: 12, col: 19, offset: 260},
							name: "_",
						},
						&labeledExpr{
							pos:   position{line: 12, col: 21, offset: 262},
							label: "expr",
							expr: &ruleRefExpr{
								pos:  position{line: 12, col: 26, offset: 267},
								name: "Expr",
							},
						},
						&ruleRefExpr{
							pos:  position{line: 12, col: 31, offset: 272},
							name: "_",
						},
						&litMatcher{
							pos:        position{line: 12, col: 33, offset: 274},
							val:        ")",
							ignoreCase: false,
							want:       "\")\"",
						},
					},
				},
			},
			leader:        false,
			leftRecursive: false,
		},
		{
			name: "Doc",
			pos:  position{line: 22, col: 1, offset: 413},
			expr: &actionExpr{
				pos: position{line: 22, col: 7, offset: 419},
				run: (*parser).callonDoc1,
				expr: &seqExpr{
					pos: position{line: 22, col: 7, offset: 419},
					exprs: []any{
						&litMatcher{
							pos:        position{line: 22, col: 7, offset: 419},
							val:        "doc",
							ignoreCase: false,
							want:       "\"doc\"",
						},
						&litMatcher{
							pos:        position{line: 22, col: 13, offset: 425},
							val:        "[",
							ignoreCase: false,
							want:       "\"[\"",
						},
						&labeledExpr{
							pos:   position{line: 22, col: 17, offset: 429},
							label: "key",
							expr: &ruleRefExpr{
								pos:  position{line: 22, col: 21, offset: 433},
								name: "Expr",
							},
						},
						&litMatcher{
							pos:        position{line: 22, col: 27, offset: 439},
							val:        "]",
							ignoreCase: false,
							want:       "\"]\"",
						},
					},
				},
			},
			leader:        false,
			leftRecursive: false,
		},
		{
			name: "Accessor",
			pos:  position{line: 32, col: 1, offset: 581},
			expr: &actionExpr{
				pos: position{line: 32, col: 12, offset: 592},
				run: (*parser).callonAccessor1,
				expr: &seqExpr{
					pos: position{line: 32, col: 12, offset: 592},
					exprs: []any{
						&labeledExpr{
							pos:   position{line: 32, col: 12, offset: 592},
							label: "expr",
							expr: &ruleRefExpr{
								pos:  position{line: 32, col: 17, offset: 597},
								name: "Expr",
							},
						},
						&litMatcher{
							pos:        position{line: 32, col: 22, offset: 602},
							val:        ".",
							ignoreCase: false,
							want:       "\".\"",
						},
						&labeledExpr{
							pos:   position{line: 32, col: 26, offset: 606},
							label: "field",
							expr: &ruleRefExpr{
								pos:  position{line: 32, col: 32, offset: 612},
								name: "Identifier",
							},
						},
					},
				},
			},
			leader:        false,
			leftRecursive: true,
		},
		{
			name: "MethodCall",
			pos:  position{line: 47, col: 1, offset: 898},
			expr: &actionExpr{
				pos: position{line: 47, col: 14, offset: 911},
				run: (*parser).callonMethodCall1,
				expr: &seqExpr{
					pos: position{line: 47, col: 14, offset: 911},
					exprs: []any{
						&labeledExpr{
							pos:   position{line: 47, col: 14, offset: 911},
							label: "expr",
							expr: &ruleRefExpr{
								pos:  position{line: 47, col: 19, offset: 916},
								name: "Expr",
							},
						},
						&litMatcher{
							pos:        position{line: 47, col: 24, offset: 921},
							val:        ".",
							ignoreCase: false,
							want:       "\".\"",
						},
						&labeledExpr{
							pos:   position{line: 47, col: 28, offset: 925},
							label: "method",
							expr: &ruleRefExpr{
								pos:  position{line: 47, col: 35, offset: 932},
								name: "Identifier",
							},
						},
						&litMatcher{
							pos:        position{line: 47, col: 46, offset: 943},
							val:        "(",
							ignoreCase: false,
							want:       "\"(\"",
						},
						&labeledExpr{
							pos:   position{line: 47, col: 50, offset: 947},
							label: "args",
							expr: &zeroOrMoreExpr{
								pos: position{line: 47, col: 55, offset: 952},
								expr: &ruleRefExpr{
									pos:  position{line: 47, col: 55, offset: 952},
									name: "Expr",
								},
							},
						},
						&zeroOrOneExpr{
							pos: position{line: 47, col: 61, offset: 958},
							expr: &litMatcher{
								pos:        position{line: 47, col: 61, offset: 958},
								val:        ",",
								ignoreCase: false,
								want:       "\",\"",
							},
						},
						&litMatcher{
							pos:        position{line: 47, col: 67, offset: 964},
							val:        ")",
							ignoreCase: false,
							want:       "\")\"",
						},
					},
				},
			},
			leader:        false,
			leftRecursive: true,
		},
		{
			name: "OpExpr",
			pos:  position{line: 91, col: 1, offset: 1853},
			expr: &actionExpr{
				pos: position{line: 91, col: 10, offset: 1862},
				run: (*parser).callonOpExpr1,
				expr: &seqExpr{
					pos: position{line: 91, col: 10, offset: 1862},
					exprs: []any{
						&labeledExpr{
							pos:   position{line: 91, col: 10, offset: 1862},
							label: "left",
							expr: &ruleRefExpr{
								pos:  position{line: 91, col: 15, offset: 1867},
								name: "Expr",
							},
						},
						&ruleRefExpr{
							pos:  position{line: 91, col: 20, offset: 1872},
							name: "_",
						},
						&labeledExpr{
							pos:   position{line: 91, col: 23, offset: 1875},
							label: "op",
							expr: &ruleRefExpr{
								pos:  position{line: 91, col: 26, offset: 1878},
								name: "Op",
							},
						},
						&ruleRefExpr{
							pos:  position{line: 91, col: 29, offset: 1881},
							name: "_",
						},
						&labeledExpr{
							pos:   position{line: 91, col: 32, offset: 1884},
							label: "right",
							expr: &ruleRefExpr{
								pos:  position{line: 91, col: 38, offset: 1890},
								name: "Expr",
							},
						},
					},
				},
			},
			leader:        false,
			leftRecursive: true,
		},
		{
			name: "Op",
			pos:  position{line: 110, col: 1, offset: 2259},
			expr: &actionExpr{
				pos: position{line: 110, col: 6, offset: 2264},
				run: (*parser).callonOp1,
				expr: &labeledExpr{
					pos:   position{line: 110, col: 6, offset: 2264},
					label: "op",
					expr: &litMatcher{
						pos:        position{line: 110, col: 9, offset: 2267},
						val:        "+",
						ignoreCase: false,
						want:       "\"+\"",
					},
				},
			},
			leader:        false,
			leftRecursive: false,
		},
		{
			name: "String",
			pos:  position{line: 114, col: 1, offset: 2308},
			expr: &actionExpr{
				pos: position{line: 114, col: 10, offset: 2317},
				run: (*parser).callonString1,
				expr: &seqExpr{
					pos: position{line: 114, col: 10, offset: 2317},
					exprs: []any{
						&litMatcher{
							pos:        position{line: 114, col: 10, offset: 2317},
							val:        "'",
							ignoreCase: false,
							want:       "\"'\"",
						},
						&labeledExpr{
							pos:   position{line: 114, col: 15, offset: 2322},
							label: "s",
							expr: &zeroOrMoreExpr{
								pos: position{line: 114, col: 17, offset: 2324},
								expr: &charClassMatcher{
									pos:        position{line: 114, col: 17, offset: 2324},
									val:        "[^']",
									chars:      []rune{'\''},
									ignoreCase: false,
									inverted:   true,
								},
							},
						},
						&litMatcher{
							pos:        position{line: 114, col: 23, offset: 2330},
							val:        "'",
							ignoreCase: false,
							want:       "\"'\"",
						},
					},
				},
			},
			leader:        false,
			leftRecursive: false,
		},
		{
			name: "Identifier",
			pos:  position{line: 121, col: 1, offset: 2453},
			expr: &actionExpr{
				pos: position{line: 121, col: 14, offset: 2466},
				run: (*parser).callonIdentifier1,
				expr: &labeledExpr{
					pos:   position{line: 121, col: 14, offset: 2466},
					label: "id",
					expr: &oneOrMoreExpr{
						pos: position{line: 121, col: 17, offset: 2469},
						expr: &charClassMatcher{
							pos:        position{line: 121, col: 17, offset: 2469},
							val:        "[a-zA-Z0-9_]",
							chars:      []rune{'_'},
							ranges:     []rune{'a', 'z', 'A', 'Z', '0', '9'},
							ignoreCase: false,
							inverted:   false,
						},
					},
				},
			},
			leader:        false,
			leftRecursive: false,
		},
		{
			name:        "_",
			displayName: "\"whitespace\"",
			pos:         position{line: 125, col: 1, offset: 2518},
			expr: &zeroOrMoreExpr{
				pos: position{line: 125, col: 19, offset: 2536},
				expr: &charClassMatcher{
					pos:        position{line: 125, col: 19, offset: 2536},
					val:        "[ \\n\\t\\r]",
					chars:      []rune{' ', '\n', '\t', '\r'},
					ignoreCase: false,
					inverted:   false,
				},
			},
			leader:        false,
			leftRecursive: false,
		},
		{
			name: "EOF",
			pos:  position{line: 127, col: 1, offset: 2548},
			expr: &notExpr{
				pos: position{line: 128, col: 5, offset: 2557},
				expr: &anyMatcher{
					line: 128, col: 6, offset: 2558,
				},
			},
			leader:        false,
			leftRecursive: false,
		},
	},
}

func (c *current) onExpr12(expr any) (any, error) {
	return expr, nil
}

func (p *parser) callonExpr12() (any, error) {
	stack := p.vstack[len(p.vstack)-1]
	_ = stack
	return p.cur.onExpr12(stack["expr"])
}

func (c *current) onEmit1(expr any) (any, error) {

	exprVal, err := ExpectExpr(expr)
	if err != nil {
		return nil, err
	}

	return &EmitExpr{Expr: exprVal}, nil
}

func (p *parser) callonEmit1() (any, error) {
	stack := p.vstack[len(p.vstack)-1]
	_ = stack
	return p.cur.onEmit1(stack["expr"])
}

func (c *current) onDoc1(key any) (any, error) {

	exprVal, err := ExpectExpr(key)
	if err != nil {
		return nil, err
	}

	return &DocExpr{FieldName: exprVal}, nil
}

func (p *parser) callonDoc1() (any, error) {
	stack := p.vstack[len(p.vstack)-1]
	_ = stack
	return p.cur.onDoc1(stack["key"])
}

func (c *current) onAccessor1(expr, field any) (any, error) {

	exprVal, err := ExpectExpr(expr)
	if err != nil {
		return nil, err
	}

	strVal, err := ExpectString(field)
	if err != nil {
		return nil, err
	}

	return &AccessorExpr{Position: c.pos.String(), Expr: exprVal, PropertyName: strVal}, nil
}

func (p *parser) callonAccessor1() (any, error) {
	stack := p.vstack[len(p.vstack)-1]
	_ = stack
	return p.cur.onAccessor1(stack["expr"], stack["field"])
}

func (c *current) onMethodCall1(expr, method, args any) (any, error) {

	exprVal, err := ExpectExpr(expr)
	if err != nil {
		return nil, err
	}

	strVal, err := ExpectString(method)
	if err != nil {
		return nil, err
	}

	var argsVal []Expr

	switch argsVals := args.(type) {

	case nil:
		argsVal = []Expr{}
	case []any:

		for _, arg := range argsVals {
			argVal, err := ExpectExpr(arg)
			if err != nil {
				return nil, err
			}
			argsVal = append(argsVal, argVal)
		}

	default:
		return nil, fmt.Errorf("Invalid type %T", args)
	}

	for _, arg := range argsVal {
		argVal, err := ExpectExpr(arg)
		if err != nil {
			return nil, err
		}
		argsVal = append(argsVal, argVal)
	}

	return &MethodCallExpr{Position: c.pos.String(), Expr: exprVal, MethodName: strVal, Args: argsVal}, nil
}

func (p *parser) callonMethodCall1() (any, error) {
	stack := p.vstack[len(p.vstack)-1]
	_ = stack
	return p.cur.onMethodCall1(stack["expr"], stack["method"], stack["args"])
}

func (c *current) onOpExpr1(left, op, right any) (any, error) {
	leftVal, err := ExpectExpr(left)
	if err != nil {
		return nil, err
	}

	rightVal, err := ExpectExpr(right)
	if err != nil {
		return nil, err
	}

	opVal, err := ExpectString(op)
	if err != nil {
		return nil, err
	}

	return &InfixOpExpr{Position: c.pos.String(), Left: leftVal, Op: opVal, Right: rightVal}, nil
}

func (p *parser) callonOpExpr1() (any, error) {
	stack := p.vstack[len(p.vstack)-1]
	_ = stack
	return p.cur.onOpExpr1(stack["left"], stack["op"], stack["right"])
}

func (c *current) onOp1(op any) (any, error) {
	return string(c.text), nil
}

func (p *parser) callonOp1() (any, error) {
	stack := p.vstack[len(p.vstack)-1]
	_ = stack
	return p.cur.onOp1(stack["op"])
}

func (c *current) onString1(s any) (any, error) {

	strVal := string(c.text)
	strVal = strings.Trim(strVal, "'")
	return &LiteralExpr{Value: strVal}, nil
}

func (p *parser) callonString1() (any, error) {
	stack := p.vstack[len(p.vstack)-1]
	_ = stack
	return p.cur.onString1(stack["s"])
}

func (c *current) onIdentifier1(id any) (any, error) {
	return string(c.text), nil
}

func (p *parser) callonIdentifier1() (any, error) {
	stack := p.vstack[len(p.vstack)-1]
	_ = stack
	return p.cur.onIdentifier1(stack["id"])
}

var (
	// errNoRule is returned when the grammar to parse has no rule.
	errNoRule = errors.New("grammar has no rule")

	// errInvalidEntrypoint is returned when the specified entrypoint rule
	// does not exit.
	errInvalidEntrypoint = errors.New("invalid entrypoint")

	// errInvalidEncoding is returned when the source is not properly
	// utf8-encoded.
	errInvalidEncoding = errors.New("invalid encoding")

	// errMaxExprCnt is used to signal that the maximum number of
	// expressions have been parsed.
	errMaxExprCnt = errors.New("max number of expressions parsed")
)

// Option is a function that can set an option on the parser. It returns
// the previous setting as an Option.
type Option func(*parser) Option

// MaxExpressions creates an Option to stop parsing after the provided
// number of expressions have been parsed, if the value is 0 then the parser will
// parse for as many steps as needed (possibly an infinite number).
//
// The default for maxExprCnt is 0.
func MaxExpressions(maxExprCnt uint64) Option {
	return func(p *parser) Option {
		oldMaxExprCnt := p.maxExprCnt
		p.maxExprCnt = maxExprCnt
		return MaxExpressions(oldMaxExprCnt)
	}
}

// Entrypoint creates an Option to set the rule name to use as entrypoint.
// The rule name must have been specified in the -alternate-entrypoints
// if generating the parser with the -optimize-grammar flag, otherwise
// it may have been optimized out. Passing an empty string sets the
// entrypoint to the first rule in the grammar.
//
// The default is to start parsing at the first rule in the grammar.
func Entrypoint(ruleName string) Option {
	return func(p *parser) Option {
		oldEntrypoint := p.entrypoint
		p.entrypoint = ruleName
		if ruleName == "" {
			p.entrypoint = g.rules[0].name
		}
		return Entrypoint(oldEntrypoint)
	}
}

// Statistics adds a user provided Stats struct to the parser to allow
// the user to process the results after the parsing has finished.
// Also the key for the "no match" counter is set.
//
// Example usage:
//
//	input := "input"
//	stats := Stats{}
//	_, err := Parse("input-file", []byte(input), Statistics(&stats, "no match"))
//	if err != nil {
//	    log.Panicln(err)
//	}
//	b, err := json.MarshalIndent(stats.ChoiceAltCnt, "", "  ")
//	if err != nil {
//	    log.Panicln(err)
//	}
//	fmt.Println(string(b))
func Statistics(stats *Stats, choiceNoMatch string) Option {
	return func(p *parser) Option {
		oldStats := p.Stats
		p.Stats = stats
		oldChoiceNoMatch := p.choiceNoMatch
		p.choiceNoMatch = choiceNoMatch
		if p.Stats.ChoiceAltCnt == nil {
			p.Stats.ChoiceAltCnt = make(map[string]map[string]int)
		}
		return Statistics(oldStats, oldChoiceNoMatch)
	}
}

// Debug creates an Option to set the debug flag to b. When set to true,
// debugging information is printed to stdout while parsing.
//
// The default is false.
func Debug(b bool) Option {
	return func(p *parser) Option {
		old := p.debug
		p.debug = b
		return Debug(old)
	}
}

// Memoize creates an Option to set the memoize flag to b. When set to true,
// the parser will cache all results so each expression is evaluated only
// once. This guarantees linear parsing time even for pathological cases,
// at the expense of more memory and slower times for typical cases.
//
// The default is false.
func Memoize(b bool) Option {
	return func(p *parser) Option {
		old := p.memoize
		p.memoize = b
		return Memoize(old)
	}
}

// AllowInvalidUTF8 creates an Option to allow invalid UTF-8 bytes.
// Every invalid UTF-8 byte is treated as a utf8.RuneError (U+FFFD)
// by character class matchers and is matched by the any matcher.
// The returned matched value, c.text and c.offset are NOT affected.
//
// The default is false.
func AllowInvalidUTF8(b bool) Option {
	return func(p *parser) Option {
		old := p.allowInvalidUTF8
		p.allowInvalidUTF8 = b
		return AllowInvalidUTF8(old)
	}
}

// Recover creates an Option to set the recover flag to b. When set to
// true, this causes the parser to recover from panics and convert it
// to an error. Setting it to false can be useful while debugging to
// access the full stack trace.
//
// The default is true.
func Recover(b bool) Option {
	return func(p *parser) Option {
		old := p.recover
		p.recover = b
		return Recover(old)
	}
}

// GlobalStore creates an Option to set a key to a certain value in
// the globalStore.
func GlobalStore(key string, value any) Option {
	return func(p *parser) Option {
		old := p.cur.globalStore[key]
		p.cur.globalStore[key] = value
		return GlobalStore(key, old)
	}
}

// InitState creates an Option to set a key to a certain value in
// the global "state" store.
func InitState(key string, value any) Option {
	return func(p *parser) Option {
		old := p.cur.state[key]
		p.cur.state[key] = value
		return InitState(key, old)
	}
}

// ParseFile parses the file identified by filename.
func ParseFile(filename string, opts ...Option) (i any, err error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer func() {
		if closeErr := f.Close(); closeErr != nil {
			err = closeErr
		}
	}()
	return ParseReader(filename, f, opts...)
}

// ParseReader parses the data from r using filename as information in the
// error messages.
func ParseReader(filename string, r io.Reader, opts ...Option) (any, error) {
	b, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}

	return Parse(filename, b, opts...)
}

// Parse parses the data from b using filename as information in the
// error messages.
func Parse(filename string, b []byte, opts ...Option) (any, error) {
	return newParser(filename, b, opts...).parse(g)
}

// position records a position in the text.
type position struct {
	line, col, offset int
}

func (p position) String() string {
	return strconv.Itoa(p.line) + ":" + strconv.Itoa(p.col) + " [" + strconv.Itoa(p.offset) + "]"
}

// savepoint stores all state required to go back to this point in the
// parser.
type savepoint struct {
	position
	rn rune
	w  int
}

type current struct {
	pos  position // start position of the match
	text []byte   // raw text of the match

	// state is a store for arbitrary key,value pairs that the user wants to be
	// tied to the backtracking of the parser.
	// This is always rolled back if a parsing rule fails.
	state storeDict

	// globalStore is a general store for the user to store arbitrary key-value
	// pairs that they need to manage and that they do not want tied to the
	// backtracking of the parser. This is only modified by the user and never
	// rolled back by the parser. It is always up to the user to keep this in a
	// consistent state.
	globalStore storeDict
}

type storeDict map[string]any

// the AST types...

type grammar struct {
	pos   position
	rules []*rule
}

type rule struct {
	pos         position
	name        string
	displayName string
	expr        any

	leader        bool
	leftRecursive bool
}

type choiceExpr struct {
	pos          position
	alternatives []any
}

type actionExpr struct {
	pos  position
	expr any
	run  func(*parser) (any, error)
}

type recoveryExpr struct {
	pos          position
	expr         any
	recoverExpr  any
	failureLabel []string
}

type seqExpr struct {
	pos   position
	exprs []any
}

type throwExpr struct {
	pos   position
	label string
}

type labeledExpr struct {
	pos   position
	label string
	expr  any
}

type expr struct {
	pos  position
	expr any
}

type (
	andExpr        expr
	notExpr        expr
	zeroOrOneExpr  expr
	zeroOrMoreExpr expr
	oneOrMoreExpr  expr
)

type ruleRefExpr struct {
	pos  position
	name string
}

type stateCodeExpr struct {
	pos position
	run func(*parser) error
}

type andCodeExpr struct {
	pos position
	run func(*parser) (bool, error)
}

type notCodeExpr struct {
	pos position
	run func(*parser) (bool, error)
}

type litMatcher struct {
	pos        position
	val        string
	ignoreCase bool
	want       string
}

type charClassMatcher struct {
	pos             position
	val             string
	basicLatinChars [128]bool
	chars           []rune
	ranges          []rune
	classes         []*unicode.RangeTable
	ignoreCase      bool
	inverted        bool
}

type anyMatcher position

// errList cumulates the errors found by the parser.
type errList []error

func (e *errList) add(err error) {
	*e = append(*e, err)
}

func (e errList) err() error {
	if len(e) == 0 {
		return nil
	}
	e.dedupe()
	return e
}

func (e *errList) dedupe() {
	var cleaned []error
	set := make(map[string]bool)
	for _, err := range *e {
		if msg := err.Error(); !set[msg] {
			set[msg] = true
			cleaned = append(cleaned, err)
		}
	}
	*e = cleaned
}

func (e errList) Error() string {
	switch len(e) {
	case 0:
		return ""
	case 1:
		return e[0].Error()
	default:
		var buf bytes.Buffer

		for i, err := range e {
			if i > 0 {
				buf.WriteRune('\n')
			}
			buf.WriteString(err.Error())
		}
		return buf.String()
	}
}

// parserError wraps an error with a prefix indicating the rule in which
// the error occurred. The original error is stored in the Inner field.
type parserError struct {
	Inner    error
	pos      position
	prefix   string
	expected []string
}

// Error returns the error message.
func (p *parserError) Error() string {
	return p.prefix + ": " + p.Inner.Error()
}

// newParser creates a parser with the specified input source and options.
func newParser(filename string, b []byte, opts ...Option) *parser {
	stats := Stats{
		ChoiceAltCnt: make(map[string]map[string]int),
	}

	p := &parser{
		filename: filename,
		errs:     new(errList),
		data:     b,
		pt:       savepoint{position: position{line: 1}},
		recover:  true,
		cur: current{
			state:       make(storeDict),
			globalStore: make(storeDict),
		},
		maxFailPos:      position{col: 1, line: 1},
		maxFailExpected: make([]string, 0, 20),
		Stats:           &stats,
		// start rule is rule [0] unless an alternate entrypoint is specified
		entrypoint: g.rules[0].name,
	}
	p.setOptions(opts)

	if p.maxExprCnt == 0 {
		p.maxExprCnt = math.MaxUint64
	}

	return p
}

// setOptions applies the options to the parser.
func (p *parser) setOptions(opts []Option) {
	for _, opt := range opts {
		opt(p)
	}
}

type resultTuple struct {
	v   any
	b   bool
	end savepoint
}

const choiceNoMatch = -1

// Stats stores some statistics, gathered during parsing
type Stats struct {
	// ExprCnt counts the number of expressions processed during parsing
	// This value is compared to the maximum number of expressions allowed
	// (set by the MaxExpressions option).
	ExprCnt uint64

	// ChoiceAltCnt is used to count for each ordered choice expression,
	// which alternative is used how may times.
	// These numbers allow to optimize the order of the ordered choice expression
	// to increase the performance of the parser
	//
	// The outer key of ChoiceAltCnt is composed of the name of the rule as well
	// as the line and the column of the ordered choice.
	// The inner key of ChoiceAltCnt is the number (one-based) of the matching alternative.
	// For each alternative the number of matches are counted. If an ordered choice does not
	// match, a special counter is incremented. The name of this counter is set with
	// the parser option Statistics.
	// For an alternative to be included in ChoiceAltCnt, it has to match at least once.
	ChoiceAltCnt map[string]map[string]int
}

type ruleWithExpsStack struct {
	rule   *rule
	estack []any
}

type parser struct {
	filename string
	pt       savepoint
	cur      current

	data []byte
	errs *errList

	depth   int
	recover bool
	debug   bool

	memoize bool
	// memoization table for the packrat algorithm:
	// map[offset in source] map[expression or rule] {value, match}
	memo map[int]map[any]resultTuple

	// rules table, maps the rule identifier to the rule node
	rules map[string]*rule
	// variables stack, map of label to value
	vstack []map[string]any
	// rule stack, allows identification of the current rule in errors
	rstack []*rule

	// parse fail
	maxFailPos            position
	maxFailExpected       []string
	maxFailInvertExpected bool

	// max number of expressions to be parsed
	maxExprCnt uint64
	// entrypoint for the parser
	entrypoint string

	allowInvalidUTF8 bool

	*Stats

	choiceNoMatch string
	// recovery expression stack, keeps track of the currently available recovery expression, these are traversed in reverse
	recoveryStack []map[string]any
}

// push a variable set on the vstack.
func (p *parser) pushV() {
	if cap(p.vstack) == len(p.vstack) {
		// create new empty slot in the stack
		p.vstack = append(p.vstack, nil)
	} else {
		// slice to 1 more
		p.vstack = p.vstack[:len(p.vstack)+1]
	}

	// get the last args set
	m := p.vstack[len(p.vstack)-1]
	if m != nil && len(m) == 0 {
		// empty map, all good
		return
	}

	m = make(map[string]any)
	p.vstack[len(p.vstack)-1] = m
}

// pop a variable set from the vstack.
func (p *parser) popV() {
	// if the map is not empty, clear it
	m := p.vstack[len(p.vstack)-1]
	if len(m) > 0 {
		// GC that map
		p.vstack[len(p.vstack)-1] = nil
	}
	p.vstack = p.vstack[:len(p.vstack)-1]
}

// push a recovery expression with its labels to the recoveryStack
func (p *parser) pushRecovery(labels []string, expr any) {
	if cap(p.recoveryStack) == len(p.recoveryStack) {
		// create new empty slot in the stack
		p.recoveryStack = append(p.recoveryStack, nil)
	} else {
		// slice to 1 more
		p.recoveryStack = p.recoveryStack[:len(p.recoveryStack)+1]
	}

	m := make(map[string]any, len(labels))
	for _, fl := range labels {
		m[fl] = expr
	}
	p.recoveryStack[len(p.recoveryStack)-1] = m
}

// pop a recovery expression from the recoveryStack
func (p *parser) popRecovery() {
	// GC that map
	p.recoveryStack[len(p.recoveryStack)-1] = nil

	p.recoveryStack = p.recoveryStack[:len(p.recoveryStack)-1]
}

func (p *parser) print(prefix, s string) string {
	if !p.debug {
		return s
	}

	fmt.Printf("%s %d:%d:%d: %s [%#U]\n",
		prefix, p.pt.line, p.pt.col, p.pt.offset, s, p.pt.rn)
	return s
}

func (p *parser) printIndent(mark string, s string) string {
	return p.print(strings.Repeat(" ", p.depth)+mark, s)
}

func (p *parser) in(s string) string {
	res := p.printIndent(">", s)
	p.depth++
	return res
}

func (p *parser) out(s string) string {
	p.depth--
	return p.printIndent("<", s)
}

func (p *parser) addErr(err error) {
	p.addErrAt(err, p.pt.position, []string{})
}

func (p *parser) addErrAt(err error, pos position, expected []string) {
	var buf bytes.Buffer
	if p.filename != "" {
		buf.WriteString(p.filename)
	}
	if buf.Len() > 0 {
		buf.WriteString(":")
	}
	buf.WriteString(fmt.Sprintf("%d:%d (%d)", pos.line, pos.col, pos.offset))
	if len(p.rstack) > 0 {
		if buf.Len() > 0 {
			buf.WriteString(": ")
		}
		rule := p.rstack[len(p.rstack)-1]
		if rule.displayName != "" {
			buf.WriteString("rule " + rule.displayName)
		} else {
			buf.WriteString("rule " + rule.name)
		}
	}
	pe := &parserError{Inner: err, pos: pos, prefix: buf.String(), expected: expected}
	p.errs.add(pe)
}

func (p *parser) failAt(fail bool, pos position, want string) {
	// process fail if parsing fails and not inverted or parsing succeeds and invert is set
	if fail == p.maxFailInvertExpected {
		if pos.offset < p.maxFailPos.offset {
			return
		}

		if pos.offset > p.maxFailPos.offset {
			p.maxFailPos = pos
			p.maxFailExpected = p.maxFailExpected[:0]
		}

		if p.maxFailInvertExpected {
			want = "!" + want
		}
		p.maxFailExpected = append(p.maxFailExpected, want)
	}
}

// read advances the parser to the next rune.
func (p *parser) read() {
	p.pt.offset += p.pt.w
	rn, n := utf8.DecodeRune(p.data[p.pt.offset:])
	p.pt.rn = rn
	p.pt.w = n
	p.pt.col++
	if rn == '\n' {
		p.pt.line++
		p.pt.col = 0
	}

	if rn == utf8.RuneError && n == 1 { // see utf8.DecodeRune
		if !p.allowInvalidUTF8 {
			p.addErr(errInvalidEncoding)
		}
	}
}

// restore parser position to the savepoint pt.
func (p *parser) restore(pt savepoint) {
	if p.debug {
		defer p.out(p.in("restore"))
	}
	if pt.offset == p.pt.offset {
		return
	}
	p.pt = pt
}

// Cloner is implemented by any value that has a Clone method, which returns a
// copy of the value. This is mainly used for types which are not passed by
// value (e.g map, slice, chan) or structs that contain such types.
//
// This is used in conjunction with the global state feature to create proper
// copies of the state to allow the parser to properly restore the state in
// the case of backtracking.
type Cloner interface {
	Clone() any
}

var statePool = &sync.Pool{
	New: func() any { return make(storeDict) },
}

func (sd storeDict) Discard() {
	for k := range sd {
		delete(sd, k)
	}
	statePool.Put(sd)
}

// clone and return parser current state.
func (p *parser) cloneState() storeDict {
	if p.debug {
		defer p.out(p.in("cloneState"))
	}

	state := statePool.Get().(storeDict)
	for k, v := range p.cur.state {
		if c, ok := v.(Cloner); ok {
			state[k] = c.Clone()
		} else {
			state[k] = v
		}
	}
	return state
}

// restore parser current state to the state storeDict.
// every restoreState should applied only one time for every cloned state
func (p *parser) restoreState(state storeDict) {
	if p.debug {
		defer p.out(p.in("restoreState"))
	}
	p.cur.state.Discard()
	p.cur.state = state
}

// get the slice of bytes from the savepoint start to the current position.
func (p *parser) sliceFrom(start savepoint) []byte {
	return p.data[start.position.offset:p.pt.position.offset]
}

func (p *parser) getMemoized(node any) (resultTuple, bool) {
	if len(p.memo) == 0 {
		return resultTuple{}, false
	}
	m := p.memo[p.pt.offset]
	if len(m) == 0 {
		return resultTuple{}, false
	}
	res, ok := m[node]
	return res, ok
}

func (p *parser) setMemoized(pt savepoint, node any, tuple resultTuple) {
	if p.memo == nil {
		p.memo = make(map[int]map[any]resultTuple)
	}
	m := p.memo[pt.offset]
	if m == nil {
		m = make(map[any]resultTuple)
		p.memo[pt.offset] = m
	}
	m[node] = tuple
}

func (p *parser) buildRulesTable(g *grammar) {
	p.rules = make(map[string]*rule, len(g.rules))
	for _, r := range g.rules {
		p.rules[r.name] = r
	}
}

func (p *parser) parse(g *grammar) (val any, err error) {
	if len(g.rules) == 0 {
		p.addErr(errNoRule)
		return nil, p.errs.err()
	}

	// TODO : not super critical but this could be generated
	p.buildRulesTable(g)

	if p.recover {
		// panic can be used in action code to stop parsing immediately
		// and return the panic as an error.
		defer func() {
			if e := recover(); e != nil {
				if p.debug {
					defer p.out(p.in("panic handler"))
				}
				val = nil
				switch e := e.(type) {
				case error:
					p.addErr(e)
				default:
					p.addErr(fmt.Errorf("%v", e))
				}
				err = p.errs.err()
			}
		}()
	}

	startRule, ok := p.rules[p.entrypoint]
	if !ok {
		p.addErr(errInvalidEntrypoint)
		return nil, p.errs.err()
	}

	p.read() // advance to first rune
	val, ok = p.parseRuleWrap(startRule)
	if !ok {
		if len(*p.errs) == 0 {
			// If parsing fails, but no errors have been recorded, the expected values
			// for the farthest parser position are returned as error.
			maxFailExpectedMap := make(map[string]struct{}, len(p.maxFailExpected))
			for _, v := range p.maxFailExpected {
				maxFailExpectedMap[v] = struct{}{}
			}
			expected := make([]string, 0, len(maxFailExpectedMap))
			eof := false
			if _, ok := maxFailExpectedMap["!."]; ok {
				delete(maxFailExpectedMap, "!.")
				eof = true
			}
			for k := range maxFailExpectedMap {
				expected = append(expected, k)
			}
			sort.Strings(expected)
			if eof {
				expected = append(expected, "EOF")
			}
			p.addErrAt(errors.New("no match found, expected: "+listJoin(expected, ", ", "or")), p.maxFailPos, expected)
		}

		return nil, p.errs.err()
	}
	return val, p.errs.err()
}

func listJoin(list []string, sep string, lastSep string) string {
	switch len(list) {
	case 0:
		return ""
	case 1:
		return list[0]
	default:
		return strings.Join(list[:len(list)-1], sep) + " " + lastSep + " " + list[len(list)-1]
	}
}

func (p *parser) parseRuleRecursiveLeader(rule *rule) (any, bool) {
	result, ok := p.getMemoized(rule)
	if ok {
		p.restore(result.end)
		return result.v, result.b
	}

	if p.debug {
		defer p.out(p.in("recursive " + rule.name))
	}

	var (
		depth      = 0
		startMark  = p.pt
		lastResult = resultTuple{nil, false, startMark}
		lastErrors = *p.errs
	)

	for {
		lastState := p.cloneState()
		p.setMemoized(startMark, rule, lastResult)
		val, ok := p.parseRule(rule)
		endMark := p.pt
		if p.debug {
			p.printIndent("RECURSIVE", fmt.Sprintf(
				"Rule %s depth %d: %t -> %s",
				rule.name, depth, ok, string(p.sliceFrom(startMark))))
		}
		if (!ok) || (endMark.offset <= lastResult.end.offset && depth != 0) {
			p.restoreState(lastState)
			*p.errs = lastErrors
			break
		}
		lastResult = resultTuple{val, ok, endMark}
		lastErrors = *p.errs
		p.restore(startMark)
		depth++
	}

	p.restore(lastResult.end)
	p.setMemoized(startMark, rule, lastResult)
	return lastResult.v, lastResult.b
}

func (p *parser) parseRuleRecursiveNoLeader(rule *rule) (any, bool) {
	return p.parseRule(rule)
}

func (p *parser) parseRuleMemoize(rule *rule) (any, bool) {
	res, ok := p.getMemoized(rule)
	if ok {
		p.restore(res.end)
		return res.v, res.b
	}

	startMark := p.pt
	val, ok := p.parseRule(rule)
	p.setMemoized(startMark, rule, resultTuple{val, ok, p.pt})

	return val, ok
}

func (p *parser) parseRuleWrap(rule *rule) (any, bool) {
	if p.debug {
		defer p.out(p.in("parseRule " + rule.name))
	}
	var (
		val       any
		ok        bool
		startMark = p.pt
	)

	if p.memoize || rule.leftRecursive {
		if rule.leader {
			val, ok = p.parseRuleRecursiveLeader(rule)
		} else if p.memoize && !rule.leftRecursive {
			val, ok = p.parseRuleMemoize(rule)
		} else {
			val, ok = p.parseRuleRecursiveNoLeader(rule)
		}
	} else {
		val, ok = p.parseRule(rule)
	}

	if ok && p.debug {
		p.printIndent("MATCH", string(p.sliceFrom(startMark)))
	}
	return val, ok
}

func (p *parser) parseRule(rule *rule) (any, bool) {
	p.rstack = append(p.rstack, rule)
	p.pushV()
	val, ok := p.parseExprWrap(rule.expr)
	p.popV()
	p.rstack = p.rstack[:len(p.rstack)-1]
	return val, ok
}

func (p *parser) parseExprWrap(expr any) (any, bool) {
	var pt savepoint

	isLeftRecursion := p.rstack[len(p.rstack)-1].leftRecursive
	if p.memoize && !isLeftRecursion {
		res, ok := p.getMemoized(expr)
		if ok {
			p.restore(res.end)
			return res.v, res.b
		}
		pt = p.pt
	}

	val, ok := p.parseExpr(expr)

	if p.memoize && !isLeftRecursion {
		p.setMemoized(pt, expr, resultTuple{val, ok, p.pt})
	}
	return val, ok
}

func (p *parser) parseExpr(expr any) (any, bool) {
	p.ExprCnt++
	if p.ExprCnt > p.maxExprCnt {
		panic(errMaxExprCnt)
	}

	var val any
	var ok bool
	switch expr := expr.(type) {
	case *actionExpr:
		val, ok = p.parseActionExpr(expr)
	case *andCodeExpr:
		val, ok = p.parseAndCodeExpr(expr)
	case *andExpr:
		val, ok = p.parseAndExpr(expr)
	case *anyMatcher:
		val, ok = p.parseAnyMatcher(expr)
	case *charClassMatcher:
		val, ok = p.parseCharClassMatcher(expr)
	case *choiceExpr:
		val, ok = p.parseChoiceExpr(expr)
	case *labeledExpr:
		val, ok = p.parseLabeledExpr(expr)
	case *litMatcher:
		val, ok = p.parseLitMatcher(expr)
	case *notCodeExpr:
		val, ok = p.parseNotCodeExpr(expr)
	case *notExpr:
		val, ok = p.parseNotExpr(expr)
	case *oneOrMoreExpr:
		val, ok = p.parseOneOrMoreExpr(expr)
	case *recoveryExpr:
		val, ok = p.parseRecoveryExpr(expr)
	case *ruleRefExpr:
		val, ok = p.parseRuleRefExpr(expr)
	case *seqExpr:
		val, ok = p.parseSeqExpr(expr)
	case *stateCodeExpr:
		val, ok = p.parseStateCodeExpr(expr)
	case *throwExpr:
		val, ok = p.parseThrowExpr(expr)
	case *zeroOrMoreExpr:
		val, ok = p.parseZeroOrMoreExpr(expr)
	case *zeroOrOneExpr:
		val, ok = p.parseZeroOrOneExpr(expr)
	default:
		panic(fmt.Sprintf("unknown expression type %T", expr))
	}
	return val, ok
}

func (p *parser) parseActionExpr(act *actionExpr) (any, bool) {
	if p.debug {
		defer p.out(p.in("parseActionExpr"))
	}

	start := p.pt
	val, ok := p.parseExprWrap(act.expr)
	if ok {
		p.cur.pos = start.position
		p.cur.text = p.sliceFrom(start)
		state := p.cloneState()
		actVal, err := act.run(p)
		if err != nil {
			p.addErrAt(err, start.position, []string{})
		}
		p.restoreState(state)

		val = actVal
	}
	if ok && p.debug {
		p.printIndent("MATCH", string(p.sliceFrom(start)))
	}
	return val, ok
}

func (p *parser) parseAndCodeExpr(and *andCodeExpr) (any, bool) {
	if p.debug {
		defer p.out(p.in("parseAndCodeExpr"))
	}

	state := p.cloneState()

	ok, err := and.run(p)
	if err != nil {
		p.addErr(err)
	}
	p.restoreState(state)

	return nil, ok
}

func (p *parser) parseAndExpr(and *andExpr) (any, bool) {
	if p.debug {
		defer p.out(p.in("parseAndExpr"))
	}

	pt := p.pt
	state := p.cloneState()
	p.pushV()
	_, ok := p.parseExprWrap(and.expr)
	p.popV()
	p.restoreState(state)
	p.restore(pt)

	return nil, ok
}

func (p *parser) parseAnyMatcher(any *anyMatcher) (any, bool) {
	if p.debug {
		defer p.out(p.in("parseAnyMatcher"))
	}

	if p.pt.rn == utf8.RuneError && p.pt.w == 0 {
		// EOF - see utf8.DecodeRune
		p.failAt(false, p.pt.position, ".")
		return nil, false
	}
	start := p.pt
	p.read()
	p.failAt(true, start.position, ".")
	return p.sliceFrom(start), true
}

func (p *parser) parseCharClassMatcher(chr *charClassMatcher) (any, bool) {
	if p.debug {
		defer p.out(p.in("parseCharClassMatcher"))
	}

	cur := p.pt.rn
	start := p.pt

	// can't match EOF
	if cur == utf8.RuneError && p.pt.w == 0 { // see utf8.DecodeRune
		p.failAt(false, start.position, chr.val)
		return nil, false
	}

	if chr.ignoreCase {
		cur = unicode.ToLower(cur)
	}

	// try to match in the list of available chars
	for _, rn := range chr.chars {
		if rn == cur {
			if chr.inverted {
				p.failAt(false, start.position, chr.val)
				return nil, false
			}
			p.read()
			p.failAt(true, start.position, chr.val)
			return p.sliceFrom(start), true
		}
	}

	// try to match in the list of ranges
	for i := 0; i < len(chr.ranges); i += 2 {
		if cur >= chr.ranges[i] && cur <= chr.ranges[i+1] {
			if chr.inverted {
				p.failAt(false, start.position, chr.val)
				return nil, false
			}
			p.read()
			p.failAt(true, start.position, chr.val)
			return p.sliceFrom(start), true
		}
	}

	// try to match in the list of Unicode classes
	for _, cl := range chr.classes {
		if unicode.Is(cl, cur) {
			if chr.inverted {
				p.failAt(false, start.position, chr.val)
				return nil, false
			}
			p.read()
			p.failAt(true, start.position, chr.val)
			return p.sliceFrom(start), true
		}
	}

	if chr.inverted {
		p.read()
		p.failAt(true, start.position, chr.val)
		return p.sliceFrom(start), true
	}
	p.failAt(false, start.position, chr.val)
	return nil, false
}

func (p *parser) incChoiceAltCnt(ch *choiceExpr, altI int) {
	choiceIdent := fmt.Sprintf("%s %d:%d", p.rstack[len(p.rstack)-1].name, ch.pos.line, ch.pos.col)
	m := p.ChoiceAltCnt[choiceIdent]
	if m == nil {
		m = make(map[string]int)
		p.ChoiceAltCnt[choiceIdent] = m
	}
	// We increment altI by 1, so the keys do not start at 0
	alt := strconv.Itoa(altI + 1)
	if altI == choiceNoMatch {
		alt = p.choiceNoMatch
	}
	m[alt]++
}

func (p *parser) parseChoiceExpr(ch *choiceExpr) (any, bool) {
	if p.debug {
		defer p.out(p.in("parseChoiceExpr"))
	}

	for altI, alt := range ch.alternatives {
		// dummy assignment to prevent compile error if optimized
		_ = altI

		state := p.cloneState()

		p.pushV()
		val, ok := p.parseExprWrap(alt)
		p.popV()
		if ok {
			p.incChoiceAltCnt(ch, altI)
			return val, ok
		}
		p.restoreState(state)
	}
	p.incChoiceAltCnt(ch, choiceNoMatch)
	return nil, false
}

func (p *parser) parseLabeledExpr(lab *labeledExpr) (any, bool) {
	if p.debug {
		defer p.out(p.in("parseLabeledExpr"))
	}

	p.pushV()
	val, ok := p.parseExprWrap(lab.expr)
	p.popV()
	if ok && lab.label != "" {
		m := p.vstack[len(p.vstack)-1]
		m[lab.label] = val
	}
	return val, ok
}

func (p *parser) parseLitMatcher(lit *litMatcher) (any, bool) {
	if p.debug {
		defer p.out(p.in("parseLitMatcher"))
	}

	start := p.pt
	for _, want := range lit.val {
		cur := p.pt.rn
		if lit.ignoreCase {
			cur = unicode.ToLower(cur)
		}
		if cur != want {
			p.failAt(false, start.position, lit.want)
			p.restore(start)
			return nil, false
		}
		p.read()
	}
	p.failAt(true, start.position, lit.want)
	return p.sliceFrom(start), true
}

func (p *parser) parseNotCodeExpr(not *notCodeExpr) (any, bool) {
	if p.debug {
		defer p.out(p.in("parseNotCodeExpr"))
	}

	state := p.cloneState()

	ok, err := not.run(p)
	if err != nil {
		p.addErr(err)
	}
	p.restoreState(state)

	return nil, !ok
}

func (p *parser) parseNotExpr(not *notExpr) (any, bool) {
	if p.debug {
		defer p.out(p.in("parseNotExpr"))
	}

	pt := p.pt
	state := p.cloneState()
	p.pushV()
	p.maxFailInvertExpected = !p.maxFailInvertExpected
	_, ok := p.parseExprWrap(not.expr)
	p.maxFailInvertExpected = !p.maxFailInvertExpected
	p.popV()
	p.restoreState(state)
	p.restore(pt)

	return nil, !ok
}

func (p *parser) parseOneOrMoreExpr(expr *oneOrMoreExpr) (any, bool) {
	if p.debug {
		defer p.out(p.in("parseOneOrMoreExpr"))
	}

	var vals []any

	for {
		p.pushV()
		val, ok := p.parseExprWrap(expr.expr)
		p.popV()
		if !ok {
			if len(vals) == 0 {
				// did not match once, no match
				return nil, false
			}
			return vals, true
		}
		vals = append(vals, val)
	}
}

func (p *parser) parseRecoveryExpr(recover *recoveryExpr) (any, bool) {
	if p.debug {
		defer p.out(p.in("parseRecoveryExpr (" + strings.Join(recover.failureLabel, ",") + ")"))
	}

	p.pushRecovery(recover.failureLabel, recover.recoverExpr)
	val, ok := p.parseExprWrap(recover.expr)
	p.popRecovery()

	return val, ok
}

func (p *parser) parseRuleRefExpr(ref *ruleRefExpr) (any, bool) {
	if p.debug {
		defer p.out(p.in("parseRuleRefExpr " + ref.name))
	}

	if ref.name == "" {
		panic(fmt.Sprintf("%s: invalid rule: missing name", ref.pos))
	}

	rule := p.rules[ref.name]
	if rule == nil {
		p.addErr(fmt.Errorf("undefined rule: %s", ref.name))
		return nil, false
	}
	return p.parseRuleWrap(rule)
}

func (p *parser) parseSeqExpr(seq *seqExpr) (any, bool) {
	if p.debug {
		defer p.out(p.in("parseSeqExpr"))
	}

	vals := make([]any, 0, len(seq.exprs))

	pt := p.pt
	state := p.cloneState()
	for _, expr := range seq.exprs {
		val, ok := p.parseExprWrap(expr)
		if !ok {
			p.restoreState(state)
			p.restore(pt)
			return nil, false
		}
		vals = append(vals, val)
	}
	return vals, true
}

func (p *parser) parseStateCodeExpr(state *stateCodeExpr) (any, bool) {
	if p.debug {
		defer p.out(p.in("parseStateCodeExpr"))
	}

	err := state.run(p)
	if err != nil {
		p.addErr(err)
	}
	return nil, true
}

func (p *parser) parseThrowExpr(expr *throwExpr) (any, bool) {
	if p.debug {
		defer p.out(p.in("parseThrowExpr"))
	}

	for i := len(p.recoveryStack) - 1; i >= 0; i-- {
		if recoverExpr, ok := p.recoveryStack[i][expr.label]; ok {
			if val, ok := p.parseExprWrap(recoverExpr); ok {
				return val, ok
			}
		}
	}

	return nil, false
}

func (p *parser) parseZeroOrMoreExpr(expr *zeroOrMoreExpr) (any, bool) {
	if p.debug {
		defer p.out(p.in("parseZeroOrMoreExpr"))
	}

	var vals []any

	for {
		p.pushV()
		val, ok := p.parseExprWrap(expr.expr)
		p.popV()
		if !ok {
			return vals, true
		}
		vals = append(vals, val)
	}
}

func (p *parser) parseZeroOrOneExpr(expr *zeroOrOneExpr) (any, bool) {
	if p.debug {
		defer p.out(p.in("parseZeroOrOneExpr"))
	}

	p.pushV()
	val, _ := p.parseExprWrap(expr.expr)
	p.popV()
	// whether it matched or not, consider it a match
	return val, true
}
