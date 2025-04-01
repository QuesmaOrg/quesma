// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package transforms

import (
	"github.com/stretchr/testify/require"
	"math/rand"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"

	lexer_core "github.com/QuesmaOrg/quesma/platform/parsers/sql/lexer/core"
	parser_core "github.com/QuesmaOrg/quesma/platform/parsers/sql/parser/core"
)

var (
	parenthesisTokenType = &lexer_core.TokenType{
		Name:        "Parenthesis",
		Description: "Parenthesis token, either '(' or ')'",
	}
	identifierTokenType = &lexer_core.TokenType{
		Name:        "Identifier",
		Description: "Identifier token",
	}

	parenthesisRuleList = lexer_core.NewRuleList(
		lexer_core.NewStringRule("(", parenthesisTokenType),
		lexer_core.NewStringRule(")", parenthesisTokenType),
		lexer_core.NewRegexRule(`[^() ]* *`, identifierTokenType),
	)
)

func assertTokensEqual(t *testing.T, expecteds []interface{}, node parser_core.Node) {
	nodeListNode, ok := node.(*parser_core.NodeListNode)
	require.True(t, ok)

	require.Equal(t, len(expecteds), len(nodeListNode.Nodes))
	for i, expected := range expecteds {
		switch expected := expected.(type) {
		case []interface{}:
			assertTokensEqual(t, expected, nodeListNode.Nodes[i])
		case string:
			tokenNode, ok := nodeListNode.Nodes[i].(parser_core.TokenNode)
			assert.True(t, ok)
			assert.Equal(t, expected, tokenNode.Token.RawValue)
		default:
			assert.Fail(t, "unexpected type in expected")
		}
	}
}

type parenthesisTestCase struct {
	name     string
	input    string
	expected []interface{}
}

var testCases = []parenthesisTestCase{
	{
		name:     "empty",
		input:    ``,
		expected: []interface{}{},
	},
	{
		name:  "no parenthesis",
		input: `a b c`,
		expected: []interface{}{
			`a `, `b `, `c`,
		},
	},
	{
		name:  "everything nested 1",
		input: `(a)`,
		expected: []interface{}{
			[]interface{}{`(`, `a`, `)`},
		},
	},
	{
		name:  "everything nested 2",
		input: `((a))`,
		expected: []interface{}{
			[]interface{}{
				`(`,
				[]interface{}{`(`, `a`, `)`},
				`)`},
		},
	},
	{
		name:  "part nested",
		input: `a(b)`,
		expected: []interface{}{
			`a`,
			[]interface{}{`(`, `b`, `)`},
		},
	},
	{
		name:  "multiple",
		input: `(a)(b)`,
		expected: []interface{}{
			[]interface{}{`(`, `a`, `)`},
			[]interface{}{`(`, `b`, `)`},
		},
	},
	// The transform should gracefully handle invalid parenthesis
	{
		name:  "invalid 1",
		input: `(a)(b))`,
		expected: []interface{}{
			[]interface{}{`(`, `a`, `)`},
			[]interface{}{`(`, `b`, `)`},
			`)`,
		},
	},
	{
		name:  "invalid 2",
		input: `(a))(b)`,
		expected: []interface{}{
			[]interface{}{`(`, `a`, `)`},
			`)`,
			[]interface{}{`(`, `b`, `)`},
		},
	},
	{
		name:  "invalid 3",
		input: `(a`,
		expected: []interface{}{
			[]interface{}{`(`, `a`},
		},
	},
	{
		name:  "invalid 4",
		input: `a)`,
		expected: []interface{}{
			`a`,
			`)`,
		},
	},
	{
		name:  "simple nested SQL",
		input: `SELECT * FROM (SELECT * FROM tabela) sub`,
		expected: []interface{}{
			`SELECT `,
			`* `,
			`FROM `,
			[]interface{}{
				`(`,
				`SELECT `,
				`* `,
				`FROM `,
				`tabela`,
				`)`,
			},
			` `,
			`sub`,
		},
	},
}

func TestGroupParenthesis(t *testing.T) {
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			lexed := lexer_core.Lex(tc.input, parenthesisRuleList)
			node := parser_core.TokensToNode(lexed)

			GroupParenthesis(node)

			assertTokensEqual(t, tc.expected, node)
		})
	}
}

func generateRandomCases() []parenthesisTestCase {
	// Randomly generate possible combinations of parenthesis (up to some small depth)
	r := rand.New(rand.NewSource(12345))

	generatedCases := []parenthesisTestCase{
		{
			input:    ``,
			expected: []interface{}{},
		},
		{
			input:    `a `,
			expected: []interface{}{`a `},
		},
		{
			input:    `b `,
			expected: []interface{}{`b `},
		},
	}
	for i := 0; i < 8; i++ {
		newGeneratedCases := slices.Clone(generatedCases)

		for _, gc := range generatedCases[0:min(500, len(generatedCases))] {
			expected := []interface{}{`(`}
			expected = append(expected, gc.expected...)
			expected = append(expected, `)`)

			newGeneratedCases = append(newGeneratedCases,
				parenthesisTestCase{
					input:    `(` + gc.input + `)`,
					expected: []interface{}{expected},
				})
		}
		for _, gc1 := range generatedCases[0:min(25, len(generatedCases))] {
			for _, gc2 := range generatedCases[0:min(25, len(generatedCases))] {
				expected := slices.Clone(gc1.expected)
				expected = append(expected, gc2.expected...)

				newGeneratedCases = append(newGeneratedCases,
					parenthesisTestCase{
						input:    gc1.input + gc2.input,
						expected: expected,
					})
			}
		}

		r.Shuffle(len(newGeneratedCases), func(i, j int) {
			newGeneratedCases[i], newGeneratedCases[j] = newGeneratedCases[j], newGeneratedCases[i]
		})
		generatedCases = newGeneratedCases
	}

	return generatedCases
}

func TestGroupParenthesisRandom(t *testing.T) {
	for _, tc := range generateRandomCases() {
		t.Run(tc.input, func(t *testing.T) {
			lexed := lexer_core.Lex(tc.input, parenthesisRuleList)
			node := parser_core.TokensToNode(lexed)

			GroupParenthesis(node)

			assertTokensEqual(t, tc.expected, node)
		})
	}
}

func FuzzGroupParenthesis(f *testing.F) {
	for _, testcase := range testCases {
		f.Add(testcase.input)
	}
	for _, testcase := range generateRandomCases() {
		f.Add(testcase.input)
	}

	f.Fuzz(func(t *testing.T, input string) {
		lexed := lexer_core.Lex(input, parenthesisRuleList)

		node := parser_core.TokensToNode(lexed)
		assert.Equal(t, input, ConcatTokenNodes(node))

		GroupParenthesis(node)
		assert.Equal(t, input, ConcatTokenNodes(node))
	})
}
