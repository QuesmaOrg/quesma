// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package core

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

var (
	whitespaceTokenType = &TokenType{
		Name:        "Whitespace",
		Description: "Whitespace token",
	}
	testKeywordTokenType = &TokenType{
		Name:        "TEST keyword",
		Description: "Token for the keyword TEST",
	}

	whitespaceRule  = NewRegexRule(`\s+`, whitespaceTokenType)
	testKeywordRule = NewStringRule("TEST", testKeywordTokenType)

	whitespaceAndTestKeywordRuleList = NewRuleList(whitespaceRule, testKeywordRule)
)

func TestLexSimpleWhitespaceAndTestKeyword(t *testing.T) {
	input := "   TEST"
	tokens := Lex(input, whitespaceAndTestKeywordRuleList)

	assert.Equal(t, 2, len(tokens))

	assert.Equal(t, "   ", tokens[0].RawValue)
	assert.Equal(t, "TEST", tokens[1].RawValue)

	assert.Equal(t, 0, tokens[0].Position)
	assert.Equal(t, 3, tokens[1].Position)

	assert.Equal(t, whitespaceTokenType, tokens[0].Type)
	assert.Equal(t, testKeywordTokenType, tokens[1].Type)
}

func TestLexMultipleWhitespaceAndTestKeywords(t *testing.T) {
	input := "  TEST \n\t  TEST   TEST\n   "
	tokens := Lex(input, whitespaceAndTestKeywordRuleList)

	assert.Equal(t, 7, len(tokens))

	// First whitespace
	assert.Equal(t, "  ", tokens[0].RawValue)
	assert.Equal(t, whitespaceTokenType, tokens[0].Type)
	assert.Equal(t, 0, tokens[0].Position)

	// First TEST
	assert.Equal(t, "TEST", tokens[1].RawValue)
	assert.Equal(t, testKeywordTokenType, tokens[1].Type)
	assert.Equal(t, 2, tokens[1].Position)

	// Middle whitespace with newline and tab
	assert.Equal(t, " \n\t  ", tokens[2].RawValue)
	assert.Equal(t, whitespaceTokenType, tokens[2].Type)
	assert.Equal(t, 6, tokens[2].Position)

	// Second TEST
	assert.Equal(t, "TEST", tokens[3].RawValue)
	assert.Equal(t, testKeywordTokenType, tokens[3].Type)
	assert.Equal(t, 11, tokens[3].Position)

	// Middle whitespace
	assert.Equal(t, "   ", tokens[4].RawValue)
	assert.Equal(t, whitespaceTokenType, tokens[4].Type)
	assert.Equal(t, 15, tokens[4].Position)

	// Third TEST
	assert.Equal(t, "TEST", tokens[5].RawValue)
	assert.Equal(t, testKeywordTokenType, tokens[5].Type)
	assert.Equal(t, 18, tokens[5].Position)

	// Final whitespace with newline
	assert.Equal(t, "\n   ", tokens[6].RawValue)
	assert.Equal(t, whitespaceTokenType, tokens[6].Type)
	assert.Equal(t, 22, tokens[6].Position)
}

func TestLexCaseInsensitiveTestKeyword(t *testing.T) {
	input := "teST"
	tokens := Lex(input, whitespaceAndTestKeywordRuleList)

	assert.Equal(t, 1, len(tokens))
	assert.Equal(t, "teST", tokens[0].RawValue)
	assert.Equal(t, 0, tokens[0].Position)
	assert.Equal(t, testKeywordTokenType, tokens[0].Type)
}

func TestLexNoMatch(t *testing.T) {
	input := "TESTING"
	tokens := Lex(input, whitespaceAndTestKeywordRuleList)

	assert.Equal(t, 2, len(tokens))

	assert.Equal(t, "TEST", tokens[0].RawValue)
	assert.Equal(t, 0, tokens[0].Position)
	assert.Equal(t, testKeywordTokenType, tokens[0].Type)

	assert.NotEqual(t, "ING", tokens[1].RawValue)
	assert.Contains(t, tokens[1].RawValue, "rule did not match")
	assert.Equal(t, 4, tokens[1].Position)
	assert.Equal(t, ErrorTokenType, tokens[1].Type)
}

func TestLexEmptyInput(t *testing.T) {
	input := ""
	tokens := Lex(input, whitespaceAndTestKeywordRuleList)

	assert.Equal(t, 0, len(tokens))
}

func FuzzLexWhitespaceAndTestKeyword(f *testing.F) {
	f.Add("TEST TEST TEST\n   ")
	f.Add("  TEST   TEST TEST\n   ")
	f.Add("  TEST TEST TEST\n   ")
	f.Add("  TEST \n\t  TEST   TEST\n   ")
	f.Add("teST")
	f.Add("TESTING")
	f.Add("quesma TESTING")
	f.Add("")

	f.Fuzz(func(t *testing.T, input string) {
		tokens := Lex(input, whitespaceAndTestKeywordRuleList)

		// Basic validation that tokens are well-formed
		for i, token := range tokens {
			// Position should never be negative
			if token.Position < 0 {
				t.Errorf("Token position is negative: %d", token.Position)
			}

			// Token raw value should not be empty
			if len(token.RawValue) == 0 {
				t.Error("Token has empty raw value")
			}

			// Basic checks for specific token types
			switch token.Type {
			case whitespaceTokenType:
				assert.NotContains(t, strings.ToUpper(token.RawValue), "TEST")
			case testKeywordTokenType:
				assert.Equal(t, "TEST", strings.ToUpper(token.RawValue))
			case ErrorTokenType:
				if i != len(tokens)-1 {
					t.Error("Error token is not the last token")
				}
			default:
				t.Errorf("Unexpected token type: %v", token.Type)
			}

			// Position should be within input string bounds
			if token.Position > len(input) {
				t.Errorf("Token position %d exceeds input length %d", token.Position, len(input))
			}
		}
	})
}

func BenchmarkLexWhitespaceAndTestKeyword(b *testing.B) {
	testCases := []string{
		"",
		"    ",
		"TEST",
		"TEST TEST TEST\n   ",
		"  TEST   TEST TEST\n   ",
		"TeSt tEsT TEST",
		"quesma",
	}

	for _, tc := range testCases {
		b.Run(tc, func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				Lex(tc, whitespaceAndTestKeywordRuleList)
			}
		})
	}
}
