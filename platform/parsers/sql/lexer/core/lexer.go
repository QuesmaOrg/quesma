// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package core

import "fmt"

func Lex(input string, rule Rule) []Token {
	var tokens []Token
	position := 0

	for position < len(input) {
		token, matched := rule.Match(input, position)

		if matched {
			tokens = append(tokens, token)
			position += len(token.RawValue)
		} else {
			errorToken := MakeToken(position, fmt.Sprintf("rule did not match input at position %d: '%.20s'", position, input[position:]), ErrorTokenType)
			tokens = append(tokens, errorToken)
			break
		}
	}

	return tokens
}
