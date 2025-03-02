// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package testutils

import (
	"bytes"
	"os"
)

type ParsedTestcase struct {
	Query          string
	ExpectedTokens []ExpectedToken
}

type ExpectedToken struct {
	TokenType  string
	TokenValue string
}

// Loads a list of test queries and their expected tokens (extracted from existing parsers).
// The structure of the file is as follows:
//
//	[QUERY1]
//	<end_of_query/>
//	[TOKEN_TYPE_1]
//	[TOKEN_VALUE_1]
//	<end_of_token/>
//	[TOKEN_TYPE_2]
//	[TOKEN_VALUE_2]
//	<end_of_token/>
//	...
//	<end_of_tokens/>
//	[QUERY2]
//	...
func LoadParsedTestcases(filename string) []ParsedTestcase {
	contents, err := os.ReadFile(filename)
	if err != nil {
		panic(err)
	}

	testcases := bytes.Split(contents, []byte("\n<end_of_tokens/>\n"))
	testcases = testcases[:len(testcases)-1]

	var parsedTestcases []ParsedTestcase
	for _, testcase := range testcases {
		endOfQuerySplit := bytes.Split(testcase, []byte("\n<end_of_query/>\n"))

		query := string(endOfQuerySplit[0])

		tokens := bytes.Split(endOfQuerySplit[1], []byte("\n<end_of_token/>\n"))
		tokens = tokens[:len(tokens)-1]

		var expectedTokens []ExpectedToken
		for _, tokenDescription := range tokens {
			tokenDescriptionSplit := bytes.SplitN(tokenDescription, []byte("\n"), 2)
			tokenType := string(tokenDescriptionSplit[0])
			tokenValue := string(tokenDescriptionSplit[1])
			expectedTokens = append(expectedTokens, ExpectedToken{tokenType, tokenValue})
		}

		parsedTestcases = append(parsedTestcases, ParsedTestcase{query, expectedTokens})
	}
	return parsedTestcases
}
