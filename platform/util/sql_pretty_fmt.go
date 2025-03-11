// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package util

import (
	"github.com/DataDog/go-sqllexer"
	"strings"
)

var newLineKeywords = map[string]bool{
	"FROM":  true,
	"WHERE": true,
	"GROUP": true,
	"LIMIT": true,
}

const (
	lineLengthLimit     = 80
	whitespacePerIndent = 2
)

func calcHowMuchNextStmtWillTake(tokens []sqllexer.Token) int {
	result := 0
	stack := []string{}
	for _, token := range tokens {
		if token.Type == sqllexer.WS {
			result += 1
			continue
		}
		if token.Value == "(" {
			stack = append(stack, token.Value)
		} else if token.Value == ")" {
			if len(stack) > 0 {
				stack = stack[:len(stack)-1]
			} else {
				return result
			}
		} else if token.Value == "," {
			if len(stack) == 0 {
				return result + 1
			}
		} else if newLineKeywords[token.Value] {
			return result
		} // maybe more breaks
		result += len(token.Value)
	}
	return result
}

func SqlPrettyPrint(sqlData []byte) string {
	lexer := sqllexer.New(string(sqlData))
	tokens := lexer.ScanAll()
	var sb strings.Builder
	lineLength := 0
	subQueryIndent := 0
	isBreakIndent := false
	stack := []string{}
	for tokenIdx, token := range tokens {
		// Super useful, uncomment to debug and run go test ./...
		// fmt.Print(token, ", ")

		// Skip original whitespace
		if token.Type == sqllexer.WS {
			token.Value = " "
			if tokenIdx > 0 && tokens[tokenIdx-1].Value == "(" {
				continue
			}
		}

		// Add new line if needed
		if newLineKeywords[token.Value] {
			sb.WriteString("\n")
			lineLength = 0
			isBreakIndent = false
		}
		if token.Value == "ORDER" && len(stack) > 0 && stack[len(stack)-1] != "(" {
			sb.WriteString("\n")
			lineLength = 0
			isBreakIndent = false
		}
		if token.Value == "WITH" {
			if len(stack) > 0 {
				sb.WriteString("\n\n")
				lineLength = 0
			}
			isBreakIndent = false
			stack = []string{token.Value}
		}
		if token.Value == "SELECT" {
			if len(stack) > 0 && stack[len(stack)-1] == "SELECT" {
				stack = stack[:len(stack)-1]
				if lineLength > 0 {
					sb.WriteString("\n\n")
					lineLength = 0
				}
			}
			if len(stack) > 0 && stack[len(stack)-1] == "WITH" {
				stack = stack[:len(stack)-1]
			}
			if len(stack) > 0 {
				subQueryIndent += 1
			}
			stack = append(stack, token.Value)
			isBreakIndent = false
			if lineLength > 0 {
				sb.WriteString("\n")
				lineLength = 0
			}
		}

		if token.Value == "(" {
			stack = append(stack, token.Value)
		} else if token.Value == ")" {
			for len(stack) > 0 {
				last := stack[len(stack)-1]
				stack = stack[:len(stack)-1]
				if last == "(" {
					break
				} else if last == "SELECT" {
					subQueryIndent -= 1
				}
			}
		}

		// Break line if needed
		if lineLength > 0 && len(token.Value)+lineLength > lineLengthLimit {
			if token.Type == sqllexer.WS && tokenIdx+1 < len(tokens) && newLineKeywords[tokens[tokenIdx+1].Value] {
				continue // we will break line in next token anyway, no need to double break
			}
			lineLength = 0
			isBreakIndent = true
			sb.WriteString("\n")
		}

		// Add indentation if needed
		if lineLength == 0 {
			currentIndentLevel := subQueryIndent * whitespacePerIndent
			if isBreakIndent {
				currentIndentLevel += whitespacePerIndent
			}
			for range currentIndentLevel {
				sb.WriteString(" ")
			}
			lineLength += currentIndentLevel
			if token.Type == sqllexer.WS {
				continue
			}
		}

		// regular print
		sb.WriteString(token.Value)
		lineLength += len(token.Value)

		// comma after , in long SELECT?
		if token.Value == "," {
			if len(stack) > 0 && stack[len(stack)-1] == "SELECT" {
				howMuchNextWillTake := calcHowMuchNextStmtWillTake(tokens[tokenIdx+1:])
				if lineLength+howMuchNextWillTake > lineLengthLimit {
					sb.WriteString("\n")
					lineLength = 0
					isBreakIndent = true
				}
			} else if len(stack) > 0 && stack[len(stack)-1] == "WITH" {
				sb.WriteString("\n")
				lineLength = 0
				isBreakIndent = false
			} else {
				if tokenIdx+1 < len(tokens) && tokens[tokenIdx+1].Type != sqllexer.WS {
					sb.WriteString(" ")
					lineLength += 1
				}
			}
		}
	}
	// Add also space
	// fmt.Println()

	return removeTrailingSpaces(sb.String())
}

func removeTrailingSpaces(input string) string {
	lines := strings.Split(input, "\n")

	for i, line := range lines {
		lines[i] = strings.TrimRight(line, " \t")
	}

	return strings.Join(lines, "\n")
}
