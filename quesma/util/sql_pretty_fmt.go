package util

import (
	"github.com/DataDog/go-sqllexer"
	"strings"
)

var newLineKeywords = map[string]bool{
	"FROM":  true,
	"WHERE": true,
	"GROUP": true,
	"ORDER": true,
	"LIMIT": true,
}

const (
	lineLengthLimit     = 80
	whitespacePerIndent = 2
)

func SqlPrettyPrint(sqlData []byte) string {
	lexer := sqllexer.New(string(sqlData))
	tokens := lexer.ScanAll()
	var sb strings.Builder
	lineLength := 0
	subQueryIndent := 0
	isBreakIndent := false
	stack := []string{}
	for _, token := range tokens {
		// Super useful, uncomment to debug and run go test ./...
		// fmt.Print(token, ", ")

		// Skip original whitespace
		if token.Type == sqllexer.WS {
			if strings.ContainsRune(token.Value, '\n') {
				continue
			} else {
				token.Value = " "
			}
		}

		// Add new line if needed
		if newLineKeywords[token.Value] {
			sb.WriteString("\n")
			lineLength = 0
			isBreakIndent = false
		}
		if token.Value == "SELECT" {
			if len(stack) > 0 && stack[len(stack)-1] == "SELECT" {
				stack = stack[:len(stack)-1]
				if lineLength > 0 {
					sb.WriteString("\n\n")
					lineLength = 0
				}
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
		}

		// regular print
		sb.WriteString(token.Value)
		lineLength += len(token.Value)
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
