package sqlfmt

import (
	sqllexer "github.com/DataDog/go-sqllexer"
	"strings"
)

var newLineKeywords = map[string]bool{
	"FROM":  true,
	"WHERE": true,
	"GROUP": true,
	"ORDER": true,
	"LIMIT": true,
}

func SqlPrettyPrint(sqlData []byte) string {
	lexer := sqllexer.New(string(sqlData))
	tokens := lexer.ScanAll()
	var sb strings.Builder
	lineLength := 0
	currentIndentLevel := 0
	for _, token := range tokens {
		// Super useful, uncomment to debug and run go test ./...
		//fmt.Println(token)
		if newLineKeywords[token.Value] {
			sb.WriteString("\n")
			lineLength = 0
			currentIndentLevel = 0
		}
		if token.Value == "SELECT" {
			currentIndentLevel = 0
			if lineLength > 0 {
				sb.WriteString("\n")
				lineLength = 0
			}
		}

		if lineLength > 0 && len(token.Value)+lineLength > 80 {
			lineLength = 0
			currentIndentLevel = 2
			sb.WriteString("\n")
		}
		if lineLength == 0 && currentIndentLevel > 0 {
			for range currentIndentLevel {
				sb.WriteString(" ")
			}
			lineLength += currentIndentLevel
		}
		sb.WriteString(token.Value)
		if token.Value == "\n" {
			lineLength = 0
		} else {
			lineLength += len(token.Value)
		}
	}
	return removeTrailingSpaces(sb.String())
}

func removeTrailingSpaces(input string) string {
	lines := strings.Split(input, "\n")

	for i, line := range lines {
		lines[i] = strings.TrimRight(line, " \t")
	}

	return strings.Join(lines, "\n")
}
