package queryparser

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestParseDateMathExpression(t *testing.T) {

	tests := []struct {
		input    string
		expected *DateMathExpression
	}{
		{"now", &DateMathExpression{intervals: []DateMathInterval{}, rounding: ""}},
		{"now-15m", &DateMathExpression{intervals: []DateMathInterval{{amount: -15, unit: "m"}}, rounding: ""}},
		{"now-15m-25s", &DateMathExpression{intervals: []DateMathInterval{{amount: -15, unit: "m"}, {amount: -25, unit: "s"}}, rounding: ""}},
		{"now-15m-25s/y", &DateMathExpression{intervals: []DateMathInterval{{amount: -15, unit: "m"}, {amount: -25, unit: "s"}}, rounding: "y"}},
		{"now-15m-25s/y", &DateMathExpression{intervals: []DateMathInterval{{amount: -15, unit: "m"}, {amount: -25, unit: "s"}}, rounding: "y"}},
	}

	for _, test := range tests {
		t.Run(test.input, func(tt *testing.T) {
			result, err := ParseDateMathExpression(test.input)
			require.NoError(tt, err)
			assert.Equal(tt, test.expected, result)
		})
	}
}

func Test_parseDateTimeInClickhouseMathLanguage(t *testing.T) {
	exprs := map[string]string{
		"now":          "now()",
		"now-15m":      "subDate(now(), INTERVAL 15 minute)",
		"now-15m+5s":   "addDate(subDate(now(), INTERVAL 15 minute), INTERVAL 5 second)",
		"now-":         "now()",
		"now-15m+/M":   "toStartOfMonth(subDate(now(), INTERVAL 15 minute))",
		"now-15m/d":    "toStartOfDay(subDate(now(), INTERVAL 15 minute))",
		"now-15m+5s/w": "toStartOfWeek(addDate(subDate(now(), INTERVAL 15 minute), INTERVAL 5 second))",
		"now-/Y":       "toStartOfYear(now())",
	}

	renderer := &DateMathAsClickhouseIntervals{}

	for expr, expected := range exprs {
		t.Run(expr, func(tt *testing.T) {

			dt, err := ParseDateMathExpression(expr)
			assert.NoError(tt, err)

			if err != nil {
				return
			}

			resultExpr, err := renderer.RenderSQL(dt)
			assert.NoError(t, err)

			if err != nil {
				return
			}

			assert.Equal(t, expected, resultExpr)

		})
	}
}

func Test_DateMathExpressionAsLiteral(t *testing.T) {
	exprs := map[string]string{
		"now":          "'2024-05-17 12:01:02'",
		"now-15m":      "'2024-05-17 11:46:02'",
		"now-15m+5s":   "'2024-05-17 11:46:07'",
		"now-":         "'2024-05-17 12:01:02'",
		"now-15m+/M":   "'2024-05-01 00:00:00'",
		"now-15m/d":    "'2024-05-17 00:00:00'",
		"now-15m+5s/w": "'2024-05-12 00:00:00'", // week starts on Sunday here so 2024-05-12 is the start of the week
		"now-/Y":       "'2024-01-01 00:00:00'",
	}

	for expr, expected := range exprs {
		t.Run(expr, func(tt *testing.T) {

			dt, err := ParseDateMathExpression(expr)
			assert.NoError(tt, err)

			if err != nil {
				return
			}

			// this renderer is single use, so we can't reuse it
			renderer := DateMathExpressionRendererFactory(DateMathExpressionFormatLiteralTest)

			resultExpr, err := renderer.RenderSQL(dt)
			assert.NoError(t, err)

			if err != nil {
				return
			}

			assert.Equal(t, expected, resultExpr)
		})
	}
}
