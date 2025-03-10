// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package elastic_query_dsl

import (
	"github.com/QuesmaOrg/quesma/platform/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
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
		"now-15m":      "subDate(now(),INTERVAL 15 minute)",
		"now-15m+5s":   "addDate(subDate(now(),INTERVAL 15 minute),INTERVAL 5 second)",
		"now-":         "now()",
		"now-15m+/M":   "toStartOfMonth(subDate(now(),INTERVAL 15 minute))",
		"now-15m/d":    "toStartOfDay(subDate(now(),INTERVAL 15 minute))",
		"now-15m+5s/w": "toStartOfWeek(addDate(subDate(now(),INTERVAL 15 minute),INTERVAL 5 second))",
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

			resultExpr, err := renderer.RenderExpr(dt)
			assert.NoError(t, err)

			if err != nil {
				return
			}

			assert.Equal(t, expected, model.AsString(resultExpr))

		})
	}
}

func Test_DateMathExpressionAsLiteral(t *testing.T) {
	now := time.Date(2024, 5, 17, 12, 1, 2, 3, time.UTC)
	tests := []struct {
		input    string
		expected time.Time
	}{
		{"now", now},
		{"now-15m", now.Add(-15 * time.Minute)},
		{"now-15m+5s", now.Add(-15 * time.Minute).Add(5 * time.Second)},
		{"now-", now},
		{"now-15m+/M", time.Date(2024, 5, 1, 0, 0, 0, 0, time.UTC)},
		{"now-15m/d", time.Date(2024, 5, 17, 0, 0, 0, 0, time.UTC)},
		{"now-15m+5s/w", time.Date(2024, 5, 12, 0, 0, 0, 0, time.UTC)}, // week starts on Sunday here so 2024-05-12 is the start of the week
		{"now-/Y", time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)},
		{"now-2M", now.AddDate(0, -2, 0)},
		{"now-1y", now.AddDate(-1, 0, 0)},
		{"now-1w", now.Add(-7 * 24 * time.Hour)},
		{"now-1s", now.Add(-1 * time.Second)},
		{"now-1m", now.Add(-1 * time.Minute)},
		{"now-1d", now.Add(-24 * time.Hour)},
	}

	for _, test := range tests {
		t.Run(test.input, func(tt *testing.T) {

			dt, err := ParseDateMathExpression(test.input)
			assert.NoError(tt, err)

			if err != nil {
				return
			}

			// this renderer is single use, so we can't reuse it
			renderer := DateMathExpressionRendererFactory(DateMathExpressionFormatLiteralTest)

			resultExpr, err := renderer.RenderExpr(dt)
			assert.NoError(t, err)

			if err != nil {
				return
			}

			assert.Equal(t, model.NewFunction(model.FromUnixTimestampMs, model.NewLiteral(model.TimeLiteral{Value: test.expected})), resultExpr)
		})
	}
}
