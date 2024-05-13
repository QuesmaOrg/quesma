package queryparser

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_parseDateTimeInClickhouseMathLanguage(t *testing.T) {
	exprs := map[string]string{
		"now-15m":      "subDate(now(), INTERVAL 15 minute)",
		"now-15m+5s":   "addDate(subDate(now(), INTERVAL 15 minute), INTERVAL 5 second)",
		"now-":         "now()",
		"now-15m+/M":   "toStartOfMonth(subDate(now(), INTERVAL 15 minute))",
		"now-15m/d":    "toStartOfDay(subDate(now(), INTERVAL 15 minute))",
		"now-15m+5s/w": "toStartOfWeek(addDate(subDate(now(), INTERVAL 15 minute), INTERVAL 5 second))",
		"now-/Y":       "toStartOfYear(now())",
	}
	cw := ClickhouseQueryTranslator{}
	for expr, expected := range exprs {
		resultExpr := cw.parseDateTimeInClickhouseMathLanguage(expr)
		assert.Equal(t, expected, resultExpr)
	}
}
