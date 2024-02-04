package quesma

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSqlPrettPrint(t *testing.T) {
	sql := `SELECT * FROM "logs-generic-default" WHERE (message LIKE '%user%' AND (timestamp>=parseDateTime64BestEffort('2024-01-22T09:26:10.299Z') AND timestamp<=parseDateTime64BestEffort('2024-12-22T09:41:10.299Z')))`

	sqlFormatted := sqlPrettyPrint([]byte(sql))

	sqlExpected := `SELECT *
FROM "logs-generic-default"
WHERE message LIKE '%user%'
AND (
    "timestamp" >= parsedatetime64besteffort('2024-01-22T09:26:10.299Z')
    AND "timestamp" <= parsedatetime64besteffort('2024-12-22T09:41:10.299Z')
  );`

	assert.Equal(t, sqlExpected, sqlFormatted)
}

// Test checking if SqlPrettyPrint deals well with backticks.
// If you don't process backticks accordingly, SqlPrettyPrint throws an error.
func TestSqlPrettPrintBackticks(t *testing.T) {
	sql := "SELECT toInt64(toUnixTimestamp64Milli(`@timestamp`)/30000), count() FROM " + `"logs-generic-default" WHERE ("@timestamp">=parseDateTime64BestEffort('2024-02-04T11:11:29.735Z') AND "@timestamp"<=parseDateTime64BestEffort('2024-02-04T11:26:29.735Z')) AND ("@timestamp">=timestamp_sub(SECOND,900, now64())) GROUP BY toInt64(toUnixTimestamp64Milli(` + "`@timestamp`)/30000)"
	sqlFormatted := sqlPrettyPrint([]byte(sql))
	assert.Greater(t, len(strings.Split(sqlFormatted, "\n")), 1) // if error, SqlPrettyPrint returns input string with len == 1
}

func TestInvalidSql(t *testing.T) {
	invalidSql := `this sql is a joke, you are not going to parse it`
	sqlFormatted := sqlPrettyPrint([]byte(invalidSql))
	assert.Equal(t, invalidSql, sqlFormatted)
}
