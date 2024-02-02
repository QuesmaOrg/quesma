package quesma

import (
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

func TestInvalidSql(t *testing.T) {
	invalidSql := `this sql is a joke, you are not going to parse it`
	sqlFormatted := sqlPrettyPrint([]byte(invalidSql))
	assert.Equal(t, invalidSql, sqlFormatted)
}
