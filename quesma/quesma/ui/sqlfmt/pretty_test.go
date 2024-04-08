package sqlfmt

import (
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
)

func TestSqlPrettyPrint(t *testing.T) {
	sql := `SELECT * FROM "logs-generic-default" WHERE (message LIKE '%user%' AND (timestamp>=parseDateTime64BestEffort('2024-01-22T09:26:10.299Z') AND timestamp<=parseDateTime64BestEffort('2024-12-22T09:41:10.299Z')))`

	sqlFormatted := SqlPrettyPrint([]byte(sql))

	sqlExpected := `SELECT *
FROM "logs-generic-default"
WHERE (message LIKE '%user%' AND (timestamp>=parseDateTime64BestEffort(
  '2024-01-22T09:26:10.299Z') AND timestamp<=parseDateTime64BestEffort(
  '2024-12-22T09:41:10.299Z')))`

	assert.Equal(t, sqlExpected, sqlFormatted)
}

func TestSqlPrettyPrint_multipleSqls(t *testing.T) {
	sql := `SELECT '', '', count() FROM "kibana_sample_data_ecommerce" WHERE (("order_date">=parseDateTime64BestEffort('2024-02-19T12:59:40.626Z') AND "order_date"<=parseDateTime64BestEffort('2024-02-26T12:59:40.626Z')) OR ("order_date"<=parseDateTime64BestEffort('2024-02-19T12:59:40.626Z') AND "order_date">=parseDateTime64BestEffort('2024-02-12T12:59:40.626Z'))) AND ("order_date">=parseDateTime64BestEffort('2024-02-19T12:59:40.626Z') AND "order_date"<=parseDateTime64BestEffort('2024-02-26T12:59:40.626Z'))
SELECT '', '', count() FROM "kibana_sample_data_ecommerce" WHERE (("order_date">=parseDateTime64BestEffort('2024-02-19T12:59:40.626Z') AND "order_date"<=parseDateTime64BestEffort('2024-02-26T12:59:40.626Z')) OR ("order_date"<=parseDateTime64BestEffort('2024-02-19T12:59:40.626Z') AND "order_date">=parseDateTime64BestEffort('2024-02-12T12:59:40.626Z'))) AND ("order_date">=parseDateTime64BestEffort('2024-02-12T12:59:40.626Z') AND "order_date"<=parseDateTime64BestEffort('2024-02-19T12:59:40.626Z'))
SELECT '', '', '', sum("taxful_total_price") FROM "kibana_sample_data_ecommerce" WHERE ("order_date">=parseDateTime64BestEffort('2024-02-19T12:59:40.626Z') AND "order_date"<=parseDateTime64BestEffort('2024-02-26T12:59:40.626Z')) OR ("order_date"<=parseDateTime64BestEffort('2024-02-19T12:59:40.626Z') AND "order_date">=parseDateTime64BestEffort('2024-02-12T12:59:40.626Z'))`

	sqlFormatted := SqlPrettyPrint([]byte(sql))
	sqlExpected := `SELECT '', '', count()
FROM "kibana_sample_data_ecommerce"
WHERE (("order_date">=parseDateTime64BestEffort('2024-02-19T12:59:40.626Z') AND
  "order_date"<=parseDateTime64BestEffort('2024-02-26T12:59:40.626Z')) OR (
  "order_date"<=parseDateTime64BestEffort('2024-02-19T12:59:40.626Z') AND
  "order_date">=parseDateTime64BestEffort('2024-02-12T12:59:40.626Z'))) AND (
  "order_date">=parseDateTime64BestEffort('2024-02-19T12:59:40.626Z') AND
  "order_date"<=parseDateTime64BestEffort('2024-02-26T12:59:40.626Z'))
SELECT '', '', count()
FROM "kibana_sample_data_ecommerce"
WHERE (("order_date">=parseDateTime64BestEffort('2024-02-19T12:59:40.626Z') AND
  "order_date"<=parseDateTime64BestEffort('2024-02-26T12:59:40.626Z')) OR (
  "order_date"<=parseDateTime64BestEffort('2024-02-19T12:59:40.626Z') AND
  "order_date">=parseDateTime64BestEffort('2024-02-12T12:59:40.626Z'))) AND (
  "order_date">=parseDateTime64BestEffort('2024-02-12T12:59:40.626Z') AND
  "order_date"<=parseDateTime64BestEffort('2024-02-19T12:59:40.626Z'))
SELECT '', '', '', sum("taxful_total_price")
FROM "kibana_sample_data_ecommerce"
WHERE ("order_date">=parseDateTime64BestEffort('2024-02-19T12:59:40.626Z') AND
  "order_date"<=parseDateTime64BestEffort('2024-02-26T12:59:40.626Z')) OR (
  "order_date"<=parseDateTime64BestEffort('2024-02-19T12:59:40.626Z') AND
  "order_date">=parseDateTime64BestEffort('2024-02-12T12:59:40.626Z'))`

	assert.Equal(t, sqlExpected, sqlFormatted)
}

// Test checking if SqlPrettyPrint deals well with backticks.
// If you don't process backticks accordingly, SqlPrettyPrint throws an error.
func TestSqlPrettPrintBackticks(t *testing.T) {
	sql := "SELECT toInt64(toUnixTimestamp64Milli(`@timestamp`)/30000), count() FROM " + `"logs-generic-default" WHERE ("@timestamp">=parseDateTime64BestEffort('2024-02-04T11:11:29.735Z') AND "@timestamp"<=parseDateTime64BestEffort('2024-02-04T11:26:29.735Z')) AND ("@timestamp">=timestamp_sub(SECOND,900, now64())) GROUP BY toInt64(toUnixTimestamp64Milli(` + "`@timestamp`)/30000)"
	sqlFormatted := SqlPrettyPrint([]byte(sql))
	assert.Greater(t, len(strings.Split(sqlFormatted, "\n")), 1) // if error, SqlPrettyPrint returns input string with len == 1
}

func TestInvalidSql(t *testing.T) {
	invalidSql := `this sql is a joke, you are not going to parse it`
	sqlFormatted := SqlPrettyPrint([]byte(invalidSql))
	assert.Equal(t, invalidSql, sqlFormatted)
}

func TestGroupBySql(t *testing.T) {
	sql := "SELECT toInt64(toUnixTimestamp64Milli(`@timestamp`)/30000), count() FROM \"logs-generic-default\" WHERE \"@timestamp\">=parseDateTime64BestEffort('2024-04-08T14:42:43.243Z') AND \"@timestamp\"<=parseDateTime64BestEffort('2024-04-08T14:57:43.243Z')  GROUP BY (toInt64(toUnixTimestamp64Milli(`@timestamp`)/30000)) ORDER BY (toInt64(toUnixTimestamp64Milli(`@timestamp`)/30000))"
	expect := "SELECT toInt64(toUnixTimestamp64Milli(`@timestamp`)/30000), count()\n" +
		"FROM \"logs-generic-default\"\n" +
		"WHERE \"@timestamp\">=parseDateTime64BestEffort('2024-04-08T14:42:43.243Z') AND\n" +
		"  \"@timestamp\"<=parseDateTime64BestEffort('2024-04-08T14:57:43.243Z')\n" +
		"GROUP BY (toInt64(toUnixTimestamp64Milli(`@timestamp`)/30000))\n" +
		"ORDER BY (toInt64(toUnixTimestamp64Milli(`@timestamp`)/30000))"
	sqlFormatted := SqlPrettyPrint([]byte(sql))
	assert.Equal(t, expect, sqlFormatted)
}
