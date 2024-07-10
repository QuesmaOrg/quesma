// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package util

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

func TestPrettySubQuery(t *testing.T) {
	sql := `SELECT "clientip", count() FROM ( SELECT "clientip" FROM "kibana_sample_data_logs" WHERE "@timestamp">=parseDateTime64BestEffort('2024-04-08T08:38:14.246Z') AND "@timestamp"<=parseDateTime64BestEffort('2024-04-09T09:38:14.246Z') LIMIT 20000) GROUP BY "clientip" ORDER BY count() DESC`
	expect := `SELECT "clientip", count()
FROM (
  SELECT "clientip"
  FROM "kibana_sample_data_logs"
  WHERE "@timestamp">=parseDateTime64BestEffort('2024-04-08T08:38:14.246Z') AND
    "@timestamp"<=parseDateTime64BestEffort('2024-04-09T09:38:14.246Z')
  LIMIT 20000)
GROUP BY "clientip"
ORDER BY count() DESC`
	sqlFormatted := SqlPrettyPrint([]byte(sql))
	assert.Equal(t, expect, sqlFormatted)
}

func TestDontExpand(t *testing.T) {
	expect := `SELECT "clientip", count()
FROM "kibana_sample_data_logs"
WHERE "@timestamp">=parseDateTime64BestEffort('2024-04-08T08:38:14.246Z') AND
  "@timestamp"<=parseDateTime64BestEffort('2024-04-09T09:38:14.246Z')
GROUP BY "clientip"
ORDER BY count() DESC`
	sqlFormatted := SqlPrettyPrint([]byte(expect))
	assert.Equal(t, expect, sqlFormatted)
}

func TestSqlWith(t *testing.T) {
	sql := `SELECT count() FROM "kibana_sample_data_ecommerce"
WITH subQuery_1 AS (SELECT "animalType" AS "subQuery_1_1", count() AS "subQuery_1_cnt" FROM "default"."animal_index" WHERE ("date">=parseDateTime64BestEffort('2024-04-17T08:53:18.456Z') AND "date"<=parseDateTime64BestEffort('2024-07-10T08:53:18.456Z')) GROUP BY "animalType" ORDER BY count() DESC, "animalType" LIMIT 5) SELECT "animalType", "zooName", count() FROM "default"."animal_index" INNER JOIN "subQuery_1" ON "animalType" = "subQuery_1_1" WHERE ("date">=parseDateTime64BestEffort('2024-04-17T08:53:18.456Z') AND "date"<=parseDateTime64BestEffort('2024-07-10T08:53:18.456Z')) GROUP BY "animalType", "zooName", subQuery_1_cnt ORDER BY subQuery_1_cnt DESC, "animalType", count() DESC, "zooName" LIMIT 6`
	expect := `SELECT count()
FROM "kibana_sample_data_ecommerce"

WITH subQuery_1 AS (
  SELECT "animalType" AS "subQuery_1_1", count() AS "subQuery_1_cnt"
  FROM "default"."animal_index"
  WHERE ("date">=parseDateTime64BestEffort('2024-04-17T08:53:18.456Z') AND
    "date"<=parseDateTime64BestEffort('2024-07-10T08:53:18.456Z'))
  GROUP BY "animalType"
  ORDER BY count() DESC, "animalType"
  LIMIT 5)
SELECT "animalType", "zooName", count()
FROM "default"."animal_index" INNER JOIN "subQuery_1" ON "animalType" =
  "subQuery_1_1"
WHERE ("date">=parseDateTime64BestEffort('2024-04-17T08:53:18.456Z') AND "date"
  <=parseDateTime64BestEffort('2024-07-10T08:53:18.456Z'))
GROUP BY "animalType", "zooName", subQuery_1_cnt
ORDER BY subQuery_1_cnt DESC, "animalType", count() DESC, "zooName"
LIMIT 6`
	sqlFormatted := SqlPrettyPrint([]byte(sql))
	assert.Equal(t, expect, sqlFormatted)
}
