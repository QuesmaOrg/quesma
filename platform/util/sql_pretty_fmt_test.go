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
	sql := `SELECT '', '', count(*) FROM "kibana_sample_data_ecommerce" WHERE (("order_date">=parseDateTime64BestEffort('2024-02-19T12:59:40.626Z') AND "order_date"<=parseDateTime64BestEffort('2024-02-26T12:59:40.626Z')) OR ("order_date"<=parseDateTime64BestEffort('2024-02-19T12:59:40.626Z') AND "order_date">=parseDateTime64BestEffort('2024-02-12T12:59:40.626Z'))) AND ("order_date">=parseDateTime64BestEffort('2024-02-19T12:59:40.626Z') AND "order_date"<=parseDateTime64BestEffort('2024-02-26T12:59:40.626Z'))
SELECT '', '', count(*) FROM "kibana_sample_data_ecommerce" WHERE (("order_date">=parseDateTime64BestEffort('2024-02-19T12:59:40.626Z') AND "order_date"<=parseDateTime64BestEffort('2024-02-26T12:59:40.626Z')) OR ("order_date"<=parseDateTime64BestEffort('2024-02-19T12:59:40.626Z') AND "order_date">=parseDateTime64BestEffort('2024-02-12T12:59:40.626Z'))) AND ("order_date">=parseDateTime64BestEffort('2024-02-12T12:59:40.626Z') AND "order_date"<=parseDateTime64BestEffort('2024-02-19T12:59:40.626Z'))
SELECT '', '', '', sum("taxful_total_price") FROM "kibana_sample_data_ecommerce" WHERE ("order_date">=parseDateTime64BestEffort('2024-02-19T12:59:40.626Z') AND "order_date"<=parseDateTime64BestEffort('2024-02-26T12:59:40.626Z')) OR ("order_date"<=parseDateTime64BestEffort('2024-02-19T12:59:40.626Z') AND "order_date">=parseDateTime64BestEffort('2024-02-12T12:59:40.626Z'))`

	sqlFormatted := SqlPrettyPrint([]byte(sql))
	sqlExpected := `SELECT '', '', count(*)
FROM "kibana_sample_data_ecommerce"
WHERE (("order_date">=parseDateTime64BestEffort('2024-02-19T12:59:40.626Z') AND
  "order_date"<=parseDateTime64BestEffort('2024-02-26T12:59:40.626Z')) OR (
  "order_date"<=parseDateTime64BestEffort('2024-02-19T12:59:40.626Z') AND
  "order_date">=parseDateTime64BestEffort('2024-02-12T12:59:40.626Z'))) AND (
  "order_date">=parseDateTime64BestEffort('2024-02-19T12:59:40.626Z') AND
  "order_date"<=parseDateTime64BestEffort('2024-02-26T12:59:40.626Z'))

SELECT '', '', count(*)
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
	sql := "SELECT toInt64(toUnixTimestamp64Milli(`@timestamp`)/30000), count(*) FROM " + `"logs-generic-default" WHERE ("@timestamp">=parseDateTime64BestEffort('2024-02-04T11:11:29.735Z') AND "@timestamp"<=parseDateTime64BestEffort('2024-02-04T11:26:29.735Z')) AND ("@timestamp">=timestamp_sub(SECOND,900, now64())) GROUP BY toInt64(toUnixTimestamp64Milli(` + "`@timestamp`)/30000)"
	sqlFormatted := SqlPrettyPrint([]byte(sql))
	assert.Greater(t, len(strings.Split(sqlFormatted, "\n")), 1) // if error, SqlPrettyPrint returns input string with len == 1
}

func TestInvalidSql(t *testing.T) {
	invalidSql := `this sql is a joke, you are not going to parse it`
	sqlFormatted := SqlPrettyPrint([]byte(invalidSql))
	assert.Equal(t, invalidSql, sqlFormatted)
}

func TestGroupBySql(t *testing.T) {
	sql := "SELECT toInt64(toUnixTimestamp64Milli(`@timestamp`)/30000), count(*) FROM \"logs-generic-default\" WHERE \"@timestamp\">=parseDateTime64BestEffort('2024-04-08T14:42:43.243Z') AND \"@timestamp\"<=parseDateTime64BestEffort('2024-04-08T14:57:43.243Z')  GROUP BY (toInt64(toUnixTimestamp64Milli(`@timestamp`)/30000)) ORDER BY (toInt64(toUnixTimestamp64Milli(`@timestamp`)/30000))"
	expect := "SELECT toInt64(toUnixTimestamp64Milli(`@timestamp`)/30000), count(*)\n" +
		"FROM \"logs-generic-default\"\n" +
		"WHERE \"@timestamp\">=parseDateTime64BestEffort('2024-04-08T14:42:43.243Z') AND\n" +
		"  \"@timestamp\"<=parseDateTime64BestEffort('2024-04-08T14:57:43.243Z')\n" +
		"GROUP BY (toInt64(toUnixTimestamp64Milli(`@timestamp`)/30000))\n" +
		"ORDER BY (toInt64(toUnixTimestamp64Milli(`@timestamp`)/30000))"
	sqlFormatted := SqlPrettyPrint([]byte(sql))
	assert.Equal(t, expect, sqlFormatted)
}

func TestPrettySubQuery(t *testing.T) {
	sql := `SELECT "clientip", count(*) FROM ( SELECT "clientip" FROM "kibana_sample_data_logs" WHERE "@timestamp">=parseDateTime64BestEffort('2024-04-08T08:38:14.246Z') AND "@timestamp"<=parseDateTime64BestEffort('2024-04-09T09:38:14.246Z') LIMIT 20000) GROUP BY "clientip" ORDER BY count(*) DESC`
	expect := `SELECT "clientip", count(*)
FROM (
  SELECT "clientip"
  FROM "kibana_sample_data_logs"
  WHERE "@timestamp">=parseDateTime64BestEffort('2024-04-08T08:38:14.246Z') AND
    "@timestamp"<=parseDateTime64BestEffort('2024-04-09T09:38:14.246Z')
  LIMIT 20000)
GROUP BY "clientip"
ORDER BY count(*) DESC`
	sqlFormatted := SqlPrettyPrint([]byte(sql))
	assert.Equal(t, expect, sqlFormatted)
}

func TestDontExpand(t *testing.T) {
	expect := `SELECT "clientip", count(*)
FROM "kibana_sample_data_logs"
WHERE "@timestamp">=parseDateTime64BestEffort('2024-04-08T08:38:14.246Z') AND
  "@timestamp"<=parseDateTime64BestEffort('2024-04-09T09:38:14.246Z')
GROUP BY "clientip"
ORDER BY count(*) DESC`
	sqlFormatted := SqlPrettyPrint([]byte(expect))
	assert.Equal(t, expect, sqlFormatted)
}

func TestSqlWith(t *testing.T) {
	sql := `SELECT count(*) FROM "kibana_sample_data_ecommerce"
WITH subQuery_1 AS (SELECT "animalType" AS "subQuery_1_1", count(*) AS "subQuery_1_cnt" FROM "default"."animal_index" WHERE ("date">=parseDateTime64BestEffort('2024-04-17T08:53:18.456Z') AND "date"<=parseDateTime64BestEffort('2024-07-10T08:53:18.456Z')) GROUP BY "animalType" ORDER BY count(*) DESC, "animalType" LIMIT 5) SELECT "animalType", "zooName", count(*) FROM "default"."animal_index" INNER JOIN "subQuery_1" ON "animalType" = "subQuery_1_1" WHERE ("date">=parseDateTime64BestEffort('2024-04-17T08:53:18.456Z') AND "date"<=parseDateTime64BestEffort('2024-07-10T08:53:18.456Z')) GROUP BY "animalType", "zooName", subQuery_1_cnt ORDER BY subQuery_1_cnt DESC, "animalType", count(*) DESC, "zooName" LIMIT 6`
	expect := `SELECT count(*)
FROM "kibana_sample_data_ecommerce"

WITH subQuery_1 AS (
  SELECT "animalType" AS "subQuery_1_1", count(*) AS "subQuery_1_cnt"
  FROM "default"."animal_index"
  WHERE ("date">=parseDateTime64BestEffort('2024-04-17T08:53:18.456Z') AND
    "date"<=parseDateTime64BestEffort('2024-07-10T08:53:18.456Z'))
  GROUP BY "animalType"
  ORDER BY count(*) DESC, "animalType"
  LIMIT 5)
SELECT "animalType", "zooName", count(*)
FROM "default"."animal_index" INNER JOIN "subQuery_1" ON "animalType" =
  "subQuery_1_1"
WHERE ("date">=parseDateTime64BestEffort('2024-04-17T08:53:18.456Z') AND "date"
  <=parseDateTime64BestEffort('2024-07-10T08:53:18.456Z'))
GROUP BY "animalType", "zooName", subQuery_1_cnt
ORDER BY subQuery_1_cnt DESC, "animalType", count(*) DESC, "zooName"
LIMIT 6`
	sqlFormatted := SqlPrettyPrint([]byte(sql))
	assert.Equal(t, expect, sqlFormatted)
}

func TestSqlPrettyPancake(t *testing.T) {
	sql := `
SELECT "aggr__2__key_0", "aggr__2__count", "aggr__2__order_1",
  "metric__2__1_col_0", "aggr__2__8__key_0", "aggr__2__8__count",
  "aggr__2__8__order_1", "metric__2__8__1_col_0", "aggr__2__8__4__key_0",
  "aggr__2__8__4__count", "aggr__2__8__4__order_1"
FROM (
  SELECT "aggr__2__key_0", "aggr__2__count", "aggr__2__order_1",
    "metric__2__1_col_0", "aggr__2__8__key_0", "aggr__2__8__count",
    "aggr__2__8__order_1", "metric__2__8__1_col_0", "aggr__2__8__4__key_0",
    "aggr__2__8__4__count", "aggr__2__8__4__order_1",
    dense_rank() OVER (PARTITION BY 1 ORDER BY "aggr__2__order_1" DESC,
    "aggr__2__key_0" ASC) AS "aggr__2__order_1_rank",
    dense_rank() OVER (PARTITION BY "aggr__2__key_0" ORDER BY
    "aggr__2__8__order_1" ASC, "aggr__2__8__key_0" ASC) AS
    "aggr__2__8__order_1_rank",
    dense_rank() OVER (PARTITION BY "aggr__2__key_0" , "aggr__2__8__key_0" ORDER
    BY "aggr__2__8__4__order_1" DESC, "aggr__2__8__4__key_0" ASC) AS
    "aggr__2__8__4__order_1_rank"
  FROM (
    SELECT "surname" AS "aggr__2__key_0",
      sum("aggr__2__count_part") OVER (PARTITION BY "aggr__2__key_0") AS
      "aggr__2__count",
      avgOrNullMerge("aggr__2__order_1_part") OVER (PARTITION BY
      "aggr__2__key_0") AS "aggr__2__order_1",
      avgOrNullMerge("metric__2__1_col_0_part") OVER (PARTITION BY
      "aggr__2__key_0") AS "metric__2__1_col_0",
      COALESCE("limbName", '__missing__') AS "aggr__2__8__key_0",
      sum("aggr__2__8__count_part") OVER (PARTITION BY "aggr__2__key_0",
      "aggr__2__8__key_0") AS "aggr__2__8__count",
      sumOrNull("aggr__2__8__order_1_part") OVER (PARTITION BY "aggr__2__key_0",
      "aggr__2__8__key_0") AS "aggr__2__8__order_1",
      sumOrNull("total") AS "metric__2__8__1_col_0",
      "organName" AS "aggr__2__8__4__key_0", count(*) AS "aggr__2__8__4__count",
      "organName" AS "aggr__2__8__4__order_1",
      count(*) AS "aggr__2__count_part",
      avgOrNullState("total") AS "aggr__2__order_1_part",
      avgOrNullState("total") AS "metric__2__1_col_0_part",
      count(*) AS "aggr__2__8__count_part",
      sumOrNull ("total") AS "aggr__2__8__order_1_part"
    FROM "logs-generic-default"
    GROUP BY "surname" AS "aggr__2__key_0",
      COALESCE("limbName", '__missing__') AS "aggr__2__8__key_0",
      "organName" AS "aggr__2__8__4__key_0"))
WHERE (("aggr__2__order_1_rank"<=201 AND "aggr__2__8__order_1_rank"<=20) AND
  "aggr__2__8__4__order_1_rank"<=2)
ORDER BY "aggr__2__order_1_rank" ASC, "aggr__2__8__order_1_rank" ASC,
  "aggr__2__8__4__order_1_rank" ASC`
	expect := strings.Trim(sql, " \n")

	sqlFormatted := SqlPrettyPrint([]byte(sql))
	/*
		println("===== Expected: ")
		println(expect)
		println("===== Actual: ")
		println(sqlFormatted)
	*/
	assert.Equal(t, expect, sqlFormatted)
}

func TestSqlPrettyPancake2(t *testing.T) {
	sql := `
SELECT "aggr__0__key_0", "aggr__0__count", "metric__0__1_col_0",
  "aggr__0__2-bucket___col_0", "metric__0__2-bucket__2-metric_col_0"
FROM (
  SELECT "aggr__0__key_0", "aggr__0__count", "metric__0__1_col_0",
    "aggr__0__2-bucket___col_0", "metric__0__2-bucket__2-metric_col_0",
    dense_rank() OVER (ORDER BY "aggr__0__key_0" ASC) AS "aggr__0__order_1_rank"
  FROM (
    SELECT toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000) AS
      "aggr__0__key_0",
      sum(count(*)) OVER (PARTITION BY "aggr__0__key_0") AS "aggr__0__count",
      sumOrNull(sumOrNull("spent")) OVER (PARTITION BY "aggr__0__key_0") AS
      "metric__0__1_col_0",
      countIf("message" iLIKE '%started%') AS "aggr__0__2-bucket___col_0",
      sumOrNullIf("multiplier", "message" iLIKE '%started%') AS
      "metric__0__2-bucket__2-metric_col_0"
    FROM "logs-generic-default"
    GROUP BY toInt64(toUnixTimestamp64Milli("@timestamp") / 3600000) AS
      "aggr__0__key_0"))
ORDER BY "aggr__0__order_1_rank" ASC`
	expect := strings.Trim(sql, " \n")

	sqlFormatted := SqlPrettyPrint([]byte(sql))
	/*
		println("===== Expected: ")
		println(expect)
		println("===== Actual: ")
		println(sqlFormatted)
	*/
	assert.Equal(t, expect, sqlFormatted)
}

func TestSqlPrettyPancakeWith(t *testing.T) {
	sql := `
WITH quesma_top_hits_group_table AS (
  SELECT "aggr__origins__parent_count", "aggr__origins__key_0",
    "aggr__origins__count", "aggr__origins__order_1",
    "aggr__origins__distinations__parent_count",
    "aggr__origins__distinations__key_0", "aggr__origins__distinations__count",
    "aggr__origins__distinations__order_1"
  FROM (
    SELECT "aggr__origins__parent_count", "aggr__origins__key_0",
      "aggr__origins__count", "aggr__origins__order_1",
      "aggr__origins__distinations__parent_count",
      "aggr__origins__distinations__key_0",
      "aggr__origins__distinations__count",
      "aggr__origins__distinations__order_1",
      dense_rank() OVER (ORDER BY "aggr__origins__order_1" DESC,
      "aggr__origins__key_0" ASC) AS "aggr__origins__order_1_rank",
      dense_rank() OVER (PARTITION BY "aggr__origins__key_0" ORDER BY
      "aggr__origins__distinations__order_1" DESC,
      "aggr__origins__distinations__key_0" ASC) AS
      "aggr__origins__distinations__order_1_rank"
    FROM (
      SELECT sum(count(*)) OVER () AS "aggr__origins__parent_count",
        "OriginAirportID" AS "aggr__origins__key_0",
        sum(count(*)) OVER (PARTITION BY "aggr__origins__key_0") AS
        "aggr__origins__count",
        sum(count(*)) OVER (PARTITION BY "aggr__origins__key_0") AS
        "aggr__origins__order_1",
        sum(count(*)) OVER (PARTITION BY "aggr__origins__key_0") AS
        "aggr__origins__distinations__parent_count",
        "DestAirportID" AS "aggr__origins__distinations__key_0",
        count(*) AS "aggr__origins__distinations__count",
        count(*) AS "aggr__origins__distinations__order_1"
      FROM __quesma_table_name
      GROUP BY "OriginAirportID" AS "aggr__origins__key_0",
        "DestAirportID" AS "aggr__origins__distinations__key_0"))
  WHERE ("aggr__origins__order_1_rank"<=10001 AND
    "aggr__origins__distinations__order_1_rank"<=10001)
  ORDER BY "aggr__origins__order_1_rank" ASC,
    "aggr__origins__distinations__order_1_rank" ASC) ,
quesma_top_hits_join AS (
  SELECT "group_table"."aggr__origins__parent_count" AS
    "aggr__origins__parent_count",
    "group_table"."aggr__origins__key_0" AS "aggr__origins__key_0",
    "group_table"."aggr__origins__count" AS "aggr__origins__count",
    "group_table"."aggr__origins__order_1" AS "aggr__origins__order_1",
    "group_table"."aggr__origins__distinations__parent_count" AS
    "aggr__origins__distinations__parent_count",
    "group_table"."aggr__origins__distinations__key_0" AS
    "aggr__origins__distinations__key_0",
    "group_table"."aggr__origins__distinations__count" AS
    "aggr__origins__distinations__count",
    "group_table"."aggr__origins__distinations__order_1" AS
    "aggr__origins__distinations__order_1",
    "hit_table"."DestLocation" AS "top_hits_1",
    ROW_NUMBER() OVER (PARTITION BY "group_table"."aggr__origins__key_0",
    "group_table"."aggr__origins__distinations__key_0") AS "top_hits_rank"
  FROM quesma_top_hits_group_table AS "group_table" LEFT OUTER JOIN
    __quesma_table_name AS "hit_table" ON (("group_table"."aggr__origins__key_0"
    ="hit_table"."OriginAirportID" AND
    "group_table"."aggr__origins__distinations__key_0"=
    "hit_table"."DestAirportID")))
SELECT "aggr__origins__parent_count", "aggr__origins__key_0",
  "aggr__origins__count", "aggr__origins__order_1",
  "aggr__origins__distinations__parent_count",
  "aggr__origins__distinations__key_0", "aggr__origins__distinations__count",
  "aggr__origins__distinations__order_1", "top_hits_1", "top_hits_rank"
FROM quesma_top_hits_join
WHERE top_hits_rank<=1`
	expect := strings.Trim(sql, " \n")

	sqlFormatted := SqlPrettyPrint([]byte(sql))
	assert.Equal(t, expect, sqlFormatted)
}
