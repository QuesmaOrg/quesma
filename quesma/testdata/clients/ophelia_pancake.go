// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package clients

import "quesma/model"

type opheliaTestsPancakeTest struct {
	TestName        string
	Sql             string
	ExpectedResults []model.QueryResultRow
}

// This test alternative pancake style SQL generation
var OpheliaTestsPancake = []opheliaTestsPancakeTest{ // take rest arguments from OpheliaTests
	{ // [0]
		TestName: "Ophelia Test 1: triple terms + default order",
		Sql: `
SELECT "aggr__2__0", "aggr__2__1", "aggr__2__8__0", "aggr__2__8__1",
  "aggr__2__8__4__0", "aggr__2__8__4__1"
FROM (
  SELECT "aggr__2__0", "aggr__2__1", "aggr__2__8__0", "aggr__2__8__1",
    "aggr__2__8__4__0", "aggr__2__8__4__1", dense_rank() OVER (PARTITION BY 1
  ORDER BY "aggr__2__1" DESC, "aggr__2__0" ASC) AS "aggr__2__1_rank", dense_rank
    () OVER (PARTITION BY "aggr__2__0"
  ORDER BY "aggr__2__8__1" DESC, "aggr__2__8__0" ASC) AS "aggr__2__8__1_rank",
    dense_rank() OVER (PARTITION BY "aggr__2__0", "aggr__2__8__0"
  ORDER BY "aggr__2__8__4__1" DESC, "aggr__2__8__4__0" ASC) AS
    "aggr__2__8__4__1_rank"
  FROM (
    SELECT "surname" AS "aggr__2__0", sum("aggr__2__1_part") OVER (PARTITION BY
      1) AS "aggr__2__1", COALESCE("limbName",'__missing__') AS "aggr__2__8__0",
       sum("aggr__2__8__1_part") OVER (PARTITION BY "aggr__2__0") AS
      "aggr__2__8__1", "organName" AS "aggr__2__8__4__0", count() AS
      "aggr__2__8__4__1", count() AS "aggr__2__1_part", count() AS
      "aggr__2__8__1_part"
    FROM "logs-generic-default"
    GROUP BY "surname" AS "aggr__2__0", COALESCE("limbName",'__missing__') AS
      "aggr__2__8__0", "organName" AS "aggr__2__8__4__0"))
WHERE (("aggr__2__1_rank"<=200 AND "aggr__2__8__1_rank"<=20) AND
  "aggr__2__8__4__1_rank"<=1)
ORDER BY "aggr__2__1_rank" ASC, "aggr__2__8__1_rank" ASC,
  "aggr__2__8__4__1_rank" ASC`,
		ExpectedResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__0", "a1"),
				model.NewQueryResultCol("aggr__2__1", 1036),
				model.NewQueryResultCol("aggr__2__8__0", "b11"),
				model.NewQueryResultCol("aggr__2__8__1", 21),
				model.NewQueryResultCol("aggr__2__8__4__0", "c11"),
				model.NewQueryResultCol("aggr__2__8__4__1", 21),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__0", "a1"),
				model.NewQueryResultCol("aggr__2__1", 1036),
				model.NewQueryResultCol("aggr__2__8__0", "b12"),
				model.NewQueryResultCol("aggr__2__8__1", 24),
				model.NewQueryResultCol("aggr__2__8__4__0", "c12"),
				model.NewQueryResultCol("aggr__2__8__4__1", 24),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__0", "a2"),
				model.NewQueryResultCol("aggr__2__1", 34),
				model.NewQueryResultCol("aggr__2__8__0", "b21"),
				model.NewQueryResultCol("aggr__2__8__1", 17),
				model.NewQueryResultCol("aggr__2__8__4__0", "c21"),
				model.NewQueryResultCol("aggr__2__8__4__1", 17),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__0", "a2"),
				model.NewQueryResultCol("aggr__2__1", 34),
				model.NewQueryResultCol("aggr__2__8__0", "b22"),
				model.NewQueryResultCol("aggr__2__8__1", 17),
				model.NewQueryResultCol("aggr__2__8__4__0", "c22"),
				model.NewQueryResultCol("aggr__2__8__4__1", 17),
			}},
		},
	},
	{
		TestName: "Ophelia Test 2: triple terms + other aggregations + default order",
		Sql: `
SELECT "aggr__2__0", "aggr__2__1", "metric__2__10", "aggr__2__8__0",
  "aggr__2__8__1", "metric__2__8__10", "aggr__2__8__4__0", "aggr__2__8__4__1",
  "metric__2__8__4__10", "metric__2__8__4__50"
FROM (
  SELECT "aggr__2__0", "aggr__2__1", "metric__2__10", "aggr__2__8__0",
    "aggr__2__8__1", "metric__2__8__10", "aggr__2__8__4__0", "aggr__2__8__4__1",
     "metric__2__8__4__10", "metric__2__8__4__50", dense_rank() OVER (PARTITION
    BY 1
  ORDER BY "aggr__2__1" DESC, "aggr__2__0" ASC) AS "aggr__2__1_rank", dense_rank
    () OVER (PARTITION BY "aggr__2__0"
  ORDER BY "aggr__2__8__1" DESC, "aggr__2__8__0" ASC) AS "aggr__2__8__1_rank",
    dense_rank() OVER (PARTITION BY "aggr__2__0", "aggr__2__8__0"
  ORDER BY "aggr__2__8__4__1" DESC, "aggr__2__8__4__0" ASC) AS
    "aggr__2__8__4__1_rank"
  FROM (
    SELECT "surname" AS "aggr__2__0", sum("aggr__2__1_part") OVER (PARTITION BY
      1) AS "aggr__2__1", sumOrNull("metric__2__10_part") OVER (PARTITION BY
      "aggr__2__0") AS "metric__2__10", COALESCE("limbName",'__missing__') AS
      "aggr__2__8__0", sum("aggr__2__8__1_part") OVER (PARTITION BY "aggr__2__0"
      ) AS "aggr__2__8__1", sumOrNull("metric__2__8__10_part") OVER (PARTITION
      BY "aggr__2__0", "aggr__2__8__0") AS "metric__2__8__10", "organName" AS
      "aggr__2__8__4__0", sum("aggr__2__8__4__1_part") OVER (PARTITION BY
      "aggr__2__0", "aggr__2__8__0") AS "aggr__2__8__4__1", sumOrNull("total")
      AS "metric__2__8__4__10", sumOrNull("some") AS "metric__2__8__4__50",
      count() AS "aggr__2__1_part", sumOrNull("total") AS "metric__2__10_part",
      count() AS "aggr__2__8__1_part", sumOrNull("total") AS
      "metric__2__8__10_part", count() AS "aggr__2__8__4__1_part"
    FROM "logs-generic-default"
    GROUP BY "surname" AS "aggr__2__0", COALESCE("limbName",'__missing__') AS
      "aggr__2__8__0", "organName" AS "aggr__2__8__4__0"))
WHERE (("aggr__2__1_rank"<=200 AND "aggr__2__8__1_rank"<=20) AND
  "aggr__2__8__4__1_rank"<=1)
ORDER BY "aggr__2__1_rank" ASC, "aggr__2__8__1_rank" ASC,
  "aggr__2__8__4__1_rank" ASC`,
		ExpectedResults: []model.QueryResultRow{}, // TODO
	},
}
