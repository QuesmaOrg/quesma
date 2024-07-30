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
SELECT "aggr__2__key_0", "aggr__2__count", "aggr__2__order_1",
  "aggr__2__8__key_0", "aggr__2__8__count", "aggr__2__8__order_1",
  "aggr__2__8__4__key_0", "aggr__2__8__4__count", "aggr__2__8__4__order_1"
FROM (
  SELECT "aggr__2__key_0", "aggr__2__count", "aggr__2__order_1",
    "aggr__2__8__key_0", "aggr__2__8__count", "aggr__2__8__order_1",
    "aggr__2__8__4__key_0", "aggr__2__8__4__count", "aggr__2__8__4__order_1",
    dense_rank() OVER (PARTITION BY 1
  ORDER BY "aggr__2__order_1" DESC, "aggr__2__key_0" ASC) AS
    "aggr__2__order_1_rank", dense_rank() OVER (PARTITION BY "aggr__2__key_0"
  ORDER BY "aggr__2__8__order_1" DESC, "aggr__2__8__key_0" ASC) AS
    "aggr__2__8__order_1_rank", dense_rank() OVER (PARTITION BY "aggr__2__key_0"
    , "aggr__2__8__key_0"
  ORDER BY "aggr__2__8__4__order_1" DESC, "aggr__2__8__4__key_0" ASC) AS
    "aggr__2__8__4__order_1_rank"
  FROM (
    SELECT "surname" AS "aggr__2__key_0", sum("aggr__2__count_part") OVER (
      PARTITION BY "aggr__2__key_0") AS "aggr__2__count", sum(
      "aggr__2__order_1_part") OVER (PARTITION BY "aggr__2__key_0") AS
      "aggr__2__order_1", COALESCE("limbName",'__missing__') AS
      "aggr__2__8__key_0", sum("aggr__2__8__count_part") OVER (PARTITION BY
      "aggr__2__key_0", "aggr__2__8__key_0") AS "aggr__2__8__count", sum(
      "aggr__2__8__order_1_part") OVER (PARTITION BY "aggr__2__key_0",
      "aggr__2__8__key_0") AS "aggr__2__8__order_1", "organName" AS
      "aggr__2__8__4__key_0", count(*) AS "aggr__2__8__4__count", count() AS
      "aggr__2__8__4__order_1", count(*) AS "aggr__2__count_part", count() AS
      "aggr__2__order_1_part", count(*) AS "aggr__2__8__count_part", count() AS
      "aggr__2__8__order_1_part"
    FROM "logs-generic-default"
    GROUP BY "surname" AS "aggr__2__key_0", COALESCE("limbName",'__missing__')
      AS "aggr__2__8__key_0", "organName" AS "aggr__2__8__4__key_0"))
WHERE (("aggr__2__order_1_rank"<=200 AND "aggr__2__8__order_1_rank"<=20) AND
  "aggr__2__8__4__order_1_rank"<=1)
ORDER BY "aggr__2__order_1_rank" ASC, "aggr__2__8__order_1_rank" ASC,
  "aggr__2__8__4__order_1_rank" ASC`,
		ExpectedResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__key_0", "a1"),
				model.NewQueryResultCol("aggr__2__count_1", 1036),
				model.NewQueryResultCol("aggr__2__order_1", 1036),
				model.NewQueryResultCol("aggr__2__8__key_0", "b11"),
				model.NewQueryResultCol("aggr__2__8__count_1", 21),
				model.NewQueryResultCol("aggr__2__8__order_1", 21),
				model.NewQueryResultCol("aggr__2__8__4__key_0", "c11"),
				model.NewQueryResultCol("aggr__2__8__4__count_1", 21),
				model.NewQueryResultCol("aggr__2__8__4__order_1", 21),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__key_0", "a1"),
				model.NewQueryResultCol("aggr__2__count_1", 1036),
				model.NewQueryResultCol("aggr__2__order_1", 1036),
				model.NewQueryResultCol("aggr__2__8__key_0", "b12"),
				model.NewQueryResultCol("aggr__2__8__count_1", 24),
				model.NewQueryResultCol("aggr__2__8__order_1", 24),
				model.NewQueryResultCol("aggr__2__8__4__key_0", "c12"),
				model.NewQueryResultCol("aggr__2__8__4__count_1", 24),
				model.NewQueryResultCol("aggr__2__8__4__order_1", 24),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__key_0", "a2"),
				model.NewQueryResultCol("aggr__2__count_1", 34),
				model.NewQueryResultCol("aggr__2__order_1", 34),
				model.NewQueryResultCol("aggr__2__8__key_0", "b21"),
				model.NewQueryResultCol("aggr__2__8__count_1", 17),
				model.NewQueryResultCol("aggr__2__8__order_1", 17),
				model.NewQueryResultCol("aggr__2__8__4__key_0", "c21"),
				model.NewQueryResultCol("aggr__2__8__4__count_1", 17),
				model.NewQueryResultCol("aggr__2__8__4__order_1", 17),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__key_0", "a2"),
				model.NewQueryResultCol("aggr__2__count_1", 34),
				model.NewQueryResultCol("aggr__2__order_1", 34),
				model.NewQueryResultCol("aggr__2__8__key_0", "b22"),
				model.NewQueryResultCol("aggr__2__8__count_1", 17),
				model.NewQueryResultCol("aggr__2__8__order_1", 17),
				model.NewQueryResultCol("aggr__2__8__4__key_0", "c22"),
				model.NewQueryResultCol("aggr__2__8__4__count_1", 17),
				model.NewQueryResultCol("aggr__2__8__4__order_1", 17),
			}},
		},
	},
	{
		TestName: "Ophelia Test 2: triple terms + other aggregations + default order",
		Sql: `
SELECT "aggr__2__key_0", "aggr__2__count", "aggr__2__order_1",
  "metric__2__1_col_0", "aggr__2__8__key_0", "aggr__2__8__count",
  "aggr__2__8__order_1", "metric__2__8__1_col_0", "aggr__2__8__4__key_0",
  "aggr__2__8__4__count", "aggr__2__8__4__order_1", "metric__2__8__4__1_col_0",
  "metric__2__8__4__5_col_0"
FROM (
  SELECT "aggr__2__key_0", "aggr__2__count", "aggr__2__order_1",
    "metric__2__1_col_0", "aggr__2__8__key_0", "aggr__2__8__count",
    "aggr__2__8__order_1", "metric__2__8__1_col_0", "aggr__2__8__4__key_0",
    "aggr__2__8__4__count", "aggr__2__8__4__order_1", "metric__2__8__4__1_col_0"
    , "metric__2__8__4__5_col_0", dense_rank() OVER (PARTITION BY 1
  ORDER BY "aggr__2__order_1" DESC, "aggr__2__key_0" ASC) AS
    "aggr__2__order_1_rank", dense_rank() OVER (PARTITION BY "aggr__2__key_0"
  ORDER BY "aggr__2__8__order_1" DESC, "aggr__2__8__key_0" ASC) AS
    "aggr__2__8__order_1_rank", dense_rank() OVER (PARTITION BY "aggr__2__key_0"
    , "aggr__2__8__key_0"
  ORDER BY "aggr__2__8__4__order_1" DESC, "aggr__2__8__4__key_0" ASC) AS
    "aggr__2__8__4__order_1_rank"
  FROM (
    SELECT "surname" AS "aggr__2__key_0", sum("aggr__2__count_part") OVER (
      PARTITION BY "aggr__2__key_0") AS "aggr__2__count", sum(
      "aggr__2__order_1_part") OVER (PARTITION BY "aggr__2__key_0") AS
      "aggr__2__order_1", sumOrNull("metric__2__1_col_0_part") OVER (PARTITION
      BY "aggr__2__key_0") AS "metric__2__1_col_0", COALESCE("limbName",
      '__missing__') AS "aggr__2__8__key_0", sum("aggr__2__8__count_part") OVER
      (PARTITION BY "aggr__2__key_0", "aggr__2__8__key_0") AS
      "aggr__2__8__count", sum("aggr__2__8__order_1_part") OVER (PARTITION BY
      "aggr__2__key_0", "aggr__2__8__key_0") AS "aggr__2__8__order_1", sumOrNull
      ("metric__2__8__1_col_0_part") OVER (PARTITION BY "aggr__2__key_0",
      "aggr__2__8__key_0") AS "metric__2__8__1_col_0", "organName" AS
      "aggr__2__8__4__key_0", count(*) AS "aggr__2__8__4__count", count() AS
      "aggr__2__8__4__order_1", sumOrNull("total") AS "metric__2__8__4__1_col_0"
      , sumOrNull("some") AS "metric__2__8__4__5_col_0", count(*) AS
      "aggr__2__count_part", count() AS "aggr__2__order_1_part", sumOrNull(
      "total") AS "metric__2__1_col_0_part", count(*) AS
      "aggr__2__8__count_part", count() AS "aggr__2__8__order_1_part", sumOrNull
      ("total") AS "metric__2__8__1_col_0_part"
    FROM "logs-generic-default"
    GROUP BY "surname" AS "aggr__2__key_0", COALESCE("limbName",'__missing__')
      AS "aggr__2__8__key_0", "organName" AS "aggr__2__8__4__key_0"))
WHERE (("aggr__2__order_1_rank"<=200 AND "aggr__2__8__order_1_rank"<=20) AND
  "aggr__2__8__4__order_1_rank"<=1)
ORDER BY "aggr__2__order_1_rank" ASC, "aggr__2__8__order_1_rank" ASC,
  "aggr__2__8__4__order_1_rank" ASC`,
		ExpectedResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__key_0", "a1"),
				model.NewQueryResultCol("aggr__2__count", 1036),
				model.NewQueryResultCol("aggr__2__order_1", 1036),
				model.NewQueryResultCol("metric__2__1_col_0", 1091661.760867),
				model.NewQueryResultCol("aggr__2__8__key_0", "b12"),
				model.NewQueryResultCol("aggr__2__8__count", 24),
				model.NewQueryResultCol("aggr__2__8__order_1", 24),
				model.NewQueryResultCol("metric__2__8__1_col_0", 45774.291766666654),
				model.NewQueryResultCol("aggr__2__8__4__key_0", "c12"),
				model.NewQueryResultCol("aggr__2__8__4__count", 24),
				model.NewQueryResultCol("aggr__2__8__4__order_1", 24),
				model.NewQueryResultCol("metric__2__8__4__1_col_0", 45774.291766666654),
				model.NewQueryResultCol("metric__2__8__4__5_col_0", 36577.89516666666),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__key_0", "a1"),
				model.NewQueryResultCol("aggr__2__order_1", 1036),
				model.NewQueryResultCol("aggr__2__count", 1036),
				model.NewQueryResultCol("metric__2__1_col_0", 1091661.760867),
				model.NewQueryResultCol("aggr__2__8__key_0", "b11"),
				model.NewQueryResultCol("aggr__2__8__count", 21),
				model.NewQueryResultCol("aggr__2__8__order_1", 21),
				model.NewQueryResultCol("metric__2__8__1_col_0", 51891.94613333333),
				model.NewQueryResultCol("aggr__2__8__4__key_0", "c11"),
				model.NewQueryResultCol("aggr__2__8__4__count", 21),
				model.NewQueryResultCol("aggr__2__8__4__order_1", 21),
				model.NewQueryResultCol("metric__2__8__4__1_col_0", 51891.94613333333),
				model.NewQueryResultCol("metric__2__8__4__5_col_0", 37988.09523333333),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__key_0", "a2"),
				model.NewQueryResultCol("aggr__2__order_1", 34),
				model.NewQueryResultCol("aggr__2__count", 34),
				model.NewQueryResultCol("metric__2__1_col_0", 630270.07765),
				model.NewQueryResultCol("aggr__2__8__key_0", "b21"),
				model.NewQueryResultCol("aggr__2__8__count", 17),
				model.NewQueryResultCol("aggr__2__8__order_1", 17),
				model.NewQueryResultCol("metric__2__8__1_col_0", 399126.7496833334),
				model.NewQueryResultCol("aggr__2__8__4__key_0", "c21"),
				model.NewQueryResultCol("aggr__2__8__4__count", 17),
				model.NewQueryResultCol("aggr__2__8__4__order_1", 17),
				model.NewQueryResultCol("metric__2__8__4__1_col_0", 399126.7496833334),
				model.NewQueryResultCol("metric__2__8__4__5_col_0", 337246.82201666664),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__key_0", "a2"),
				model.NewQueryResultCol("aggr__2__order_1", 34),
				model.NewQueryResultCol("aggr__2__count", 34),
				model.NewQueryResultCol("metric__2__1_col_0", 231143.3279666666),
				model.NewQueryResultCol("aggr__2__8__key_0", "b22"),
				model.NewQueryResultCol("aggr__2__8__count", 17),
				model.NewQueryResultCol("aggr__2__8__order_1", 17),
				model.NewQueryResultCol("metric__2__8__1_col_0", 231143.3279666666),
				model.NewQueryResultCol("aggr__2__8__4__key_0", "c22"),
				model.NewQueryResultCol("aggr__2__8__4__count", 17),
				model.NewQueryResultCol("aggr__2__8__4__order_1", 17),
				model.NewQueryResultCol("metric__2__8__4__1_col_0", 231143.3279666666),
				model.NewQueryResultCol("metric__2__8__4__5_col_0", 205408.48849999998),
			}},
		},
	},
	/*
			{
				TestName: "Ophelia Test 3: 5x terms + a lot of other aggregations",
				Sql: `
		SELECT "aggr__2__key_0", "aggr__2__order_1", "metric__2__1_col_0",
		  "aggr__2__7__key_0", "aggr__2__7__order_1", "metric__2__7__1_col_0",
		  "aggr__2__7__8__key_0", "aggr__2__7__8__order_1", "metric__2__7__8__1_col_0",
		  "aggr__2__7__8__4__key_0", "aggr__2__7__8__4__order_1",
		  "metric__2__7__8__4__1_col_0", "aggr__2__7__8__4__3__key_0",
		  "aggr__2__7__8__4__3__order_1", "metric__2__7__8__4__3__1_col_0",
		  "metric__2__7__8__4__3__5_col_0", "metric__2__7__8__4__3__6_col_0"
		FROM (
		  SELECT "aggr__2__key_0", "aggr__2__order_1", "metric__2__1_col_0",
		    "aggr__2__7__key_0", "aggr__2__7__order_1", "metric__2__7__1_col_0",
		    "aggr__2__7__8__key_0", "aggr__2__7__8__order_1", "metric__2__7__8__1_col_0"
		    , "aggr__2__7__8__4__key_0", "aggr__2__7__8__4__order_1",
		    "metric__2__7__8__4__1_col_0", "aggr__2__7__8__4__3__key_0",
		    "aggr__2__7__8__4__3__order_1", "metric__2__7__8__4__3__1_col_0",
		    "metric__2__7__8__4__3__5_col_0", "metric__2__7__8__4__3__6_col_0",
		    dense_rank() OVER (PARTITION BY 1
		  ORDER BY "aggr__2__order_1" DESC, "aggr__2__key_0" ASC) AS
		    "aggr__2__order_1_rank", dense_rank() OVER (PARTITION BY "aggr__2__key_0"
		  ORDER BY "aggr__2__7__order_1" DESC, "aggr__2__7__key_0" ASC) AS
		    "aggr__2__7__order_1_rank", dense_rank() OVER (PARTITION BY "aggr__2__key_0"
		    , "aggr__2__7__key_0"
		  ORDER BY "aggr__2__7__8__order_1" DESC, "aggr__2__7__8__key_0" ASC) AS
		    "aggr__2__7__8__order_1_rank", dense_rank() OVER (PARTITION BY
		    "aggr__2__key_0", "aggr__2__7__key_0", "aggr__2__7__8__key_0"
		  ORDER BY "aggr__2__7__8__4__order_1" DESC, "aggr__2__7__8__4__key_0" ASC) AS
		    "aggr__2__7__8__4__order_1_rank", dense_rank() OVER (PARTITION BY
		    "aggr__2__key_0", "aggr__2__7__key_0", "aggr__2__7__8__key_0",
		    "aggr__2__7__8__4__key_0"
		  ORDER BY "aggr__2__7__8__4__3__order_1" DESC, "aggr__2__7__8__4__3__key_0" ASC
		    ) AS "aggr__2__7__8__4__3__order_1_rank"
		  FROM (
		    SELECT "surname" AS "aggr__2__key_0", sumOrNull("aggr__2__order_1_part")
		      OVER (PARTITION BY "aggr__2__key_0") AS "aggr__2__order_1", sumOrNull(
		      "metric__2__1_col_0_part") OVER (PARTITION BY "aggr__2__key_0") AS
		      "metric__2__1_col_0", COALESCE("limbName",'__missing__') AS
		      "aggr__2__7__key_0", sumOrNull("aggr__2__7__order_1_part") OVER (PARTITION
		       BY "aggr__2__key_0", "aggr__2__7__key_0") AS "aggr__2__7__order_1",
		      sumOrNull("metric__2__7__1_col_0_part") OVER (PARTITION BY
		      "aggr__2__key_0", "aggr__2__7__key_0") AS "metric__2__7__1_col_0",
		      COALESCE("organName",'__missing__') AS "aggr__2__7__8__key_0", sumOrNull(
		      "aggr__2__7__8__order_1_part") OVER (PARTITION BY "aggr__2__key_0",
		      "aggr__2__7__key_0", "aggr__2__7__8__key_0") AS "aggr__2__7__8__order_1",
		      sumOrNull("metric__2__7__8__1_col_0_part") OVER (PARTITION BY
		      "aggr__2__key_0", "aggr__2__7__key_0", "aggr__2__7__8__key_0") AS
		      "metric__2__7__8__1_col_0", "doctorName" AS "aggr__2__7__8__4__key_0",
		      sumOrNull("aggr__2__7__8__4__order_1_part") OVER (PARTITION BY
		      "aggr__2__key_0", "aggr__2__7__key_0", "aggr__2__7__8__key_0",
		      "aggr__2__7__8__4__key_0") AS "aggr__2__7__8__4__order_1", sumOrNull(
		      "metric__2__7__8__4__1_col_0_part") OVER (PARTITION BY "aggr__2__key_0",
		      "aggr__2__7__key_0", "aggr__2__7__8__key_0", "aggr__2__7__8__4__key_0") AS
		       "metric__2__7__8__4__1_col_0", "height" AS "aggr__2__7__8__4__3__key_0",
		      sumOrNull("total") AS "aggr__2__7__8__4__3__order_1", sumOrNull("total")
		      AS "metric__2__7__8__4__3__1_col_0", sumOrNull("some") AS
		      "metric__2__7__8__4__3__5_col_0", sumOrNull("cost") AS
		      "metric__2__7__8__4__3__6_col_0", sumOrNull("total") AS
		      "aggr__2__order_1_part", sumOrNull("total") AS "metric__2__1_col_0_part",
		      sumOrNull("total") AS "aggr__2__7__order_1_part", sumOrNull("total") AS
		      "metric__2__7__1_col_0_part", sumOrNull("total") AS
		      "aggr__2__7__8__order_1_part", sumOrNull("total") AS
		      "metric__2__7__8__1_col_0_part", sumOrNull("total") AS
		      "aggr__2__7__8__4__order_1_part", sumOrNull("total") AS
		      "metric__2__7__8__4__1_col_0_part"
		    FROM "logs-generic-default"
		    GROUP BY "surname" AS "aggr__2__key_0", COALESCE("limbName",'__missing__')
		      AS "aggr__2__7__key_0", COALESCE("organName",'__missing__') AS
		      "aggr__2__7__8__key_0", "doctorName" AS "aggr__2__7__8__4__key_0",
		      "height" AS "aggr__2__7__8__4__3__key_0"))
		WHERE (((("aggr__2__order_1_rank"<=100 AND "aggr__2__7__order_1_rank"<=10) AND
		  "aggr__2__7__8__order_1_rank"<=10) AND "aggr__2__7__8__4__order_1_rank"<=6)
		  AND "aggr__2__7__8__4__3__order_1_rank"<=1)
		ORDER BY "aggr__2__order_1_rank" ASC, "aggr__2__7__order_1_rank" ASC,
		  "aggr__2__7__8__order_1_rank" ASC, "aggr__2__7__8__4__order_1_rank" ASC,
		  "aggr__2__7__8__4__3__order_1_rank" ASC`,
				ExpectedResults: []model.QueryResultRow{ // TODO: fix
					{Cols: []model.QueryResultCol{
						model.NewQueryResultCol("aggr__2__key_0", 12345),
						model.NewQueryResultCol("aggr__2__order_1", 12345),
						model.NewQueryResultCol("metric__2__1_col_0", 12345),
						model.NewQueryResultCol("aggr__2__7__key_0", 12345),
						model.NewQueryResultCol("aggr__2__7__order_1", 12345),
						model.NewQueryResultCol("metric__2__7__1_col_0", 12345),
						model.NewQueryResultCol("aggr__2__7__8__key_0", 12345),
						model.NewQueryResultCol("aggr__2__7__8__order_1", 12345),
						model.NewQueryResultCol("metric__2__7__8__1_col_0", 12345),
						model.NewQueryResultCol("aggr__2__7__8__4__key_0", 12345),
						model.NewQueryResultCol("aggr__2__7__8__4__order_1", 12345),
						model.NewQueryResultCol("metric__2__7__8__4__1_col_0", 12345),
						model.NewQueryResultCol("aggr__2__7__8__4__3__key_0", 12345),
						model.NewQueryResultCol("aggr__2__7__8__4__3__order_1", 12345),
						model.NewQueryResultCol("metric__2__7__8__4__3__1_col_0", 12345),
						model.NewQueryResultCol("metric__2__7__8__4__3__5_col_0", 12345),
						model.NewQueryResultCol("metric__2__7__8__4__3__6_col_0", 12345),
					}},
				}},
	*/
	{
		TestName: "Ophelia Test 4: triple terms + order by another aggregations",
		Sql: `
SELECT "aggr__2__key_0", "aggr__2__count", "aggr__2__order_1",
  "metric__2__1_col_0", "aggr__2__8__key_0", "aggr__2__8__count",
  "aggr__2__8__order_1", "metric__2__8__1_col_0", "aggr__2__8__4__key_0",
  "aggr__2__8__4__count", "aggr__2__8__4__order_1"
FROM (
  SELECT "aggr__2__key_0", "aggr__2__count", "aggr__2__order_1",
    "metric__2__1_col_0", "aggr__2__8__key_0", "aggr__2__8__count",
    "aggr__2__8__order_1", "metric__2__8__1_col_0", "aggr__2__8__4__key_0",
    "aggr__2__8__4__count", "aggr__2__8__4__order_1", dense_rank() OVER (
    PARTITION BY 1
  ORDER BY "aggr__2__order_1" DESC, "aggr__2__key_0" ASC) AS
    "aggr__2__order_1_rank", dense_rank() OVER (PARTITION BY "aggr__2__key_0"
  ORDER BY "aggr__2__8__order_1" ASC, "aggr__2__8__key_0" ASC) AS
    "aggr__2__8__order_1_rank", dense_rank() OVER (PARTITION BY "aggr__2__key_0"
    , "aggr__2__8__key_0"
  ORDER BY "aggr__2__8__4__order_1" DESC, "aggr__2__8__4__key_0" ASC) AS
    "aggr__2__8__4__order_1_rank"
  FROM (
    SELECT "surname" AS "aggr__2__key_0", sum("aggr__2__count_part") OVER (
      PARTITION BY "aggr__2__key_0") AS "aggr__2__count", avgMerge(
      "aggr__2__order_1_part") OVER (PARTITION BY "aggr__2__key_0") AS
      "aggr__2__order_1", avgMerge("metric__2__1_col_0_part") OVER (PARTITION BY
       "aggr__2__key_0") AS "metric__2__1_col_0", COALESCE("limbName",
      '__missing__') AS "aggr__2__8__key_0", sum("aggr__2__8__count_part") OVER
      (PARTITION BY "aggr__2__key_0", "aggr__2__8__key_0") AS
      "aggr__2__8__count", sumOrNull("aggr__2__8__order_1_part") OVER (PARTITION
       BY "aggr__2__key_0", "aggr__2__8__key_0") AS "aggr__2__8__order_1",
      sumOrNull("total") AS "metric__2__8__1_col_0", "organName" AS
      "aggr__2__8__4__key_0", count(*) AS "aggr__2__8__4__count", "organName" AS
       "aggr__2__8__4__order_1", count(*) AS "aggr__2__count_part", avgState(
      "total") AS "aggr__2__order_1_part", avgState("total") AS
      "metric__2__1_col_0_part", count(*) AS "aggr__2__8__count_part", sumOrNull
      ("total") AS "aggr__2__8__order_1_part"
    FROM "logs-generic-default"
    GROUP BY "surname" AS "aggr__2__key_0", COALESCE("limbName",'__missing__')
      AS "aggr__2__8__key_0", "organName" AS "aggr__2__8__4__key_0"))
WHERE (("aggr__2__order_1_rank"<=200 AND "aggr__2__8__order_1_rank"<=20) AND
  "aggr__2__8__4__order_1_rank"<=1)
ORDER BY "aggr__2__order_1_rank" ASC, "aggr__2__8__order_1_rank" ASC,
  "aggr__2__8__4__order_1_rank" ASC`,
		ExpectedResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__key_0", "a1"),
				model.NewQueryResultCol("aggr__2__count", 1036),
				model.NewQueryResultCol("aggr__2__order_1", 1091661.7608666667),
				model.NewQueryResultCol("metric__2__1_col_0", 1091661.7608666667),
				model.NewQueryResultCol("aggr__2__8__key_0", "b12"),
				model.NewQueryResultCol("aggr__2__8__count", 24),
				model.NewQueryResultCol("aggr__2__8__order_1", 45774.291766666654),
				model.NewQueryResultCol("metric__2__8__1_col_0", 45774.291766666654),
				model.NewQueryResultCol("aggr__2__8__4__key_0", "c12"),
				model.NewQueryResultCol("aggr__2__8__4__count", 24),
				model.NewQueryResultCol("aggr__2__8__4__order_1", "c12"),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__key_0", "a1"),
				model.NewQueryResultCol("aggr__2__count", 1036),
				model.NewQueryResultCol("aggr__2__order_1", 1091661.7608666667),
				model.NewQueryResultCol("metric__2__1_col_0", 1091661.7608666667),
				model.NewQueryResultCol("aggr__2__8__key_0", "b11"),
				model.NewQueryResultCol("aggr__2__8__count", 21),
				model.NewQueryResultCol("aggr__2__8__order_1", 51891.94613333333),
				model.NewQueryResultCol("metric__2__8__1_col_0", 51891.94613333333),
				model.NewQueryResultCol("aggr__2__8__4__key_0", "c11"),
				model.NewQueryResultCol("aggr__2__8__4__count", 21),
				model.NewQueryResultCol("aggr__2__8__4__order_1", "c11"),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__key_0", "a2"),
				model.NewQueryResultCol("aggr__2__count", 34),
				model.NewQueryResultCol("aggr__2__order_1", 630270.07765),
				model.NewQueryResultCol("metric__2__1_col_0", 630270.07765),
				model.NewQueryResultCol("aggr__2__8__key_0", "b22"),
				model.NewQueryResultCol("aggr__2__8__count", 17),
				model.NewQueryResultCol("aggr__2__8__order_1", 231143.3279666666),
				model.NewQueryResultCol("metric__2__8__1_col_0", 231143.3279666666),
				model.NewQueryResultCol("aggr__2__8__4__key_0", "c22"),
				model.NewQueryResultCol("aggr__2__8__4__count", 17),
				model.NewQueryResultCol("aggr__2__8__4__order_1", "c22"),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__key_0", "a2"),
				model.NewQueryResultCol("aggr__2__count", 34),
				model.NewQueryResultCol("aggr__2__order_1", 630270.07765),
				model.NewQueryResultCol("metric__2__1_col_0", 630270.07765),
				model.NewQueryResultCol("aggr__2__8__key_0", "b21"),
				model.NewQueryResultCol("aggr__2__8__count", 17),
				model.NewQueryResultCol("aggr__2__8__order_1", 399126.7496833334),
				model.NewQueryResultCol("metric__2__8__1_col_0", 399126.7496833334),
				model.NewQueryResultCol("aggr__2__8__4__key_0", "c21"),
				model.NewQueryResultCol("aggr__2__8__4__count", 17),
				model.NewQueryResultCol("aggr__2__8__4__order_1", "c21"),
			}},
		},
	},
	{
		TestName: "Ophelia Test 5: 4x terms + order by another aggregations",
		Sql: `
SELECT "aggr__2__key_0", "aggr__2__order_1", "aggr__2__8__key_0",
  "aggr__2__8__order_1", "metric__2__8__1_col_0", "aggr__2__8__4__key_0",
  "aggr__2__8__4__order_1", "aggr__2__8__4__5__key_0",
  "aggr__2__8__4__5__order_1"
FROM (
  SELECT "aggr__2__key_0", "aggr__2__order_1", "aggr__2__8__key_0",
    "aggr__2__8__order_1", "metric__2__8__1_col_0", "aggr__2__8__4__key_0",
    "aggr__2__8__4__order_1", "aggr__2__8__4__5__key_0",
    "aggr__2__8__4__5__order_1", dense_rank() OVER (PARTITION BY 1
  ORDER BY "aggr__2__order_1" DESC, "aggr__2__key_0" ASC) AS
    "aggr__2__order_1_rank", dense_rank() OVER (PARTITION BY "aggr__2__key_0"
  ORDER BY "aggr__2__8__order_1" ASC, "aggr__2__8__key_0" ASC) AS
    "aggr__2__8__order_1_rank", dense_rank() OVER (PARTITION BY "aggr__2__key_0"
    , "aggr__2__8__key_0"
  ORDER BY "aggr__2__8__4__order_1" DESC, "aggr__2__8__4__key_0" ASC) AS
    "aggr__2__8__4__order_1_rank", dense_rank() OVER (PARTITION BY
    "aggr__2__key_0", "aggr__2__8__key_0", "aggr__2__8__4__key_0"
  ORDER BY "aggr__2__8__4__5__order_1" DESC, "aggr__2__8__4__5__key_0" ASC) AS
    "aggr__2__8__4__5__order_1_rank"
  FROM (
    SELECT "surname" AS "aggr__2__key_0", "surname" AS "aggr__2__order_1",
      COALESCE("limbName",'__missing__') AS "aggr__2__8__key_0", sumOrNull(
      "aggr__2__8__order_1_part") OVER (PARTITION BY "aggr__2__key_0",
      "aggr__2__8__key_0") AS "aggr__2__8__order_1", sumOrNull(
      "metric__2__8__1_col_0_part") OVER (PARTITION BY "aggr__2__key_0",
      "aggr__2__8__key_0") AS "metric__2__8__1_col_0", "organName" AS
      "aggr__2__8__4__key_0", "organName" AS "aggr__2__8__4__order_1",
      "organName" AS "aggr__2__8__4__5__key_0", count() AS
      "aggr__2__8__4__5__order_1", sumOrNull("total") AS
      "aggr__2__8__order_1_part", sumOrNull("total") AS
      "metric__2__8__1_col_0_part"
    FROM "logs-generic-default"
    GROUP BY "surname" AS "aggr__2__key_0", COALESCE("limbName",'__missing__')
      AS "aggr__2__8__key_0", "organName" AS "aggr__2__8__4__key_0", "organName"
       AS "aggr__2__8__4__5__key_0"))
WHERE ((("aggr__2__order_1_rank"<=200 AND "aggr__2__8__order_1_rank"<=20) AND
  "aggr__2__8__4__order_1_rank"<=1) AND "aggr__2__8__4__5__order_1_rank"<=2)
ORDER BY "aggr__2__order_1_rank" ASC, "aggr__2__8__order_1_rank" ASC,
  "aggr__2__8__4__order_1_rank" ASC, "aggr__2__8__4__5__order_1_rank" ASC`,
		ExpectedResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__key_0", "a2"),
				model.NewQueryResultCol("aggr__2__order_1", "a2"),
				model.NewQueryResultCol("aggr__2__8__key_0", "b22"),
				model.NewQueryResultCol("aggr__2__8__order_1", 231143.3279666666),
				model.NewQueryResultCol("metric__2__8__1_col_0", 231143.3279666666),
				model.NewQueryResultCol("aggr__2__8__4__key_0", "c22"),
				model.NewQueryResultCol("aggr__2__8__4__order_1", "c22"),
				model.NewQueryResultCol("aggr__2__8__4__5__key_0", "c22"),
				model.NewQueryResultCol("aggr__2__8__4__5__order_1", 17),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__key_0", "a2"),
				model.NewQueryResultCol("aggr__2__order_1", "a2"),
				model.NewQueryResultCol("aggr__2__8__key_0", "b21"),
				model.NewQueryResultCol("aggr__2__8__order_1", 399126.7496833334),
				model.NewQueryResultCol("metric__2__8__1_col_0", 399126.7496833334),
				model.NewQueryResultCol("aggr__2__8__4__key_0", "c21"),
				model.NewQueryResultCol("aggr__2__8__4__order_1", "c21"),
				model.NewQueryResultCol("aggr__2__8__4__5__key_0", "c21"),
				model.NewQueryResultCol("aggr__2__8__4__5__order_1", 17),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__key_0", "a1"),
				model.NewQueryResultCol("aggr__2__order_1", "a1"),
				model.NewQueryResultCol("aggr__2__8__key_0", "b12"),
				model.NewQueryResultCol("aggr__2__8__order_1", 45774.291766666654),
				model.NewQueryResultCol("metric__2__8__1_col_0", 45774.291766666654),
				model.NewQueryResultCol("aggr__2__8__4__key_0", "c12"),
				model.NewQueryResultCol("aggr__2__8__4__order_1", "c12"),
				model.NewQueryResultCol("aggr__2__8__4__5__key_0", "c12"),
				model.NewQueryResultCol("aggr__2__8__4__5__order_1", 24),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__key_0", "a1"),
				model.NewQueryResultCol("aggr__2__order_1", "a1"),
				model.NewQueryResultCol("aggr__2__8__key_0", "b11"),
				model.NewQueryResultCol("aggr__2__8__order_1", 51891.94613333333),
				model.NewQueryResultCol("metric__2__8__1_col_0", 51891.94613333333),
				model.NewQueryResultCol("aggr__2__8__4__key_0", "c11"),
				model.NewQueryResultCol("aggr__2__8__4__order_1", "c11"),
				model.NewQueryResultCol("aggr__2__8__4__5__key_0", "c12"),
				model.NewQueryResultCol("aggr__2__8__4__5__order_1", 21),
			}},
		},
	},
	{
		TestName: "Ophelia Test 6: triple terms + other aggregations + order by another aggregations",
		Sql: `
SELECT "aggr__2__key_0", "aggr__2__order_1", "metric__2__1_col_0",
  "aggr__2__8__key_0", "aggr__2__8__order_1", "metric__2__8__1_col_0",
  "aggr__2__8__4__key_0", "aggr__2__8__4__order_1", "metric__2__8__4__1_col_0",
  "metric__2__8__4__5_col_0"
FROM (
  SELECT "aggr__2__key_0", "aggr__2__order_1", "metric__2__1_col_0",
    "aggr__2__8__key_0", "aggr__2__8__order_1", "metric__2__8__1_col_0",
    "aggr__2__8__4__key_0", "aggr__2__8__4__order_1", "metric__2__8__4__1_col_0"
    , "metric__2__8__4__5_col_0", dense_rank() OVER (PARTITION BY 1
  ORDER BY "aggr__2__order_1" DESC, "aggr__2__key_0" ASC) AS
    "aggr__2__order_1_rank", dense_rank() OVER (PARTITION BY "aggr__2__key_0"
  ORDER BY "aggr__2__8__order_1" DESC, "aggr__2__8__key_0" ASC) AS
    "aggr__2__8__order_1_rank", dense_rank() OVER (PARTITION BY "aggr__2__key_0"
    , "aggr__2__8__key_0"
  ORDER BY "aggr__2__8__4__order_1" DESC, "aggr__2__8__4__key_0" ASC) AS
    "aggr__2__8__4__order_1_rank"
  FROM (
    SELECT "surname" AS "aggr__2__key_0", sumOrNull("aggr__2__order_1_part")
      OVER (PARTITION BY "aggr__2__key_0") AS "aggr__2__order_1", sumOrNull(
      "metric__2__1_col_0_part") OVER (PARTITION BY "aggr__2__key_0") AS
      "metric__2__1_col_0", COALESCE("limbName",'__missing__') AS
      "aggr__2__8__key_0", sumOrNull("aggr__2__8__order_1_part") OVER (PARTITION
       BY "aggr__2__key_0", "aggr__2__8__key_0") AS "aggr__2__8__order_1",
      sumOrNull("metric__2__8__1_col_0_part") OVER (PARTITION BY
      "aggr__2__key_0", "aggr__2__8__key_0") AS "metric__2__8__1_col_0",
      "organName" AS "aggr__2__8__4__key_0", sumOrNull("total") AS
      "aggr__2__8__4__order_1", sumOrNull("total") AS "metric__2__8__4__1_col_0"
      , sumOrNull("some") AS "metric__2__8__4__5_col_0", sumOrNull("total") AS
      "aggr__2__order_1_part", sumOrNull("total") AS "metric__2__1_col_0_part",
      sumOrNull("total") AS "aggr__2__8__order_1_part", sumOrNull("total") AS
      "metric__2__8__1_col_0_part"
    FROM "logs-generic-default"
    GROUP BY "surname" AS "aggr__2__key_0", COALESCE("limbName",'__missing__')
      AS "aggr__2__8__key_0", "organName" AS "aggr__2__8__4__key_0"))
WHERE (("aggr__2__order_1_rank"<=200 AND "aggr__2__8__order_1_rank"<=20) AND
  "aggr__2__8__4__order_1_rank"<=1)
ORDER BY "aggr__2__order_1_rank" ASC, "aggr__2__8__order_1_rank" ASC,
  "aggr__2__8__4__order_1_rank" ASC`,
		ExpectedResults: []model.QueryResultRow{
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__key_0", "a1"),
				model.NewQueryResultCol("aggr__2__order_1", 1091661.7608666667),
				model.NewQueryResultCol("metric__2__1_col_0", 1091661.7608666667),
				model.NewQueryResultCol("aggr__2__8__key_0", "b11"),
				model.NewQueryResultCol("aggr__2__8__order_1", 51891.94613333333),
				model.NewQueryResultCol("metric__2__8__1_col_0", 51891.94613333333),
				model.NewQueryResultCol("aggr__2__8__4__key_0", "c11"),
				model.NewQueryResultCol("aggr__2__8__4__order_1", 51891.94613333333),
				model.NewQueryResultCol("aggr__2__8__4__1__col_0", 51891.94613333333),
				model.NewQueryResultCol("aggr__2__8__4__5__col_0", 37988.09523333333),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__key_0", "a1"),
				model.NewQueryResultCol("aggr__2__order_1", 1091661.7608666667),
				model.NewQueryResultCol("metric__2__1_col_0", 1091661.7608666667),
				model.NewQueryResultCol("aggr__2__8__key_0", "b12"),
				model.NewQueryResultCol("aggr__2__8__order_1", 45774.291766666654),
				model.NewQueryResultCol("metric__2__8__1_col_0", 45774.291766666654),
				model.NewQueryResultCol("aggr__2__8__4__key_0", "c12"),
				model.NewQueryResultCol("aggr__2__8__4__order_1", 45774.291766666654),
				model.NewQueryResultCol("aggr__2__8__4__1__col_0", 45774.291766666654),
				model.NewQueryResultCol("aggr__2__8__4__5__col_0", 36577.89516666666),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__key_0", "a2"),
				model.NewQueryResultCol("aggr__2__order_1", 630270.07765),
				model.NewQueryResultCol("metric__2__1_col_0", 630270.07765),
				model.NewQueryResultCol("aggr__2__8__key_0", "b21"),
				model.NewQueryResultCol("aggr__2__8__order_1", 399126.7496833334),
				model.NewQueryResultCol("metric__2__8__1_col_0", 399126.7496833334),
				model.NewQueryResultCol("aggr__2__8__4__key_0", "c21"),
				model.NewQueryResultCol("aggr__2__8__4__order_1", 399126.7496833334),
				model.NewQueryResultCol("aggr__2__8__4__1__col_0", 399126.7496833334),
				model.NewQueryResultCol("aggr__2__8__4__5__col_0", 337246.82201666664),
			}},
			{Cols: []model.QueryResultCol{
				model.NewQueryResultCol("aggr__2__key_0", "a2"),
				model.NewQueryResultCol("aggr__2__order_1", 630270.07765),
				model.NewQueryResultCol("metric__2__1_col_0", 630270.07765),
				model.NewQueryResultCol("aggr__2__8__key_0", "b22"),
				model.NewQueryResultCol("aggr__2__8__order_1", 231143.3279666666),
				model.NewQueryResultCol("metric__2__8__1_col_0", 231143.3279666666),
				model.NewQueryResultCol("aggr__2__8__4__key_0", "c22"),
				model.NewQueryResultCol("aggr__2__8__4__order_1", 231143.3279666666),
				model.NewQueryResultCol("aggr__2__8__4__1__col_0", 231143.3279666666),
				model.NewQueryResultCol("aggr__2__8__4__5__col_0", 205408.48849999998),
			}},
		},
	},
	/*
			{
				TestName: "Ophelia Test 7: 5x terms + a lot of other aggregations",
				Sql: `
		SELECT "aggr__2__key_0", "aggr__2__order_1", "metric__2__1_col_0",
		  "aggr__2__7__key_0", "aggr__2__7__order_1", "metric__2__7__1_col_0",
		  "aggr__2__7__8__key_0", "aggr__2__7__8__order_1", "metric__2__7__8__1_col_0",
		  "aggr__2__7__8__4__key_0", "aggr__2__7__8__4__order_1",
		  "metric__2__7__8__4__1_col_0", "aggr__2__7__8__4__3__key_0",
		  "aggr__2__7__8__4__3__order_1", "metric__2__7__8__4__3__1_col_0",
		  "metric__2__7__8__4__3__5_col_0", "metric__2__7__8__4__3__6_col_0"
		FROM (
		  SELECT "aggr__2__key_0", "aggr__2__order_1", "metric__2__1_col_0",
		    "aggr__2__7__key_0", "aggr__2__7__order_1", "metric__2__7__1_col_0",
		    "aggr__2__7__8__key_0", "aggr__2__7__8__order_1", "metric__2__7__8__1_col_0"
		    , "aggr__2__7__8__4__key_0", "aggr__2__7__8__4__order_1",
		    "metric__2__7__8__4__1_col_0", "aggr__2__7__8__4__3__key_0",
		    "aggr__2__7__8__4__3__order_1", "metric__2__7__8__4__3__1_col_0",
		    "metric__2__7__8__4__3__5_col_0", "metric__2__7__8__4__3__6_col_0",
		    dense_rank() OVER (PARTITION BY 1
		  ORDER BY "aggr__2__order_1" DESC, "aggr__2__key_0" ASC) AS
		    "aggr__2__order_1_rank", dense_rank() OVER (PARTITION BY "aggr__2__key_0"
		  ORDER BY "aggr__2__7__order_1" DESC, "aggr__2__7__key_0" ASC) AS
		    "aggr__2__7__order_1_rank", dense_rank() OVER (PARTITION BY "aggr__2__key_0"
		    , "aggr__2__7__key_0"
		  ORDER BY "aggr__2__7__8__order_1" DESC, "aggr__2__7__8__key_0" ASC) AS
		    "aggr__2__7__8__order_1_rank", dense_rank() OVER (PARTITION BY
		    "aggr__2__key_0", "aggr__2__7__key_0", "aggr__2__7__8__key_0"
		  ORDER BY "aggr__2__7__8__4__order_1" DESC, "aggr__2__7__8__4__key_0" ASC) AS
		    "aggr__2__7__8__4__order_1_rank", dense_rank() OVER (PARTITION BY
		    "aggr__2__key_0", "aggr__2__7__key_0", "aggr__2__7__8__key_0",
		    "aggr__2__7__8__4__key_0"
		  ORDER BY "aggr__2__7__8__4__3__order_1" DESC, "aggr__2__7__8__4__3__key_0" ASC
		    ) AS "aggr__2__7__8__4__3__order_1_rank"
		  FROM (
		    SELECT "surname" AS "aggr__2__key_0", sumOrNull("aggr__2__order_1_part")
		      OVER (PARTITION BY "aggr__2__key_0") AS "aggr__2__order_1", sumOrNull(
		      "metric__2__1_col_0_part") OVER (PARTITION BY "aggr__2__key_0") AS
		      "metric__2__1_col_0", COALESCE("limbName",'__missing__') AS
		      "aggr__2__7__key_0", sumOrNull("aggr__2__7__order_1_part") OVER (PARTITION
		       BY "aggr__2__key_0", "aggr__2__7__key_0") AS "aggr__2__7__order_1",
		      sumOrNull("metric__2__7__1_col_0_part") OVER (PARTITION BY
		      "aggr__2__key_0", "aggr__2__7__key_0") AS "metric__2__7__1_col_0",
		      COALESCE("organName",'__missing__') AS "aggr__2__7__8__key_0", sumOrNull(
		      "aggr__2__7__8__order_1_part") OVER (PARTITION BY "aggr__2__key_0",
		      "aggr__2__7__key_0", "aggr__2__7__8__key_0") AS "aggr__2__7__8__order_1",
		      sumOrNull("metric__2__7__8__1_col_0_part") OVER (PARTITION BY
		      "aggr__2__key_0", "aggr__2__7__key_0", "aggr__2__7__8__key_0") AS
		      "metric__2__7__8__1_col_0", "doctorName" AS "aggr__2__7__8__4__key_0",
		      sumOrNull("aggr__2__7__8__4__order_1_part") OVER (PARTITION BY
		      "aggr__2__key_0", "aggr__2__7__key_0", "aggr__2__7__8__key_0",
		      "aggr__2__7__8__4__key_0") AS "aggr__2__7__8__4__order_1", sumOrNull(
		      "metric__2__7__8__4__1_col_0_part") OVER (PARTITION BY "aggr__2__key_0",
		      "aggr__2__7__key_0", "aggr__2__7__8__key_0", "aggr__2__7__8__4__key_0") AS
		       "metric__2__7__8__4__1_col_0", "height" AS "aggr__2__7__8__4__3__key_0",
		      sumOrNull("total") AS "aggr__2__7__8__4__3__order_1", sumOrNull("total")
		      AS "metric__2__7__8__4__3__1_col_0", sumOrNull("some") AS
		      "metric__2__7__8__4__3__5_col_0", sumOrNull("cost") AS
		      "metric__2__7__8__4__3__6_col_0", sumOrNull("total") AS
		      "aggr__2__order_1_part", sumOrNull("total") AS "metric__2__1_col_0_part",
		      sumOrNull("total") AS "aggr__2__7__order_1_part", sumOrNull("total") AS
		      "metric__2__7__1_col_0_part", sumOrNull("total") AS
		      "aggr__2__7__8__order_1_part", sumOrNull("total") AS
		      "metric__2__7__8__1_col_0_part", sumOrNull("total") AS
		      "aggr__2__7__8__4__order_1_part", sumOrNull("total") AS
		      "metric__2__7__8__4__1_col_0_part"
		    FROM "logs-generic-default"
		    GROUP BY "surname" AS "aggr__2__key_0", COALESCE("limbName",'__missing__')
		      AS "aggr__2__7__key_0", COALESCE("organName",'__missing__') AS
		      "aggr__2__7__8__key_0", "doctorName" AS "aggr__2__7__8__4__key_0",
		      "height" AS "aggr__2__7__8__4__3__key_0"))
		WHERE (((("aggr__2__order_1_rank"<=100 AND "aggr__2__7__order_1_rank"<=10) AND
		  "aggr__2__7__8__order_1_rank"<=10) AND "aggr__2__7__8__4__order_1_rank"<=6)
		  AND "aggr__2__7__8__4__3__order_1_rank"<=1)
		ORDER BY "aggr__2__order_1_rank" ASC, "aggr__2__7__order_1_rank" ASC,
		  "aggr__2__7__8__order_1_rank" ASC, "aggr__2__7__8__4__order_1_rank" ASC,
		  "aggr__2__7__8__4__3__order_1_rank" ASC`,
				ExpectedResults: []model.QueryResultRow{ // TODO: Fix values
					{Cols: []model.QueryResultCol{
						model.NewQueryResultCol("aggr__2__key_0", 12345),
						model.NewQueryResultCol("aggr__2__order_1", 12345),
						model.NewQueryResultCol("metric__2__1_col_0", 12345),
						model.NewQueryResultCol("aggr__2__7__key_0", 12345),
						model.NewQueryResultCol("aggr__2__7__order_1", 12345),
						model.NewQueryResultCol("metric__2__7__1_col_0", 12345),
						model.NewQueryResultCol("aggr__2__7__8__key_0", 12345),
						model.NewQueryResultCol("aggr__2__7__8__order_1", 12345),
						model.NewQueryResultCol("metric__2__7__8__1_col_0", 12345),
						model.NewQueryResultCol("aggr__2__7__8__4__key_0", 12345),
						model.NewQueryResultCol("aggr__2__7__8__4__order_1", 12345),
						model.NewQueryResultCol("metric__2__7__8__4__1_col_0", 12345),
						model.NewQueryResultCol("aggr__2__7__8__4__3__key_0", 12345),
						model.NewQueryResultCol("aggr__2__7__8__4__3__order_1", 12345),
						model.NewQueryResultCol("metric__2__7__8__4__3__1_col_0", 12345),
						model.NewQueryResultCol("metric__2__7__8__4__3__5_col_0", 12345),
						model.NewQueryResultCol("metric__2__7__8__4__3__6_col_0", 12345),
					}},
				},
			},
	*/
}
