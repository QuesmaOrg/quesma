// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package bucket_aggregations

import "github.com/QuesmaOrg/quesma/quesma/model"

// CombinatorAggregationInterface It is a special case of bucket aggregation for: filters, range and dataRange.
// They are by using combinators such as `countIf` instead of group by.
// Example:
// SELECT countIf("ftd_session_time"<1000) AS "range_0__aggr__2__count",
//
//	uniqIf("ftd_session_time", "ftd_session_time"<1000) AS "range_0__metric__2__1_col_0",
//	countIf("ftd_session_time">=-100) AS "range_1__aggr__2__count",
//	uniqIf("ftd_session_time", "ftd_session_time">=-100) AS	"range_1__metric__2__1_col_0"
//
// FROM "logs-generic-default"
type CombinatorAggregationInterface interface {
	CombinatorGroups() []CombinatorGroup
	CombinatorTranslateSqlResponseToJson(subGroup CombinatorGroup, rows []model.QueryResultRow) model.JsonMap
	DoesNotHaveGroupBy() bool           // defined is NoGroupByInterface which is broader group
	CombinatorSplit() []model.QueryType // split into new aggregations, each for one group
}

type CombinatorGroup struct {
	idx         int
	Prefix      string
	Key         string
	WhereClause model.Expr
}
