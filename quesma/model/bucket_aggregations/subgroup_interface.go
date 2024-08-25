// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package bucket_aggregations

import "quesma/model"

type SubGroup struct {
	idx         int
	Prefix      string
	Key         string
	WhereClause model.Expr
}

type SubGroupInterface interface {
	SubGroups() []SubGroup
	SubGroupTranslateSqlResponseToJson(subGroup SubGroup, rows []model.QueryResultRow) model.JsonMap
}
