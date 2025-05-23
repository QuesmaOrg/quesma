// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package model

import "context"

type QueryTransformer interface {
	Transform(plan *ExecutionPlan) (*ExecutionPlan, error)
}

type ResultTransformer interface {
	Transform(plan *ExecutionPlan, result [][]QueryResultRow) (*ExecutionPlan, [][]QueryResultRow, error)
}

type QueryRowsTransformer interface {
	Transform(ctx context.Context, rows []QueryResultRow) []QueryResultRow
}
