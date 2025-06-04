// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package frontend_connectors

import (
	"github.com/QuesmaOrg/quesma/platform/model"
	"github.com/QuesmaOrg/quesma/platform/parsers/painful"
	"github.com/QuesmaOrg/quesma/platform/schema"
)

type replaceColumNamesWithFieldNames struct {
	indexSchema schema.Schema
}

func (t *replaceColumNamesWithFieldNames) Transform(plan *model.ExecutionPlan, result [][]model.QueryResultRow) (*model.ExecutionPlan, [][]model.QueryResultRow, error) {

	schemaInstance := t.indexSchema
	for _, rows := range result {
		for i, row := range rows {
			for j := range row.Cols {

				if field, exists := schemaInstance.ResolveFieldByInternalName(rows[i].Cols[j].ColName); exists {
					rows[i].Cols[j].ColName = field.PropertyName.AsString()
				}
			}
		}
	}
	return plan, result, nil
}

type EvalPainlessScriptOnColumnsTransformer struct {
	FieldScripts map[string]painful.Expr
}

func (t *EvalPainlessScriptOnColumnsTransformer) Transform(plan *model.ExecutionPlan, result [][]model.QueryResultRow) (*model.ExecutionPlan, [][]model.QueryResultRow, error) {

	for _, rows := range result {
		for _, row := range rows {
			doc := make(map[string]any)
			for j := range row.Cols {
				doc[row.Cols[j].ColName] = row.Cols[j].Value
			}

			for j := range row.Cols {

				if script, exists := t.FieldScripts[row.Cols[j].ColName]; exists {
					env := &painful.Env{
						Doc: doc,
					}

					_, err := script.Eval(env)
					if err != nil {
						return plan, nil, err
					}
					row.Cols[j].Value = env.EmitValue
				}
			}
		}
	}
	return plan, result, nil
}

// SiblingsTransformer is a transformer that merges the results of sibling queries
// into a single result set. The general idea is that a query might be split into
// multiple subqueries, which are executed in parallel. Their results are then
// merged together.
//
// This approach is useful when only a subset of the results is needed.
// For example, Elasticsearch often returns the last 500 hits. If we already
// get 500 hits within the last 15 minutes, we can stop execution early.
// Otherwise, we can collect and merge results from all sibling queries
// to retrieve more data.

type SiblingsTransformer struct {
}

func (t *SiblingsTransformer) Transform(plan *model.ExecutionPlan, results [][]model.QueryResultRow) (*model.ExecutionPlan, [][]model.QueryResultRow, error) {
	if plan.MergeSiblingResults != nil {
		plan, results = plan.MergeSiblingResults(plan, results)
	}
	return plan, results, nil
}
