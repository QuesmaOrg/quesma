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

func (t *replaceColumNamesWithFieldNames) Transform(result [][]model.QueryResultRow) ([][]model.QueryResultRow, error) {

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
	return result, nil
}

type EvalPainlessScriptOnColumnsTransformer struct {
	FieldScripts map[string]painful.Expr
}

func (t *EvalPainlessScriptOnColumnsTransformer) Transform(result [][]model.QueryResultRow) ([][]model.QueryResultRow, error) {

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
						return nil, err
					}
					row.Cols[j].Value = env.EmitValue
				}
			}
		}
	}
	return result, nil
}
