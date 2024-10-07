// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package quesma

import (
	"quesma/model"
	"quesma/schema"
)

type TransformationPipeline struct {
	transformers []model.QueryTransformer
}

func (o *TransformationPipeline) Transform(queries []*model.Query) ([]*model.Query, error) {
	var err error
	for _, transformer := range o.transformers {
		queries, err = transformer.Transform(queries)
		if err != nil {
			return nil, err
		}
	}
	return queries, nil
}

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
