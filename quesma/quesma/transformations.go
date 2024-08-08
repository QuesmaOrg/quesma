// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package quesma

import (
	"quesma/model"
	"strings"
)

type TransformationPipeline struct {
	transformers []model.QueryTransformer
}

func (o *TransformationPipeline) Transform(queries []*model.Query) ([]*model.Query, error) {
	for _, transformer := range o.transformers {
		queries, _ = transformer.Transform(queries)
	}
	return queries, nil
}

type replaceColumNamesWithFieldNames struct {
}

func (t *replaceColumNamesWithFieldNames) Transform(result [][]model.QueryResultRow) ([][]model.QueryResultRow, error) {

	const doubleColons = "::"
	const dot = "."

	for _, rows := range result {
		for i, row := range rows {
			for j := range row.Cols {
				rows[i].Cols[j].ColName = strings.ReplaceAll(row.Cols[j].ColName, doubleColons, dot)
			}
		}
	}
	return result, nil
}
