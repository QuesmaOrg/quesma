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
}

func (t *replaceColumNamesWithFieldNames) Transform(result [][]model.QueryResultRow) ([][]model.QueryResultRow, error) {
	// TODO fix that
	// we need revert mapping from field name to column name
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
