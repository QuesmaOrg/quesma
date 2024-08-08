// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package plugins

import (
	"quesma/model"
)

type ResultTransformerPipeline []ResultTransformer

func (pipe ResultTransformerPipeline) Transform(result [][]model.QueryResultRow) ([][]model.QueryResultRow, error) {
	for _, transformer := range pipe {
		var err error
		result, err = transformer.Transform(result)
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}
