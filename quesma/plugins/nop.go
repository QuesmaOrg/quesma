// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package plugins

import (
	"quesma/model"
)

type NopResultTransformer struct {
}

func (*NopResultTransformer) Transform(result [][]model.QueryResultRow) ([][]model.QueryResultRow, error) {
	return result, nil
}
