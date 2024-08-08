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

type NopFieldCapsTransformer struct {
}

func (*NopFieldCapsTransformer) Transform(fieldCaps map[string]map[string]model.FieldCapability) (map[string]map[string]model.FieldCapability, error) {
	return fieldCaps, nil
}
