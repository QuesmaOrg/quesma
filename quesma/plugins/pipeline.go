// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package plugins

import (
	"quesma/model"
	"quesma/quesma/types"
)

type QueryTransformerPipeline []QueryTransformer

func (pipe QueryTransformerPipeline) Transform(query []*model.Query) ([]*model.Query, error) {
	for _, transformer := range pipe {
		var err error
		query, err = transformer.Transform(query)
		if err != nil {
			return nil, err
		}
	}
	return query, nil
}

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

type FieldCapsTransformerPipeline []FieldCapsTransformer

func (pipe FieldCapsTransformerPipeline) Transform(fieldCaps map[string]map[string]model.FieldCapability) (map[string]map[string]model.FieldCapability, error) {
	for _, transformer := range pipe {
		var err error
		fieldCaps, err = transformer.Transform(fieldCaps)
		if err != nil {
			return fieldCaps, err
		}
	}
	return fieldCaps, nil
}

type IngestTransformerPipeline []IngestTransformer

func (pipe IngestTransformerPipeline) Transform(document types.JSON) (types.JSON, error) {
	for _, transformer := range pipe {
		var err error
		document, err = transformer.Transform(document)
		if err != nil {
			return document, err
		}
	}
	return document, nil
}
