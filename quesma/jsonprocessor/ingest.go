// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package jsonprocessor

import (
	"quesma/quesma/config"
	"quesma/quesma/types"
)

type IngestTransformer interface {
	Transform(document types.JSON) (types.JSON, error)
}

type ingestTransformer struct {
	separator string
}

func (t *ingestTransformer) Transform(document types.JSON) (types.JSON, error) {
	return FlattenMap(document, t.separator), nil
}

// right now all transformers are the same, but we can add more in the future
func IngestTransformerFor(table string, cfg config.QuesmaConfiguration) IngestTransformer {

	var transformers []IngestTransformer

	transformers = append(transformers, &ingestTransformer{separator: "::"})

	return ingestTransformerPipeline(transformers)
}

type ingestTransformerPipeline []IngestTransformer

func (pipe ingestTransformerPipeline) Transform(document types.JSON) (types.JSON, error) {
	for _, transformer := range pipe {
		var err error
		document, err = transformer.Transform(document)
		if err != nil {
			return document, err
		}
	}
	return document, nil
}
