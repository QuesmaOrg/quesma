// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package jsonprocessor

import (
	"quesma/quesma/config"
	"quesma/quesma/types"
	"strings"
)

type IngestTransformer interface {
	Transform(document types.JSON) (types.JSON, error)
}

type flattenMapTransformer struct {
	separator string
}

func (t *flattenMapTransformer) Transform(document types.JSON) (types.JSON, error) {
	return FlattenMap(document, t.separator), nil
}

type removeFieldsTransformer struct {
	fields []config.FieldName
}

func (t *removeFieldsTransformer) Transform(document types.JSON) (types.JSON, error) {
	for _, field := range t.fields {
		delete(document, field.AsString())
		delete(document, strings.Replace(field.AsString(), ".", "::", -1))
	}
	return document, nil
}

func IngestTransformerFor(table string, cfg *config.QuesmaConfiguration) IngestTransformer {
	var transformers []IngestTransformer

	transformers = append(transformers, &flattenMapTransformer{separator: "::"})

	if indexConfig, found := cfg.IndexConfig[table]; found && indexConfig.SchemaOverrides != nil {
		// FIXME: don't get ignored fields from schema config, but store
		// them in the schema registry - that way we don't have to manually replace '.' with '::'
		// in removeFieldsTransformer's Transform method
		transformers = append(transformers, &removeFieldsTransformer{fields: indexConfig.SchemaOverrides.IgnoredFields()})
	}

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
