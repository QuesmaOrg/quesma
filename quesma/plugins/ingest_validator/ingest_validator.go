// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package ingest_validator

import (
	"quesma/plugins"
	"quesma/quesma/config"
	"quesma/quesma/types"
	"quesma/schema"
)

type IngestValidator struct {
	cfg            config.QuesmaConfiguration
	schemaRegistry schema.Registry
	table          string
}

func (*IngestValidator) Transform(document types.JSON) (types.JSON, error) {
	return document, nil
}

func (p *IngestValidator) ApplyIngestTransformers(table string, cfg config.QuesmaConfiguration, schema schema.Registry, transformers []plugins.IngestTransformer) []plugins.IngestTransformer {
	transformers = append(transformers, &IngestValidator{cfg: cfg, schemaRegistry: schema, table: table})
	return transformers
}

func (p *IngestValidator) ApplyQueryTransformers(table string, cfg config.QuesmaConfiguration, schema schema.Registry, transformers []plugins.QueryTransformer) []plugins.QueryTransformer {
	return transformers
}

func (p *IngestValidator) ApplyResultTransformers(table string, cfg config.QuesmaConfiguration, schema schema.Registry, transformers []plugins.ResultTransformer) []plugins.ResultTransformer {
	return transformers
}

func (p *IngestValidator) ApplyFieldCapsTransformers(table string, cfg config.QuesmaConfiguration, schema schema.Registry, transformers []plugins.FieldCapsTransformer) []plugins.FieldCapsTransformer {
	return transformers
}

func (p *IngestValidator) GetTableColumnFormatter(table string, cfg config.QuesmaConfiguration, schema schema.Registry) plugins.TableColumNameFormatter {
	return nil
}
