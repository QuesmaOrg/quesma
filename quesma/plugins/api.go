// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package plugins

import (
	"quesma/model"
	"quesma/quesma/config"
	"quesma/quesma/types"
	"quesma/schema"
)

// Legit Interfaces

type ResultTransformer interface {
	Transform(result [][]model.QueryResultRow) ([][]model.QueryResultRow, error)
}

type QueryTransformer interface {
	Transform(query []*model.Query) ([]*model.Query, error)
}

type IngestTransformer interface {
	Transform(document types.JSON) (types.JSON, error)
}

// not so legit API

type FieldCapsTransformer interface {
	Transform(fieldCaps map[string]map[string]model.FieldCapability) (map[string]map[string]model.FieldCapability, error)
}

// this one is used to format column names on table creation
// it's too specific to be a transformer, we should have a sth different here
// maybe whole "buildCreateTableQueryNoOurFields" should be moved to a plugin
type TableColumNameFormatter interface {
	Format(namespace, columnName string) string
}

///

// Plugin changes the behavior of Quesma by changing the pipeline of transformers
type Plugin interface {
	ApplyIngestTransformers(table string, cfg config.QuesmaConfiguration, schema schema.Registry, transformers []IngestTransformer) []IngestTransformer
	ApplyFieldCapsTransformers(table string, cfg config.QuesmaConfiguration, schema schema.Registry, transformers []FieldCapsTransformer) []FieldCapsTransformer
	ApplyQueryTransformers(table string, cfg config.QuesmaConfiguration, schema schema.Registry, transformers []QueryTransformer) []QueryTransformer
	ApplyResultTransformers(table string, cfg config.QuesmaConfiguration, schema schema.Registry, transformers []ResultTransformer) []ResultTransformer
	GetTableColumnFormatter(table string, cfg config.QuesmaConfiguration, schema schema.Registry) TableColumNameFormatter
}
