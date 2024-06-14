package plugins

import (
	"mitmproxy/quesma/model"
	"mitmproxy/quesma/quesma/config"
	"mitmproxy/quesma/quesma/types"
)

// Interfaces

type ResultTransformer interface {
	Transform(result [][]model.QueryResultRow) ([][]model.QueryResultRow, error)
}

type FieldCapsTransformer interface {
	Transform(fieldCaps model.FieldCapsResponse) (model.FieldCapsResponse, error)
}

type QueryTransformer interface {
	Transform(query []*model.Query) ([]*model.Query, error)
}

type IngestTransformer interface {
	Transform(document types.JSON) (types.JSON, error)
}

// this one is used to format column names on table creation
// it's too specific to be a transformer, we should have a sth different here
// maybe whole "buildCreateTableQueryNoOurFields" should be moved to a plugin
type TableColumNameFormatter interface {
	Format(namespace, columnName string) string
}

///

// Plugin provides implementations of transformers
type Plugin interface {
	ApplyIngestTransformers(table string, cfg config.QuesmaConfiguration, transformers []IngestTransformer) []IngestTransformer
	ApplyFieldCapsTransformers(table string, cfg config.QuesmaConfiguration, transformers []FieldCapsTransformer) []FieldCapsTransformer
	ApplyQueryTransformers(table string, cfg config.QuesmaConfiguration, transformers []QueryTransformer) []QueryTransformer
	ApplyResultTransformers(table string, cfg config.QuesmaConfiguration, transformers []ResultTransformer) []ResultTransformer
	GetTableColumnFormatter(table string, cfg config.QuesmaConfiguration) TableColumNameFormatter
}
