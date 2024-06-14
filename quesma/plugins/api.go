package plugins

import (
	"mitmproxy/quesma/model"
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
	ResultTransformer() ResultTransformer
	FieldCapsTransformer() FieldCapsTransformer
	QueryTransformer() QueryTransformer
	IngestTransformer() IngestTransformer
	TableColumNameFormatter() TableColumNameFormatter
}
