// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package plugins

import (
	"quesma/model"
	"quesma/quesma/types"
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
