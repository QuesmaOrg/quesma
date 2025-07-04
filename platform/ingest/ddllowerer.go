// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package ingest

import (
	chLib "github.com/QuesmaOrg/quesma/platform/database_common"
	"github.com/QuesmaOrg/quesma/platform/schema"
	"github.com/QuesmaOrg/quesma/platform/types"
)

// The main purpose of Lowerer interface is to infers a schema from input JSON
// data and then generates a backend-specific DDL (Data Definition Language) representation, such as a CREATE TABLE statement.
// or other DDL commands that are needed to create or modify a table in the database.
type Lowerer interface {
	LowerToDDL(validatedJsons []types.JSON,
		table *chLib.Table,
		invalidJsons []types.JSON,
		encodings map[schema.FieldEncodingKey]schema.EncodedFieldName,
		createTableCmd CreateTableStatement) ([]string, error)
}
