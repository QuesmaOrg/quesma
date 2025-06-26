// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package ingest

import (
	chLib "github.com/QuesmaOrg/quesma/platform/clickhouse"
	"github.com/QuesmaOrg/quesma/platform/schema"
	"github.com/QuesmaOrg/quesma/platform/types"
)

type Lowerer interface {
	LowerToDDL(validatedJsons []types.JSON,
		table *chLib.Table,
		invalidJsons []types.JSON,
		encodings map[schema.FieldEncodingKey]schema.EncodedFieldName,
		createTableCmd CreateTableStatement) ([]string, error)
}
