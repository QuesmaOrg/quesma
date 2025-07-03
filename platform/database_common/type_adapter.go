// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package database_common

import (
	"github.com/QuesmaOrg/quesma/platform/schema"
)

type SchemaTypeConverter interface {
	Convert(string) (schema.QuesmaType, bool)
}
