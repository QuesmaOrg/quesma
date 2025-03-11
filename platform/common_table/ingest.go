// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package common_table

import "github.com/QuesmaOrg/quesma/platform/types"

// IngestAddIndexNameTransformer is a transformer that adds an index name to the JSON
type IngestAddIndexNameTransformer struct {
	IndexName string
}

func (t *IngestAddIndexNameTransformer) Transform(json types.JSON) (types.JSON, error) {
	json[IndexNameColumn] = t.IndexName
	return json, nil
}
