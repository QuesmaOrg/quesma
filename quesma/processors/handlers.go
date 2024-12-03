// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package processors

import (
	"context"
	"quesma/quesma/functionality/bulk"
	"quesma/quesma/types"
)

// handleDocIndex assembles the payload into bulk format to reusing existing logic of bulk ingest
func handleDocIndex(payload types.JSON, targetTableName string) {
	newPayload := []types.JSON{
		map[string]interface{}{"index": map[string]interface{}{"_index": targetTableName}},
		payload,
	}
	_, _ = bulk.Write(context.Background(), &targetTableName, newPayload, ip, cfg, phoneHomeAgent, registry)
}

func handleBulkIndex(payload types.NDJSON, targetTableName string) {
	_, _ = bulk.Write(context.Background(), &targetTableName, payload, ip, cfg, phoneHomeAgent, registry)
}
