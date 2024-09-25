// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package doc

import (
	"context"
	"quesma/ingest"
	"quesma/quesma/config"
	"quesma/quesma/functionality/bulk"
	"quesma/quesma/types"
	"quesma/telemetry"
)

func Write(ctx context.Context, tableName *string, body types.JSON, ip *ingest.IngestProcessor, cfg *config.QuesmaConfiguration, phoneHomeAgent telemetry.PhoneHomeAgent) (bulk.BulkItem, error) {
	// Translate single doc write to a bulk request, reusing exiting logic of bulk ingest
	results, err := bulk.Write(ctx, tableName, []types.JSON{
		map[string]interface{}{"index": map[string]interface{}{"_index": *tableName}},
		body,
	}, ip, cfg, phoneHomeAgent)
	return results[0], err
}
