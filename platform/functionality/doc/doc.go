// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package doc

import (
	"context"
	"github.com/QuesmaOrg/quesma/platform/backend_connectors"
	"github.com/QuesmaOrg/quesma/platform/functionality/bulk"
	"github.com/QuesmaOrg/quesma/platform/ingest"
	"github.com/QuesmaOrg/quesma/platform/table_resolver"
	"github.com/QuesmaOrg/quesma/platform/types"
	"github.com/QuesmaOrg/quesma/platform/v2/core/diag"
)

func Write(ctx context.Context, tableName *string, body types.JSON, ip *ingest.IngestProcessor, ingestStatsEnabled bool, phoneHomeAgent diag.PhoneHomeClient, registry table_resolver.TableResolver, elasticsearchConnector *backend_connectors.ElasticsearchBackendConnector) (bulk.BulkItem, error) {
	// Translate single doc write to a bulk request, reusing exiting logic of bulk ingest
	payload := []types.JSON{
		map[string]interface{}{"index": map[string]interface{}{"_index": *tableName}},
		body,
	}
	results, err := bulk.Write(ctx, tableName, payload, ip, ingestStatsEnabled, elasticsearchConnector, phoneHomeAgent, registry)

	if err != nil {
		return bulk.BulkItem{}, err
	}

	return results[0], err
}
