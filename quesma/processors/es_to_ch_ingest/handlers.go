// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package es_to_ch_ingest

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/QuesmaOrg/quesma/quesma/logger"
	"github.com/QuesmaOrg/quesma/quesma/queryparser"
	"github.com/QuesmaOrg/quesma/quesma/quesma/config"
	bulkmodel "github.com/QuesmaOrg/quesma/quesma/quesma/functionality/bulk"
	"github.com/QuesmaOrg/quesma/quesma/quesma/recovery"
	"github.com/QuesmaOrg/quesma/quesma/quesma/types"
	"io"
	"net/http"
)

// handleDocIndex assembles the payload into bulk format to reusing existing logic of bulk ingest
func (p *ElasticsearchToClickHouseIngestProcessor) handleDocIndex(payload types.JSON, targetTableName string) (bulkmodel.BulkItem, error) {
	newPayload := []types.JSON{
		map[string]interface{}{"index": map[string]interface{}{"_index": targetTableName}},
		payload,
	}

	if results, err := p.Write(context.Background(), &targetTableName, newPayload); err != nil {
		return bulkmodel.BulkItem{}, err
	} else {
		return results[0], nil
	}
}

func (p *ElasticsearchToClickHouseIngestProcessor) handleBulkIndex(payload types.NDJSON, targetTableName string) ([]bulkmodel.BulkItem, error) {
	results, err := p.Write(context.Background(), &targetTableName, payload)
	if err != nil {
		fmt.Printf("failed writing: %v", err)
		return []bulkmodel.BulkItem{}, err
	}
	return results, nil
}

func (p *ElasticsearchToClickHouseIngestProcessor) Write(ctx context.Context, defaultIndex *string, bulk types.NDJSON) (results []bulkmodel.BulkItem, err error) {
	defer recovery.LogPanic()

	bulkSize := len(bulk) / 2 // we divided payload by 2 so that we don't take into account the `action_and_meta_data` line, ref: https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html

	// The returned results should be in the same order as the input request, however splitting the bulk might change the order.
	// Therefore, each BulkRequestEntry has a corresponding pointer to the result entry, allowing us to freely split and reshuffle the bulk.
	results, clickhouseDocumentsToInsert, elasticRequestBody, elasticBulkEntries, err := splitBulk(ctx, defaultIndex, bulk, bulkSize, p.config)
	if err != nil {
		return []bulkmodel.BulkItem{}, err
	}

	err = p.sendToElastic(elasticRequestBody, elasticBulkEntries)
	if err != nil {
		return []bulkmodel.BulkItem{}, err
	}

	fmt.Printf("would send to clickhouse: [%v]\n", clickhouseDocumentsToInsert)
	p.sendToClickhouse(ctx, clickhouseDocumentsToInsert)

	return results, nil
}

func (p *ElasticsearchToClickHouseIngestProcessor) sendToElastic(elasticRequestBody []byte, elasticBulkEntries []BulkRequestEntry) error {
	if len(elasticRequestBody) == 0 {
		return nil
	}

	response, err := p.legacyIngestProcessor.RequestToElasticsearch(context.Background(), "POST", "/_bulk", elasticRequestBody, http.Header{"Content-Type": {"application/x-ndjson"}})
	if err != nil {
		return err
	}

	if response.StatusCode != http.StatusOK {
		responseBody, _ := io.ReadAll(response.Body)
		return fmt.Errorf("error sending bulk request (%v): %s", response.StatusCode, responseBody)
	}

	responseBody, err := io.ReadAll(response.Body)
	if err != nil {
		return err
	}

	var elasticBulkResponse bulkmodel.BulkResponse
	err = json.Unmarshal(responseBody, &elasticBulkResponse)
	if err != nil {
		return err
	}

	// Copy Elastic's response entries to our response (pointers to results array)
	for i, entry := range elasticBulkResponse.Items {
		*elasticBulkEntries[i].response = entry
	}
	return nil
}

func (p *ElasticsearchToClickHouseIngestProcessor) sendToClickhouse(ctx context.Context, clickhouseDocumentsToInsert map[string][]BulkRequestEntry) {
	for indexName, documents := range clickhouseDocumentsToInsert {
		//phoneHomeAgent.IngestCounters().Add(indexName, int64(len(documents)))

		//for _, document := range documents {
		//	stats.GlobalStatistics.Process(cfg, indexName, document.document, clickhouse.NestedSeparator)
		//}
		// if the index is mapped to specified database table in the configuration, use that table
		// TODO: Index name override ignored for now

		if len(p.config.IndexConfig[indexName].Override) > 0 {
			indexName = p.config.IndexConfig[indexName].Override
		}

		inserts := make([]types.JSON, len(documents))
		for i, document := range documents {
			inserts[i] = document.document
		}

		err := p.legacyIngestProcessor.Ingest(ctx, indexName, inserts)

		for _, document := range documents {
			bulkSingleResponse := bulkmodel.BulkSingleResponse{
				ID:          "fakeId",
				Index:       document.index,
				PrimaryTerm: 1,
				SeqNo:       0,
				Shards: bulkmodel.BulkShardsResponse{
					Failed:     0,
					Successful: 1,
					Total:      1,
				},
				Version: 0,
				Result:  "created",
				Status:  201,
				Type:    "_doc",
			}

			if err != nil {
				bulkSingleResponse.Result = ""
				bulkSingleResponse.Status = 400
				bulkSingleResponse.Shards = bulkmodel.BulkShardsResponse{
					Failed:     1,
					Successful: 0,
					Total:      1,
				}
				bulkSingleResponse.Error = queryparser.Error{
					RootCause: []queryparser.RootCause{
						{
							Type:   "quesma_error",
							Reason: err.Error(),
						},
					},
					Type:   "quesma_error",
					Reason: err.Error(),
				}
			}

			// Fill out the response pointer (a pointer to the results array we will return for a bulk)
			switch document.operation {
			case "create":
				document.response.Create = bulkSingleResponse

			case "index":
				document.response.Index = bulkSingleResponse

			default:
				logger.Error().Msgf("unsupported bulk operation type: %s. Document: %v", document.operation, document.document)
			}
		}
	}
}

func splitBulk(ctx context.Context, defaultIndex *string, bulk types.NDJSON, bulkSize int, processorConfig config.QuesmaProcessorConfig) ([]bulkmodel.BulkItem, map[string][]BulkRequestEntry, []byte, []BulkRequestEntry, error) {
	results := make([]bulkmodel.BulkItem, bulkSize)

	clickhouseDocumentsToInsert := make(map[string][]BulkRequestEntry, bulkSize)
	var elasticRequestBody []byte
	var elasticBulkEntries []BulkRequestEntry

	err := bulk.BulkForEach(func(entryNumber int, op types.BulkOperation, rawOp types.JSON, document types.JSON) error {
		index := op.GetIndex()
		operation := op.GetOperation()

		entryWithResponse := BulkRequestEntry{
			operation: operation,
			index:     index,
			document:  document,
			response:  &results[entryNumber],
		}

		if index == "" {
			if defaultIndex != nil {
				index = *defaultIndex
			} else {
				// Elastic also fails the entire bulk in such case
				logger.ErrorWithCtxAndReason(ctx, "no index name in _bulk").Msgf("no index name in _bulk")
				return fmt.Errorf("no index name in _bulk. Operation: %v, Document: %v", rawOp, document)
			}
		}

		// OLD WAY WAS TO ASK TableResolver
		//decision := tableResolver.Resolve(table_resolver.IngestPipeline, index)
		ingestTarget := findIngestTarget(index, processorConfig)

		// TODO: think if we need that
		//if decision.IsClosed || len(decision.UseConnectors) == 0 {
		//	bulkSingleResponse := BulkSingleResponse{
		//		Shards: BulkShardsResponse{
		//			Failed:     1,
		//			Successful: 0,
		//			Total:      1,
		//		},
		//		Status: 403,
		//		Type:   "_doc",
		//		Error: queryparser.Error{
		//			RootCause: []queryparser.RootCause{
		//				{
		//					Type:   "index_closed_exception",
		//					Reason: fmt.Sprintf("index %s is not routed to any connector", index),
		//				},
		//			},
		//			Type:   "index_closed_exception",
		//			Reason: fmt.Sprintf("index %s is not routed to any connector", index),
		//		},
		//	}
		//	switch operation {
		//	case "create":
		//		entryWithResponse.response.Create = bulkSingleResponse
		//
		//	case "index":
		//		entryWithResponse.response.Index = bulkSingleResponse
		//
		//	default:
		//		return fmt.Errorf("unsupported bulk operation type: %s. Document: %v", operation, document)
		//	}
		//}

		switch ingestTarget {

		case config.ElasticsearchTarget:
			// Bulk entry for Elastic - forward the request as-is
			opBytes, err := rawOp.Bytes()
			if err != nil {
				return err
			}
			elasticRequestBody = append(elasticRequestBody, opBytes...)
			elasticRequestBody = append(elasticRequestBody, '\n')

			documentBytes, err := document.Bytes()
			if err != nil {
				return err
			}
			elasticRequestBody = append(elasticRequestBody, documentBytes...)
			elasticRequestBody = append(elasticRequestBody, '\n')

			elasticBulkEntries = append(elasticBulkEntries, entryWithResponse)

		case config.ClickhouseTarget:
			// Bulk entry for Clickhouse
			if operation != "create" && operation != "index" {
				// Elastic also fails the entire bulk in such case
				logger.ErrorWithCtxAndReason(ctx, "unsupported bulk operation type").Msgf("unsupported bulk operation type: %s", operation)
				return fmt.Errorf("unsupported bulk operation type: %s. Operation: %v, Document: %v", operation, rawOp, document)
			}

			clickhouseDocumentsToInsert[index] = append(clickhouseDocumentsToInsert[index], entryWithResponse)

		default:
			return fmt.Errorf("why are we even here without a target")
		}

		return nil
	})

	return results, clickhouseDocumentsToInsert, elasticRequestBody, elasticBulkEntries, err
}

func findIngestTarget(index string, processorConfig config.QuesmaProcessorConfig) string {
	//unsafe, but config validation should have caught this
	defaultTarget := processorConfig.IndexConfig["*"].IngestTarget[0]
	_, found := processorConfig.IndexConfig[index]
	if !found {
		return defaultTarget
	} else { // per legacy syntax, if present means it's a clickhouse target
		return config.ClickhouseTarget
	}
}
