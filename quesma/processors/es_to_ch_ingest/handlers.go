// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package es_to_ch_ingest

import (
	"context"
	"fmt"
	"quesma/ingest"
	"quesma/logger"
	"quesma/queryparser"
	bulkmodel "quesma/quesma/functionality/bulk"
	"quesma/quesma/recovery"
	"quesma/quesma/types"
)

// handleDocIndex assembles the payload into bulk format to reusing existing logic of bulk ingest
func handleDocIndex(payload types.JSON, targetTableName string, temporaryIngestProcessor *ingest.IngestProcessor2) (bulkmodel.BulkItem, error) {
	newPayload := []types.JSON{
		map[string]interface{}{"index": map[string]interface{}{"_index": targetTableName}},
		payload,
	}

	if results, err := Write(context.Background(), &targetTableName, newPayload, temporaryIngestProcessor); err != nil {
		return bulkmodel.BulkItem{}, err
	} else {
		return results[0], nil
	}
}

func handleBulkIndex(payload types.NDJSON, targetTableName string, temporaryIngestProcessor *ingest.IngestProcessor2) ([]bulkmodel.BulkItem, error) {
	results, err := Write(context.Background(), &targetTableName, payload, temporaryIngestProcessor)
	if err != nil {
		fmt.Printf("failed writing: %v", err)
		return []bulkmodel.BulkItem{}, err
	}
	return results, nil
}

func Write(ctx context.Context, defaultIndex *string, bulk types.NDJSON, ip *ingest.IngestProcessor2) (results []bulkmodel.BulkItem, err error) {
	defer recovery.LogPanic()

	bulkSize := len(bulk) / 2 // we divided payload by 2 so that we don't take into account the `action_and_meta_data` line, ref: https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html

	// The returned results should be in the same order as the input request, however splitting the bulk might change the order.
	// Therefore, each BulkRequestEntry has a corresponding pointer to the result entry, allowing us to freely split and reshuffle the bulk.
	results, clickhouseDocumentsToInsert, _, _, err := splitBulk(ctx, defaultIndex, bulk, bulkSize)
	if err != nil {
		return []bulkmodel.BulkItem{}, err
	}

	// we fail if there are some documents to insert into Clickhouse but ingest processor is not available
	//if len(clickhouseDocumentsToInsert) > 0 && ip == nil {
	//
	//	indexes := make(map[string]struct{})
	//	for index := range clickhouseDocumentsToInsert {
	//		indexes[index] = struct{}{}
	//	}
	//
	//	indexesAsList := make([]string, 0, len(indexes))
	//	for index := range indexes {
	//		indexesAsList = append(indexesAsList, index)
	//	}
	//	sort.Strings(indexesAsList)
	//
	//	return []BulkItem{}, end_user_errors.ErrNoIngest.New(fmt.Errorf("ingest processor is not available, but documents are targeted to Clickhouse indexes: %s", strings.Join(indexesAsList, ",")))
	//}

	// No place for that here
	//err = sendToElastic(elasticRequestBody, cfg, elasticBulkEntries)
	//if err != nil {
	//	return []BulkItem{}, err
	//}

	//if ip != nil {
	fmt.Printf("woudl send to clickhouse: [%v]", clickhouseDocumentsToInsert)
	sendToClickhouse(ctx, clickhouseDocumentsToInsert, ip)
	//}

	return results, nil
}

func sendToClickhouse(ctx context.Context, clickhouseDocumentsToInsert map[string][]BulkRequestEntry, ip *ingest.IngestProcessor2) {
	for indexName, documents := range clickhouseDocumentsToInsert {
		//phoneHomeAgent.IngestCounters().Add(indexName, int64(len(documents)))

		//for _, document := range documents {
		//	stats.GlobalStatistics.Process(cfg, indexName, document.document, clickhouse.NestedSeparator)
		//}
		// if the index is mapped to specified database table in the configuration, use that table
		// TODO: Index name override seems to be the only point to keep configuration in here
		//if len(cfg.IndexConfig[indexName].Override) > 0 {
		//	indexName = cfg.IndexConfig[indexName].Override
		//}

		inserts := make([]types.JSON, len(documents))
		for i, document := range documents {
			inserts[i] = document.document
		}

		err := ip.Ingest(ctx, indexName, inserts)

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

func splitBulk(ctx context.Context, defaultIndex *string, bulk types.NDJSON, bulkSize int) ([]bulkmodel.BulkItem, map[string][]BulkRequestEntry, []byte, []BulkRequestEntry, error) {
	results := make([]bulkmodel.BulkItem, bulkSize)

	clickhouseDocumentsToInsert := make(map[string][]BulkRequestEntry, bulkSize)
	//var elasticRequestBody []byte
	//var elasticBulkEntries []BulkRequestEntry

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

		//decision := tableResolver.Resolve(table_resolver.IngestPipeline, index)
		//
		//if decision.Err != nil {
		//	return decision.Err
		//}

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

		//for _, connector := range decision.UseConnectors {

		//switch connector.(type) {

		//case *table_resolver.ConnectorDecisionElastic:
		//	// Bulk entry for Elastic - forward the request as-is
		//	opBytes, err := rawOp.Bytes()
		//	if err != nil {
		//		return err
		//	}
		//	elasticRequestBody = append(elasticRequestBody, opBytes...)
		//	elasticRequestBody = append(elasticRequestBody, '\n')
		//
		//	documentBytes, err := document.Bytes()
		//	if err != nil {
		//		return err
		//	}
		//	elasticRequestBody = append(elasticRequestBody, documentBytes...)
		//	elasticRequestBody = append(elasticRequestBody, '\n')
		//
		//	elasticBulkEntries = append(elasticBulkEntries, entryWithResponse)

		//case *table_resolver.ConnectorDecisionClickhouse:

		// Bulk entry for Clickhouse
		if operation != "create" && operation != "index" {
			// Elastic also fails the entire bulk in such case
			logger.ErrorWithCtxAndReason(ctx, "unsupported bulk operation type").Msgf("unsupported bulk operation type: %s", operation)
			return fmt.Errorf("unsupported bulk operation type: %s. Operation: %v, Document: %v", operation, rawOp, document)
		}

		clickhouseDocumentsToInsert[index] = append(clickhouseDocumentsToInsert[index], entryWithResponse)

		//default:
		//	return fmt.Errorf("unsupported connector type: %T", connector)
		//}

		//}

		return nil
	})

	return results, clickhouseDocumentsToInsert, nil, nil, err
}
