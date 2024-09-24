// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package bulk

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"quesma/clickhouse"
	"quesma/ingest"
	"quesma/jsonprocessor"
	"quesma/logger"
	"quesma/queryparser"
	"quesma/quesma/config"
	"quesma/quesma/recovery"
	"quesma/quesma/types"
	"quesma/stats"
	"quesma/telemetry"
	"sync"
)

type (
	BulkRequestEntry struct {
		operation string
		index     string
		document  types.JSON
		response  *BulkItem
	}

	// Model of the response from Elastic's _bulk API
	// https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html#bulk-api-response-body
	BulkResponse struct {
		Errors bool       `json:"errors"`
		Items  []BulkItem `json:"items"`
		Took   int        `json:"took"`
	}
	// Create, Index, Update, Delete are of BulkSingleResponse type,
	// but declaring them as 'any' to preserve any excess fields Elastic might send
	BulkItem struct {
		Create any `json:"create,omitempty"`
		Index  any `json:"index,omitempty"`
		Update any `json:"update,omitempty"`
		Delete any `json:"delete,omitempty"`
	}
	BulkSingleResponse struct {
		ID          string             `json:"_id"`
		Index       string             `json:"_index"`
		PrimaryTerm int                `json:"_primary_term"`
		SeqNo       int                `json:"_seq_no"`
		Shards      BulkShardsResponse `json:"_shards"`
		Version     int                `json:"_version"`
		Result      string             `json:"result,omitempty"`
		Status      int                `json:"status"`
		Error       any                `json:"error,omitempty"`
		Type        string             `json:"_type"` // ES 7.x Java Client requires this field
	}
	BulkShardsResponse struct {
		Failed     int `json:"failed"`
		Successful int `json:"successful"`
		Total      int `json:"total"`
	}
)

func Write(ctx context.Context, defaultIndex *string, bulk types.NDJSON, ip *ingest.IngestProcessor,
	cfg *config.QuesmaConfiguration, phoneHomeAgent telemetry.PhoneHomeAgent) (results []BulkItem, err error) {
	defer recovery.LogPanic()

	bulkSize := len(bulk) / 2 // we divided payload by 2 so that we don't take into account the `action_and_meta_data` line, ref: https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html
	maybeLogBatchSize(bulkSize)

	// The returned results should be in the same order as the input request, however splitting the bulk might change the order.
	// Therefore, each BulkRequestEntry has a corresponding pointer to the result entry, allowing us to freely split and reshuffle the bulk.
	results, clickhouseDocumentsToInsert, elasticRequestBody, elasticBulkEntries, err := splitBulk(ctx, defaultIndex, bulk, bulkSize, cfg)
	if err != nil {
		return []BulkItem{}, err
	}

	err = sendToElastic(elasticRequestBody, cfg, elasticBulkEntries)
	if err != nil {
		return []BulkItem{}, err
	}

	sendToClickhouse(ctx, clickhouseDocumentsToInsert, phoneHomeAgent, cfg, ip)

	return results, nil
}

func splitBulk(ctx context.Context, defaultIndex *string, bulk types.NDJSON, bulkSize int, cfg *config.QuesmaConfiguration) ([]BulkItem, map[string][]BulkRequestEntry, []byte, []BulkRequestEntry, error) {
	results := make([]BulkItem, bulkSize)

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

		indexConfig, found := cfg.IndexConfig[index]
		if !found || indexConfig.IsElasticIngestEnabled() {
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
		}
		if found && indexConfig.IsClickhouseIngestEnabled() {
			// Bulk entry for Clickhouse
			if operation != "create" && operation != "index" {
				// Elastic also fails the entire bulk in such case
				logger.ErrorWithCtxAndReason(ctx, "unsupported bulk operation type").Msgf("unsupported bulk operation type: %s", operation)
				return fmt.Errorf("unsupported bulk operation type: %s. Operation: %v, Document: %v", operation, rawOp, document)
			}

			clickhouseDocumentsToInsert[index] = append(clickhouseDocumentsToInsert[index], entryWithResponse)
		}
		if indexConfig.IsIngestDisabled() {
			bulkSingleResponse := BulkSingleResponse{
				Shards: BulkShardsResponse{
					Failed:     1,
					Successful: 0,
					Total:      1,
				},
				Status: 403,
				Type:   "_doc",
				Error: queryparser.Error{
					RootCause: []queryparser.RootCause{
						{
							Type:   "index_closed_exception",
							Reason: fmt.Sprintf("index %s is not routed to any connector", index),
						},
					},
					Type:   "index_closed_exception",
					Reason: fmt.Sprintf("index %s is not routed to any connector", index),
				},
			}
			switch operation {
			case "create":
				entryWithResponse.response.Create = bulkSingleResponse

			case "index":
				entryWithResponse.response.Index = bulkSingleResponse

			default:
				return fmt.Errorf("unsupported bulk operation type: %s. Document: %v", operation, document)
			}
		}
		return nil
	})

	return results, clickhouseDocumentsToInsert, elasticRequestBody, elasticBulkEntries, err
}

func sendToElastic(elasticRequestBody []byte, cfg *config.QuesmaConfiguration, elasticBulkEntries []BulkRequestEntry) error {
	if len(elasticRequestBody) == 0 {
		// Fast path - no need to contact Elastic!
		return nil
	}

	req, _ := http.NewRequest("POST", cfg.Elasticsearch.Url.String()+"/_bulk", bytes.NewBuffer(elasticRequestBody))
	req.Header.Set("Content-Type", "application/x-ndjson")
	client := http.Client{} // FIXME
	response, err := client.Do(req)
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

	var elasticBulkResponse BulkResponse
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

func sendToClickhouse(ctx context.Context, clickhouseDocumentsToInsert map[string][]BulkRequestEntry, phoneHomeAgent telemetry.PhoneHomeAgent, cfg *config.QuesmaConfiguration, ip *ingest.IngestProcessor) {
	for indexName, documents := range clickhouseDocumentsToInsert {
		phoneHomeAgent.IngestCounters().Add(indexName, int64(len(documents)))

		config.RunConfiguredIngest(ctx, cfg, indexName, make(types.JSON), func() error {
			for _, document := range documents {
				stats.GlobalStatistics.Process(cfg, indexName, document.document, clickhouse.NestedSeparator)
			}
			// if the index is mapped to specified database table in the configuration, use that table
			if len(cfg.IndexConfig[indexName].Override) > 0 {
				indexName = cfg.IndexConfig[indexName].Override
			}

			inserts := make([]types.JSON, len(documents))
			for i, document := range documents {
				inserts[i] = document.document
			}

			nameFormatter := clickhouse.DefaultColumnNameFormatter()
			transformer := jsonprocessor.IngestTransformerFor(indexName, cfg)
			err := ip.ProcessInsertQuery(ctx, indexName, inserts, transformer, nameFormatter)

			for _, document := range documents {
				bulkSingleResponse := BulkSingleResponse{
					ID:          "fakeId",
					Index:       document.index,
					PrimaryTerm: 1,
					SeqNo:       0,
					Shards: BulkShardsResponse{
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
					bulkSingleResponse.Shards = BulkShardsResponse{
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
					return fmt.Errorf("unsupported bulk operation type: %s. Document: %v", document.operation, document.document)
				}
			}
			return nil
		})
	}
}

// Global set to keep track of logged batch sizes
var loggedBatchSizes = make(map[int]struct{})
var mutex sync.Mutex

// maybeLogBatchSize logs only unique batch sizes
func maybeLogBatchSize(batchSize int) {
	mutex.Lock()
	defer mutex.Unlock()
	if _, alreadyLogged := loggedBatchSizes[batchSize]; !alreadyLogged {
		logger.Info().Msgf("Ingesting via _bulk API, batch size=%d documents", batchSize)
		loggedBatchSizes[batchSize] = struct{}{}
	}
}
