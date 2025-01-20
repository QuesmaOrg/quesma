// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package bulk

import (
	"context"
	"fmt"
	"github.com/QuesmaOrg/quesma/quesma/backend_connectors"
	"github.com/QuesmaOrg/quesma/quesma/clickhouse"
	"github.com/QuesmaOrg/quesma/quesma/end_user_errors"
	"github.com/QuesmaOrg/quesma/quesma/ingest"
	"github.com/QuesmaOrg/quesma/quesma/logger"
	"github.com/QuesmaOrg/quesma/quesma/queryparser"
	"github.com/QuesmaOrg/quesma/quesma/quesma/recovery"
	"github.com/QuesmaOrg/quesma/quesma/quesma/types"
	"github.com/QuesmaOrg/quesma/quesma/stats"
	"github.com/QuesmaOrg/quesma/quesma/table_resolver"
	"github.com/QuesmaOrg/quesma/quesma/v2/core"
	"github.com/QuesmaOrg/quesma/quesma/v2/core/diag"
	"github.com/goccy/go-json"
	"io"
	"net/http"
	"sort"
	"strings"
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
	ingestStatsEnabled bool, esBackendConn *backend_connectors.ElasticsearchBackendConnector, phoneHomeClient diag.PhoneHomeClient, tableResolver table_resolver.TableResolver) (results []BulkItem, err error) {
	defer recovery.LogPanic()

	maxBulkSize := len(bulk) /// 2 // we divided payload by 2 so that we don't take into account the `action_and_meta_data` line, ref: https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html
	maybeLogBatchSize(maxBulkSize)

	// The returned results should be in the same order as the input request, however splitting the bulk might change the order.
	// Therefore, each BulkRequestEntry has a corresponding pointer to the result entry, allowing us to freely split and reshuffle the bulk.
	results, clickhouseBulkEntries, elasticRequestBody, elasticBulkEntries, err := splitBulk(ctx, defaultIndex, bulk, maxBulkSize, tableResolver)
	if err != nil {
		return []BulkItem{}, err
	}

	// we fail if there are some documents to insert into Clickhouse but ingest processor is not available
	if len(clickhouseBulkEntries) > 0 && ip == nil {

		indexes := make(map[string]struct{})
		for index := range clickhouseBulkEntries {
			indexes[index] = struct{}{}
		}

		indexesAsList := make([]string, 0, len(indexes))
		for index := range indexes {
			indexesAsList = append(indexesAsList, index)
		}
		sort.Strings(indexesAsList)

		return []BulkItem{}, end_user_errors.ErrNoIngest.New(fmt.Errorf("ingest processor is not available, but documents are targeted to Clickhouse indexes: %s", strings.Join(indexesAsList, ",")))
	}

	err = sendToElastic(elasticRequestBody, esBackendConn, elasticBulkEntries)
	if err != nil {
		return []BulkItem{}, err
	}

	if ip != nil {
		sendToClickhouse(ctx, clickhouseBulkEntries, phoneHomeClient, ingestStatsEnabled, ip)
	}

	return results, nil
}

func splitBulk(ctx context.Context, defaultIndex *string, bulk types.NDJSON, maxBulkSize int, tableResolver table_resolver.TableResolver) ([]BulkItem, map[string][]BulkRequestEntry, []byte, []BulkRequestEntry, error) {
	results := make([]BulkItem, maxBulkSize)

	clickhouseBulkEntries := make(map[string][]BulkRequestEntry, maxBulkSize)
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

		decision := tableResolver.Resolve(quesma_api.IngestPipeline, index)

		if decision.Err != nil {
			return decision.Err
		}

		if decision.IsClosed || len(decision.UseConnectors) == 0 {
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

		for _, connector := range decision.UseConnectors {

			switch connector.(type) {

			case *quesma_api.ConnectorDecisionElastic:
				// Bulk entry for Elastic - forward the request as-is
				opBytes, err := rawOp.Bytes()
				if err != nil {
					return err
				}
				elasticRequestBody = append(elasticRequestBody, opBytes...)
				elasticRequestBody = append(elasticRequestBody, '\n')

				if operation != "delete" {
					documentBytes, err := document.Bytes()
					if err != nil {
						return err
					}
					elasticRequestBody = append(elasticRequestBody, documentBytes...)
					elasticRequestBody = append(elasticRequestBody, '\n')
				}
				elasticBulkEntries = append(elasticBulkEntries, entryWithResponse)

			case *quesma_api.ConnectorDecisionClickhouse:

				// Bulk entry for Clickhouse
				if operation != "create" && operation != "index" {
					// Elastic also fails the entire bulk in such case
					logger.ErrorWithCtxAndReason(ctx, "unsupported bulk operation type").Msgf("unsupported bulk operation type: %s", operation)
					return fmt.Errorf("unsupported bulk operation type: %s. Operation: %v, Document: %v", operation, rawOp, document)
				}

				clickhouseBulkEntries[index] = append(clickhouseBulkEntries[index], entryWithResponse)

			default:
				return fmt.Errorf("unsupported connector type: %T", connector)
			}

		}

		return nil
	})
	elasticRequestBody = append(elasticRequestBody, '\n')
	return results, clickhouseBulkEntries, elasticRequestBody, elasticBulkEntries, err
}

func sendToElastic(elasticRequestBody []byte, esBackendConn *backend_connectors.ElasticsearchBackendConnector, elasticBulkEntries []BulkRequestEntry) error {
	if len(elasticRequestBody) == 0 {
		// Fast path - no need to contact Elastic!
		return nil
	}

	response, err := esBackendConn.RequestWithHeaders(context.Background(), "POST", "/_bulk", elasticRequestBody, http.Header{"Content-Type": {"application/x-ndjson"}})
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

func sendToClickhouse(ctx context.Context, clickhouseBulkEntries map[string][]BulkRequestEntry, emptyPhoneHomeClient diag.PhoneHomeClient, ingestStatsEnabled bool, ip *ingest.IngestProcessor) {
	for indexName, documents := range clickhouseBulkEntries {
		emptyPhoneHomeClient.IngestCounters().Add(indexName, int64(len(documents)))

		for _, document := range documents {
			stats.GlobalStatistics.Process(ingestStatsEnabled, indexName, document.document, clickhouse.NestedSeparator)
		}

		inserts := make([]types.JSON, len(documents))
		for i, document := range documents {
			inserts[i] = document.document
		}

		err := ip.Ingest(ctx, indexName, inserts)

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
				logger.Error().Msgf("unsupported bulk operation type: %s. Document: %v", document.operation, document.document)
			}
		}
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
