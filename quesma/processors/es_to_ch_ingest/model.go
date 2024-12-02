// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package es_to_ch_ingest

import "quesma/quesma/types"

type (
	BulkRequestEntry struct {
		operation string
		index     string
		document  types.JSON
		response  *BulkItem
	}

	BulkResponse struct {
		// Model of the response from Elastic's _bulk API
		// https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html#bulk-api-response-body
		Errors bool       `json:"errors"`
		Items  []BulkItem `json:"items"`
		Took   int        `json:"took"`
	}
	BulkItem struct {
		// Create, Index, Update, Delete are of BulkSingleResponse type,
		// but declaring them as 'any' to preserve any excess fields Elastic might send
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
