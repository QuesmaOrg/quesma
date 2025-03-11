// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package model

import "github.com/goccy/go-json"

type JsonMap = map[string]interface{}

type Reason struct {
	Type   string `json:"type"`
	Reason string `json:"reason"`
}

type ResponseShardsFailure struct {
	Shard  int    `json:"shard"`
	Index  any    `json:"index"`
	Reason Reason `json:"reason"`
}

type ResponseShards struct {
	Total      int `json:"total"`
	Successful int `json:"successful"`
	Failed     int `json:"failed"`
	Skipped    int `json:"skipped"`
}

type SearchHit struct {
	Index  string                   `json:"_index"`
	ID     string                   `json:"_id"`
	Score  float32                  `json:"_score"`
	Source json.RawMessage          `json:"_source,omitempty"`
	Fields map[string][]interface{} `json:"fields,omitempty"`

	// fields below only in ListAllFields request type
	Version   int                 `json:"_version,omitempty"`
	Highlight map[string][]string `json:"highlight,omitempty"`

	Type string `json:"_type,omitempty"` // Deprecated field
	Sort []any  `json:"sort,omitempty"`
}

func NewSearchHit(index string) *SearchHit {
	return &SearchHit{Index: index, Fields: make(map[string][]interface{}), Highlight: make(map[string][]string)}
}

type SearchHits struct {
	Total    *Total      `json:"total,omitempty"`
	MaxScore *float32    `json:"max_score"`
	Hits     []SearchHit `json:"hits"`
	Events   []SearchHit `json:"events,omitempty"` // this one is used by EQL
}

type Total struct {
	Value    int    `json:"value"`
	Relation string `json:"relation"`
}

type SearchResp struct {
	Took              int            `json:"took"`
	Timeout           bool           `json:"timed_out"`
	DidTerminateEarly *bool          `json:"terminated_early,omitempty"` // needs to be *bool https://stackoverflow.com/questions/37756236/json-golang-boolean-omitempty
	Shards            ResponseShards `json:"_shards"`
	Hits              SearchHits     `json:"hits"`
	Aggregations      JsonMap        `json:"aggregations,omitempty"`
	ScrollID          *string        `json:"_scroll_id,omitempty"`
}

func (response *SearchResp) Marshal() ([]byte, error) {
	return json.Marshal(response)
}

type AsyncSearchEntireResp struct {
	StartTimeInMillis      uint64  `json:"start_time_in_millis"`
	CompletionTimeInMillis uint64  `json:"completion_time_in_millis"`
	ExpirationTimeInMillis uint64  `json:"expiration_time_in_millis"`
	ID                     *string `json:"id,omitempty"`
	IsRunning              bool    `json:"is_running"`
	IsPartial              bool    `json:"is_partial"`
	// CompletionStatus If the async search completed, this field shows the status code of the
	// search.
	// For example, 200 indicates that the async search was successfully completed.
	// 503 indicates that the async search was completed with an error.
	CompletionStatus *int       `json:"completion_status,omitempty"`
	Response         SearchResp `json:"response,omitempty"`
}

func (response *AsyncSearchEntireResp) Marshal() ([]byte, error) {
	return json.Marshal(response)
}
