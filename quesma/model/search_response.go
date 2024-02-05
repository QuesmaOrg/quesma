package model

import "encoding/json"

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
	Index  string          `json:"_index"`
	ID     string          `json:"_id"`
	Score  float32         `json:"_score"`
	Source json.RawMessage `json:"_source"`
	Type   string          `json:"_type,omitempty"` // Deprecated field
	Sort   []any           `json:"sort,omitempty"`
}

type AsyncSearchHit struct {
	Index  string                   `json:"_index"`
	ID     string                   `json:"_id"`
	Score  float32                  `json:"_score"`
	Fields map[string][]interface{} `json:"fields,omitempty"`

	// fields below only in ListAllFields request type
	Version   int                 `json:"_version,omitempty"`
	Highlight map[string][]string `json:"highlight,omitempty"`
	Sort      []any               `json:"sort,omitempty"`
}

type SearchHits struct {
	Total    Total       `json:"total"`
	MaxScore float32     `json:"max_score"`
	Hits     []SearchHit `json:"hits"`
}

type AsyncSearchHits struct {
	Total    *Total           `json:"total,omitempty"` // doesn't work without pointer
	MaxScore float32          `json:"max_score"`
	Hits     []AsyncSearchHit `json:"hits"`
}

type Total struct {
	Value    int    `json:"value"`
	Relation string `json:"relation"`
}

type Aggregations = map[string]JsonMap

type SearchResp struct {
	Took              int            `json:"took"`
	Timeout           bool           `json:"timed_out"`
	DidTerminateEarly *bool          `json:"terminated_early,omitempty"` // needs to be *bool https://stackoverflow.com/questions/37756236/json-golang-boolean-omitempty
	Shards            ResponseShards `json:"_shards"`
	Hits              SearchHits     `json:"hits"`
	Aggregations      Aggregations   `json:"aggregations,omitempty"`
	ScrollID          *string        `json:"_scroll_id,omitempty"`
}

type AsyncSearchResp struct {
	Took         int             `json:"took"`
	Timeout      bool            `json:"timed_out"`
	Shards       ResponseShards  `json:"_shards"`
	Hits         AsyncSearchHits `json:"hits"`
	Aggregations Aggregations    `json:"aggregations,omitempty"`
}

type AsyncSearchEntireResp struct {
	StartTimeInMillis      uint64          `json:"start_time_in_millis"`
	CompletionTimeInMillis uint64          `json:"completion_time_in_millis"`
	ExpirationTimeInMillis uint64          `json:"expiration_time_in_millis"`
	ID                     *string         `json:"id,omitempty"`
	IsRunning              bool            `json:"is_running"`
	IsPartial              bool            `json:"is_partial"`
	Response               AsyncSearchResp `json:"response"`
}
