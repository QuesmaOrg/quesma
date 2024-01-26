package model

import "encoding/json"

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
	Total      int                     `json:"total"`
	Successful int                     `json:"successful"`
	Failed     int                     `json:"failed"`
	Failures   []ResponseShardsFailure `json:"failures"`
	Skipped    int                     `json:"skipped"`
}

type SearchHit struct {
	Index  string          `json:"_index"`
	ID     string          `json:"_id"`
	Score  float32         `json:"_score"`
	Source json.RawMessage `json:"_source"`
	Type   string          `json:"_type"` // Deprecated field
	Sort   []any           `json:"sort"`
}

type Hits struct {
	Total    Total       `json:"total"`
	MaxScore float32     `json:"max_score"`
	Hits     []SearchHit `json:"hits"`
}

type Total struct {
	Value    int    `json:"value"`
	Relation string `json:"relation"`
}

type SearchResp struct {
	Took         int             `json:"took"`
	Timeout      bool            `json:"timed_out"`
	Shards       ResponseShards  `json:"_shards"`
	Hits         Hits            `json:"hits"`
	Errors       bool            `json:"errors"`
	Aggregations json.RawMessage `json:"aggregations"`
	ScrollID     *string         `json:"_scroll_id,omitempty"`
}
