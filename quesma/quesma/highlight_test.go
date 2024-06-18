package quesma

import (
	"context"
	"encoding/json"
	"github.com/stretchr/testify/assert"
	"mitmproxy/quesma/clickhouse"
	"mitmproxy/quesma/model"
	"mitmproxy/quesma/queryparser"
	"mitmproxy/quesma/quesma/config"
	"mitmproxy/quesma/telemetry"
	"testing"
)

func TestParseHighLight(t *testing.T) {

	query := `
{
  "_source": false,
  "fields": [
    {
      "field": "*",
      "include_unmapped": "true"
    },
    {
      "field": "@timestamp",
      "format": "strict_date_optional_time"
    }
  ],
  "highlight": {
    "fields": {
      "*": {}
    },
    "fragment_size": 2147483647,
    "post_tags": [
      "@/kibana-highlighted-field@"
    ],
    "pre_tags": [
      "@kibana-highlighted-field@"
    ]
  },
  "query": {
    "bool": {
      "filter": [
        {
          "multi_match": {
            "lenient": true,
            "query": "User deleted",
            "type": "best_fields"
          }
        },
        {
          "range": {
            "@timestamp": {
              "format": "strict_date_optional_time",
              "gte": "2024-03-11T10:24:54.962Z",
              "lte": "2024-03-11T10:39:54.962Z"
            }
          }
        }
      ],
      "must": [],
      "must_not": [],
      "should": []
    }
  },
  "runtime_mappings": {},
  "script_fields": {},
  "size": 500,
  "sort": [
    {
      "@timestamp": {
        "format": "strict_date_optional_time",
        "order": "desc",
        "unmapped_type": "boolean"
      }
    },
    {
      "_doc": {
        "order": "desc",
        "unmapped_type": "boolean"
      }
    }
  ],
  "stored_fields": [
    "*"
  ],
  "track_total_hits": false,
  "version": true
}

`
	col := clickhouse.Column{
		Name:            "message",
		Type:            clickhouse.NewBaseType("String"),
		IsFullTextMatch: true,
	}

	cols := make(map[string]*clickhouse.Column)
	cols["message"] = &col

	table := clickhouse.Table{
		Name:   "test",
		Cols:   cols,
		Config: clickhouse.NewDefaultCHConfig(),
	}

	lm := clickhouse.NewEmptyLogManager(config.QuesmaConfiguration{}, nil, telemetry.NewPhoneHomeEmptyAgent(), nil)

	cw := queryparser.ClickhouseQueryTranslator{
		ClickhouseLM: lm,
		Table:        &table,
		Ctx:          context.Background(),
	}

	queryAsMap := make(queryparser.QueryMap)
	err := json.Unmarshal([]byte(query), &queryAsMap)

	assert.Nil(t, err, "Error parsing query %v", err)

	highlighter := cw.ParseHighlighter(queryAsMap)
	highlighter.Tokens = map[string]struct{}{
		"user deleted": {}, "user": {}, "deleted": {},
	}
	assert.NotNil(t, highlighter, "Error parsing highlight %v", highlighter)

	assert.Equal(t, 1, len(highlighter.PreTags))
	assert.Equal(t, "@kibana-highlighted-field@", highlighter.PreTags[0])
	assert.Equal(t, 1, len(highlighter.PostTags))
	assert.Equal(t, "@/kibana-highlighted-field@", highlighter.PostTags[0])
}

func TestHighLightResults(t *testing.T) {

	highLighter := model.Highlighter{
		Tokens: map[string]struct{}{
			"user": {}, "deleted": {},
		},
		PreTags:  []string{"<b>"},
		PostTags: []string{"</b>"},
		Fields:   make(map[string]bool),
	}
	highLighter.Fields["message"] = true

	tests := []struct {
		name       string
		tokens     map[string]struct{}
		field      string
		highlight  bool
		value      string
		highlights []string
	}{
		{
			name:       "highlighted",
			tokens:     map[string]struct{}{"user": {}, "deleted": {}},
			field:      "message",
			highlight:  true,
			value:      "User logged",
			highlights: []string{"<b>User</b>"},
		},
		{
			name:       "highlighted original case",
			tokens:     map[string]struct{}{"user": {}, "deleted": {}},
			field:      "message",
			highlight:  true,
			value:      "uSeR logged",
			highlights: []string{"<b>uSeR</b>"},
		},
		{
			name:       "highlighted both",
			tokens:     map[string]struct{}{"user": {}, "deleted": {}},
			field:      "message",
			highlight:  true,
			value:      "User  deleted",
			highlights: []string{"<b>User</b>", "<b>deleted</b>"},
		},
		{
			name:       "not highlighted",
			tokens:     map[string]struct{}{"user": {}, "deleted": {}},
			field:      "other_field",
			highlight:  false,
			value:      "User logged",
			highlights: nil,
		},
		{
			name:       "multiple highlights",
			tokens:     map[string]struct{}{"password": {}},
			field:      "message",
			highlight:  true,
			value:      "InvalidPassword: user provided invalid password",
			highlights: []string{"<b>Password</b>", "<b>password</b>"},
		},
		{
			name:       "multiple highlights security team #1",
			tokens:     map[string]struct{}{"invalidpassword": {}, "password": {}},
			field:      "message",
			highlight:  true,
			value:      "InvalidPassword: user provided invalid password",
			highlights: []string{"<b>InvalidPassword</b>", "<b>password</b>"},
		},
		{
			name:       "multiple highlights security team #2",
			tokens:     map[string]struct{}{"password": {}, "invalidpassword": {}},
			field:      "message",
			highlight:  true,
			value:      "InvalidPassword: user provided invalid password",
			highlights: []string{"<b>InvalidPassword</b>", "<b>password</b>"},
		},
		{
			name:       "merge highlights",
			tokens:     map[string]struct{}{"password": {}, "lidpass": {}},
			field:      "message",
			highlight:  true,
			value:      "InvalidPassword: user provided invalid password",
			highlights: []string{"<b>lidPassword</b>", "<b>password</b>"},
		},
		{
			name:       "merge highlights",
			tokens:     map[string]struct{}{"password": {}, "pass": {}},
			field:      "message",
			highlight:  true,
			value:      "InvalidPassword",
			highlights: []string{"<b>Password</b>"},
		},
		{
			name:       "no highlights",
			tokens:     map[string]struct{}{},
			field:      "message",
			highlight:  true,
			value:      "InvalidPassword",
			highlights: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			highLighter := model.Highlighter{
				Tokens:   tt.tokens,
				PreTags:  []string{"<b>"},
				PostTags: []string{"</b>"},
				Fields:   make(map[string]bool),
			}
			highLighter.Fields["message"] = true

			mustHighlighter := highLighter.ShouldHighlight(tt.field)

			assert.Equal(t, mustHighlighter, tt.highlight, "Field %s should be highlightable", tt.field)

			if mustHighlighter {
				highlights := highLighter.HighlightValue(tt.value)
				assert.Equal(t, tt.highlights, highlights)
			}
		})
	}

}
