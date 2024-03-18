package quesma

import (
	"encoding/json"
	"github.com/stretchr/testify/assert"
	"mitmproxy/quesma/clickhouse"
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

	lm := clickhouse.NewEmptyLogManager(config.QuesmaConfiguration{}, nil, telemetry.NewPhoneHomeEmptyAgent())

	cw := queryparser.ClickhouseQueryTranslator{
		ClickhouseLM: lm,
		Table:        &table,
	}

	queryAsMap := make(queryparser.QueryMap)
	err := json.Unmarshal([]byte(query), &queryAsMap)

	assert.Nil(t, err, "Error parsing query %v", err)

	highlighter := cw.ParseHighlighter(queryAsMap)
	tokens := []string{"User deleted", "User", "deleted"}
	highlighter.SetTokens(tokens)
	assert.NotNil(t, highlighter, "Error parsing highlight %v", highlighter)

	assert.Equal(t, 1, len(highlighter.PreTags))
	assert.Equal(t, "@kibana-highlighted-field@", highlighter.PreTags[0])
	assert.Equal(t, 1, len(highlighter.PostTags))
	assert.Equal(t, "@/kibana-highlighted-field@", highlighter.PostTags[0])
	assert.Equal(t, []string{"user deleted", "deleted", "user"}, highlighter.Tokens)
}

func TestHighLightResults(t *testing.T) {

	highLighter := queryparser.Highlighter{
		Tokens:   []string{"user", "deleted"},
		PreTags:  []string{"<b>"},
		PostTags: []string{"</b>"},
		Fields:   make(map[string]bool),
	}
	highLighter.Fields["message"] = true

	tests := []struct {
		name       string
		tokens     []string
		field      string
		highlight  bool
		value      string
		highlights []string
	}{
		{
			name:       "highlighted",
			tokens:     []string{"user", "deleted"},
			field:      "message",
			highlight:  true,
			value:      "User logged",
			highlights: []string{"<b>User</b>"},
		},
		{
			name:       "highlighted original case",
			tokens:     []string{"user", "deleted"},
			field:      "message",
			highlight:  true,
			value:      "uSeR logged",
			highlights: []string{"<b>uSeR</b>"},
		},
		{
			name:       "highlighted both",
			tokens:     []string{"user", "deleted"},
			field:      "message",
			highlight:  true,
			value:      "User  deleted",
			highlights: []string{"<b>User</b>", "<b>deleted</b>"},
		},
		{
			name:       "not highlighted",
			tokens:     []string{"User", "deleted"},
			field:      "other_field",
			highlight:  false,
			value:      "User logged",
			highlights: nil,
		},
		{
			name:       "multiple highlights",
			tokens:     []string{"password"},
			field:      "message",
			highlight:  true,
			value:      "InvalidPassword: user provided invalid password",
			highlights: []string{"<b>Password</b>", "<b>password</b>"},
		},
		{
			name:       "multiple highlights security team #1",
			tokens:     []string{"invalidPassword", "password"},
			field:      "message",
			highlight:  true,
			value:      "InvalidPassword: user provided invalid password",
			highlights: []string{"<b>InvalidPassword</b>", "<b>password</b>"},
		},
		{
			name:       "multiple highlights security team #2",
			tokens:     []string{"password", "InvalidPassword"},
			field:      "message",
			highlight:  true,
			value:      "InvalidPassword: user provided invalid password",
			highlights: []string{"<b>InvalidPassword</b>", "<b>password</b>"},
		},
		{
			name:       "merge highlights",
			tokens:     []string{"password", "lidPass"},
			field:      "message",
			highlight:  true,
			value:      "InvalidPassword: user provided invalid password",
			highlights: []string{"<b>lidPassword</b>", "<b>password</b>"},
		},
		{
			name:       "merge highlights",
			tokens:     []string{"password", "pass"},
			field:      "message",
			highlight:  true,
			value:      "InvalidPassword",
			highlights: []string{"<b>Password</b>"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			highLighter := queryparser.Highlighter{
				PreTags:  []string{"<b>"},
				PostTags: []string{"</b>"},
				Fields:   make(map[string]bool),
			}
			highLighter.Fields["message"] = true

			highLighter.SetTokens(tt.tokens)

			mustHighlighter := highLighter.ShouldHighlight(tt.field)

			assert.Equal(t, mustHighlighter, tt.highlight, "Field %s should be highlightable", tt.field)

			if mustHighlighter {
				highlights := highLighter.HighlightValue(tt.value)
				assert.Equal(t, tt.highlights, highlights)
			}
		})
	}

}
