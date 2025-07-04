// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package elastic_query_dsl

import (
	"context"
	"github.com/QuesmaOrg/quesma/platform/database_common"
	"github.com/QuesmaOrg/quesma/platform/model"
	"github.com/QuesmaOrg/quesma/platform/util"
	"github.com/goccy/go-json"
	"github.com/stretchr/testify/assert"
	"testing"
)

const columnName = "message"

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
	col := database_common.Column{
		Name: columnName,
		Type: database_common.NewBaseType("String"),
	}

	cols := make(map[string]*database_common.Column)
	cols[columnName] = &col

	table := database_common.Table{
		Name:   "test",
		Cols:   cols,
		Config: database_common.NewDefaultCHConfig(),
	}

	cw := ClickhouseQueryTranslator{
		Table: &table,
		Ctx:   context.Background(),
	}

	queryAsMap := make(QueryMap)
	err := json.Unmarshal([]byte(query), &queryAsMap)

	assert.Nil(t, err, "Error parsing query %v", err)

	highlighter := cw.ParseHighlighter(queryAsMap)
	highlighter.Tokens = map[string]model.Tokens{
		columnName: map[string]struct{}{
			"user deleted": {}, "user": {}, "deleted": {},
		},
	}
	assert.NotNil(t, highlighter, "Error parsing highlight %v", highlighter)

	assert.Equal(t, 1, len(highlighter.PreTags))
	assert.Equal(t, "@kibana-highlighted-field@", highlighter.PreTags[0])
	assert.Equal(t, 1, len(highlighter.PostTags))
	assert.Equal(t, "@/kibana-highlighted-field@", highlighter.PostTags[0])
}

func TestHighLightResults(t *testing.T) {

	tests := []struct {
		name       string
		tokens     map[string]model.Tokens
		highlight  bool
		value      string
		highlights []string
	}{
		{
			name: "highlighted",
			tokens: map[string]model.Tokens{
				columnName: map[string]struct{}{
					"user": {}, "deleted": {},
				},
			},
			highlight:  true,
			value:      "User logged",
			highlights: []string{"<b>User</b>"},
		},
		{
			name: "highlighted original case",
			tokens: map[string]model.Tokens{
				columnName: map[string]struct{}{
					"user": {}, "deleted": {},
				},
			},
			highlight:  true,
			value:      "uSeR logged",
			highlights: []string{"<b>uSeR</b>"},
		},
		{
			name: "highlighted both",
			tokens: map[string]model.Tokens{
				columnName: map[string]struct{}{
					"user": {}, "deleted": {},
				},
			},
			highlight:  true,
			value:      "User  deleted",
			highlights: []string{"<b>User</b>", "<b>deleted</b>"},
		},
		{
			name: "not highlighted",
			tokens: map[string]model.Tokens{
				"other_field": map[string]struct{}{
					"user": {}, "deleted": {},
				},
			},
			highlight:  false,
			value:      "User logged",
			highlights: nil,
		},
		{
			name: "multiple highlights",
			tokens: map[string]model.Tokens{
				columnName: map[string]struct{}{
					"password": {},
				},
			},

			highlight:  true,
			value:      "InvalidPassword: user provided invalid password",
			highlights: []string{"<b>Password</b>", "<b>password</b>"},
		},
		{
			name: "multiple highlights security team #1",
			tokens: map[string]model.Tokens{
				columnName: map[string]struct{}{
					"invalidpassword": {}, "password": {},
				},
			},
			highlight:  true,
			value:      "InvalidPassword: user provided invalid password",
			highlights: []string{"<b>InvalidPassword</b>", "<b>password</b>"},
		},
		{
			name: "multiple highlights security team #2",
			tokens: map[string]model.Tokens{
				columnName: map[string]struct{}{
					"password": {}, "invalidpassword": {},
				},
			},
			highlight:  true,
			value:      "InvalidPassword: user provided invalid password",
			highlights: []string{"<b>InvalidPassword</b>", "<b>password</b>"},
		},
		{
			name: "merge highlights",
			tokens: map[string]model.Tokens{
				columnName: map[string]struct{}{
					"password": {}, "lidpass": {},
				},
			},
			highlight:  true,
			value:      "InvalidPassword: user provided invalid password",
			highlights: []string{"<b>lidPassword</b>", "<b>password</b>"},
		},
		{
			name: "merge highlights",
			tokens: map[string]model.Tokens{
				columnName: map[string]struct{}{
					"password": {}, "pass": {},
				},
			},
			highlight:  true,
			value:      "InvalidPassword",
			highlights: []string{"<b>Password</b>"},
		},
		{
			name: "no highlights",
			tokens: map[string]model.Tokens{
				columnName: map[string]struct{}{},
			},
			highlight:  true,
			value:      "InvalidPassword",
			highlights: []string{},
		},
	}

	for i, tt := range tests {
		t.Run(util.PrettyTestName(tt.name, i), func(t *testing.T) {

			highLighter := model.Highlighter{
				Tokens:   tt.tokens,
				PreTags:  []string{"<b>"},
				PostTags: []string{"</b>"},
			}

			mustHighlighter := highLighter.ShouldHighlight(columnName)

			assert.Equal(t, mustHighlighter, tt.highlight, "Field %s should be highlightable", columnName)

			if mustHighlighter {
				highlights := highLighter.HighlightValue(columnName, tt.value)
				assert.Equal(t, tt.highlights, highlights)
			}
		})
	}

}
