// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package ast

import (
	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"github.com/goccy/go-json"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

const queryContent = `{
  "query": {
    "bool": {
      "must": [
        { "term": { "field1": "value1" }},
        {
          "bool": {
            "should": [
              { "term": { "field2": "value2" }},
              { "term": { "field3": "value3" }}
            ]
          }
        }
      ]
    }
  }
}`

type QueryCountingVisitor struct {
	boolQueriesCounter int
	termQueriesCounter int
	QueryVisitor
}

func (v *QueryCountingVisitor) PreVisitBool(b *types.BoolQuery, path []string) {
	v.boolQueriesCounter++
}
func (v *QueryCountingVisitor) PreVisitTerm(t *types.TermQuery, field string, path []string) {
	v.termQueriesCounter++
}

func Test_queryTraversal(t *testing.T) {
	body := []byte(queryContent)
	query := &types.Query{}
	err := json.Unmarshal(body, query)
	require.NoError(t, err)
	path := []string{}
	visitor := &QueryCountingVisitor{QueryVisitor: QueryBasicVisitor{}}
	queryTraverser := QueryTraverser{Debug: true, PathMatched: false}
	queryTraverser.TraverseQuery(query, visitor, path)
	assert.Equal(t, 2, visitor.boolQueriesCounter)
	assert.Equal(t, 3, visitor.termQueriesCounter)
}
