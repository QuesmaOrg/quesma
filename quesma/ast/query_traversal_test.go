package ast

import (
	"encoding/json"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
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
}

func (QueryCountingVisitor) PreVisitQuery(q *types.Query, path []string)  {}
func (QueryCountingVisitor) PostVisitQuery(q *types.Query, path []string) {}
func (v *QueryCountingVisitor) PreVisitBool(b *types.BoolQuery, path []string) {
	v.boolQueriesCounter++
}
func (QueryCountingVisitor) PostVisitBool(b *types.BoolQuery, path []string)              {}
func (QueryCountingVisitor) PreVisitBoostingQuery(b *types.BoostingQuery, path []string)  {}
func (QueryCountingVisitor) PostVisitBoostingQuery(b *types.BoostingQuery, path []string) {}
func (QueryCountingVisitor) PreVisitTypeQuery(t *types.TypeQuery, path []string)          {}
func (QueryCountingVisitor) PostVisitTypeQuery(t *types.TypeQuery, path []string)         {}
func (QueryCountingVisitor) PreVisitCommonTermsQuery(ctq *types.CommonTermsQuery, field string, path []string) {
}
func (QueryCountingVisitor) PostVisitCommonTermsQuery(ctq *types.CommonTermsQuery, field string, path []string) {
}
func (QueryCountingVisitor) PreVisitCombinedFieldsQuery(ctq *types.CombinedFieldsQuery, path []string) {
}
func (QueryCountingVisitor) PostVisitCombinedFieldsQuery(ctq *types.CombinedFieldsQuery, path []string) {
}
func (v *QueryCountingVisitor) PreVisitTerm(t *types.TermQuery, field string, path []string) {
	v.termQueriesCounter++
}
func (QueryCountingVisitor) PostVisitTerm(t *types.TermQuery, field string, path []string) {}
func (QueryCountingVisitor) PreVisitConstantScoreQuery(csq *types.ConstantScoreQuery, path []string) {
}
func (QueryCountingVisitor) PostVisitConstantScoreQuery(csq *types.ConstantScoreQuery, path []string) {
}
func (QueryCountingVisitor) PreVisitDisMaxQuery(dmq *types.DisMaxQuery, path []string)  {}
func (QueryCountingVisitor) PostVisitDisMaxQuery(dmq *types.DisMaxQuery, path []string) {}
func (QueryCountingVisitor) PreVisitDistanceFeatureQuery(dfq types.DistanceFeatureQuery, path []string) {
}
func (QueryCountingVisitor) PostVisitDistanceFeatureQuery(dfq types.DistanceFeatureQuery, path []string) {
}
func (QueryCountingVisitor) PreVisitExistsQuery(eq *types.ExistsQuery, path []string)              {}
func (QueryCountingVisitor) PostVisitExistsQuery(eq *types.ExistsQuery, path []string)             {}
func (QueryCountingVisitor) PreVisitMatchQuery(mq *types.MatchQuery, field string, path []string)  {}
func (QueryCountingVisitor) PostVisitMatchQuery(mq *types.MatchQuery, field string, path []string) {}
func (QueryCountingVisitor) PreVisitMatchAllQuery(mq *types.MatchAllQuery, path []string)          {}
func (QueryCountingVisitor) PostVisitMatchAllQuery(mq *types.MatchAllQuery, path []string)         {}
func (QueryCountingVisitor) PreVisitMatchBoolPrefixQuery(mq *types.MatchBoolPrefixQuery, field string, path []string) {
}
func (QueryCountingVisitor) PostVisitMatchBoolPrefixQuery(mq *types.MatchBoolPrefixQuery, field string, path []string) {
}

func Test_queryTraversal(t *testing.T) {
	body := []byte(queryContent)
	query := &types.Query{}
	err := json.Unmarshal(body, query)
	require.NoError(t, err)
	path := []string{}
	visitor := &QueryCountingVisitor{}
	queryTraverser := QueryTraverser{Debug: true, PathMatched: false}
	queryTraverser.TraverseQuery(query, visitor, path)
	assert.Equal(t, 2, visitor.boolQueriesCounter)
	assert.Equal(t, 3, visitor.termQueriesCounter)
}
