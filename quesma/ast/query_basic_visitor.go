// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package ast

import "github.com/elastic/go-elasticsearch/v8/typedapi/types"

type QueryBasicVisitor struct {
	QueryVisitor
}

func (QueryBasicVisitor) PreVisitQuery(q *types.Query, path []string)                  {}
func (QueryBasicVisitor) PostVisitQuery(q *types.Query, path []string)                 {}
func (QueryBasicVisitor) PreVisitBool(b *types.BoolQuery, path []string)               {}
func (QueryBasicVisitor) PostVisitBool(b *types.BoolQuery, path []string)              {}
func (QueryBasicVisitor) PreVisitBoostingQuery(b *types.BoostingQuery, path []string)  {}
func (QueryBasicVisitor) PostVisitBoostingQuery(b *types.BoostingQuery, path []string) {}
func (QueryBasicVisitor) PreVisitTypeQuery(t *types.TypeQuery, path []string)          {}
func (QueryBasicVisitor) PostVisitTypeQuery(t *types.TypeQuery, path []string)         {}
func (QueryBasicVisitor) PreVisitCommonTermsQuery(ctq *types.CommonTermsQuery, field string, path []string) {
}
func (QueryBasicVisitor) PostVisitCommonTermsQuery(ctq *types.CommonTermsQuery, field string, path []string) {
}
func (QueryBasicVisitor) PostVisitCombinedFieldsQuery(ctq *types.CombinedFieldsQuery, path []string) {
}
func (QueryBasicVisitor) PreVisitTerm(t *types.TermQuery, field string, path []string)             {}
func (QueryBasicVisitor) PostVisitTerm(t *types.TermQuery, field string, path []string)            {}
func (QueryBasicVisitor) PreVisitConstantScoreQuery(csq *types.ConstantScoreQuery, path []string)  {}
func (QueryBasicVisitor) PostVisitConstantScoreQuery(csq *types.ConstantScoreQuery, path []string) {}
func (QueryBasicVisitor) PreVisitDisMaxQuery(dmq *types.DisMaxQuery, path []string)                {}
func (QueryBasicVisitor) PostVisitDisMaxQuery(dmq *types.DisMaxQuery, path []string)               {}
func (QueryBasicVisitor) PreVisitDistanceFeatureQuery(dfq types.DistanceFeatureQuery, path []string) {
}
func (QueryBasicVisitor) PostVisitDistanceFeatureQuery(dfq types.DistanceFeatureQuery, path []string) {
}
func (QueryBasicVisitor) PreVisitExistsQuery(eq *types.ExistsQuery, path []string)              {}
func (QueryBasicVisitor) PostVisitExistsQuery(eq *types.ExistsQuery, path []string)             {}
func (QueryBasicVisitor) PreVisitMatchQuery(mq *types.MatchQuery, field string, path []string)  {}
func (QueryBasicVisitor) PostVisitMatchQuery(mq *types.MatchQuery, field string, path []string) {}
func (QueryBasicVisitor) PreVisitMatchAllQuery(mq *types.MatchAllQuery, path []string)          {}
func (QueryBasicVisitor) PostVisitMatchAllQuery(mq *types.MatchAllQuery, path []string)         {}
func (QueryBasicVisitor) PreVisitMatchBoolPrefixQuery(mq *types.MatchBoolPrefixQuery, field string, path []string) {
}
func (QueryBasicVisitor) PostVisitMatchBoolPrefixQuery(mq *types.MatchBoolPrefixQuery, field string, path []string) {
}
