// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package ast

import "github.com/elastic/go-elasticsearch/v8/typedapi/types"

type QueryVisitor interface {
	PreVisitQuery(q *types.Query, path []string)
	PostVisitQuery(q *types.Query, path []string)
	PreVisitBool(b *types.BoolQuery, path []string)
	PostVisitBool(b *types.BoolQuery, path []string)
	PreVisitBoostingQuery(b *types.BoostingQuery, path []string)
	PostVisitBoostingQuery(b *types.BoostingQuery, path []string)
	PreVisitTypeQuery(t *types.TypeQuery, path []string)
	PostVisitTypeQuery(t *types.TypeQuery, path []string)
	PreVisitCommonTermsQuery(ctq *types.CommonTermsQuery, field string, path []string)
	PostVisitCommonTermsQuery(ctq *types.CommonTermsQuery, field string, path []string)
	PreVisitCombinedFieldsQuery(ctq *types.CombinedFieldsQuery, path []string)
	PostVisitCombinedFieldsQuery(ctq *types.CombinedFieldsQuery, path []string)
	PreVisitTerm(t *types.TermQuery, field string, path []string)
	PostVisitTerm(t *types.TermQuery, field string, path []string)
	PreVisitConstantScoreQuery(csq *types.ConstantScoreQuery, path []string)
	PostVisitConstantScoreQuery(csq *types.ConstantScoreQuery, path []string)
	PreVisitDisMaxQuery(dmq *types.DisMaxQuery, path []string)
	PostVisitDisMaxQuery(dmq *types.DisMaxQuery, path []string)
	PreVisitDistanceFeatureQuery(dfq types.DistanceFeatureQuery, path []string)
	PostVisitDistanceFeatureQuery(dfq types.DistanceFeatureQuery, path []string)
	PreVisitExistsQuery(eq *types.ExistsQuery, path []string)
	PostVisitExistsQuery(eq *types.ExistsQuery, path []string)
	PreVisitMatchQuery(mq *types.MatchQuery, field string, path []string)
	PostVisitMatchQuery(mq *types.MatchQuery, field string, path []string)
	PreVisitMatchAllQuery(mq *types.MatchAllQuery, path []string)
	PostVisitMatchAllQuery(mq *types.MatchAllQuery, path []string)
	PreVisitMatchBoolPrefixQuery(mq *types.MatchBoolPrefixQuery, field string, path []string)
	PostVisitMatchBoolPrefixQuery(mq *types.MatchBoolPrefixQuery, field string, path []string)
}
