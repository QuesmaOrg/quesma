// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package ast

import (
	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
)

type QueryTraverser struct {
	Debug       bool
	PathMatched bool
}

func (qt *QueryTraverser) TraverseBooleanQuery(b *types.BoolQuery, v QueryVisitor, path []string) {
	if b == nil {
		return
	}
	v.PreVisitBool(b, path)
	for _, q := range b.Filter {
		if qt.Debug {
			path = append(path, "filter", "*")
		}
		qt.TraverseQuery(&q, v, path)
		if qt.Debug {
			path = path[:len(path)-2]
		}
	}
	for _, q := range b.Should {
		if qt.Debug {
			path = append(path, "should", "*")
		}
		qt.TraverseQuery(&q, v, path)
		if qt.Debug {
			path = path[:len(path)-2]
		}
	}
	for _, q := range b.Must {
		if qt.Debug {
			path = append(path, "must", "*")
		}
		qt.TraverseQuery(&q, v, path)
		if qt.Debug {
			path = path[:len(path)-2]
		}
	}
	for _, q := range b.MustNot {
		if qt.Debug {
			path = append(path, "must_not", "*")
		}
		qt.TraverseQuery(&q, v, path)
		if qt.Debug {
			path = path[:len(path)-2]
		}
	}
	v.PostVisitBool(b, path)
}

func (qt *QueryTraverser) TraverseCombinedFieldsQuery(cfq *types.CombinedFieldsQuery, v QueryVisitor, path []string) {
	if cfq == nil {
		return
	}
	v.PreVisitCombinedFieldsQuery(cfq, path)
	v.PostVisitCombinedFieldsQuery(cfq, path)
}

func (qt *QueryTraverser) TraverseTypeQuery(t *types.TypeQuery, v QueryVisitor, path []string) {
	if t == nil {
		return
	}
	v.PreVisitTypeQuery(t, path)
	v.PostVisitTypeQuery(t, path)
}

func (qt *QueryTraverser) TraverseBoostingQuery(b *types.BoostingQuery, v QueryVisitor, path []string) {
	if b == nil {
		return
	}
	v.PreVisitBoostingQuery(b, path)
	qt.TraverseQuery(b.Positive, v, path)
	qt.TraverseQuery(b.Negative, v, path)
	v.PostVisitBoostingQuery(b, path)
}

func (qt *QueryTraverser) TraverseCommonTermsQuery(ctq *types.CommonTermsQuery, field string, v QueryVisitor, path []string) {
	if ctq == nil {
		return
	}
	v.PreVisitCommonTermsQuery(ctq, field, path)
	v.PostVisitCommonTermsQuery(ctq, field, path)
}

func (qt *QueryTraverser) TraverseTermQuery(t *types.TermQuery, field string, v QueryVisitor, path []string) {
	if t == nil {
		return
	}
	if qt.Debug {
		path = append(path, "term", field)
	}
	v.PreVisitTerm(t, field, path)
	v.PostVisitTerm(t, field, path)
	if qt.Debug {
		path = path[:len(path)-2]
		_ = path
	}
}

func (qt *QueryTraverser) TraverseConstantScoreQuery(csq *types.ConstantScoreQuery, v QueryVisitor, path []string) {
	if csq == nil {
		return
	}
	v.PreVisitConstantScoreQuery(csq, path)
	qt.TraverseQuery(csq.Filter, v, path)
	v.PostVisitConstantScoreQuery(csq, path)
}

func (qt *QueryTraverser) TraverseDistanceFeatureQuery(dfq types.DistanceFeatureQuery, v QueryVisitor, path []string) {
	if dfq == nil {
		return
	}
	v.PreVisitDistanceFeatureQuery(dfq, path)
	v.PostVisitDistanceFeatureQuery(dfq, path)

}

func (qt *QueryTraverser) TraverseDismaxQuery(dq *types.DisMaxQuery, v QueryVisitor, path []string) {
	if dq == nil {
		return
	}
	v.PreVisitDisMaxQuery(dq, path)
	for _, q := range dq.Queries {
		qt.TraverseQuery(&q, v, path)
	}
	v.PostVisitDisMaxQuery(dq, path)
}

func (*QueryTraverser) TraverseExistsQuery(eq *types.ExistsQuery, v QueryVisitor, path []string) {
	if eq == nil {
		return
	}

	v.PreVisitExistsQuery(eq, path)
	v.PostVisitExistsQuery(eq, path)
}

func (*QueryTraverser) TraverseMatchQuery(mq *types.MatchQuery, field string, v QueryVisitor, path []string) {
	if mq == nil {
		return
	}
	v.PreVisitMatchQuery(mq, field, path)
	v.PostVisitMatchQuery(mq, field, path)
}

func (qt *QueryTraverser) TraverseMatchAllQuery(mq *types.MatchAllQuery, v QueryVisitor, path []string) {
	if mq == nil {
		return
	}
	v.PreVisitMatchAllQuery(mq, path)
	v.PostVisitMatchAllQuery(mq, path)
}

func (qt *QueryTraverser) TraverseMatchBoolPrefixQuery(mbpq *types.MatchBoolPrefixQuery, field string, v QueryVisitor, path []string) {
	if mbpq == nil {
		return
	}
	v.PreVisitMatchBoolPrefixQuery(mbpq, field, path)
	v.PostVisitMatchBoolPrefixQuery(mbpq, field, path)
}

func (qt *QueryTraverser) TraverseQuery(q *types.Query, v QueryVisitor, path []string) {
	if q == nil {
		return
	}
	if qt.Debug && !qt.PathMatched {
		path = append(path, "query")
		qt.PathMatched = true
	}
	v.PreVisitQuery(q, path)
	if qt.Debug {
		path = append(path, "bool")
	}
	qt.TraverseBooleanQuery(q.Bool, v, path)
	if qt.Debug {
		path = path[:len(path)-1]
	}
	for field, commonTermsQ := range q.Common {
		qt.TraverseCommonTermsQuery(&commonTermsQ, field, v, path)
	}
	qt.TraverseTypeQuery(q.Type, v, path)
	qt.TraverseBoostingQuery(q.Boosting, v, path)
	qt.TraverseDismaxQuery(q.DisMax, v, path)
	qt.TraverseCombinedFieldsQuery(q.CombinedFields, v, path)
	qt.TraverseConstantScoreQuery(q.ConstantScore, v, path)

	qt.TraverseDistanceFeatureQuery(q.DistanceFeature, v, path)

	qt.TraverseExistsQuery(q.Exists, v, path)

	for field, matchQ := range q.Match {
		qt.TraverseMatchQuery(&matchQ, field, v, path)
	}
	qt.TraverseMatchAllQuery(q.MatchAll, v, path)

	for field, matchBoolPrefixQ := range q.MatchBoolPrefix {
		qt.TraverseMatchBoolPrefixQuery(&matchBoolPrefixQ, field, v, path)
	}
	for field, termQ := range q.Term {
		qt.TraverseTermQuery(&termQ, field, v, path)
	}

	v.PostVisitQuery(q, path)
	if qt.Debug {
		path = path[:len(path)-1]
		_ = path
	}
}
