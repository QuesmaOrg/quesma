// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package model

// PipelineQueryType is an interface for pipeline aggregations
// It's an extension to QueryType interface
// Adds a method to calculate result rows from its parent aggregation
type PipelineQueryType interface {
	// TranslateSqlResponseToJson 'level' - we want to translate [level:] (metrics aggr) or [level-1:] (bucket aggr) columns to JSON
	// Previous columns are used for bucketing.
	// For 'bucket' aggregation result is a slice of buckets, for 'metrics' aggregation it's a single bucket (only look at [0])
	TranslateSqlResponseToJson(rows []QueryResultRow, level int) JsonMap

	// IsBucketAggregation if true, result from 'MakeResponse' will be a slice of buckets
	// if false, it's a metrics aggregation and result from 'MakeResponse' will be a single bucket
	IsBucketAggregation() bool

	// CalculateResultWhenMissing calculates the result of this aggregation when it's a NoDBQuery
	// (we don't query the DB for the results, but calculate them from the parent aggregation)
	CalculateResultWhenMissing(query *Query, parentRows []QueryResultRow) []QueryResultRow

	String() string
}
