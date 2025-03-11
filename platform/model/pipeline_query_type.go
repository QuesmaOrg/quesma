// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package model

// PipelineQueryType is an interface for pipeline aggregations
// It's an extension to QueryType interface
// Adds a method to calculate result rows from its parent aggregation
type PipelineQueryType interface {
	// TranslateSqlResponseToJson .
	// For 'bucket' aggregation result is a slice of buckets, for 'metrics' aggregation it's a single bucket (only look at [0])
	TranslateSqlResponseToJson(rows []QueryResultRow) JsonMap

	// Should always return PipelineAggregation
	AggregationType() AggregationType

	// PipelineAggregationType returns the type of the pipeline aggregation (parent or sibling)
	// https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-pipeline.html
	PipelineAggregationType() PipelineAggregationType

	// CalculateResultWhenMissing calculates the result of this aggregation when it's a NoDBQuery
	// (we don't query the DB for the results, but calculate them from the parent aggregation)
	CalculateResultWhenMissing(parentRows []QueryResultRow) []QueryResultRow

	String() string
	GetParent() string
	GetPathToParent() []string
	IsCount() bool

	GetParentBucketAggregation() QueryType
	SetParentBucketAggregation(parentBucketAggregation QueryType)
}
