package model

type QueryType interface {
	// TranslateSqlResponseToJson 'level' - we want to translate [level:] (metrics aggr) or [level-1:] (bucket aggr) columns to JSON
	// Previous columns are used for bucketing.
	// For 'bucket' aggregation result is a slice of buckets, for 'metrics' aggregation it's a single bucket (only look at [0])
	TranslateSqlResponseToJson(rows []QueryResultRow, level int) []JsonMap

	// IsBucketAggregation if true, result from 'MakeResponse' will be a slice of buckets
	// if false, it's a metrics aggregation and result from 'MakeResponse' will be a single bucket
	IsBucketAggregation() bool
	String() string
}
