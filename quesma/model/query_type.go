package model

type QueryType interface {
	TranslateSqlResponseToJson([]QueryResultRow) []JsonMap
	// IsBucketAggregation if true, result from 'MakeResponse' will be a slice of buckets
	// if false, it's a metrics aggregation and result from 'MakeResponse' will be a single bucket
	IsBucketAggregation() bool
	String() string
}

func MetricsTranslateSqlResponseToJson(rows []QueryResultRow) []JsonMap {
	return []JsonMap{{
		"value": rows[0].Cols[0].Value,
	}}
}
