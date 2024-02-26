package model

// Content of this file is a copy of
// https://github.com/elastic/go-elasticsearch/blob/main/typedapi/types/fieldcapability.go
// with the exception of two properties that are currently
// omitted:
// - TimeSeriesMetric *timeseriesmetrictype.TimeSeriesMetricType `json:"time_series_metric,omitempty"`
// - Meta Metadata `json:"meta,omitempty"`

// FieldCapability type.
type FieldCapability struct {
	// Aggregatable Whether this field can be aggregated on all indices.
	Aggregatable bool `json:"aggregatable"`
	// Indices The list of indices where this field has the same type family, or null if all
	// indices have the same type family for the field.
	Indices []string `json:"indices,omitempty"`
	// MetadataField Whether this field is registered as a metadata field.
	MetadataField *bool `json:"metadata_field,omitempty"`
	// MetricConflictsIndices The list of indices where this field is present if these indices
	// don’t have the same `time_series_metric` value for this field.
	MetricConflictsIndices []string `json:"metric_conflicts_indices,omitempty"`
	// NonAggregatableIndices The list of indices where this field is not aggregatable, or null if all
	// indices have the same definition for the field.
	NonAggregatableIndices []string `json:"non_aggregatable_indices,omitempty"`
	// NonDimensionIndices If this list is present in response then some indices have the
	// field marked as a dimension and other indices, the ones in this list, do not.
	NonDimensionIndices []string `json:"non_dimension_indices,omitempty"`
	// NonSearchableIndices The list of indices where this field is not searchable, or null if all
	// indices have the same definition for the field.
	NonSearchableIndices []string `json:"non_searchable_indices,omitempty"`
	// Searchable Whether this field is indexed for search on all indices.
	Searchable bool `json:"searchable"`
	// TimeSeriesDimension Whether this field is used as a time series dimension.
	TimeSeriesDimension *bool  `json:"time_series_dimension,omitempty"`
	Type                string `json:"type"`
}
