// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package elasticsearch_field_types

// https://opensearch.org/docs/latest/field-types/supported-field-types/index/

// Common types
const (
	FieldTypeBinary           string = "binary"
	FieldTypeBoolean          string = "boolean"
	FieldTypeByte             string = "byte"
	FieldTypeKeyword          string = "keyword"
	FieldTypeConstantKeyword  string = "constant_keyword"
	FieldTypeConstantWildcard string = "wildcard"
	FieldTypeLong             string = "long"
	FieldTypeUnsignedLong     string = "unsigned_long"
	FieldTypeInteger          string = "integer"
	FieldTypeShort            string = "short"
	FieldTypeDouble           string = "double"
	FieldTypeFloat            string = "float"
	FieldTypeHalfFloat        string = "half_float"
	FieldTypeDate             string = "date"
	FieldTypeDateNanos        string = "date_nanos"
	FieldTypeAlias            string = "alias"
)

// Objects and relational types
const (
	FieldTypeObject    string = "object"
	FieldTypeFlattened string = "flattened"
	FieldTypeNested    string = "nested"
	FieldTypeJoin      string = "join"
)

// Structured data types
const (
	FieldTypeLongRange   string = "long_range"
	FieldTypeDoubleRange string = "double_range"
	FieldTypeDateRange   string = "date_range"
	FieldTypeIpRange     string = "ip_range"
	FieldTypeIp          string = "ip"
	FieldTypeTypeVersion string = "version"
	FieldTypeTypeMurMur3 string = "murmur3"
)

// Aggregate data types
const (
	FieldTypeAggregateMetricDouble string = "aggregate_metric_double"
	FieldTypeHistogram             string = "histogram"
)

// Text search types
const (
	FieldTypeText            string = "text"
	FieldTypeAnnotatedText   string = "annotated_text"
	FieldTypeMatchOnlyText   string = "match_only_text"
	FieldTypeCompletion      string = "completion"
	FieldTypeSearchAsYouType string = "search_as_you_type"
	FieldTypeTokenCount      string = "token_count"
)

// Document ranking types
const (
	FieldTypeRankFeature  string = "rank_feature"
	FieldTypeRankFeatures string = "rank_features"
	FieldTypeSparseVector string = "sparse_vector"
	FieldTypeDenseVector  string = "dense_vector"
)

// Spatial data types
const (
	FieldTypeGeoPoint string = "geo_point"
	FieldTypeGeoShape string = "geo_shape"
	FieldTypePoint    string = "point"
	FieldTypeShape    string = "shape"
)

// Other types
const (
	FieldTypePercolator string = "percolator"
)

var AllTypes = map[string]bool{
	FieldTypeBinary:                true,
	FieldTypeBoolean:               true,
	FieldTypeByte:                  true,
	FieldTypeShort:                 true,
	FieldTypeInteger:               true,
	FieldTypeFloat:                 true,
	FieldTypeKeyword:               true,
	FieldTypeConstantKeyword:       true,
	FieldTypeConstantWildcard:      true,
	FieldTypeLong:                  true,
	FieldTypeUnsignedLong:          true,
	FieldTypeDouble:                true,
	FieldTypeDate:                  true,
	FieldTypeDateNanos:             true,
	FieldTypeAlias:                 true,
	FieldTypeObject:                true,
	FieldTypeFlattened:             true,
	FieldTypeNested:                true,
	FieldTypeJoin:                  true,
	FieldTypeLongRange:             true,
	FieldTypeDoubleRange:           true,
	FieldTypeDateRange:             true,
	FieldTypeIpRange:               true,
	FieldTypeIp:                    true,
	FieldTypeTypeVersion:           true,
	FieldTypeTypeMurMur3:           true,
	FieldTypeAggregateMetricDouble: true,
	FieldTypeHistogram:             true,
	FieldTypeText:                  true,
	FieldTypeAnnotatedText:         true,
	FieldTypeMatchOnlyText:         true,
	FieldTypeCompletion:            true,
	FieldTypeSearchAsYouType:       true,
	FieldTypeTokenCount:            true,
	FieldTypeRankFeature:           true,
	FieldTypeRankFeatures:          true,
	FieldTypeSparseVector:          true,
	FieldTypeDenseVector:           true,
	FieldTypeGeoPoint:              true,
	FieldTypeGeoShape:              true,
	FieldTypePoint:                 true,
	FieldTypeShape:                 true,
	FieldTypePercolator:            true,
}

func IsValid(fieldType string) bool {
	return AllTypes[fieldType]
}
