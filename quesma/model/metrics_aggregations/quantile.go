package metrics_aggregations

import (
	"mitmproxy/quesma/model"
)

type Quantile struct{}

func (query Quantile) IsBucketAggregation() bool {
	return false
}

// TODO implement correct
func (query Quantile) TranslateSqlResponseToJson(rows []model.QueryResultRow, level int) []model.JsonMap {
	// For now we just always return 7 default percentiles
	valueMap := make(map[string]float64)
	percentiles := []string{"1.0", "5.0", "25.0", "50.0", "75.0", "95.0", "99.0"}

	if len(rows) == 0 {
		return emptyPercentilesResult
	}
	if len(rows[0].Cols) == 0 {
		return emptyPercentilesResult
	}

	countedPercentiles := rows[0].Cols[level].Value
	for i, percentile := range countedPercentiles.([]float64) {
		valueMap[percentiles[i]] = percentile
	}

	return []model.JsonMap{{
		"values": valueMap,
	}}
}

func (query Quantile) String() string {
	return "quantile"
}

var emptyPercentilesResult = []model.JsonMap{{
	"values": 0,
}}
