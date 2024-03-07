package metrics_aggregations

import (
	"mitmproxy/quesma/model"
	"strings"
)

type Quantile struct{}

func (query Quantile) IsBucketAggregation() bool {
	return false
}

func (query Quantile) TranslateSqlResponseToJson(rows []model.QueryResultRow, level int) []model.JsonMap {
	valueMap := make(map[string]float64)

	if len(rows) == 0 {
		return emptyPercentilesResult
	}
	if len(rows[0].Cols) == 0 {
		return emptyPercentilesResult
	}

	for _, res := range rows[0].Cols {
		if strings.HasPrefix(res.ColName, "quantile") {
			percentile := res.Value.([]float64)
			percentileName, _ := strings.CutPrefix(res.ColName, "quantile_")
			valueMap[percentileName] = percentile[0]
		}
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
