package metrics_aggregations

import (
	"mitmproxy/quesma/model"
)

func metricsTranslateSqlResponseToJson(rows []model.QueryResultRow, level int) []model.JsonMap {
	var value any = nil
	if len(rows) > 0 {
		value = rows[0].Cols[level].Value
	}
	// fmt.Println("common level: ", level, "value: ", value) // very needed for debugging in next PRs
	return []model.JsonMap{{
		"value": value,
	}}
}
