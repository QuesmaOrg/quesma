package metrics_aggregations

import "mitmproxy/quesma/model"

func metricsTranslateSqlResponseToJson(rows []model.QueryResultRow, level int) []model.JsonMap {
	var value any = nil
	if len(rows) > 0 {
		value = rows[0].Cols[level].Value
	}
	return []model.JsonMap{{
		"value": value,
	}}
}
