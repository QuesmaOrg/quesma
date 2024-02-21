package metrics_aggregations

import "mitmproxy/quesma/model"

func metricsTranslateSqlResponseToJson(rows []model.QueryResultRow, level int) []model.JsonMap {
	return []model.JsonMap{{
		"value": rows[0].Cols[level].Value,
	}}
}
