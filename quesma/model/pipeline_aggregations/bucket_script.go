package pipeline_aggregations

import (
	"context"
	"mitmproxy/quesma/logger"
	"mitmproxy/quesma/model"
)

type BucketScript struct {
	ctx context.Context
}

func NewBucketScript(ctx context.Context) BucketScript {
	return BucketScript{ctx: ctx}
}

func (query BucketScript) IsBucketAggregation() bool {
	return false
}

func (query BucketScript) TranslateSqlResponseToJson(rows []model.QueryResultRow, level int) []model.JsonMap {
	if len(rows) == 0 {
		logger.WarnWithCtx(query.ctx).Msg("no rows returned for bucket script aggregation")
		return []model.JsonMap{{"value": 0}}
	}
	var response []model.JsonMap
	for _, row := range rows {
		response = append(response, model.JsonMap{"value": row.Cols[level].Value})
	}
	return response
}

func (query BucketScript) String() string {
	return "bucket script"
}
