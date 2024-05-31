package bucket_aggregations

import (
	"context"
	"fmt"
	"mitmproxy/quesma/logger"
	"mitmproxy/quesma/model"
)

type MultiTerms struct {
	ctx      context.Context
	fieldsNr int // over how many fields we split into buckets
}

func NewMultiTerms(ctx context.Context, fieldsNr int) MultiTerms {
	return MultiTerms{ctx: ctx, fieldsNr: fieldsNr}
}

func (query MultiTerms) IsBucketAggregation() bool {
	return true
}

func (query MultiTerms) TranslateSqlResponseToJson(rows []model.QueryResultRow, level int) []model.JsonMap {
	var response []model.JsonMap
	minimumExpectedColNr := query.fieldsNr + 1 // +1 for doc_count. Can be more, if this MultiTerms has parent aggregations, but never fewer.
	if len(rows) > 0 && len(rows[0].Cols) < minimumExpectedColNr {
		logger.ErrorWithCtx(query.ctx).Msgf(
			"unexpected number of columns in terms aggregation response, len: %d, expected (at least): %d, rows[0]: %v", len(rows[0].Cols), minimumExpectedColNr, rows[0])
	}
	const delimiter = '|' // between keys in key_as_string
	for _, row := range rows {
		docCount := row.Cols[len(row.Cols)-1].Value

		keys := make([]any, 0, query.fieldsNr)
		var keyAsString string
		startIndex := len(row.Cols) - query.fieldsNr - 1
		if startIndex < 0 {
			logger.WarnWithCtx(query.ctx).Msgf("startIndex < 0 - too few columns. row: %+v", row)
			startIndex = 0
		}
		keyColumns := row.Cols[startIndex : len(row.Cols)-1] // last col isn't a key, it's doc_count
		for _, col := range keyColumns {
			keys = append(keys, col.Value)
			keyAsString += fmt.Sprintf("%v%c", col.Value, delimiter)
		}
		if len(keyAsString) > 0 {
			keyAsString = keyAsString[:len(keyAsString)-1] // remove trailing delimiter
		}

		bucket := model.JsonMap{
			"key":           keys,
			"key_as_string": keyAsString,
			"doc_count":     docCount,
		}
		response = append(response, bucket)
	}
	return response
}

func (query MultiTerms) String() string {
	return fmt.Sprintf("multi_terms(fieldsNr: %d)", query.fieldsNr)
}

func (query MultiTerms) PostprocessResults(rowsFromDB []model.QueryResultRow) []model.QueryResultRow {
	return rowsFromDB
}
