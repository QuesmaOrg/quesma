package bucket_aggregations

import (
	"context"
	"fmt"
	"mitmproxy/quesma/logger"
	"mitmproxy/quesma/model"
)

type MultiTerms struct {
	ctx    context.Context
	fields []string
	size   int
}

func NewMultiTerms(ctx context.Context, fields []string, size int) MultiTerms {
	return MultiTerms{ctx: ctx, fields: fields, size: size}
}

func (query MultiTerms) IsBucketAggregation() bool {
	return true
}

func (query MultiTerms) TranslateSqlResponseToJson(rows []model.QueryResultRow, level int) []model.JsonMap {
	var response []model.JsonMap
	fieldsNr := len(query.fields)
	expectedColNrLowerBound := fieldsNr + 1 // +1 for doc_count
	if len(rows) > 0 && len(rows[0].Cols) < expectedColNrLowerBound {
		logger.ErrorWithCtx(query.ctx).Msgf(
			"unexpected number of columns in terms aggregation response, len: %d, expected (at least): %d, rows[0]: %v", len(rows[0].Cols), expectedColNrLowerBound, rows[0])
	}
	const delimiter = '|' // between keys in key_as_string
	for _, row := range rows {
		docCount := row.Cols[len(row.Cols)-1].Value

		keys := make([]any, 0, fieldsNr)
		var keyAsString string
		keyColumns := row.Cols[len(row.Cols)-expectedColNrLowerBound : len(row.Cols)-1]
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
	return fmt.Sprintf("multi_terms(fields: %v, size: %d)", query.fields, query.size)
}

func (query MultiTerms) PostprocessResults(rowsFromDB []model.QueryResultRow) []model.QueryResultRow {
	return rowsFromDB
}
