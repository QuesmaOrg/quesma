// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package bucket_aggregations

import (
	"context"
	"fmt"
	"github.com/QuesmaOrg/quesma/quesma/logger"
	"github.com/QuesmaOrg/quesma/quesma/model"
	"github.com/QuesmaOrg/quesma/quesma/util"
	"strings"
)

type MultiTerms struct {
	ctx      context.Context
	fieldsNr int // over how many fields we split into buckets
}

func NewMultiTerms(ctx context.Context, fieldsNr int) MultiTerms {
	return MultiTerms{ctx: ctx, fieldsNr: fieldsNr}
}

func (query MultiTerms) AggregationType() model.AggregationType {
	return model.BucketAggregation
}

func (query MultiTerms) TranslateSqlResponseToJson(rows []model.QueryResultRow) model.JsonMap {
	minimumExpectedColNr := query.fieldsNr + 1 // +1 for doc_count. Can be more, if this MultiTerms has parent aggregations, but never fewer.
	if len(rows) > 0 && len(rows[0].Cols) < minimumExpectedColNr {
		logger.ErrorWithCtx(query.ctx).Msgf(
			"unexpected number of columns in terms aggregation response, len: %d, expected (at least): %d, rows[0]: %v", len(rows[0].Cols), minimumExpectedColNr, rows[0])
	}
	var response []model.JsonMap
	const delimiter = '|' // between keys in key_as_string
	for _, row := range rows {
		startIndex := len(row.Cols) - query.fieldsNr - 1
		if startIndex < 0 {
			logger.WarnWithCtx(query.ctx).Msgf("startIndex < 0 - too few columns. row: %+v", row)
			startIndex = 0
		}
		keyColumns := row.Cols[startIndex : len(row.Cols)-1] // last col isn't a key, it's doc_count
		keys := make([]any, 0, query.fieldsNr)
		var keyAsString strings.Builder
		for i, col := range keyColumns {
			keys = append(keys, col.Value)
			if i > 0 {
				keyAsString.WriteRune(delimiter)
			}
			keyAsString.WriteString(fmt.Sprintf("%v", col.Value))
		}

		bucket := model.JsonMap{
			"key":           keys,
			"key_as_string": keyAsString.String(),
			"doc_count":     query.docCount(row),
		}
		response = append(response, bucket)
	}
	sumOtherDocCount := 0
	if len(rows) > 0 {
		parentCount, err := util.ExtractInt64(query.parentCount(rows[0]))
		if err != nil {
			logger.Error().Err(err)
		}
		sumOtherDocCount = int(parentCount) - query.sumDocCounts(rows)
	}
	return model.JsonMap{
		"sum_other_doc_count":         sumOtherDocCount,
		"doc_count_error_upper_bound": 0,
		"buckets":                     response,
	}
}

func (query MultiTerms) String() string {
	return fmt.Sprintf("multi_terms(fieldsNr: %d)", query.fieldsNr)
}

func (query MultiTerms) sumDocCounts(rows []model.QueryResultRow) int {
	sum := 0
	if len(rows) > 0 {
		switch query.docCount(rows[0]).(type) {
		case int:
			for _, row := range rows {
				sum += query.docCount(row).(int)
			}
		case int64:
			for _, row := range rows {
				sum += int(query.docCount(row).(int64))
			}
		case uint64:
			for _, row := range rows {
				sum += int(query.docCount(row).(uint64))
			}
		default:
			logger.WarnWithCtx(query.ctx).Msgf("unknown type for terms doc_count: %T, value: %v",
				query.docCount(rows[0]), query.docCount(rows[0]))
		}
	}
	return sum
}

func (query MultiTerms) docCount(row model.QueryResultRow) any {
	return row.Cols[len(row.Cols)-1].Value
}

func (query MultiTerms) parentCountIdx(row model.QueryResultRow) int {
	return len(row.Cols) - query.fieldsNr - 2
}
func (query MultiTerms) parentCount(row model.QueryResultRow) any {
	return row.Cols[query.parentCountIdx(row)].Value
}
