// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package model

import (
	"context"
	"reflect"
)

type QueryProcessor struct {
	ctx context.Context
}

func NewQueryProcessor(ctx context.Context) QueryProcessor {
	return QueryProcessor{ctx: ctx}
}

// Returns if row1 and row2 have the same values for the first level fields
func (qp *QueryProcessor) sameGroupByFields(row1, row2 QueryResultRow, level int) bool {

	isArray := func(val interface{}) bool {
		if val == nil {
			return false
		}
		v := reflect.ValueOf(val)
		return v.Kind() == reflect.Slice || v.Kind() == reflect.Array
	}

	for i := 0; i < level; i++ {
		val1 := row1.Cols[i].Value
		val2 := row2.Cols[i].Value
		isArray1 := isArray(val1)
		isArray2 := isArray(val2)

		if !isArray1 && !isArray2 {
			if row1.Cols[i].ExtractValue(qp.ctx) != row2.Cols[i].ExtractValue(qp.ctx) {
				return false
			}
		} else if isArray1 && isArray2 {
			return reflect.DeepEqual(val1, val2)
		} else {
			return false
		}
	}
	return true
}

// Splits ResultSet into buckets, based on the first level fields
// E.g. if level == 0, we split into buckets based on the first field,
// e.g. [row(1, ...), row(1, ...), row(2, ...), row(2, ...), row(3, ...)] -> [[row(1, ...), row(1, ...)], [row(2, ...), row(2, ...)], [row(3, ...)]]
func (qp *QueryProcessor) SplitResultSetIntoBuckets(ResultSet []QueryResultRow, level int) [][]QueryResultRow {
	if len(ResultSet) == 0 {
		return [][]QueryResultRow{{}}
	}

	lastRow := ResultSet[0]
	buckets := [][]QueryResultRow{{lastRow}}
	for _, row := range ResultSet[1:] {
		if qp.sameGroupByFields(row, lastRow, level) {
			buckets[len(buckets)-1] = append(buckets[len(buckets)-1], row)
		} else {
			buckets = append(buckets, []QueryResultRow{row})
		}
		lastRow = row
	}
	return buckets
}
