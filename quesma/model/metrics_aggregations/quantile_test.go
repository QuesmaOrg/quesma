// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package metrics_aggregations

import (
	"context"
	"github.com/QuesmaOrg/quesma/quesma/clickhouse"
	"github.com/QuesmaOrg/quesma/quesma/util"
	"math"
	"strconv"
	"testing"
	"time"
)

func equalFloats(a, b float64) bool {
	if math.IsNaN(a) && math.IsNaN(b) {
		return true
	}
	return math.Abs(a-b) < 1e-9
}

func equalStrings(a, b *string) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return *a == *b
}

func Test_processResult(t *testing.T) {
	quantile := NewQuantile(context.Background(), []string{}, false, clickhouse.DateTime)
	colName := "not-important"
	wantedStr := "2024-05-02T21:58:16.297Z"
	tests := []struct {
		percentileReturnedByClickhouse any
		wantedPercentile               float64
		wantedPercentileAsString       *string
	}{
		{nil, math.NaN(), nil},
		{"", math.NaN(), nil},
		{"0", math.NaN(), nil},
		{0, math.NaN(), nil},
		{0.0, math.NaN(), nil},
		{[]string{"1.0"}, math.NaN(), nil},
		{[]string{"1.0", "5"}, math.NaN(), nil},
		{[]any{"1.0", "5"}, math.NaN(), nil},
		{[]any{"1.0", "5"}, math.NaN(), nil},
		{[]int{1}, math.NaN(), nil},
		{[]int{}, math.NaN(), nil},
		{[]float64{}, math.NaN(), nil},
		{[]float64{1.0}, 1.0, nil},
		{[]float64{1.0, 2.0}, 1.0, nil},
		{[]any{float64(1.0), 5}, 1.0, nil},
		{[]any{5, float64(1.0)}, math.NaN(), nil},
		{[]time.Time{util.ParseTime("2024-05-02T21:58:16.297Z"), util.ParseTime("5")}, 1714687096297.0, &wantedStr},
		{[]time.Time{util.ParseTime("2024-05-02T21:58:16.297Z")}, 1714687096297.0, &wantedStr},
		{[]any{util.ParseTime("2024-05-02T21:58:16.297Z"), 5, 10, 5.2}, 1714687096297.0, &wantedStr},
		{[]any{util.ParseTime("2024-05-02T21:58:16.297Z")}, 1714687096297.0, &wantedStr},
	}
	for i, tt := range tests {
		t.Run("testing processResult"+strconv.Itoa(i), func(t *testing.T) {
			percentile, percentileAsString, _ := quantile.processResult(colName, tt.percentileReturnedByClickhouse)
			if !equalFloats(percentile, tt.wantedPercentile) {
				t.Errorf("got %v, wanted %v", percentile, tt.wantedPercentile)
			}
			if !equalStrings(percentileAsString, tt.wantedPercentileAsString) {
				t.Errorf("got %v, wanted %v", percentileAsString, tt.wantedPercentileAsString)
			}
		})
	}
}
