// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package painless

import (
	"github.com/stretchr/testify/assert"
	"quesma/model"
	"testing"
)

func TestParsePainlessScriptToExpr(t *testing.T) {
	tests := []struct {
		name string
		s    string
		want model.Expr
	}{
		{
			name: "hour of a day",
			s:    "emit(doc['timestamp'].value.getHour());",
			want: model.NewFunction(model.DateHourFunction, model.NewColumnRef(model.TimestampFieldName)),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParsePainlessV2ScriptToExpr(tt.s)
			assert.NoError(t, err)
			if !model.PartlyImplementedIsEqual(got, tt.want) {
				t.Errorf("ParsePainlessScriptToExpr(\"%s\") = %v, want %v", tt.s, got, tt.want)
			}
		})
	}
}
