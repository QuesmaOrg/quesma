// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package elasticsearch

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestDateManager_MissingInDateHistogramToUnixTimestamp(t *testing.T) {
	tests := []struct {
		missing             any
		wantUnixTimestamp   int64
		wantParsingSucceded bool
	}{
		{"2024", 1704067200000, true},
		{int64(123), 123, true},
		{"4234324223", 4234324223, true},
		{"4234", 71444937600000, true},
		{"42340", 42340, true},
		{"42340.234", 42340, true},
		{"2024/02", -1, false},
		{"2024-02", 1706745600000, true},
		{"2024-2", -1, false},
		{"2024-02-02", 1706832000000, true},
		{"2024-02-3", -1, false},
		{"2024-02-30", -1, false},
		{"2024-02-25T13:00:00", 1, true},
		{nil, -1, false},
		{"2024-02-25T13", 1, true},
		{"2024-02-25 13:00:00", -1, false},
		{"2024-02-25T13:1", -1, false},
		{"2024-02-25T13:11", -1, true},
		{"2024-02-25T25:00:00", -1, false},
		//{"2024-02-25T13:00:00+05:00", 1, true},
		//{123456789, 123456789, true},
		//{123456789.23, 123456789, true},
		//{543510004234324320.32, 543510004234324320, true},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("%v", tt.missing), func(t *testing.T) {
			dm := DateManager{}
			gotUnixTs, gotParsingSucceded := dm.MissingInDateHistogramToUnixTimestamp(tt.missing)
			assert.Equalf(t, tt.wantUnixTimestamp, gotUnixTs, "MissingInDateHistogramToUnixTimestamp(%v)", tt.missing)
			assert.Equalf(t, tt.wantParsingSucceded, gotParsingSucceded, "MissingInDateHistogramToUnixTimestamp(%v)", tt.missing)
		})
	}
}
