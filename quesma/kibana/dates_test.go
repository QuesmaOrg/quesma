// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package kibana

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestDateManager_parseStrictDateOptionalTimeOrEpochMillis(t *testing.T) {
	tests := []struct {
		missing              any
		wantUnixTimestamp    int64
		wantParsingSucceeded bool
	}{
		{nil, -1, false},
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
		{"2024-02-25T1", 1708822800000, true}, // this fails in Kibana, so we're better
		{"2024-02-25T13:00:00", 1708866000000, true},
		{"2024-02-25 13:00:00", -1, false},
		{"2024-02-25T13:11", 1708866660000, true},
		{"2024-02-25T25:00:00", -1, false},
		{"2024-02-25T13:00:00+05", 1708848000000, true},
		{"2024-02-25T13:00:00+05:00", 1708848000000, true},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("%v", tt.missing), func(t *testing.T) {
			dm := NewDateManager(context.Background())
			gotUnixTs, gotParsingSucceeded := dm.parseStrictDateOptionalTimeOrEpochMillis(tt.missing)
			assert.Equalf(t, tt.wantUnixTimestamp, gotUnixTs, "MissingInDateHistogramToUnixTimestamp(%v)", tt.missing)
			assert.Equalf(t, tt.wantParsingSucceeded, gotParsingSucceeded, "MissingInDateHistogramToUnixTimestamp(%v)", tt.missing)
		})
	}
}
