// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package elastic_query_dsl

import (
	"context"
	"fmt"
	"github.com/QuesmaOrg/quesma/platform/util"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestDateManager_parseStrictDateOptionalTimeOrEpochMillis(t *testing.T) {
	empty := time.Time{}
	tests := []struct {
		input                  any
		wantedTimestamp        time.Time
		wantedParsingSucceeded bool
	}{
		{nil, empty, false},
		{"2024", time.UnixMilli(1704067200000), true},
		{int64(123), time.UnixMilli(123), true},
		{"4234324223", time.UnixMilli(4234324223), true},
		{"4234", time.UnixMilli(71444937600000), true},
		{"42340", time.UnixMilli(42340), true},
		{"42340.234", time.UnixMilli(42340), true},
		{"2024/02", empty, false},
		{"2024-02", time.UnixMilli(1706745600000), true},
		{"2024-2", empty, false},
		{"2024-02-02", time.UnixMilli(1706832000000), true},
		{"2024-02-3", empty, false},
		{"2024-02-30", empty, false},
		{"2024-02-25T1", time.UnixMilli(1708822800000), true}, // this fails in Kibana, so we're better
		{"2024-02-25T13:00:00", time.UnixMilli(1708866000000), true},
		{"2024-02-25 13:00:00", empty, false},
		{"2024-02-25T13:11", time.UnixMilli(1708866660000), true},
		{"2024-02-25T25:00:00", empty, false},
		{"2024-02-25T13:00:00+05", time.UnixMilli(1708848000000), true},
		{"2024-02-25T13:00:00+05:00", time.UnixMilli(1708848000000), true},
		{"2024-02-25T13:00:00.123", time.UnixMilli(1708866000123), true},
		{"2024-02-25T13:00:00.123Z", time.UnixMilli(1708866000123), true},
		{"2024-02-25T13:00:00.123456789", time.Unix(1708866000, 123456789), true},
		{"2024-02-25T13:00:00.123456789Z", time.Unix(1708866000, 123456789), true},
	}
	for i, tt := range tests {
		t.Run(util.PrettyTestName(fmt.Sprintf("%v", tt.input), i), func(t *testing.T) {
			dm := NewDateManager(context.Background())
			gotUnixTs, gotParsingSucceeded := dm.parseStrictDateOptionalTimeOrEpochMillis(tt.input)
			assert.Truef(t, tt.wantedTimestamp.Equal(gotUnixTs), "MissingInDateHistogramToUnixTimestamp(%v)", tt.input)
			assert.Equalf(t, tt.wantedParsingSucceeded, gotParsingSucceeded, "MissingInDateHistogramToUnixTimestamp(%v)", tt.input)
		})
	}
}
