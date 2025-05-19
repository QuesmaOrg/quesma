// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package util

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestFindTimestampPrecision(t *testing.T) {
	var testcases = []struct {
		timestamp         string
		expectedPrecision int
		expectedSuccess   bool
	}{
		{"2024-12-21 07:29:03.3", 1, true},
		{"2024-12-21 07:29:03", 0, true},
		{"2024-12-21 07:29:3.3312", -1, false}, // we expect :03, not :3
		{"2024-12-21 07:29:03.123456789", 9, true},
		{"2024-12-21 25:25:25.123", 3, true}, // we don't check for actual time validity
	}
	for _, tc := range testcases {
		precision, success := FindTimestampPrecision(tc.timestamp)
		assert.Equal(t, tc.expectedSuccess, success)
		if success {
			assert.Equal(t, tc.expectedPrecision, precision)
		}
	}
}
