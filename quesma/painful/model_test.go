// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package painful

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestExpectDate(t *testing.T) {
	expectedLocation := time.FixedZone("CEST", 2*60*60) // +0200 offset
	res, e := ExpectDate("2024-10-16 13:43:40 +0200 CEST")
	assert.NoError(t, e)
	res = res.In(expectedLocation)
	assert.Equal(t, res, time.Date(2024, 10, 16, 13, 43, 40, 0, expectedLocation))
}
