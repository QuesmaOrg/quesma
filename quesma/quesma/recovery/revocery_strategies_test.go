// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package recovery

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestLogPanic(t *testing.T) {

	fn := func() {
		defer LogPanic()
		panic("test")
	}

	before := PanicCounter.Load()
	fn()

	assert.Equal(t, before+1, PanicCounter.Load())

}
