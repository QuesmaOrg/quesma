// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package main

import (
	"context"
	"testing"
	"time"
)

// TestMain - just to make sure that the new func is used, this test will be removed
func TestMain(m *testing.M) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
	defer cancel()
	done := make(chan struct{})
	go func() {
		main2()
		close(done)
	}()

	select {
	case <-ctx.Done():
	case <-done:
	}
}
