// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package main

import "testing"

// just to make sure that the buildIngestOnlyQuesma is used
func TestMain(m *testing.M) {
	_ = buildIngestOnlyQuesma()
}
