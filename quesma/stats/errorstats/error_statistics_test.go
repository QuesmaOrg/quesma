// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package errorstats

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestErrorStatisticsStore_ReturnTop5Errors(t *testing.T) {
	store := ErrorStatisticsStore{}

	t.Run("empty", func(t *testing.T) {
		top5 := store.ReturnTopErrors(5)
		assert.Equal(t, 0, len(top5))
	})

	t.Run("one", func(t *testing.T) {
		store.RecordUnknownError(nil, "error1")
		top5 := store.ReturnTopErrors(5)
		assert.Equal(t, 1, len(top5))
		assert.Equal(t, 1, top5[0].Count)
		assert.Equal(t, "Unknown", top5[0].Reason)
	})

	t.Run("two", func(t *testing.T) {
		panicStr := "Panic"
		store.RecordKnownError(panicStr, nil, "error1")
		store.RecordKnownError(panicStr, nil, "error2")
		top5 := store.ReturnTopErrors(5)
		assert.Equal(t, 2, len(top5))
		assert.Equal(t, 2, top5[0].Count)
		assert.Equal(t, panicStr, top5[0].Reason)
		assert.Equal(t, 1, top5[1].Count)
		assert.Equal(t, "Unknown", top5[1].Reason)
	})

	t.Run("many", func(t *testing.T) {
		for i := 0; i < 10; i++ {
			reason := fmt.Sprintf("Reason %d", i)
			for j := 0; j < i+10; j++ {
				store.RecordKnownError(reason, nil, "Another error")
			}
		}
		top5 := store.ReturnTopErrors(5)
		assert.Equal(t, 5, len(top5))
		assert.Equal(t, 19, top5[0].Count)
		assert.Equal(t, "Reason 9", top5[0].Reason)
		assert.Equal(t, 18, top5[1].Count)
		assert.Equal(t, "Reason 8", top5[1].Reason)
		assert.Equal(t, 17, top5[2].Count)
		assert.Equal(t, "Reason 7", top5[2].Reason)
		assert.Equal(t, 16, top5[3].Count)
		assert.Equal(t, "Reason 6", top5[3].Reason)
		assert.Equal(t, 15, top5[4].Count)
		assert.Equal(t, "Reason 5", top5[4].Reason)
	})

	t.Run("cleanup", func(t *testing.T) {
		var newStore ErrorStatisticsStore
		newStore.RecordKnownError("error", nil, "error")
		for i := 0; i < maxRecentErrors+maxRecentErrorsCleanEvery; i++ {
			newStore.RecordUnknownError(nil, "error")
		}
		top5 := newStore.ReturnTopErrors(5)
		assert.Equal(t, 1, len(top5))
		assert.Equal(t, "Unknown", top5[0].Reason)
	})
}
