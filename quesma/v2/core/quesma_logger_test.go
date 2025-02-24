// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package quesma_api

import (
	"bytes"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
)

func TestDeduplicatedEvents(t *testing.T) {
	var buf bytes.Buffer

	log := zerolog.New(&buf).Level(zerolog.DebugLevel)
	logger := NewQuesmaLogger(log)

	logger.DeduplicatedInfo().Msgf("info test %d", 42)
	logger.DeduplicatedInfo().Msgf("info test %d", 42) // duplicate should be skipped

	logger.DeduplicatedWarn().Msgf("warn test %d", 42)
	logger.DeduplicatedWarn().Msgf("warn test %d", 42)   // duplicate should be skipped
	logger.DeduplicatedWarn().Msgf("warn test %d", 1000) // distinct argument
	logger.DeduplicatedWarn().Msgf("WARN test %d", 42)   // distinct format string

	logger.DeduplicatedWarn().Msgf("two args %d %s %v", 42, "asda", []byte{1, 2, 3})
	logger.DeduplicatedWarn().Msgf("two args %d %s %v", 42, "asda", []byte{1, 2, 3})  // duplicate should be skipped
	logger.DeduplicatedWarn().Msgf("two args %d %s %v", 45, "asda", []byte{1, 2, 3})  // distinct argument
	logger.DeduplicatedWarn().Msgf("two args %d %s %v", 42, "asda2", []byte{1, 2, 3}) // distinct argument
	logger.DeduplicatedWarn().Msgf("two args %d %s %v", 42, "asda", []byte{5, 2, 3})  // distinct argument
	logger.DeduplicatedWarn().Msgf("two args %d %s %v", 42, "asda2", []byte{1, 2, 3}) // duplicate should be skipped

	output := buf.String()

	assert.Equal(t, 1, strings.Count(output, "info test 42"))
	assert.Equal(t, 1, strings.Count(output, "warn test 42"))
	assert.Equal(t, 1, strings.Count(output, "warn test 1000"))
	assert.Equal(t, 1, strings.Count(output, "WARN test 42"))
	assert.Equal(t, 1, strings.Count(output, "two args 42 asda [1 2 3]"))
	assert.Equal(t, 1, strings.Count(output, "two args 45 asda [1 2 3]"))
	assert.Equal(t, 1, strings.Count(output, "two args 42 asda2 [1 2 3]"))
	assert.Equal(t, 1, strings.Count(output, "two args 42 asda [5 2 3]"))
}
