// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package logger

import (
	"github.com/rs/zerolog"
	"os"
)

type errorFileLogger struct {
	file *os.File
}

// Doesn't do anything, it's never used if `WriteLevel` is also implemented.
// Only here to satisfy the interface.
func (e errorFileLogger) Write(p []byte) (n int, err error) {
	return len(p), nil
}

func (e errorFileLogger) WriteLevel(level zerolog.Level, p []byte) (n int, err error) {
	if level >= zerolog.WarnLevel {
		return e.file.Write(p)
	}
	return len(p), nil
}
