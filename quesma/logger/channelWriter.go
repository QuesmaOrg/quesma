// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package logger

import (
	"github.com/rs/zerolog"
)

type channelWriter struct {
	ch chan LogWithLevel
}

func (w *channelWriter) Write(p []byte) (n int, err error) {
	s := string(p)
	w.ch <- LogWithLevel{Level: zerolog.NoLevel, Msg: s}
	return len(s), nil
}

func (w *channelWriter) WriteLevel(level zerolog.Level, p []byte) (n int, err error) {
	s := string(p)
	w.ch <- LogWithLevel{Level: level, Msg: s}
	return len(s), nil
}
