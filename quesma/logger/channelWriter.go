package logger

import (
	"github.com/rs/zerolog"
	"mitmproxy/quesma/tracing"
)

type channelWriter struct {
	ch chan tracing.LogWithLevel
}

func (w channelWriter) Write(p []byte) (n int, err error) {
	s := string(p)
	w.ch <- tracing.LogWithLevel{Level: zerolog.NoLevel, Msg: s}
	return len(s), nil
}

func (w channelWriter) WriteLevel(level zerolog.Level, p []byte) (n int, err error) {
	s := string(p)
	w.ch <- tracing.LogWithLevel{Level: level, Msg: s}
	return len(s), nil
}
