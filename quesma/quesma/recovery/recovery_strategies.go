package recovery

import (
	"errors"
	"mitmproxy/quesma/logger"
	"runtime/debug"
	"sync/atomic"
)

// This counter is used by Phone Home Agent.
// We don't call the agent directly from here to
// make the recovery simple as possible.
var PanicCounter atomic.Int64

func LogPanic() {
	r := recover()
	if r != nil {
		PanicCounter.Add(1)
		var err error
		switch t := r.(type) {
		case string:
			err = errors.New(t)
		case error:
			err = t
		default:
			err = errors.New("unknown error")
		}
		logger.Error().Msgf("Panic recovered: %s\n%s", err, string(debug.Stack()))
	}
}
