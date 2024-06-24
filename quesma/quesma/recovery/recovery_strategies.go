package recovery

import (
	"context"
	"errors"
	"github.com/rs/zerolog"
	"quesma/logger"
	"runtime/debug"
	"sync/atomic"
)

// This counter is used by Phone Home Agent.
// We don't call the agent directly from here to
// make the recovery simple as possible.
var PanicCounter atomic.Int64

func commonRecovery(r any, panicLogger func() *zerolog.Event) {
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
	panicLogger().Msgf("Panic recovered: %s\n%s", err, string(debug.Stack()))
}

func LogPanic() {
	if r := recover(); r != nil {
		commonRecovery(r, logger.Error)
	}
}

func LogPanicWithCtx(ctx context.Context) {
	if r := recover(); r != nil {
		commonRecovery(r, func() *zerolog.Event {
			return logger.ErrorWithCtx(ctx)
		})
	}
}

func LogAndHandlePanic(ctx context.Context, cleanupHandler func(err error)) {
	if r := recover(); r != nil {
		commonRecovery(r, func() *zerolog.Event {
			return logger.ErrorWithCtx(ctx)
		})
		cleanupHandler(errors.New("panic recovered " + string(debug.Stack())))
	}
}
