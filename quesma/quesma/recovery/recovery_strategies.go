// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
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

func recoveredToError(r any) error {
	switch t := r.(type) {
	case string:
		return errors.New(t)
	case error:
		return t
	default:
		return errors.New("unknown error")
	}
}

func commonRecovery(r any, panicLogger func() *zerolog.Event) {
	PanicCounter.Add(1)
	panicLogger().Msgf("Panic recovered: %s\n%s", recoveredToError(r), string(debug.Stack()))
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
		cleanupHandler(errors.New("Panic recovered: " + recoveredToError(r).Error() + "\n" + string(debug.Stack())))
	}
}
