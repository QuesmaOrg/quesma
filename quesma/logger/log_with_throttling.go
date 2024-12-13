// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package logger

import (
	"context"
	"quesma/util"
	"time"
)

var throttleMap = util.SyncMap[string, time.Time]{}

// WarnWithCtxAndThrottling - logs a warning message when encountering unexpected parameter in query.
// Very simple mechanism, probably will require (slight) refactor, if you need it for other use cases.
func WarnWithCtxAndThrottling(ctx context.Context, aggrName, paramName, format string, v ...any) {
	mapKey := aggrName + paramName
	timestamp, ok := throttleMap.Load(mapKey)
	weThrottle := ok && time.Since(timestamp) < 3*time.Minute
	if !weThrottle {
		WarnWithCtx(ctx).Msgf(format, v...)
		throttleMap.Store(mapKey, time.Now())
	}
}
