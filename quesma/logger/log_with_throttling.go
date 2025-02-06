// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package logger

import (
	"context"
	"github.com/QuesmaOrg/quesma/quesma/util"
	"time"
)

// throttleMap: (reason name -> last logged time)
// We log only once per throttleDuration for each reason name, so that we don't spam the logs.
var throttleMap = util.NewSyncMap[string, time.Time]()

const throttleDuration = 30 * time.Minute

// WarnWithCtxAndThrottling - logs a warning message when encountering unexpected parameter in query.
// We only log once per throttleDuration for each aggrName+paramName combination, so that we don't spam the logs.
// Very simple mechanism, good enough for this specific use case.
// Probably will require (at least slight) refactor, if you need it for some other things.
func WarnWithCtxAndThrottling(ctx context.Context, aggrName, paramName, format string, v ...any) {
	mapKey := aggrName + paramName
	timestamp, ok := throttleMap.Load(mapKey)
	weThrottle := ok && time.Since(timestamp) < throttleDuration
	if !weThrottle {
		WarnWithCtx(ctx).Msgf(format, v...)
		throttleMap.Store(mapKey, time.Now())
	}
}

// WarnWithThrottling - logs a warning message with throttling.
// We only log once per throttleDuration for each warnName, so that we don't spam the logs.
func WarnWithThrottling(reasonName, format string, v ...any) {
	timestamp, ok := throttleMap.Load(reasonName)
	weThrottle := ok && time.Since(timestamp) < throttleDuration
	if !weThrottle {
		Warn().Msgf(format, v...)
		throttleMap.Store(reasonName, time.Now())
	}
}
