package errorstats

import (
	"fmt"
	"github.com/rs/zerolog"
	"mitmproxy/quesma/tracing"
	"sort"
	"sync"
	"time"
)

type (
	ErrorStatisticsStore struct {
		mutex        sync.Mutex
		RecentErrors []ErrorReport
	}

	ErrorReport struct {
		ReportedAt   time.Time
		CommonReason *string
		RequestId    *string
		DebugMessage string
	}

	ErrorStatistics struct {
		Count  int
		Reason string
	}
)

const maxRecentErrors = 10000
const maxRecentErrorsCleanEvery = 100

var GlobalErrorStatistics ErrorStatisticsStore

func (e *ErrorStatisticsStore) RecordKnownError(reason string, requestId *string, debugMessage string) {
	e.recordError(&reason, requestId, debugMessage)
}

func (e *ErrorStatisticsStore) RecordUnknownError(requestId *string, debugMessage string) {
	e.recordError(nil, requestId, debugMessage)
}

func (e *ErrorStatisticsStore) recordError(commonReason *string, requestId *string, debugMessage string) {
	e.mutex.Lock()
	defer e.mutex.Unlock()

	e.RecentErrors = append(e.RecentErrors, ErrorReport{
		ReportedAt:   time.Now(),
		CommonReason: commonReason,
		RequestId:    requestId,
		DebugMessage: debugMessage})
	if len(e.RecentErrors) > maxRecentErrors+maxRecentErrorsCleanEvery {
		e.RecentErrors = e.RecentErrors[maxRecentErrorsCleanEvery:]
	}
}

func (e *ErrorStatisticsStore) ErrorReportsForReason(reason string) []ErrorReport {
	e.mutex.Lock()
	defer e.mutex.Unlock()

	var reports []ErrorReport
	if reason == "Unknown" {
		for _, errorReport := range e.RecentErrors {
			if errorReport.CommonReason == nil {
				reports = append(reports, errorReport)
			}
		}
	} else {
		for _, errorReport := range e.RecentErrors {
			if errorReport.CommonReason != nil && *errorReport.CommonReason == reason {
				reports = append(reports, errorReport)
			}
		}
	}
	return reports
}

func (e *ErrorStatisticsStore) ReturnTopErrors(count int) []ErrorStatistics {
	if count <= 0 {
		return []ErrorStatistics{}
	}

	e.mutex.Lock()
	defer e.mutex.Unlock()

	// Count errors by reason
	countByReason := make(map[string]int)
	for _, errorReport := range e.RecentErrors {
		reason := "Unknown"
		if errorReport.CommonReason != nil {
			reason = *errorReport.CommonReason
		}
		if count, ok := countByReason[reason]; !ok {
			countByReason[reason] = 1
		} else {
			countByReason[reason] = count + 1
		}
	}

	// Convert to slice
	var errorStatistics []ErrorStatistics
	for reason, count := range countByReason {
		errorStatistics = append(errorStatistics, ErrorStatistics{Count: count, Reason: reason})
	}

	// Sort by count
	sort.Slice(errorStatistics, func(i, j int) bool {
		return errorStatistics[i].Count > errorStatistics[j].Count
	})

	// Return top 5
	if len(errorStatistics) < count {
		return errorStatistics
	} else {
		return errorStatistics[:count]
	}
}

type GlobalErrorHook struct{}

func (s *GlobalErrorHook) Run(e *zerolog.Event, level zerolog.Level, message string) {
	fmt.Println("JM: ", e, level, message)
	if level == zerolog.ErrorLevel || level == zerolog.FatalLevel || level == zerolog.PanicLevel {
		var requestId *string
		var reason *string
		if e != nil {
			if requestTmp, ok := e.GetCtx().Value(tracing.RequestIdCtxKey).(string); ok {
				requestId = &requestTmp
			}
			if reasonTmp, ok := e.GetCtx().Value(tracing.ReasonCtxKey).(string); ok {
				reason = &reasonTmp
			}
		}
		if reason != nil {
			GlobalErrorStatistics.RecordKnownError(*reason, requestId, "From hook known: "+message)
		} else {
			GlobalErrorStatistics.RecordUnknownError(requestId, "From hook unknown: "+message)
		}
	}
}
