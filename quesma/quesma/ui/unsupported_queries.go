package ui

import (
	"fmt"
	"github.com/rs/zerolog"
	"mitmproxy/quesma/logger"
	"mitmproxy/quesma/model"
	"mitmproxy/quesma/tracing"
	"regexp"
	"sync"
)

const UnrecognizedQueryType = "unrecognized"

var unsupportedSearchQueryRegex, _ = regexp.Compile(logger.Reason + `":"` + logger.ReasonPrefixUnsupportedQueryType + `([[:word:]]+)"`)

type UnsupportedSearchQueries struct {
	mutex                   sync.Mutex // it's a rare situation to not support some query, let's do everything here under this mutex for simplicity 	// how many we saved (max 10 per type)
	totalUnsupportedQueries int        // we many we've seen total
}

func processUnsupportedLogMessage(log tracing.LogWithLevel) *string {
	if log.Level != zerolog.ErrorLevel && log.Level != zerolog.WarnLevel { // only error and log
		return nil
	}
	match := unsupportedSearchQueryRegex.FindStringSubmatch(log.Msg)
	if len(match) < 2 {
		// there's no unsupported_search_query in the log message
		return nil
	}
	searchQueryType := match[1]

	knownType := false
	for _, queryType := range model.AggregationQueryTypes {
		if queryType == searchQueryType {
			knownType = true
			break
		}
	}

	if !knownType {
		searchQueryType = UnrecognizedQueryType
	}
	//fmt.Println("JM searchQueryType:", searchQueryType, "log.Msg", log.Msg)

	return &searchQueryType
}

func (qmc *QuesmaManagementConsole) generateReportForUnsupportedRequests() []byte {
	var debugKeyValueSlice []DebugKeyValue

	qmc.mutex.Lock()
	for i := len(qmc.debugLastMessages) - 1; i >= 0; i-- {
		debugInfo := qmc.debugInfoMessages[qmc.debugLastMessages[i]]
		if debugInfo.unsupported != nil && len(debugKeyValueSlice) < maxLastMessages {
			debugKeyValueSlice = append(debugKeyValueSlice,
				DebugKeyValue{qmc.debugLastMessages[i], qmc.debugInfoMessages[qmc.debugLastMessages[i]]})
		}
	}
	qmc.mutex.Unlock()

	return qmc.generateReportForRequests("Report for unsupported requests", debugKeyValueSlice)
}

func (qmc *QuesmaManagementConsole) generateUnsupportedQuerySidePanel() []byte {
	qmc.mutex.Lock()
	totalErrorsCount := qmc.totalUnsupportedQueries
	qmc.mutex.Unlock()

	typesCount := qmc.GetUnsupportedTypesWithCount()
	savedErrorsCount := 0
	for _, count := range typesCount {
		savedErrorsCount += count
	}
	typesSeenCount := len(typesCount)
	unkownTypeCount := 0
	if value, ok := typesCount[UnrecognizedQueryType]; ok {
		unkownTypeCount = value
	}

	var buffer HtmlBuffer
	linkToMainView := `<li><a href="/unsupported-requests/"`
	buffer.Html(`<ul id="unsupported-queries-stats" hx-swap-oob="true">`)
	if totalErrorsCount > 0 {
		buffer.Html(fmt.Sprintf(`%s class="debug-warn-log"">%d total (%d recent)</a></li>`, linkToMainView, totalErrorsCount, savedErrorsCount))
		plural := "s"
		if typesSeenCount == 1 {
			plural = ""
		}
		buffer.Html(fmt.Sprintf(`%s class="debug-warn-log"">%d different type%s</a></li>`, linkToMainView, typesSeenCount, plural))
		if unkownTypeCount > 0 {
			buffer.Html(fmt.Sprintf(`%s class="debug-error-log"">%d of unrecognized type</a></li>`, linkToMainView, unkownTypeCount))
		}
	} else {
		buffer.Html(`<li>None!</a></li>`)
	}
	buffer.Html(`</ul>`)

	return buffer.Bytes()
}

func (qmc *QuesmaManagementConsole) GetTotalUnsupportedQueries() int {
	qmc.mutex.Lock()
	defer qmc.mutex.Unlock()
	return qmc.totalUnsupportedQueries
}

func (qmc *QuesmaManagementConsole) GetSavedUnsupportedQueries() int {
	unsupportedSaveQuery := 0

	qmc.mutex.Lock()
	defer qmc.mutex.Unlock()
	for i := len(qmc.debugLastMessages) - 1; i >= 0; i-- {
		debugInfo := qmc.debugInfoMessages[qmc.debugLastMessages[i]]
		if debugInfo.unsupported != nil {
			unsupportedSaveQuery++
		}
	}

	return unsupportedSaveQuery
}

func (qmc *QuesmaManagementConsole) GetUnsupportedTypesWithCount() map[string]int {
	types := make(map[string]int)

	qmc.mutex.Lock()
	defer qmc.mutex.Unlock()
	for i := len(qmc.debugLastMessages) - 1; i >= 0; i-- {
		debugInfo := qmc.debugInfoMessages[qmc.debugLastMessages[i]]
		if debugInfo.unsupported != nil {
			if value, ok := types[*debugInfo.unsupported]; !ok {
				types[*debugInfo.unsupported] = 1
			} else {
				types[*debugInfo.unsupported] = value + 1
			}
		}
	}

	return types
}

func (qmc *QuesmaManagementConsole) QueriesWithUnsupportedType(typeName string) []DebugKeyValue {
	var debugKeyValueSlice []DebugKeyValue

	qmc.mutex.Lock()
	for i := len(qmc.debugLastMessages) - 1; i >= 0; i-- {
		debugInfo := qmc.debugInfoMessages[qmc.debugLastMessages[i]]
		if debugInfo.unsupported != nil && len(debugKeyValueSlice) < maxLastMessages {
			if *debugInfo.unsupported == typeName {
				debugKeyValueSlice = append(debugKeyValueSlice,
					DebugKeyValue{qmc.debugLastMessages[i], qmc.debugInfoMessages[qmc.debugLastMessages[i]]})
			}
		}
	}
	qmc.mutex.Unlock()

	return debugKeyValueSlice
}

func (qmc *QuesmaManagementConsole) generateReportForUnsupportedType(typeName string) []byte {
	debugKeyValueSlice := qmc.QueriesWithUnsupportedType(typeName)
	return qmc.generateReportForRequests("Report for unsupported request '"+typeName+"'", debugKeyValueSlice)
}
