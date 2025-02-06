// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package ui

import (
	"fmt"
	"github.com/QuesmaOrg/quesma/quesma/logger"
	"github.com/QuesmaOrg/quesma/quesma/model"
	"github.com/QuesmaOrg/quesma/quesma/quesma/ui/internal/builder"
	"github.com/rs/zerolog"
	"net/url"
	"regexp"
	"sort"
)

const UnrecognizedQueryType = "unrecognized"

var unsupportedSearchQueryRegex, _ = regexp.Compile(logger.Reason + `":"` + logger.ReasonPrefixUnsupportedQueryType + `([[:word:]]+)"`)

func processUnsupportedLogMessage(log logger.LogWithLevel) *string {
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
	for _, queryType := range model.AllQueryTypes {
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
	var debugKeyValueSlice []queryDebugInfoWithId

	qmc.mutex.Lock()
	for i := len(qmc.debugLastMessages) - 1; i >= 0; i-- {
		debugInfo := qmc.debugInfoMessages[qmc.debugLastMessages[i]]
		if debugInfo.unsupported != nil && len(debugKeyValueSlice) < maxLastMessages {
			debugKeyValueSlice = append(debugKeyValueSlice,
				queryDebugInfoWithId{qmc.debugLastMessages[i], qmc.debugInfoMessages[qmc.debugLastMessages[i]]})
		}
	}
	qmc.mutex.Unlock()

	types := qmc.GetUnsupportedTypesWithCount()

	type typeCount struct {
		name  string
		count int
	}
	var slice []typeCount
	for name, count := range types {
		slice = append(slice, typeCount{name, count})
	}
	sort.Slice(slice, func(i, j int) bool {
		return slice[i].count > slice[j].count
	})

	var buffer builder.HtmlBuffer
	buffer.Html("<br />")
	buffer.Html(`<h3>Unsupported queries by type</h3>`)
	buffer.Html(`<ul id="unsupported-queries-stats">`)
	for _, t := range slice {
		buffer.Html(fmt.Sprintf(`<li><a class="debug-warn-log" href="/unsupported-requests/%s">`, url.PathEscape(t.name)))
		buffer.Text(fmt.Sprintf(`%s: %d`, t.name, t.count))
		buffer.Html("</a></li>\n")
	}
	buffer.Html("</ul>")

	return qmc.generateReportForRequests("Unsupported requests", debugKeyValueSlice, buffer.Bytes())
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

func (qmc *QuesmaManagementConsole) QueriesWithUnsupportedType(typeName string) []queryDebugInfoWithId {
	var debugKeyValueSlice []queryDebugInfoWithId

	qmc.mutex.Lock()
	for i := len(qmc.debugLastMessages) - 1; i >= 0; i-- {
		debugInfo := qmc.debugInfoMessages[qmc.debugLastMessages[i]]
		if debugInfo.unsupported != nil && len(debugKeyValueSlice) < maxLastMessages {
			if *debugInfo.unsupported == typeName {
				debugKeyValueSlice = append(debugKeyValueSlice,
					queryDebugInfoWithId{qmc.debugLastMessages[i], qmc.debugInfoMessages[qmc.debugLastMessages[i]]})
			}
		}
	}
	qmc.mutex.Unlock()

	return debugKeyValueSlice
}

func (qmc *QuesmaManagementConsole) generateReportForUnsupportedType(typeName string) []byte {
	requests := qmc.QueriesWithUnsupportedType(typeName)
	return qmc.generateReportForRequests("Report for unsupported request '"+typeName+"'", requests, []byte{})
}
