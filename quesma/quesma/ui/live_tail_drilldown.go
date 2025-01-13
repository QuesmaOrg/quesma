// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package ui

import (
	"encoding/base64"
	"fmt"
	"github.com/QuesmaOrg/quesma/quesma/quesma/ui/internal/builder"
	"github.com/QuesmaOrg/quesma/quesma/util"
	"github.com/QuesmaOrg/quesma/v2/core/diag"
	tracing "github.com/QuesmaOrg/quesma/v2/core/tracing"
	"github.com/goccy/go-json"
	"gopkg.in/yaml.v3"
	"strings"
)

func (qmc *QuesmaManagementConsole) generateReportForRequestId(requestId string) []byte {
	var request queryDebugInfo
	var requestFound bool
	qmc.mutex.Lock()
	if strings.HasPrefix(requestId, tracing.AsyncIdPrefix) {
		for _, debugInfo := range qmc.debugInfoMessages {
			if debugInfo.AsyncId == requestId {
				request = debugInfo
				requestFound = true
				break
			}
		}
	} else {
		request, requestFound = qmc.debugInfoMessages[requestId]
	}
	qmc.mutex.Unlock()

	logMessages := generateLogMessages(request.logMessages, []string{})

	buffer := newBufferWithHead()
	if requestFound {
		if len(request.AsyncId) > 0 {
			buffer.Write(generateSimpleTop("Report for request id " + requestId + " and async id " + request.AsyncId))
		} else {
			buffer.Write(generateSimpleTop("Report for request id " + requestId))
		}
	} else {
		buffer.Write(generateSimpleTop("Report not found for request UUID " + requestId))
	}

	buffer.Html(`<main id="request-info">` + "\n")

	// Show Request and SQL
	if requestFound {

		buffer.Html(`<div>` + "\n")
		buffer.Html(`<div class="query-body">` + "\n")
		buffer.Html("<p class=\"title\">Original query:</p>\n")
		buffer.Html(`<pre>`)
		buffer.Text(string(request.IncomingQueryBody))
		buffer.Html("\n</pre>")
		buffer.Html(`</div>` + "\n")

		buffer.Html(`<div class="query-body-translated">` + "\n")
		buffer.Html("<p class=\"title\">Translated SQL:</p>\n")

		printQueries := func(queries []diag.TranslatedSQLQuery) {

			for _, queryBody := range queries {
				prettyQueryBody := util.SqlPrettyPrint(queryBody.Query)
				if qmc.cfg.ClickHouse.AdminUrl != nil {
					// ClickHouse web UI /play expects a base64-encoded query
					// in the URL:
					base64QueryBody := base64.StdEncoding.EncodeToString([]byte(prettyQueryBody))
					buffer.Html(`<a href="`).Text(qmc.cfg.ClickHouse.AdminUrl.String()).Text("/play#").Text(base64QueryBody).Html(`">`)
				}
				buffer.Html(`<pre>`)
				buffer.Text(prettyQueryBody)
				buffer.Html("\n</pre>")
				if qmc.cfg.ClickHouse.AdminUrl != nil {
					buffer.Html(`</a>`)
				}
				buffer.Html(`<pre>`)
				buffer.Text("\n")
				qmc.printPerformanceResult(&buffer, queryBody)
				buffer.Html("\n</pre>")
			}
		}

		printQueries(request.QueryBodyTranslated)

		if request.alternativePlanDebugSecondarySource != nil {
			buffer.Html("\n--  Alternative plan queries ---------------------\n")
			printQueries(request.alternativePlanDebugSecondarySource.QueryBodyTranslated)
		}

		buffer.Html(`</div>` + "\n")

		buffer.Html(`<div class="elastic-response">` + "\n")
		if len(request.QueryDebugPrimarySource.QueryResp) > 0 {
			tookStr := fmt.Sprintf(" took %d ms:", request.PrimaryTook.Milliseconds())
			buffer.Html("<p class=\"title\">Elastic response").Text(tookStr).Html("</p>\n")
			buffer.Html(`<pre>`)
			buffer.Text(string(request.QueryDebugPrimarySource.QueryResp))
			buffer.Html("\n</pre>")
		} else {
			buffer.Html("<p class=\"title\">No Elastic response for this request</p>\n")
		}
		buffer.Html(`</div>` + "\n")

		buffer.Html(`<div class="quesma-response">` + "\n")
		if len(request.QueryDebugSecondarySource.QueryTranslatedResults) > 0 {
			buffer.Html("<p class=\"title\">Quesma response").Html("</p>\n")
			buffer.Html(`<pre>`)
			buffer.Text(string(request.QueryDebugSecondarySource.QueryTranslatedResults))
			buffer.Html("\n</pre>")
		} else {
			buffer.Html("<p class=\"title\">No Quesma response for this request</p>\n")
		}

		if request.alternativePlanDebugSecondarySource != nil {
			if len(request.alternativePlanDebugSecondarySource.QueryTranslatedResults) > 0 {
				buffer.Html("<p class=\"title\">Quesma alternative plan response").Html("</p>\n")
				buffer.Html(`<pre>`)
				buffer.Text(string(request.alternativePlanDebugSecondarySource.QueryTranslatedResults))
				buffer.Html("\n</pre>")
			} else {
				buffer.Html("<p class=\"title\">No Quesma alternative plan response for this request</p>\n")
			}

			// TODO add JSON diff here
		}

		buffer.Html(`</div>` + "\n")

		buffer.Html(`</div>` + "\n")
	}

	buffer.Html("\n\n")
	buffer.Html(`<div class="debug-body">`)

	buffer.Html(`<p class="title title-logs">`)
	if requestFound && len(request.logMessages) > 0 {
		buffer.Html("Logs:</p>\n")
		buffer.Write(logMessages)
	} else {
		buffer.Html("No logs for this request</p>\n")
	}
	buffer.Html("\n</div>\n")

	buffer.Html("\n</main>\n")
	buffer.Html(`<div class="menu">`)
	buffer.Html("\n<h2>Menu</h2>")

	buffer.Html(`<form action="/live">&nbsp;<input class="btn" type="submit" value="Back to live tail" /></form>`)
	buffer.Html(`<br>`)

	if requestFound {
		buffer.Html("\n<h2>Request info</h2>")
		buffer.Html("<ul>\n")
		buffer.Html("<li>").Text("Request id: ").Text(requestId).Html("</li>\n")
		buffer.Html("<li>").Text("Path: ").Text(request.Path).Html("</li>\n")
		if len(request.AsyncId) > 0 {
			buffer.Html("<li>").Text("Async id: ").Text(request.AsyncId).Html("</li>\n")
		}
		if request.OpaqueId != "" {
			buffer.Html("<li>").Text("Opaque id: ").Text(request.OpaqueId).Html("</li>\n")
		}

		if request.unsupported != nil {
			buffer.Html("<li>").Text("Unsupported: ").Text(*request.unsupported).Html("</li>\n")
		}
		tookStr := fmt.Sprintf("Took: %d ms", request.SecondaryTook.Milliseconds())
		buffer.Html("<li>").Text(tookStr).Html("</li>")
		buffer.Html("</ul>\n")
	}

	buffer.Html("\n<h2>Log types</h2>")
	if requestFound {
		buffer.Html(`<ul>`)
		if request.errorLogCount > 0 {
			buffer.Html(fmt.Sprintf(`<li class="debug-error-log">%d error logs</li>`, request.errorLogCount))
		} else {
			buffer.Html(`<li>0 error logs</li>`)
		}
		if request.warnLogCount > 0 {
			buffer.Html(fmt.Sprintf(`<li class="debug-warn-log">%d warn logs</li>`, request.warnLogCount))
		} else {
			buffer.Html(`<li>0 warn logs</li>`)
		}

		buffer.Html(`</ul>`)
	}

	buffer.Html("\n</div>")
	buffer.Html("\n</body>")
	buffer.Html("\n</html>")
	return buffer.Bytes()
}

// links might be empty, then table won't have any links within.
// if i < len(logMessages) && i < len(links) then logMessages[i] will have link links[i]
func generateLogMessages(logMessages []string, links []string) []byte {
	// adds a link to the table row if there is a link for it
	addOpeningLink := func(row, column int) string {
		if row < len(links) {
			link := `<a href="` + links[row] + `" class="row-link"`
			if column != 0 {
				link += ` tabindex="-1"` // some way to make links in table prettier, see https://robertcooper.me/post/table-row-links
			}
			return link + ">"
		}
		return ""
	}
	addClosingLink := func(i int) string {
		if i < len(links) {
			return "</a>"
		}
		return ""
	}

	var buffer builder.HtmlBuffer
	buffer.Html("<table>\n")
	buffer.Html("<thead>\n")
	buffer.Html("<tr>\n")
	buffer.Html(`<th class="time">Time Level</th>`)
	buffer.Html(`<th class="message">Message</th>`)
	buffer.Html(`<th class="fields">Fields</th>`)
	buffer.Html("</tr>\n")

	buffer.Html("</thead>\n")
	buffer.Html("<tbody>\n")

	for i, logMessage := range logMessages {
		buffer.Html("<tr>\n")

		var fields map[string]interface{}

		if err := json.Unmarshal([]byte(logMessage), &fields); err != nil {
			// error print
			buffer.Html("<td></td><td>error</td><td></td>").Text(err.Error()).Html("<td>")
			continue
		}
		// time
		buffer.Html(`<td class="time">` + addOpeningLink(i, 0))
		buffer.Html("<span>")
		if _, ok := fields["time"]; ok {
			time := fields["time"].(string)
			time = strings.Replace(time, "T", " ", 1)
			time = strings.Replace(time, ".", " ", 1)
			buffer.Text(time)
			delete(fields, "time")
		} else {
			buffer.Html("missing time")
		}
		buffer.Html("</span>\n<br>\n")
		// level
		if level, ok := fields["level"].(string); ok {
			if level == "error" {
				buffer.Html(`<span class="debug-error-log">`)
			} else if level == "warn" {
				buffer.Html(`<span class="debug-warn-log">`)
			} else {
				buffer.Html(`<span>`)
			}
			buffer.Text(level).Html("</span></td>")
			delete(fields, "level")
		} else {
			buffer.Html("missing level")
		}
		buffer.Html(addClosingLink(i) + "</td>")

		// get rid of request_id and async_id
		delete(fields, "request_id")
		delete(fields, "async_id")

		// message
		buffer.Html(`<td class="message">` + addOpeningLink(i, 2))
		if message, ok := fields["message"].(string); ok {
			buffer.Text(message)
			delete(fields, "message")
		}
		buffer.Html(addClosingLink(i) + "</td>")

		// fields
		buffer.Html(`<td class="fields">` + addOpeningLink(i, 3))
		if rest, err := yaml.Marshal(&fields); err == nil {
			buffer.Text(string(rest))
		}
		buffer.Html(addClosingLink(i) + "</td></tr>\n")
	}

	buffer.Html("</tbody>\n")
	buffer.Html("</table>\n")
	return buffer.Bytes()
}

func (qmc *QuesmaManagementConsole) generateReportForRequestsWithStr(requestStr string) []byte {
	var debugKeyValueSlice []queryDebugInfoWithId

	qmc.mutex.Lock()
	for i := len(qmc.debugLastMessages) - 1; i >= 0; i-- {
		debugInfo := qmc.debugInfoMessages[qmc.debugLastMessages[i]]
		if debugInfo.requestContains(requestStr) && len(debugKeyValueSlice) < maxLastMessages {
			debugKeyValueSlice = append(debugKeyValueSlice,
				queryDebugInfoWithId{qmc.debugLastMessages[i], qmc.debugInfoMessages[qmc.debugLastMessages[i]]})
		}
	}
	qmc.mutex.Unlock()

	title := fmt.Sprintf("Report for str '%s' with %d results", requestStr, len(debugKeyValueSlice))
	return qmc.generateReportForRequests(title, debugKeyValueSlice, []byte{})
}

func (qmc *QuesmaManagementConsole) generateReportForRequestsWithError() []byte {
	var debugKeyValueSlice []queryDebugInfoWithId

	qmc.mutex.Lock()
	for i := len(qmc.debugLastMessages) - 1; i >= 0; i-- {
		debugInfo := qmc.debugInfoMessages[qmc.debugLastMessages[i]]
		if debugInfo.errorLogCount > 0 && len(debugKeyValueSlice) < maxLastMessages {
			debugKeyValueSlice = append(debugKeyValueSlice,
				queryDebugInfoWithId{qmc.debugLastMessages[i], qmc.debugInfoMessages[qmc.debugLastMessages[i]]})
		}
	}
	qmc.mutex.Unlock()

	return qmc.generateReportForRequests("Report for requests with errors", debugKeyValueSlice, []byte{})
}

func (qmc *QuesmaManagementConsole) generateReportForRequestsWithWarning() []byte {
	var debugKeyValueSlice []queryDebugInfoWithId

	qmc.mutex.Lock()
	for i := len(qmc.debugLastMessages) - 1; i >= 0; i-- {
		debugInfo := qmc.debugInfoMessages[qmc.debugLastMessages[i]]
		if debugInfo.warnLogCount > 0 && len(debugKeyValueSlice) < maxLastMessages {
			debugKeyValueSlice = append(debugKeyValueSlice,
				queryDebugInfoWithId{qmc.debugLastMessages[i], qmc.debugInfoMessages[qmc.debugLastMessages[i]]})
		}
	}
	qmc.mutex.Unlock()

	return qmc.generateReportForRequests("Report for requests with warnings", debugKeyValueSlice, []byte{})
}

func (qmc *QuesmaManagementConsole) generateReportForRequests(title string, requests []queryDebugInfoWithId, sidebar []byte) []byte {
	buffer := newBufferWithHead()
	buffer.Write(generateSimpleTop(title))

	buffer.Html("\n</div>\n\n")

	buffer.Html(`<main id="queries">`)

	buffer.Write(qmc.populateQueries(requests, true))

	buffer.Html("\n</main>\n\n")

	buffer.Html(`<div class="menu">`)
	buffer.Html("\n<h2>Menu</h2>")

	buffer.Html(`<form action="/live">&nbsp;<input class="btn" type="submit" value="Back to live tail" /></form>`)
	buffer.Write(sidebar)

	buffer.Html("\n</div>")
	buffer.Html("\n</body>")
	buffer.Html("\n</html>")

	return buffer.Bytes()
}
