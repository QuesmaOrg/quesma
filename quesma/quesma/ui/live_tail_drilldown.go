package ui

import (
	"encoding/json"
	"fmt"
	"gopkg.in/yaml.v3"
	"mitmproxy/quesma/quesma/ui/internal/buffer"
	"strings"
)

func (qmc *QuesmaManagementConsole) generateReportForRequestId(requestId string) []byte {
	qmc.mutex.Lock()
	request, requestFound := qmc.debugInfoMessages[requestId]
	qmc.mutex.Unlock()

	buffer := newBufferWithHead()
	if requestFound {
		buffer.Write(generateSimpleTop("Report for request UUID " + requestId))
	} else {
		buffer.Write(generateSimpleTop("Report not found for request UUID " + requestId))
	}

	buffer.Html(`<main id="queries">`)

	debugKeyValueSlice := []DebugKeyValue{}
	if requestFound {
		debugKeyValueSlice = append(debugKeyValueSlice, DebugKeyValue{requestId, request})
	}

	buffer.Write(generateQueries(debugKeyValueSlice, false))

	buffer.Html("\n</main>\n")
	buffer.Html(`<div class="menu">`)
	buffer.Html("\n<h2>Menu</h2>")

	buffer.Html(`<form action="/live">&nbsp;<input class="btn" type="submit" value="Back to live tail" /></form>`)
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
	buffer.Html(`<form action="/log/`).Text(requestId).Html(`">&nbsp;<input class="btn" type="submit" value="Go to log" /></form>`)

	buffer.Html("\n</div>")
	buffer.Html("\n</body>")
	buffer.Html("\n</html>")
	return buffer.Bytes()
}

func (qmc *QuesmaManagementConsole) generateLogForRequestId(requestId string) []byte {
	qmc.mutex.Lock()
	request, requestFound := qmc.debugInfoMessages[requestId]
	qmc.mutex.Unlock()

	logMessages, optAsyncId := generateLogMessages(request.logMessages, []string{})

	buffer := newBufferWithHead()
	if requestFound {
		if optAsyncId != nil {
			buffer.Write(generateSimpleTop("Log for request id " + requestId + " and async id " + *optAsyncId))
		} else {
			buffer.Write(generateSimpleTop("Log for request id " + requestId))
		}
	} else {
		buffer.Write(generateSimpleTop("Log not found for request id " + requestId))
	}

	buffer.Html(`<main class="center" id="request-log-messages">`)
	buffer.Html("\n\n")
	buffer.Html(`<div class="debug-body">`)

	buffer.Write(logMessages)

	buffer.Html("\n</div>\n")
	buffer.Html("\n</main>\n")
	buffer.Html(`<div class="menu">`)
	buffer.Html("\n<h2>Menu</h2>")

	buffer.Html(`<form action="/live">&nbsp;<input class="btn" type="submit" value="Back to live tail" /></form>`)
	buffer.Html(`<br>`)
	buffer.Html(`<form action="/request-id/`).Text(requestId).Html(`">&nbsp;<input class="btn" type="submit" value="Back to request info" /></form>`)

	buffer.Html("\n</div>")
	buffer.Html("\n</body>")
	buffer.Html("\n</html>")
	return buffer.Bytes()
}

// links might be empty, then table won't have any links within.
// if i < len(logMessages) && i < len(links) then logMessages[i] will have link links[i]
func generateLogMessages(logMessages []string, links []string) ([]byte, *string) {
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

	var buffer buffer.HtmlBuffer
	buffer.Html("<table>\n")
	buffer.Html("<thead>\n")
	buffer.Html("<tr>\n")
	buffer.Html(`<th class="time">Time</th>`)
	buffer.Html(`<th class="level">Level</th>`)
	buffer.Html(`<th class="message">Message</th>`)
	buffer.Html(`<th class="fields">Fields</th>`)
	buffer.Html("</tr>\n")

	buffer.Html("</thead>\n")
	buffer.Html("<tbody>\n")

	var asyncId *string

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
		if _, ok := fields["time"]; ok {
			time := fields["time"].(string)
			time = strings.Replace(time, "T", " ", 1)
			time = strings.Replace(time, ".", " ", 1)
			buffer.Text(time)
			delete(fields, "time")
		} else {
			buffer.Html("missing time")
		}
		buffer.Html(addClosingLink(i) + "</td>")

		// get rid of request_id and async_id
		delete(fields, "request_id")
		if id, ok := fields["async_id"].(string); ok {
			asyncId = &id
			delete(fields, "async_id")
		}

		// level
		buffer.Html(`<td class="level">` + addOpeningLink(i, 1))
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
	return buffer.Bytes(), asyncId
}

func (qmc *QuesmaManagementConsole) generateReportForRequestsWithStr(requestStr string) []byte {
	var debugKeyValueSlice []DebugKeyValue

	qmc.mutex.Lock()
	for i := len(qmc.debugLastMessages) - 1; i >= 0; i-- {
		debugInfo := qmc.debugInfoMessages[qmc.debugLastMessages[i]]
		if debugInfo.requestContains(requestStr) && len(debugKeyValueSlice) < maxLastMessages {
			debugKeyValueSlice = append(debugKeyValueSlice,
				DebugKeyValue{qmc.debugLastMessages[i], qmc.debugInfoMessages[qmc.debugLastMessages[i]]})
		}
	}
	qmc.mutex.Unlock()

	title := fmt.Sprintf("Report for str '%s' with %d results", requestStr, len(debugKeyValueSlice))
	return qmc.generateReportForRequests(title, debugKeyValueSlice, []byte{})
}

func (qmc *QuesmaManagementConsole) generateReportForRequestsWithError() []byte {
	var debugKeyValueSlice []DebugKeyValue

	qmc.mutex.Lock()
	for i := len(qmc.debugLastMessages) - 1; i >= 0; i-- {
		debugInfo := qmc.debugInfoMessages[qmc.debugLastMessages[i]]
		if debugInfo.errorLogCount > 0 && len(debugKeyValueSlice) < maxLastMessages {
			debugKeyValueSlice = append(debugKeyValueSlice,
				DebugKeyValue{qmc.debugLastMessages[i], qmc.debugInfoMessages[qmc.debugLastMessages[i]]})
		}
	}
	qmc.mutex.Unlock()

	return qmc.generateReportForRequests("Report for requests with errors", debugKeyValueSlice, []byte{})
}

func (qmc *QuesmaManagementConsole) generateReportForRequestsWithWarning() []byte {
	var debugKeyValueSlice []DebugKeyValue

	qmc.mutex.Lock()
	for i := len(qmc.debugLastMessages) - 1; i >= 0; i-- {
		debugInfo := qmc.debugInfoMessages[qmc.debugLastMessages[i]]
		if debugInfo.warnLogCount > 0 && len(debugKeyValueSlice) < maxLastMessages {
			debugKeyValueSlice = append(debugKeyValueSlice,
				DebugKeyValue{qmc.debugLastMessages[i], qmc.debugInfoMessages[qmc.debugLastMessages[i]]})
		}
	}
	qmc.mutex.Unlock()

	return qmc.generateReportForRequests("Report for requests with warnings", debugKeyValueSlice, []byte{})
}

func (qmc *QuesmaManagementConsole) generateReportForRequests(title string, requests []DebugKeyValue, sidebar []byte) []byte {
	buffer := newBufferWithHead()
	buffer.Write(generateSimpleTop(title))

	buffer.Html("\n</div>\n\n")

	buffer.Html(`<main id="queries">`)

	buffer.Write(generateQueries(requests, true))

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
