package ui

import (
	"bytes"
	"fmt"
	"mitmproxy/quesma/stats"
	"mitmproxy/quesma/util"
	"strings"
)

func generateQueries(debugKeyValueSlice []DebugKeyValue, withLinks bool) []byte {
	var buffer bytes.Buffer

	buffer.WriteString("\n" + `<div class="left" Id="left">` + "\n")
	buffer.WriteString(`<div class="title-bar">Query`)
	buffer.WriteString("\n</div>\n")
	buffer.WriteString(`<div class="debug-body">`)
	for _, v := range debugKeyValueSlice {
		if withLinks {
			buffer.WriteString(`<a href="/request-Id/` + v.Key + `">`)
		}
		buffer.WriteString("<p>RequestID:" + v.Key + "</p>\n")
		buffer.WriteString(`<pre Id="query` + v.Key + `">`)
		buffer.WriteString(util.JsonPrettify(string(v.Value.IncomingQueryBody), true))
		buffer.WriteString("\n</pre>")
		if withLinks {
			buffer.WriteString("\n</a>")
		}
	}
	buffer.WriteString("\n</div>")
	buffer.WriteString("\n</div>\n")

	buffer.WriteString(`<div class="right" Id="right">` + "\n")
	buffer.WriteString(`<div class="title-bar">Elasticsearch response` + "\n" + `</div>`)
	buffer.WriteString(`<div class="debug-body">`)
	for _, v := range debugKeyValueSlice {
		if withLinks {
			buffer.WriteString(`<a href="/request-Id/` + v.Key + `">`)
		}
		buffer.WriteString("<p>ResponseID:" + v.Key + "</p>\n")
		buffer.WriteString(`<pre Id="response` + v.Key + `">`)
		buffer.WriteString(util.JsonPrettify(string(v.Value.QueryResp), true))
		buffer.WriteString("\n</pre>")
		if withLinks {
			buffer.WriteString("\n</a>")
		}
	}
	buffer.WriteString("\n</div>")
	buffer.WriteString("\n</div>\n")

	buffer.WriteString(`<div class="bottom_left" Id="bottom_left">` + "\n")
	buffer.WriteString(`<div class="title-bar">Clickhouse translated query` + "\n" + `</div>`)
	buffer.WriteString(`<div class="debug-body">`)
	for _, v := range debugKeyValueSlice {
		if withLinks {
			buffer.WriteString(`<a href="/request-Id/` + v.Key + `">`)
		}
		buffer.WriteString("<p>RequestID:" + v.Key + "</p>\n")
		buffer.WriteString(`<pre Id="second_query` + v.Key + `">`)
		buffer.WriteString(sqlPrettyPrint(v.Value.QueryBodyTranslated))
		buffer.WriteString("\n</pre>")
		if withLinks {
			buffer.WriteString("\n</a>")
		}
	}
	buffer.WriteString("\n</div>")
	buffer.WriteString("\n</div>\n")

	buffer.WriteString(`<div class="bottom_right" Id="bottom_right">` + "\n")
	buffer.WriteString(`<div class="title-bar">Clickhouse response` + "\n" + `</div>`)
	buffer.WriteString(`<div class="debug-body">`)
	for _, v := range debugKeyValueSlice {
		if withLinks {
			buffer.WriteString(`<a href="/request-Id/` + v.Key + `">`)
		}
		buffer.WriteString("<p>ResponseID:" + v.Key + "</p>\n")
		buffer.WriteString(`<pre Id="second_response` + v.Key + `">`)
		buffer.WriteString(util.JsonPrettify(string(v.Value.QueryTranslatedResults), true))
		buffer.WriteString("\n\nThere are more results ...")
		buffer.WriteString("\n</pre>")
		if withLinks {
			buffer.WriteString("\n</a>")
		}
	}
	buffer.WriteString("\n</div>")
	buffer.WriteString("\n</div>\n")

	return buffer.Bytes()
}

func (qmc *QuesmaManagementConsole) generateStatistics() []byte {
	var buffer bytes.Buffer
	const maxTopValues = 5

	statistics := stats.GlobalStatistics

	for _, index := range statistics.SortedIndexNames() {
		buffer.WriteString("\n" + fmt.Sprintf(`<h2>Stats for "%s" <small>from %d requests</small></h2>`, index.IndexName, index.Requests) + "\n")

		buffer.WriteString("<table>\n")

		buffer.WriteString("<thead>\n")
		buffer.WriteString("<tr>\n")
		buffer.WriteString(`<th class="key">Key</th>` + "\n")
		buffer.WriteString(`<th class="key-count">Count</th>` + "\n")
		buffer.WriteString(`<th class="value">Value</th>` + "\n")
		buffer.WriteString(`<th class="value-count">Count</th>` + "\n")
		buffer.WriteString(`<th class="types">Potential type</th>` + "\n")
		buffer.WriteString("</tr>\n")
		buffer.WriteString("</thead>\n")
		buffer.WriteString("<tbody>\n")

		for _, keyStats := range index.SortedKeyStatistics() {
			topValuesCount := maxTopValues
			if len(keyStats.Values) < maxTopValues {
				topValuesCount = len(keyStats.Values)
			}

			buffer.WriteString("<tr>\n")
			buffer.WriteString(fmt.Sprintf(`<td class="key" rowspan="%d">%s</td>`+"\n", topValuesCount, keyStats.KeyName))
			buffer.WriteString(fmt.Sprintf(`<td class="key-count" rowspan="%d">%d</td>`+"\n", topValuesCount, keyStats.Occurrences))

			for i, value := range keyStats.TopNValues(topValuesCount) {
				if i > 0 {
					buffer.WriteString("</tr>\n<tr>\n")
				}

				buffer.WriteString(fmt.Sprintf(`<td class="value">%s</td>`, value.ValueName))
				buffer.WriteString(fmt.Sprintf(`<td class="value-count">%d</td>`, value.Occurrences))
				buffer.WriteString(fmt.Sprintf(`<td class="types">%s</td>`, strings.Join(value.Types, ", ")))
			}
			buffer.WriteString("</tr>\n")
		}

		buffer.WriteString("</tbody>\n")

		buffer.WriteString("</table>\n")
	}

	return buffer.Bytes()
}

var tmpNum int

func (qmc *QuesmaManagementConsole) generateDashboardPanel() []byte {
	var buffer bytes.Buffer
	buffer.WriteString(`<h2>`)
	tmpNum += 1
	buffer.WriteString(fmt.Sprintf(`TODO %d`, tmpNum))
	buffer.WriteString(`</h2>`)

	return buffer.Bytes()
}

func generateTopNavigation(target string) []byte {
	var buffer bytes.Buffer
	buffer.WriteString(`<div class="topnav">` + "\n")
	buffer.WriteString(`<div class="topnav-menu">` + "\n")
	buffer.WriteString("<h3>Quesma</h3>\n")
	buffer.WriteString("<ul>\n")
	buffer.WriteString("<li")
	if target == "queries" {
		buffer.WriteString(` class="active"`)
	}
	buffer.WriteString(`><a href="/">Live tail</a></li>`)
	buffer.WriteString("<li")
	if target == "dashboard" {
		buffer.WriteString(` class="active"`)
	}
	buffer.WriteString(`><a href="/dashboard">Dashboard</a></li>`)
	buffer.WriteString("<li")
	if target == "statistics" {
		buffer.WriteString(` class="active"`)
	}
	buffer.WriteString(`><a href="/ingest-statistics">Ingest statistics</a></li>`)
	buffer.WriteString("\n</ul>\n")
	buffer.WriteString("\n</div>\n")

	buffer.WriteString(`<div class="autorefresh-box">` + "\n")
	buffer.WriteString(`<div class="autorefresh">`)
	buffer.WriteString(fmt.Sprintf(
		`<input type="checkbox" Id="autorefresh" name="autorefresh" hx-target="#%s" hx-get="/panel/%s"
				hx-trigger="every 1s [htmx.find('#autorefresh').checked]" checked />`, target, target))
	buffer.WriteString(`<label for="autorefresh">Autorefresh every 1s</label>`)
	buffer.WriteString("\n</div>")
	buffer.WriteString("\n</div>\n")
	buffer.WriteString("\n</div>\n\n")
	return buffer.Bytes()
}

func (qmc *QuesmaManagementConsole) generateStatisticsLiveTail() []byte {
	buffer := newBufferWithHead()
	buffer.Write(generateTopNavigation("statistics"))

	buffer.WriteString(`<div id="statistics">`)
	buffer.Write(qmc.generateStatistics())
	buffer.WriteString("\n</div>\n\n")

	buffer.WriteString(`<div class="menu">`)
	buffer.WriteString("\n<h2>Menu</h2>")

	buffer.WriteString(`<form action="/">&nbsp;<input class="btn" type="submit" value="Back to live tail" /></form>`)

	buffer.WriteString("\n</div>")

	buffer.WriteString("\n</body>")
	buffer.WriteString("\n</html>")
	return buffer.Bytes()
}

func (qmc *QuesmaManagementConsole) generateLiveTail() []byte {
	buffer := newBufferWithHead()
	buffer.Write(generateTopNavigation("queries"))

	buffer.WriteString(`<div id="queries">`)
	buffer.Write(qmc.generateQueries())
	buffer.WriteString("\n</div>\n\n")

	buffer.WriteString(`<div class="menu">`)
	buffer.WriteString("\n<h2>Menu</h2>")
	buffer.WriteString("\n<h3>Find query</h3><br>\n")

	buffer.WriteString(`<form onsubmit="location.href = '/request-Id/' + find_query_by_id_input.value; return false;">`)
	buffer.WriteString("\n")
	buffer.WriteString(`&nbsp;<input Id="find_query_by_id_button" type="submit" class="btn" value="By Id" /><br>`)
	buffer.WriteString(`&nbsp;<input type="text" Id="find_query_by_id_input" class="input" name="find_query_by_id_input" value="" required size="32"><br><br>`)
	buffer.WriteString("</form>")

	buffer.WriteString(`<form onsubmit="location.href = '/requests-by-str/' + find_query_by_str_input.value; return false;">`)
	buffer.WriteString(`&nbsp;<input Id="find_query_by_str_button" type="submit" class="btn" value="By keyword in request" /><br>`)
	buffer.WriteString(`&nbsp;<input type="text" Id="find_query_by_str_input" class="input" name="find_query_by_str_input" value="" required size="32"><br><br>`)
	buffer.WriteString("</form>")

	buffer.WriteString(`<h3>Useful links</h3>`)
	buffer.WriteString(`<ul>`)
	buffer.WriteString(`<li><a href="http://localhost:5601/app/observability-log-explorer/">Kibana Log Explorer</a></li>`)
	buffer.WriteString(`<li><a href="http://localhost:8081">mitmproxy</a></li>`)
	buffer.WriteString(`<li><a href="http://localhost:8123/play">Clickhouse</a></li>`)
	buffer.WriteString(`</ul>`)

	buffer.WriteString(`<h3>Details</h3>`)
	buffer.WriteString(`<ul>`)
	buffer.WriteString("<li><small>Mode: " + qmc.config.Mode.String() + "</small></li>")

	buffer.WriteString("\n</div>")
	buffer.WriteString("\n</body>")
	buffer.WriteString("\n</html>")
	return buffer.Bytes()
}

func (qmc *QuesmaManagementConsole) generateDashboard() []byte {
	buffer := newBufferWithHead()
	buffer.Write(generateTopNavigation("dashboard"))

	buffer.WriteString(`<div id="dashboard">`)
	buffer.Write(qmc.generateDashboardPanel())
	buffer.WriteString("\n</div>\n\n")

	buffer.WriteString(`<div class="menu">`)
	buffer.WriteString("\n<h2>Menu</h2>")
	buffer.WriteString("\n<h3>Find query</h3><br>\n")

	buffer.WriteString(`<h3>Useful links</h3>`)
	buffer.WriteString(`<ul>`)
	buffer.WriteString(`<li><a href="http://localhost:5601/app/observability-log-explorer/">Kibana Log Explorer</a></li>`)
	buffer.WriteString(`<li><a href="http://localhost:8081">mitmproxy</a></li>`)
	buffer.WriteString(`<li><a href="http://localhost:8123/play">Clickhouse</a></li>`)
	buffer.WriteString(`<li><a href="/ingest-statistics">Ingest statistics</a></li>`)
	buffer.WriteString(`</ul>`)

	buffer.WriteString(`<h3>Details</h3>`)
	buffer.WriteString(`<ul>`)
	buffer.WriteString("<li><small>Mode: " + qmc.config.Mode.String() + "</small></li>")

	buffer.WriteString("\n</div>")
	buffer.WriteString("\n</body>")
	buffer.WriteString("\n</html>")
	return buffer.Bytes()
}

func (qmc *QuesmaManagementConsole) generateReportForRequestId(requestId string) []byte {
	qmc.mutex.Lock()
	request, requestFound := qmc.debugInfoMessages[requestId]
	qmc.mutex.Unlock()

	buffer := newBufferWithHead()
	buffer.WriteString(`<div class="topnav">`)
	if requestFound {
		buffer.WriteString("\n<h3>Quesma Report for request Id " + requestId + "</h3>")
	} else {
		buffer.WriteString("\n<h3>Quesma Report not found for " + requestId + "</h3>")
	}

	buffer.WriteString("\n</div>\n")
	buffer.WriteString(`<div Id="queries">`)

	debugKeyValueSlice := []DebugKeyValue{}
	if requestFound {
		debugKeyValueSlice = append(debugKeyValueSlice, DebugKeyValue{requestId, request})
	}

	buffer.Write(generateQueries(debugKeyValueSlice, false))

	buffer.WriteString("\n</div>\n")
	buffer.WriteString(`<div class="menu">`)
	buffer.WriteString("\n<h2>Menu</h2>")

	buffer.WriteString(`<form action="/">&nbsp;<input class="btn" type="submit" value="Back to live tail" /></form>`)
	buffer.WriteString(`<form action="/log/` + requestId + `">&nbsp;<input class="btn" type="submit" value="Go to log" /></form>`)

	buffer.WriteString("\n</div>")
	buffer.WriteString("\n</body>")
	buffer.WriteString("\n</html>")
	return buffer.Bytes()
}

func (qmc *QuesmaManagementConsole) generateLogForRequestId(requestId string) []byte {
	qmc.mutex.Lock()
	request, requestFound := qmc.debugInfoMessages[requestId]
	qmc.mutex.Unlock()

	buffer := newBufferWithHead()
	buffer.WriteString(`<div class="topnav">`)
	if requestFound {
		buffer.WriteString("\n<h3>Quesma Log for request id " + requestId + "</h3>")
	} else {
		buffer.WriteString("\n<h3>Quesma Log not found for " + requestId + "</h3>")
	}
	buffer.WriteString("\n</div>\n")

	buffer.WriteString(`<div class="center" id="center">`)
	buffer.WriteString("\n\n")
	buffer.WriteString(`<div class="title-bar">Query`)
	buffer.WriteString("\n</div>\n")
	buffer.WriteString(`<div class="debug-body">`)

	buffer.WriteString("<p>RequestID:" + requestId + "</p>\n")
	buffer.WriteString(`<pre id="query` + requestId + `">`)
	buffer.WriteString(request.log)
	buffer.WriteString("\n</pre>")

	buffer.WriteString("\n</div>\n")
	buffer.WriteString("\n</div>\n")
	buffer.WriteString(`<div class="menu">`)
	buffer.WriteString("\n<h2>Menu</h2>")

	buffer.WriteString(`<form action="/">&nbsp;<input class="btn" type="submit" value="Back to live tail" /></form>`)
	buffer.WriteString(`<form action="/request-Id/` + requestId + `">&nbsp;<input class="btn" type="submit" value="Back to request info" /></form>`)

	buffer.WriteString("\n</div>")
	buffer.WriteString("\n</body>")
	buffer.WriteString("\n</html>")
	return buffer.Bytes()
}

func (qmc *QuesmaManagementConsole) generateReportForRequests(requestStr string) []byte {
	qmc.mutex.Lock()
	localQueryDebugInfo := copyMap(qmc.debugInfoMessages)
	lastMessages := qmc.debugLastMessages
	qmc.mutex.Unlock()

	var debugKeyValueSlice []DebugKeyValue
	for i := len(lastMessages) - 1; i >= 0; i-- {
		debugInfo := localQueryDebugInfo[lastMessages[i]]
		if debugInfo.requestContains(requestStr) {
			debugKeyValueSlice = append(debugKeyValueSlice, DebugKeyValue{lastMessages[i], localQueryDebugInfo[lastMessages[i]]})
		}
	}

	buffer := newBufferWithHead()
	buffer.WriteString(`<div class="topnav">`)
	title := fmt.Sprintf("Quesma Report for str '%s' with %d results", requestStr, len(debugKeyValueSlice))
	buffer.WriteString("\n<h3>" + title + "</h3>")

	buffer.WriteString("\n</div>\n\n")

	buffer.WriteString(`<div Id="queries">`)

	buffer.Write(generateQueries(debugKeyValueSlice, true))

	buffer.WriteString("\n</div>\n\n")

	buffer.WriteString(`<div class="menu">`)
	buffer.WriteString("\n<h2>Menu</h2>")

	buffer.WriteString(`<form action="/">&nbsp;<input class="btn" type="submit" value="Back to live tail" /></form>`)

	buffer.WriteString("\n</div>")
	buffer.WriteString("\n</body>")
	buffer.WriteString("\n</html>")

	return buffer.Bytes()
}
