package ui

import (
	"bytes"
	"fmt"
	"mitmproxy/quesma/quesma/mux"
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

func (qmc *QuesmaManagementConsole) generateRouterStatistics() []byte {
	var buffer bytes.Buffer

	matchStatistics := mux.MatchStatistics()

	buffer.WriteString("\n<h2>Matched URLs</h2>\n<ul>")
	for _, url := range matchStatistics.Matched {
		buffer.WriteString(fmt.Sprintf("<li>%s</li>\n", url))
	}

	buffer.WriteString("</ul>\n")
	buffer.WriteString("\n<h2>Not matched URLs</h2>\n<ul>")
	for _, url := range matchStatistics.Nonmatched {
		buffer.WriteString(fmt.Sprintf("<li>%s</li>\n", url))
	}
	buffer.WriteString("</ul>\n")

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

			buffer.WriteString("<tr class='group-divider'>\n")
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

func (qmc *QuesmaManagementConsole) generateDashboardPanel() []byte {
	var buffer bytes.Buffer

	buffer.WriteString(`<div id="dashboard-kibana" class="component">`)
	buffer.WriteString(`<h3>Kibana</h3>`)
	buffer.WriteString(`<div class="status">OK</div>`)
	buffer.WriteString(`</div>`)

	buffer.WriteString(`<div id="dashboard-ingest" class="component">`)
	buffer.WriteString(`<h3>Ingest</h3>`)
	buffer.WriteString(`<div class="status">OK</div>`)
	buffer.WriteString(`</div>`)

	buffer.WriteString(`<div id="dashboard-elasticsearch" class="component">`)
	buffer.WriteString(`<h3>Elastic</h3><h3>search</h3>`)
	buffer.WriteString(`<div class="status">OK</div>`)
	buffer.WriteString(`</div>`)

	buffer.WriteString(`<div id="dashboard-clickhouse" class="component">`)
	buffer.WriteString(`<h3>ClickHouse</h3>`)
	buffer.WriteString(`<div class="status">OK</div>`)
	buffer.WriteString(`</div>`)

	buffer.WriteString(`<div id="dashboard-traffic" class="component">`)

	buffer.WriteString(`<div id="dashboard-quesma" class="component">`)
	buffer.WriteString(`<h3>Quesma</h3>`)
	buffer.WriteString(`<div class="status">CPU: 17%</div>`)
	buffer.WriteString(`<div class="status">RAM: 1.3GB</div>`)
	buffer.WriteString(`<div class="status">Uptime: 1d4h</div>`)
	buffer.WriteString(`</div>`)

	buffer.WriteString(`<div id="dashboard-errors" class="component">`)
	buffer.WriteString(`<h3>Top errors:</h3>`)
	buffer.WriteString(`<div class="status">7: Unknown error</div>`)
	buffer.WriteString(`<div class="status">2: Parsing error</div>`)
	buffer.WriteString(`<div class="status">1: Request out of bound</div>`)
	buffer.WriteString(`</div>`)
	buffer.WriteString(`</div>`)

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
	buffer.WriteString("<li")
	if target == "routing-statistics" {
		buffer.WriteString(` class="active"`)
	}
	buffer.WriteString(`><a href="/routing-statistics">Routing statistics</a></li>`)

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

func (qmc *QuesmaManagementConsole) generateRouterStatisticsLiveTail() []byte {
	buffer := newBufferWithHead()
	buffer.Write(generateTopNavigation("routing-statistics"))

	buffer.WriteString(`<main id="routing-statistics">`)
	buffer.Write(qmc.generateRouterStatistics())
	buffer.WriteString("\n</main>\n\n")

	buffer.WriteString(`<div class="menu">`)
	buffer.WriteString("\n<h2>Menu</h2>")

	buffer.WriteString(`<form action="/">&nbsp;<input class="btn" type="submit" value="Back to live tail" /></form>`)

	buffer.WriteString("\n</div>")

	buffer.WriteString("\n</body>")
	buffer.WriteString("\n</html>")
	return buffer.Bytes()
}

func (qmc *QuesmaManagementConsole) generateStatisticsLiveTail() []byte {
	buffer := newBufferWithHead()
	buffer.Write(generateTopNavigation("statistics"))

	buffer.WriteString(`<main id="statistics">`)
	buffer.Write(qmc.generateStatistics())
	buffer.WriteString("\n</main>\n\n")

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

	buffer.WriteString(`<main id="queries">`)
	buffer.Write(qmc.generateQueries())
	buffer.WriteString("\n</main>\n\n")

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

	buffer.WriteString(`<main id="dashboard-main">` + "\n")

	// Unfortunately, we need tiny bit of javascript to pause the animation.
	buffer.WriteString(`<script type="text/javascript">`)
	buffer.WriteString(`var checkbox = document.getElementById("autorefresh");`)
	buffer.WriteString(`var dashboard = document.getElementById("dashboard-main");`)
	buffer.WriteString(`checkbox.addEventListener('change', function() {`)
	buffer.WriteString(`if (this.checked) {`)
	buffer.WriteString(`dashboard.classList.remove("paused");`)
	buffer.WriteString(`} else {`)
	buffer.WriteString(`dashboard.classList.add("paused");`)
	buffer.WriteString(`}`)
	buffer.WriteString(`});`)
	buffer.WriteString(`</script>` + "\n")

	buffer.WriteString(`<svg width="100%" height="100%" viewBox="0 0 1000 1000">` + "\n")
	// Clickhouse -> Kibana
	buffer.WriteString(`<path d="M 1000 250 L 0 250" fill="none" stroke="red" />`)
	buffer.WriteString(`<text x="500" y="240" class="red">4 rps / 0% err / 10ms</text>`)
	// Elasticsearch -> Kibana
	buffer.WriteString(`<path d="M 1000 700 L 150 700 L 150 350 L 0 350" fill="none" stroke="green" />`)
	buffer.WriteString(`<text x="500" y="690" class="green">7 rps / 3% err / 17ms</text>`)

	// Ingest -> Clickhouse
	buffer.WriteString(`<path d="M 0 650 L 300 650 L 300 350 L 1000 350" fill="none" stroke="green" />`)
	buffer.WriteString(`<text x="500" y="340" class="green">29 rps / 1% err / 3ms</text>`)
	// Ingest -> Elasticsearch
	buffer.WriteString(`<path d="M 0 800 L 1000 800" fill="none" stroke="green" />`)
	buffer.WriteString(`<text x="500" y="790" class="green">29 rps / 0% err / 1ms</text>`)

	buffer.WriteString(`</svg>` + "\n")
	buffer.WriteString(`<div id="dashboard">` + "\n")
	buffer.Write(qmc.generateDashboardPanel())
	buffer.WriteString("</div>\n")
	buffer.WriteString("\n</main>\n\n")

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
	buffer.WriteString(`<main id="queries">`)

	debugKeyValueSlice := []DebugKeyValue{}
	if requestFound {
		debugKeyValueSlice = append(debugKeyValueSlice, DebugKeyValue{requestId, request})
	}

	buffer.Write(generateQueries(debugKeyValueSlice, false))

	buffer.WriteString("\n</main>\n")
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

	buffer.WriteString(`<main class="center" id="center">`)
	buffer.WriteString("\n\n")
	buffer.WriteString(`<div class="title-bar">Query`)
	buffer.WriteString("\n</div>\n")
	buffer.WriteString(`<div class="debug-body">`)

	buffer.WriteString("<p>RequestID:" + requestId + "</p>\n")
	buffer.WriteString(`<pre id="query` + requestId + `">`)
	buffer.WriteString(request.log)
	buffer.WriteString("\n</pre>")

	buffer.WriteString("\n</div>\n")
	buffer.WriteString("\n</main>\n")
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

	buffer.WriteString(`<main id="queries">`)

	buffer.Write(generateQueries(debugKeyValueSlice, true))

	buffer.WriteString("\n</main>\n\n")

	buffer.WriteString(`<div class="menu">`)
	buffer.WriteString("\n<h2>Menu</h2>")

	buffer.WriteString(`<form action="/">&nbsp;<input class="btn" type="submit" value="Back to live tail" /></form>`)

	buffer.WriteString("\n</div>")
	buffer.WriteString("\n</body>")
	buffer.WriteString("\n</html>")

	return buffer.Bytes()
}
