package ui

import (
	"bytes"
	"fmt"
)

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
	buffer.WriteString(`var isSafari = /^((?!chrome|android).)*safari/i.test(navigator.userAgent);`)
	buffer.WriteString(`var checkbox = document.getElementById("autorefresh");`)
	buffer.WriteString(`var dashboard = document.getElementById("dashboard-main");`)
	buffer.WriteString(`if (isSafari) { dashboard.classList.add("paused"); }`)
	buffer.WriteString(`checkbox.addEventListener('change', function() {`)
	buffer.WriteString(`if (this.checked && !isSafari) {`)
	buffer.WriteString(`dashboard.classList.remove("paused");`)
	buffer.WriteString(`} else {`)
	buffer.WriteString(`dashboard.classList.add("paused");`)
	buffer.WriteString(`}`)
	buffer.WriteString(`});`)
	buffer.WriteString(`</script>` + "\n")

	buffer.WriteString(`<svg width="100%" height="100%" viewBox="0 0 1000 1000">` + "\n")
	// Clickhouse -> Kibana
	if qmc.config.ReadsFromClickhouse() {
		buffer.WriteString(`<path d="M 1000 250 L 0 250" fill="none" stroke="red" />`)
	}
	// Elasticsearch -> Kibana
	if qmc.config.ReadsFromElasticsearch() {
		buffer.WriteString(`<path d="M 1000 700 L 150 700 L 150 350 L 0 350" fill="none" stroke="green" />`)
	}

	// Ingest -> Clickhouse
	if qmc.config.WritesToClickhouse() {
		buffer.WriteString(`<path d="M 0 650 L 300 650 L 300 350 L 1000 350" fill="none" stroke="green" />`)
	}
	// Ingest -> Elasticsearch
	if qmc.config.WritesToElasticsearch() {
		buffer.WriteString(`<path d="M 0 800 L 1000 800" fill="none" stroke="green" />`)
	}
	buffer.WriteString(`</svg>` + "\n")

	buffer.WriteString(`<div hx-get="/panel/dashboard-traffic" hx-trigger="every 1s [htmx.find('#autorefresh').checked]">`)
	buffer.Write(qmc.generateDashboardTrafficPanel())
	buffer.WriteString(`</div>`)

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
