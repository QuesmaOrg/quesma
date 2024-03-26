package ui

import (
	"encoding/json"
	"fmt"
	"mitmproxy/quesma/clickhouse"
	"mitmproxy/quesma/logger"
	"mitmproxy/quesma/stats/errorstats"
	"mitmproxy/quesma/util"
	"net/url"
	"sort"
	"strings"
)

func generateTopNavigation(target string) []byte {
	var buffer HtmlBuffer
	buffer.Html(`<div class="topnav">` + "\n")
	buffer.Html(`<div class="topnav-menu">` + "\n")
	buffer.Html("<h3>Quesma</h3>\n")
	buffer.Html("<ul>\n")
	buffer.Html("<li")
	if target == "queries" {
		buffer.Html(` class="active"`)
	}
	buffer.Html(`><a href="/">Live tail</a></li>`)
	buffer.Html("<li")
	if target == "dashboard" {
		buffer.Html(` class="active"`)
	}
	buffer.Html(`><a href="/dashboard">Dashboard</a></li>`)
	buffer.Html("<li")
	if target == "statistics" {
		buffer.Html(` class="active"`)
	}
	buffer.Html(`><a href="/ingest-statistics">Ingest</a></li>`)
	buffer.Html("<li")
	if target == "routing-statistics" {
		buffer.Html(` class="active"`)
	}
	buffer.Html(`><a href="/routing-statistics">Routing</a></li>`)

	buffer.Html("<li")
	if target == "schema" {
		buffer.Html(` class="active"`)
	}
	buffer.Html(`><a href="/schema">Schema</a></li>`)

	buffer.Html("<li")
	if target == "phone-home" {
		buffer.Html(` class="active"`)
	}
	buffer.Html(`><a href="/telemetry">Telemetry</a></li>`)

	buffer.Html("\n</ul>\n")
	buffer.Html("\n</div>\n")

	if target != "schema" && target != "telemetry" {
		buffer.Html(`<div class="autorefresh-box">` + "\n")
		buffer.Html(`<div class="autorefresh">`)
		buffer.Html(fmt.Sprintf(
			`<input type="checkbox" Id="autorefresh" name="autorefresh" hx-target="#%s" hx-get="/panel/%s"
				hx-trigger="every 1s [htmx.find('#autorefresh').checked]" checked />`,
			url.PathEscape(target), url.PathEscape(target)))
		buffer.Html(`<label for="autorefresh">Autorefresh every 1s</label>`)
		buffer.Html("\n</div>")
		buffer.Html("\n</div>\n")
	}
	buffer.Html("\n</div>\n\n")
	return buffer.Bytes()
}

func (qmc *QuesmaManagementConsole) generateSchema() []byte {
	type menuEntry struct {
		label  string
		target string
	}

	var menuEntries []menuEntry

	type tableColumn struct {
		name             string
		typeName         string
		isAttribute      bool
		isFullTextSearch bool
		warning          *string
	}

	buffer := newBufferWithHead()
	buffer.Write(generateTopNavigation("schema"))
	buffer.Html(`<main id="schema">`)

	if qmc.logManager != nil {

		// Not sure if we should read directly from the TableMap or we should use the Snapshot of it.
		// Let's leave it as is for now.
		schema := qmc.logManager.GetTableDefinitions()

		tableNames := schema.Keys()
		sort.Strings(tableNames)

		buffer.Html("\n<table>")

		for i, tableName := range tableNames {
			table, ok := schema.Load(tableName)
			if !ok {
				continue
			}

			id := fmt.Sprintf("schema-table-%d", i)
			var menu menuEntry
			menu.label = table.Name
			menu.target = fmt.Sprintf("#%s", id)
			menuEntries = append(menuEntries, menu)

			buffer.Html(`<tr class="tableName"`)
			buffer.Html(fmt.Sprintf(` id="%s"`, id))
			buffer.Html(`>`)
			buffer.Html(`<th colspan=2><h2>`)
			buffer.Html(`Table: `)
			buffer.Text(table.Name)
			buffer.Html(`</h2></th>`)
			buffer.Html(`</tr>`)

			buffer.Html(`<tr>`)
			buffer.Html(`<th>`)
			buffer.Html(`Name`)
			buffer.Html(`</th>`)
			buffer.Html(`<th>`)
			buffer.Html(`Type`)
			buffer.Html(`</th>`)
			buffer.Html(`</tr>`)

			var columnNames []string
			var columnMap = make(map[string]tableColumn)

			// standard columns, visible for the user
			for k := range table.Cols {
				c := tableColumn{}

				c.name = k
				if table.Cols[k].Type != nil {
					c.typeName = table.Cols[k].Type.StringWithNullable()
				} else {
					c.typeName = "n/a"
				}

				c.isAttribute = false
				c.isFullTextSearch = table.Cols[k].IsFullTextMatch

				columnNames = append(columnNames, k)
				columnMap[k] = c
			}

			for _, a := range qmc.config.AliasFields(table.Name) {

				// check for collisions
				if field, collide := columnMap[a.SourceFieldName]; collide {
					field.warning = util.Pointer("alias declared with the same name")
					columnMap[a.SourceFieldName] = field
					continue
				}

				// check if target exists
				c := tableColumn{}
				c.name = a.SourceFieldName
				if aliasedField, ok := columnMap[a.TargetFieldName]; ok {
					c.typeName = fmt.Sprintf("alias of '%s', %s", a.TargetFieldName, aliasedField.typeName)
					c.isFullTextSearch = aliasedField.isFullTextSearch
					c.isAttribute = aliasedField.isAttribute
				} else {
					c.warning = util.Pointer("alias points to non-existing field '" + a.TargetFieldName + "'")
					c.typeName = "dangling alias"
				}

				columnNames = append(columnNames, a.SourceFieldName)
				columnMap[a.SourceFieldName] = c
			}

			// columns added by Quesma, not visible for the user
			//
			// this part is based on addOurFieldsToCreateTableQuery in log_manager.go
			attributes := table.Config.GetAttributes()
			if len(attributes) > 0 {
				for _, a := range attributes {
					_, ok := table.Cols[a.KeysArrayName]
					if !ok {
						c := tableColumn{}
						c.name = a.KeysArrayName
						c.typeName = clickhouse.CompoundType{Name: "Array", BaseType: clickhouse.NewBaseType("String")}.StringWithNullable()
						c.isAttribute = true
						columnNames = append(columnNames, c.name)
						columnMap[c.name] = c
					}
					_, ok = table.Cols[a.ValuesArrayName]
					if !ok {
						c := tableColumn{}
						c.name = a.ValuesArrayName
						c.typeName = clickhouse.CompoundType{Name: "Array", BaseType: a.Type}.StringWithNullable()
						c.isAttribute = true
						columnNames = append(columnNames, c.name)
						columnMap[c.name] = c
					}
				}
			}

			sort.Strings(columnNames)

			for _, columnName := range columnNames {
				column, ok := columnMap[columnName]
				if !ok {
					continue
				}

				buffer.Html(`<tr class="`)

				if column.isAttribute {
					buffer.Html(`columnAttribute `)
				}
				if column.warning != nil {
					buffer.Html(`columnWarning `)
				}
				buffer.Html(`column`)

				buffer.Html(`">`)
				buffer.Html(`<td class="columnName">`)

				buffer.Text(column.name)
				buffer.Html(`</td>`)
				buffer.Html(`<td class="columnType">`)

				buffer.Text(column.typeName)
				if column.isFullTextSearch {
					buffer.Html(` <i>(Full text match)</i>`)
				}

				if column.warning != nil {
					buffer.Html(` <span class="columnWarningText">WARNING: `)
					buffer.Text(*column.warning)
					buffer.Html(`</span>`)
				}

				buffer.Html(`</td>`)
				buffer.Html(`</tr>`)
			}

		}

		buffer.Html("\n</table>")

	} else {
		buffer.Html(`<p>Schema is not available</p>`)
	}

	buffer.Html("\n<table>")
	buffer.Html(`<tr class="tableName" id="quesma-config">`)
	buffer.Html(`<th colspan=3><h2>`)
	buffer.Html(`Quesma Config`)
	buffer.Html(`</h2></th>`)
	buffer.Html(`</tr>`)

	buffer.Html(`<tr>`)
	buffer.Html(`<th>`)
	buffer.Html(`Name Pattern`)
	buffer.Html(`</th>`)
	buffer.Html(`<th>`)
	buffer.Html(`Enabled?`)
	buffer.Html(`</th>`)
	buffer.Html(`<th>`)
	buffer.Html(`Full Text Search Fields`)
	buffer.Html(`</th>`)

	buffer.Html(`</tr>`)

	for _, cfg := range qmc.config.IndexConfig {
		buffer.Html(`<tr>`)
		buffer.Html(`<td>`)
		buffer.Text(cfg.NamePattern)
		buffer.Html(`</td>`)
		buffer.Html(`<td>`)
		if cfg.Enabled {
			buffer.Text("true")
		} else {
			buffer.Text("false")
		}
		buffer.Html(`</td>`)

		buffer.Html(`<td>`)
		buffer.Text(strings.Join(cfg.FullTextFields, ", "))
		buffer.Html(`</td>`)

		buffer.Html(`</tr>`)
	}

	buffer.Html("\n</table>")

	buffer.Html("\n</main>\n\n")

	buffer.Html(`<div class="menu">`)
	buffer.Html("\n<h2>Menu</h2>")

	buffer.Html(`<h3>Admin</h3>`)
	buffer.Html(`<ul>`)

	buffer.Html(`<li><button hx-post="/schema/reload" hx-target="body">Reload Schemas</button></li>`)

	buffer.Html(`</ul>`)

	buffer.Html(`<h3>Tables:</h3>`)

	buffer.Html("<ol>")

	for _, menu := range menuEntries {
		buffer.Html(`<li><a href="`)
		buffer.Text(menu.target)
		buffer.Html(`">`)
		buffer.Text(menu.label)
		buffer.Html(`</a></li>`)
	}

	buffer.Html("</ol>")

	buffer.Html(`<a href="#quesma-config">Quesma Config</a>`)

	buffer.Html(`<form action="/">&nbsp;<input class="btn" type="submit" value="Back to live tail" /></form>`)

	buffer.Html("\n</div>")

	buffer.Html("\n</body>")
	buffer.Html("\n</html>")
	return buffer.Bytes()
}

func (qmc *QuesmaManagementConsole) generatePhoneHome() []byte {

	buffer := newBufferWithHead()
	buffer.Write(generateTopNavigation("telemetry"))
	buffer.Html(`<main id="telemetry">`)

	buffer.Html(`<h2>Telemetry</h2>`)
	buffer.Html("<pre>")

	stats, available := qmc.phoneHomeAgent.RecentStats()
	if available {
		asBytes, err := json.MarshalIndent(stats, "", "  ")

		if err != nil {
			logger.Error().Err(err).Msg("Error marshalling phone home stats")
			buffer.Html("Telemetry Stats are unable to be displayed. This is a bug.")
		} else {
			buffer.Html(string(asBytes))
		}

	} else {
		buffer.Html("Telemetry Stats are not available yet.")
	}

	buffer.Html("</pre>")

	buffer.Html("\n</body>")
	buffer.Html("\n</html>")
	return buffer.Bytes()
}

func (qmc *QuesmaManagementConsole) generateRouterStatisticsLiveTail() []byte {
	buffer := newBufferWithHead()
	buffer.Write(generateTopNavigation("routing-statistics"))

	buffer.Html(`<main id="routing-statistics">`)
	buffer.Write(qmc.generateRouterStatistics())
	buffer.Html("\n</main>\n\n")

	buffer.Html(`<div class="menu">`)
	buffer.Html("\n<h2>Menu</h2>")

	buffer.Html(`<form action="/">&nbsp;<input class="btn" type="submit" value="Back to live tail" /></form>`)

	buffer.Html("\n</div>")

	buffer.Html("\n</body>")
	buffer.Html("\n</html>")
	return buffer.Bytes()
}

func (qmc *QuesmaManagementConsole) generateStatisticsLiveTail() []byte {
	buffer := newBufferWithHead()
	buffer.Write(generateTopNavigation("statistics"))

	buffer.Html(`<main id="statistics">`)
	buffer.Write(qmc.generateStatistics())
	buffer.Html("\n</main>\n\n")

	buffer.Html(`<div class="menu">`)
	buffer.Html("\n<h2>Menu</h2>")

	buffer.Html(`<form action="/">&nbsp;<input class="btn" type="submit" value="Back to live tail" /></form>`)

	buffer.Html("\n</div>")

	buffer.Html("\n</body>")
	buffer.Html("\n</html>")
	return buffer.Bytes()
}

func (qmc *QuesmaManagementConsole) generateLiveTail() []byte {
	buffer := newBufferWithHead()
	buffer.Write(generateTopNavigation("queries"))

	buffer.Html(`<main id="queries">`)
	buffer.Write(qmc.generateQueries())
	buffer.Html("\n</main>\n\n")

	buffer.Html(`<div class="menu">`)
	buffer.Html("\n<h2>Menu</h2>")
	buffer.Html("\n<h3>Find query</h3><br>\n")

	buffer.Html(`<form onsubmit="location.href = '/request-Id/' + find_query_by_id_input.value; return false;">`)
	buffer.Html("\n")
	buffer.Html(`&nbsp;<input Id="find_query_by_id_button" type="submit" class="btn" value="By Id" /><br>`)
	buffer.Html(`&nbsp;<input type="text" Id="find_query_by_id_input" class="input" name="find_query_by_id_input" value="" required size="32"><br><br>`)
	buffer.Html("</form>")

	buffer.Html(`<form onsubmit="location.href = '/requests-by-str/' + find_query_by_str_input.value; return false;">`)
	buffer.Html(`&nbsp;<input Id="find_query_by_str_button" type="submit" class="btn" value="By keyword in request" /><br>`)
	buffer.Html(`&nbsp;<input type="text" Id="find_query_by_str_input" class="input" name="find_query_by_str_input" value="" required size="32"><br><br>`)
	buffer.Html("</form>")

	buffer.Html(`<h3>Useful links</h3>`)
	buffer.Html(`<ul>`)
	buffer.Html(`<li><a href="http://localhost:5601/app/observability-log-explorer/">Kibana Log Explorer</a></li>`)
	buffer.Html(`<li><a href="http://localhost:8081">mitmproxy</a></li>`)
	buffer.Html(`<li><a href="http://localhost:8123/play">Clickhouse</a></li>`)
	buffer.Html(`</ul>`)

	buffer.Html(`<h3>Details</h3>`)
	buffer.Html(`<ul>`)
	buffer.Html("<li><small>Mode: ").Text(qmc.config.Mode.String()).Html("</small></li>")
	buffer.Html(`</ul>`)

	buffer.Html("\n</div>")
	buffer.Html("\n</body>")
	buffer.Html("\n</html>")
	return buffer.Bytes()
}

func (qmc *QuesmaManagementConsole) generateDashboard() []byte {
	buffer := newBufferWithHead()
	buffer.Write(generateTopNavigation("dashboard"))

	buffer.Html(`<main id="dashboard-main">` + "\n")

	// Unfortunately, we need tiny bit of javascript to pause the animation.
	buffer.Html(`<script type="text/javascript">`)
	buffer.Html(`var checkbox = document.getElementById("autorefresh");`)
	buffer.Html(`var dashboard = document.getElementById("dashboard-main");`)
	buffer.Html(`checkbox.addEventListener('change', function() {`)
	buffer.Html(`if (this.checked) {`)
	buffer.Html(`dashboard.classList.remove("paused");`)
	buffer.Html(`} else {`)
	buffer.Html(`dashboard.classList.add("paused");`)
	buffer.Html(`}`)
	buffer.Html(`});`)
	buffer.Html(`</script>` + "\n")

	buffer.Html(`<svg width="100%" height="100%" viewBox="0 0 1000 1000">` + "\n")
	// One limitation is that, we don't update color of paths after initial draw.
	// They rarely change, so it's not a big deal for now.
	// Clickhouse -> Kibana
	if qmc.config.ReadsFromClickhouse() {
		status, _ := qmc.generateDashboardTrafficText(RequestStatisticKibana2Clickhouse)
		buffer.Html(fmt.Sprintf(`<path d="M 0 250 L 1000 250" fill="none" stroke="%s" />`, status))
	}
	// Elasticsearch -> Kibana
	if qmc.config.ReadsFromElasticsearch() {
		status, _ := qmc.generateDashboardTrafficText(RequestStatisticKibana2Elasticsearch)
		buffer.Html(fmt.Sprintf(`<path d="M 0 350 L 150 350 L 150 700 L 1000 700" fill="none" stroke="%s" />`, status))
	}

	// Ingest -> Clickhouse
	if qmc.config.WritesToClickhouse() {
		status, _ := qmc.generateDashboardTrafficText(RequestStatisticIngest2Clickhouse)
		buffer.Html(fmt.Sprintf(`<path d="M 1000 350 L 300 350 L 300 650 L 0 650" fill="none" stroke="%s" />`, status))
	}
	// Ingest -> Elasticsearch
	if qmc.config.WritesToElasticsearch() {
		status, _ := qmc.generateDashboardTrafficText(RequestStatisticIngest2Elasticsearch)
		buffer.Html(fmt.Sprintf(`<path d="M 1000 800 L 0 800" fill="none" stroke="%s" />`, status))
	}
	buffer.Html(`</svg>` + "\n")

	buffer.Html(`<div hx-get="/panel/dashboard-traffic" hx-trigger="every 1s [htmx.find('#autorefresh').checked]">`)
	buffer.Write(qmc.generateDashboardTrafficPanel())
	buffer.Html(`</div>`)

	buffer.Html(`<div id="dashboard">` + "\n")
	buffer.Write(qmc.generateDashboardPanel())
	buffer.Html("</div>\n")
	buffer.Html("\n</main>\n\n")

	buffer.Html(`<div class="menu">`)
	buffer.Html("\n<h2>Menu</h2>")

	buffer.Html(`<h3>Useful links</h3>`)
	buffer.Html(`<ul>`)
	buffer.Html(`<li><a href="http://localhost:5601/app/observability-log-explorer/">Kibana Log Explorer</a></li>`)
	buffer.Html(`<li><a href="http://localhost:8081">mitmproxy</a></li>`)
	buffer.Html(`<li><a href="http://localhost:8123/play">Clickhouse</a></li>`)
	buffer.Html(`<li><a href="/ingest-statistics">Ingest statistics</a></li>`)
	buffer.Html(`</ul>`)

	buffer.Html("\n</div>")
	buffer.Html("\n</body>")
	buffer.Html("\n</html>")
	return buffer.Bytes()
}

func (qmc *QuesmaManagementConsole) generateReportForRequestId(requestId string) []byte {
	qmc.mutex.Lock()
	request, requestFound := qmc.debugInfoMessages[requestId]
	qmc.mutex.Unlock()

	buffer := newBufferWithHead()
	buffer.Html(`<div class="topnav">`)
	if requestFound {
		buffer.Html("\n<h3>Quesma Report for request Id ").Text(requestId).Html("</h3>")
	} else {
		buffer.Html("\n<h3>Quesma Report not found for ").Text(requestId).Html("</h3>")
	}

	buffer.Html("\n</div>\n")
	buffer.Html(`<main id="queries">`)

	debugKeyValueSlice := []DebugKeyValue{}
	if requestFound {
		debugKeyValueSlice = append(debugKeyValueSlice, DebugKeyValue{requestId, request})
	}

	buffer.Write(generateQueries(debugKeyValueSlice, false))

	buffer.Html("\n</main>\n")
	buffer.Html(`<div class="menu">`)
	buffer.Html("\n<h2>Menu</h2>")

	buffer.Html(`<form action="/">&nbsp;<input class="btn" type="submit" value="Back to live tail" /></form>`)
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

	buffer := newBufferWithHead()
	buffer.Html(`<div class="topnav">`)
	if requestFound {
		buffer.Html("\n<h3>Quesma Log for request id ").Text(requestId).Html("</h3>")
	} else {
		buffer.Html("\n<h3>Quesma Log not found for ").Text(requestId).Html("</h3>")
	}
	buffer.Html("\n</div>\n")

	buffer.Html(`<main class="center" id="center">`)
	buffer.Html("\n\n")
	buffer.Html(`<div class="title-bar">Query`)
	buffer.Html("\n</div>\n")
	buffer.Html(`<div class="debug-body">`)

	buffer.Html("<p>RequestID:").Text(requestId).Html("</p>\n")
	buffer.Html(`<pre id="query`).Text(requestId).Html(`">`)
	buffer.Text(request.log)
	buffer.Html("\n</pre>")

	buffer.Html("\n</div>\n")
	buffer.Html("\n</main>\n")
	buffer.Html(`<div class="menu">`)
	buffer.Html("\n<h2>Menu</h2>")

	buffer.Html(`<form action="/">&nbsp;<input class="btn" type="submit" value="Back to live tail" /></form>`)
	buffer.Html(`<form action="/request-Id/`).Text(requestId).Html(`">&nbsp;<input class="btn" type="submit" value="Back to request info" /></form>`)

	buffer.Html("\n</div>")
	buffer.Html("\n</body>")
	buffer.Html("\n</html>")
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
	buffer.Html(`<div class="topnav">`)
	title := fmt.Sprintf("Quesma Report for str '%s' with %d results", requestStr, len(debugKeyValueSlice))
	buffer.Html("\n<h3>" + title + "</h3>")

	buffer.Html("\n</div>\n\n")

	buffer.Html(`<main id="queries">`)

	buffer.Write(generateQueries(debugKeyValueSlice, true))

	buffer.Html("\n</main>\n\n")

	buffer.Html(`<div class="menu">`)
	buffer.Html("\n<h2>Menu</h2>")

	buffer.Html(`<form action="/">&nbsp;<input class="btn" type="submit" value="Back to live tail" /></form>`)

	buffer.Html("\n</div>")
	buffer.Html("\n</body>")
	buffer.Html("\n</html>")

	return buffer.Bytes()
}

func (qmc *QuesmaManagementConsole) generateErrorForReason(reason string) []byte {
	buffer := newBufferWithHead()
	buffer.Html(`<div class="topnav">`)
	title := fmt.Sprintf("Quesma Errors with reason '%s'", reason)
	buffer.Html("\n<h3>").Text(title).Html("</h3>")
	buffer.Html("\n</div>\n\n")

	buffer.Html(`<main id="errors">`)
	errors := errorstats.GlobalErrorStatistics.ErrorReportsForReason(reason)
	// TODO: Make it nicer
	for _, errorReport := range errors {
		buffer.Html("<p>").Text(errorReport.ReportedAt.String() + " " + errorReport.DebugMessage).Html("</p>\n")
	}
	buffer.Html("\n</main>\n\n")

	buffer.Html(`<div class="menu">`)
	buffer.Html("\n<h2>Menu</h2>")

	buffer.Html(`<form action="/dashboard">&nbsp;<input class="btn" type="submit" value="Back to dashboard" /></form>`)
	// TODO: implement
	// buffer.Html(`<form action="/dashboard">&nbsp;<input class="btn" type="submit" value="See requests with errors" /></form>`)
	buffer.Html("\n</div>")

	buffer.Html("\n</body>")
	buffer.Html("\n</html>")

	return buffer.Bytes()
}
