package ui

import (
	"encoding/json"
	"fmt"
	"gopkg.in/yaml.v3"
	"mitmproxy/quesma/buildinfo"
	"mitmproxy/quesma/clickhouse"
	"mitmproxy/quesma/logger"
	"mitmproxy/quesma/stats/errorstats"
	"mitmproxy/quesma/util"
	"net/url"
	"slices"
	"sort"
	"strings"
)

func generateSimpleTop(title string) []byte {
	var buffer HtmlBuffer
	buffer.Html(`<div class="topnav">` + "\n")
	buffer.Html(`<div class="topnav-menu">` + "\n")
	buffer.Html(`<img src="/static/asset/quesma-logo-white-full.svg" alt="Quesma logo" class="quesma-logo" />` + "\n")
	buffer.Html(`<h3>`).Text(title).Html(`</h3>`)
	buffer.Html("\n</div>\n</div>\n\n")
	return buffer.Bytes()
}

func generateTopNavigation(target string) []byte {
	var buffer HtmlBuffer
	buffer.Html(`<div class="topnav">` + "\n")
	buffer.Html(`<div class="topnav-menu">` + "\n")
	buffer.Html(`<img src="/static/asset/quesma-logo-white-full.svg" alt="Quesma logo" class="quesma-logo" />` + "\n")
	buffer.Html("<ul>\n")
	buffer.Html("<li")
	if target == "dashboard" {
		buffer.Html(` class="active"`)
	}
	buffer.Html(`><a href="/">Dashboard</a></li>`)
	buffer.Html("<li")

	if target == "queries" {
		buffer.Html(` class="active"`)
	}
	buffer.Html(`><a href="/live">Live tail</a></li>`)
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

	buffer.Html("<li")
	if target == "datasources" {
		buffer.Html(` class="active"`)
	}
	buffer.Html(`><a href="/data-sources">Data sources</a></li>`)

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

			if table.Comment != "" {
				buffer.Text(" (")
				buffer.Text(table.Comment)
				buffer.Text(")")
			}

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
		buffer.Text(cfg.Name)
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

	buffer.Html(`<h3>Admin actions</h3>`)
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

	buffer.Html(`<h3><a href="#quesma-config">Jump to Quesma Config</a></h3>`)

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

func (qmc *QuesmaManagementConsole) generateDatasources() []byte {
	buffer := newBufferWithHead()
	buffer.Write(generateTopNavigation("datasources"))

	buffer.Html(`<main id="datasources">`)
	buffer.Html(`<h2>Data sources</h2>`)

	buffer.Html(`<h3>Clickhouse</h3>`)

	buffer.Html(`<ul>`)

	tableNames := []string{}
	for tableName := range qmc.config.IndexConfig {
		tableNames = append(tableNames, tableName)
	}
	slices.Sort(tableNames)
	tables := qmc.logManager.GetTableDefinitions()
	slices.Sort(tableNames)
	for _, tableName := range tableNames {
		if _, exist := tables.Load(tableName); exist {
			buffer.Html(fmt.Sprintf(`<li>%s (table exists)</li>`, tableName))
		} else {
			buffer.Html(fmt.Sprintf(`<li>%s</li>`, tableName))
		}
	}
	buffer.Html(`</ul>`)

	buffer.Html(`<h3>Elasticsearch</h3>`)

	buffer.Html(`<ul>`)

	qmc.indexManagement.Start()
	indexNames := []string{}
	internalIndexNames := []string{}
	for indexName := range qmc.indexManagement.GetSourceNames() {
		if strings.HasPrefix(indexName, ".") {
			internalIndexNames = append(internalIndexNames, indexName)
		} else {
			indexNames = append(indexNames, indexName)
		}
	}

	slices.Sort(indexNames)
	slices.Sort(internalIndexNames)
	for _, indexName := range indexNames {
		buffer.Html(fmt.Sprintf(`<li>%s</li>`, indexName))
	}

	if len(internalIndexNames) > 0 {
		buffer.Html(`<ul>`)

		for _, indexName := range internalIndexNames {
			buffer.Html(fmt.Sprintf(`<li><small>%s</small></li>`, indexName))
		}
		buffer.Html(`</ul>`)
	}

	buffer.Html(`</ul>`)

	buffer.Html("\n</main>\n\n")

	buffer.Html(`<div class="menu">`)
	buffer.Html("\n<h2>Menu</h2>")

	buffer.Html(`<form action="/live">&nbsp;<input class="btn" type="submit" value="Back to live tail" /></form>`)

	buffer.Html("\n</div>")

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

	buffer.Html(`<form action="/">&nbsp;<input class="btn" type="submit" value="Back to dashboard" /></form>`)

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

	buffer.Html(`<form action="/">&nbsp;<input class="btn" type="submit" value="Back to dashboard" /></form>`)

	buffer.Html("\n</div>")

	buffer.Html("\n</body>")
	buffer.Html("\n</html>")
	return buffer.Bytes()
}

func (qmc *QuesmaManagementConsole) generateLiveTail() []byte {
	buffer := newBufferWithHead()
	buffer.Write(generateTopNavigation("queries"))

	// This preserves scrolling, but does not work if new queries appear.
	buffer.Html(`<script>
let containerNames = ["query-left", "query-right", "query-bottom-left", "query-bottom-right"];
let scrollPosition = [false, false, false, false];

document.body.addEventListener('htmx:beforeSwap', function(event) {
	if (event.target.id == 'queries') {
		for (let i = 0; i < containerNames.length; i++) {
			let container = document.getElementById(containerNames[i]);
			if (container.matches(":hover")) {
				scrollPosition[i] = {
					top: container.scrollTop,
					left: container.scrollLeft,
					behavior: 'instant'
				};
			} else {
				scrollPosition[i] = false;
			}
		}
	}
});
document.body.addEventListener('htmx:afterSwap', function(event) {
	if (event.target.id == 'queries') {
		for (let i = 0; i < containerNames.length; i++) {
			if (scrollPosition[i]) {
				let container = document.getElementById(containerNames[i]);
				container.scrollTo(scrollPosition[i]);
			}
		}
	}
});
</script>`)

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

	buffer.Html(`<h3>Queries with problems</h3>`)
	buffer.Write(qmc.generateQueriesStatsPanel())

	// Don't get foiled by warning, this detects whether it's our development Quesma
	if buildinfo.LicenseKey == buildinfo.DevelopmentLicenseKey || buildinfo.LicenseKey == "" {
		buffer.Html(`<h3>Useful links</h3>`)
		buffer.Html(`<ul>`)
		buffer.Html(`<li><a href="http://localhost:5601/app/observability-log-explorer/">Kibana Log Explorer</a></li>`)
		buffer.Html(`<li><a href="http://localhost:8081">mitmproxy</a></li>`)
		buffer.Html(`<li><a href="http://localhost:8123/play">Clickhouse</a></li>`)
		buffer.Html(`</ul>`)
	}

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

	buffer.Html(`<svg width="100%" height="100%" viewBox="0 0 1000 1000" preserveAspectRatio="none">` + "\n")
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

	logMessages, optAsyncId := generateLogMessages(request.logMessages)

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

func generateLogMessages(logMessages []string) ([]byte, *string) {
	var buffer HtmlBuffer
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

	for _, logMessage := range logMessages {
		buffer.Html("<tr>\n")

		var fields map[string]interface{}

		if err := json.Unmarshal([]byte(logMessage), &fields); err != nil {
			// error print
			buffer.Html("<td></td><td>error</td><td></td>").Text(err.Error()).Html("<td>")
			continue
		}
		// time
		buffer.Html(`<td class="time">`)
		if _, ok := fields["time"]; ok {
			time := fields["time"].(string)
			time = strings.Replace(time, "T", " ", 1)
			time = strings.Replace(time, ".", " ", 1)
			buffer.Text(time).Html("</td>")
			delete(fields, "time")
		} else {
			buffer.Html("missing time</td>")
		}

		// get rid of request_id and async_id
		delete(fields, "request_id")
		if id, ok := fields["async_id"].(string); ok {
			asyncId = &id
			delete(fields, "async_id")
		}

		// level
		buffer.Html(`<td class="level">`)
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
			buffer.Html("missing level</td>")
		}

		// message
		buffer.Html(`<td class="message">`)
		if message, ok := fields["message"].(string); ok {
			buffer.Text(message).Html("</td>")
			delete(fields, "message")
		} else {
			buffer.Html("</td>")
		}

		// fields
		buffer.Html(`<td class="fields">`)
		if rest, err := yaml.Marshal(&fields); err == nil {
			buffer.Text(string(rest)).Html("</td>")
		} else {
			buffer.Html("</td>")
		}
		buffer.Html("</tr>\n")
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
	return qmc.generateReportForRequests(title, debugKeyValueSlice)
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

	return qmc.generateReportForRequests("Report for requests with errors", debugKeyValueSlice)
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

	return qmc.generateReportForRequests("Report for requests with warnings", debugKeyValueSlice)
}

func (qmc *QuesmaManagementConsole) generateReportForRequests(title string, requests []DebugKeyValue) []byte {
	buffer := newBufferWithHead()
	buffer.Write(generateSimpleTop(title))

	buffer.Html("\n</div>\n\n")

	buffer.Html(`<main id="queries">`)

	buffer.Write(generateQueries(requests, true))

	buffer.Html("\n</main>\n\n")

	buffer.Html(`<div class="menu">`)
	buffer.Html("\n<h2>Menu</h2>")

	buffer.Html(`<form action="/live">&nbsp;<input class="btn" type="submit" value="Back to live tail" /></form>`)

	buffer.Html("\n</div>")
	buffer.Html("\n</body>")
	buffer.Html("\n</html>")

	return buffer.Bytes()
}

func (qmc *QuesmaManagementConsole) generateErrorForReason(reason string) []byte {
	buffer := newBufferWithHead()
	buffer.Write(generateTopNavigation(fmt.Sprintf("Errors with reason '%s'", reason)))

	buffer.Html(`<main id="errors">`)
	errors := errorstats.GlobalErrorStatistics.ErrorReportsForReason(reason)
	// TODO: Make it nicer
	for _, errorReport := range errors {
		buffer.Html("<p>").Text(errorReport.ReportedAt.String() + " " + errorReport.DebugMessage).Html("</p>\n")
	}
	buffer.Html("\n</main>\n\n")

	buffer.Html(`<div class="menu">`)
	buffer.Html("\n<h2>Menu</h2>")

	buffer.Html(`<form action="/">&nbsp;<input class="btn" type="submit" value="Back to dashboard" /></form>`)
	// TODO: implement
	// buffer.Html(`<form action="/dashboard">&nbsp;<input class="btn" type="submit" value="See requests with errors" /></form>`)
	buffer.Html("\n</div>")

	buffer.Html("\n</body>")
	buffer.Html("\n</html>")

	return buffer.Bytes()
}
