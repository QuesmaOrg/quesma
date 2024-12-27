// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package ui

import (
	"encoding/base64"
	"fmt"
	"quesma/buildinfo"
	"quesma/quesma/ui/internal/builder"
	"quesma/util"
	"quesma_v2/core/diag"
	"strconv"
	"strings"
)

func (qmc *QuesmaManagementConsole) generateLiveTail() []byte {
	buffer := newBufferWithHead()
	buffer.Write(qmc.generateTopNavigation("queries"))

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

	buffer.Html(`<form onsubmit="location.href = '/request-id/' + find_query_by_id_input.value; return false;">`)
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
	buffer.Html(`<h3>Unsupported queries</h3>`)
	buffer.Write(qmc.generateUnsupportedQuerySidePanel())

	// Don't get foiled by warning, this detects whether it's our development Quesma
	if buildinfo.Version == "development" {
		buffer.Html(`<h3>Useful links</h3>`)
		buffer.Html(`<ul>`)
		buffer.Html(`<li><a href="http://localhost:5601/app/observability-log-explorer/">Kibana Log Explorer</a></li>`)
		buffer.Html(`<li><a href="http://localhost:8081">mitmproxy</a></li>`)
		buffer.Html(`<li><a href="http://localhost:8123/play">Clickhouse</a></li>`)
		buffer.Html(`</ul>`)
	}

	buffer.Html(`<h3>Details</h3>`)
	buffer.Html(`<ul>`)
	buffer.Html("<li><small>Transparent proxy: ").Text(strconv.FormatBool(qmc.cfg.TransparentProxy)).Html("</small></li>")
	buffer.Html(`</ul>`)

	buffer.Html("\n</div>")
	buffer.Html("\n</body>")
	buffer.Html("\n</html>")
	return buffer.Bytes()
}

func (qmc *QuesmaManagementConsole) generateQueries() []byte {
	// Take last MAX_LAST_MESSAGES to display, e.g. 100 out of potentially 10m000
	qmc.mutex.Lock()
	lastMessages := qmc.debugLastMessages
	debugKeyValueSlice := []queryDebugInfoWithId{}
	count := 0
	for i := len(lastMessages) - 1; i >= 0 && count < maxLastMessages; i-- {
		debugInfoMessage := qmc.debugInfoMessages[lastMessages[i]]
		if len(debugInfoMessage.QueryDebugSecondarySource.IncomingQueryBody) > 0 {
			debugKeyValueSlice = append(debugKeyValueSlice, queryDebugInfoWithId{lastMessages[i], debugInfoMessage})
			count++
		}
	}
	qmc.mutex.Unlock()

	queriesBytes := qmc.populateQueries(debugKeyValueSlice, true)
	queriesStats := qmc.generateQueriesStatsPanel()
	unsupportedQueriesStats := qmc.generateUnsupportedQuerySidePanel()
	return append(queriesBytes, append(queriesStats, unsupportedQueriesStats...)...)
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
	unknownTypeCount := 0
	if value, ok := typesCount[UnrecognizedQueryType]; ok {
		unknownTypeCount = value
	}

	var buffer builder.HtmlBuffer
	linkToMainView := `<li><a href="/unsupported-requests"`
	buffer.Html(`<ul id="unsupported-queries-stats" hx-swap-oob="true">`)
	if totalErrorsCount > 0 {
		buffer.Html(fmt.Sprintf(`%s class="debug-warn-log"">%d total (%d recent)</a></li>`, linkToMainView, totalErrorsCount, savedErrorsCount))
		plural := "s"
		if typesSeenCount == 1 {
			plural = ""
		}
		buffer.Html(fmt.Sprintf(`%s class="debug-warn-log"">%d different type%s</a></li>`, linkToMainView, typesSeenCount, plural))
		if unknownTypeCount > 0 {
			buffer.Html(fmt.Sprintf(`<li><a href="/unsupported-requests/%s"" class="debug-error-log">`, UnrecognizedQueryType))
			buffer.Html(fmt.Sprintf(`%d of unrecognized type</a></li>`, unknownTypeCount))
		}
	} else {
		buffer.Html(`<li>None!</a></li>`)
	}
	buffer.Html(`</ul>`)

	return buffer.Bytes()
}

func (qmc *QuesmaManagementConsole) generateQueriesStatsPanel() []byte {
	qmc.mutex.Lock()
	errorCount := 0
	warnCount := 0
	for _, msg := range qmc.debugInfoMessages {
		if msg.errorLogCount > 0 {
			errorCount++
		}
		if msg.warnLogCount > 0 {
			warnCount++
		}
	}
	qmc.mutex.Unlock()

	var buffer builder.HtmlBuffer

	buffer.Html(`<ul id="queries-stats" hx-swap-oob="true">`)
	buffer.Html(`<li><a href="/requests-with-error/"`)
	if errorCount > 0 {
		buffer.Html(fmt.Sprintf(` class="debug-error-log"">%d with errors</a></li>`, errorCount))
	} else {
		buffer.Html(`>0 with errors</a></li>`)
	}
	buffer.Html(`<li><a href="/requests-with-warning/"`)
	if warnCount > 0 {
		buffer.Html(fmt.Sprintf(` class="debug-warn-log"">%d with warnings</a></li>`, warnCount))
	} else {
		buffer.Html(`>0 with warnings</a></li>`)
	}
	buffer.Html(`</ul>`)

	return buffer.Bytes()
}

func (qmc *QuesmaManagementConsole) populateQueries(debugKeyValueSlice []queryDebugInfoWithId, withLinks bool) []byte {
	var buffer builder.HtmlBuffer

	buffer.Html("\n" + `<div class="left" id="query-left">` + "\n")
	buffer.Html(`<div class="title-bar">Query`)
	buffer.Html("\n</div>\n")
	buffer.Html(`<div class="debug-body">`)
	for _, v := range debugKeyValueSlice {
		if withLinks {
			buffer.Html(`<a href="/request-id/`).Text(v.id).Html(`">`)
		}
		buffer.Html("<p>UUID:").Text(v.id).Html(" Path: ")

		if v.query.OpaqueId != "" {
			buffer.Text("OpaqueId: ").Text(v.query.OpaqueId)
		}

		buffer.Text(v.query.Path).Html("</p>\n")
		buffer.Html(`<pre Id="query`).Text(v.id).Html(`">`)
		buffer.Text(string(v.query.IncomingQueryBody))
		buffer.Html("\n</pre>")
		if withLinks {
			buffer.Html("\n</a>")
		}
	}
	buffer.Html("\n</div>")
	buffer.Html("\n</div>\n")

	buffer.Html(`<div class="right" id="query-right">` + "\n")

	// TODO: if no A/B testing with Elastic is enabled in the configuration, then add "(not applicable)" to the title
	buffer.Html(`<div class="title-bar">Elasticsearch response` + "\n" + `</div>`)

	buffer.Html(`<div class="debug-body">`)
	for _, v := range debugKeyValueSlice {
		if withLinks {
			buffer.Html(`<a href="/request-id/`).Text(v.id).Html(`">`)
		}
		tookStr := fmt.Sprintf(" took %d ms", v.query.PrimaryTook.Milliseconds())
		buffer.Html("<p>UUID:").Text(v.id).Text(tookStr).Html("</p>\n")
		buffer.Html(`<pre Id="response`).Text(v.id).Html(`">`)
		if len(v.query.QueryResp) > 0 {
			buffer.Text(string(v.query.QueryResp))
		} else {
			buffer.Text("(empty, request was not sent to Elasticsearch)")
		}
		buffer.Html("\n</pre>")
		if withLinks {
			buffer.Html("\n</a>")
		}
	}
	buffer.Html("\n</div>")

	buffer.Html("\n</div>\n")

	buffer.Html(`<div class="bottom_left" id="query-bottom-left">` + "\n")
	buffer.Html(`<div class="title-bar">Clickhouse translated query` + "\n" + `</div>`)
	buffer.Html(`<div class="debug-body">`)

	printQueries := func(queries []diag.TranslatedSQLQuery) {
		for _, q := range queries {
			buffer.Text(util.SqlPrettyPrint(q.Query))
			buffer.Text("\n\n")
			qmc.printPerformanceResult(&buffer, q)
			buffer.Text("\n")
		}
	}

	for _, v := range debugKeyValueSlice {
		if withLinks {
			buffer.Html(`<a href="/request-id/`).Text(v.id).Html(`">`)
		}
		tookStr := fmt.Sprintf(" took %d ms", v.query.SecondaryTook.Milliseconds())
		buffer.Html("<p>UUID:").Text(v.id).Text(tookStr).Html(errorBanner(v.query)).Html("</p>\n")
		buffer.Html(`<pre Id="second_query`).Text(v.id).Html(`">`)
		printQueries(v.query.QueryBodyTranslated)

		if v.query.alternativePlanDebugSecondarySource != nil {
			buffer.Text("--  Alternative plan queries --------------------- \n\n")
			printQueries(v.query.alternativePlanDebugSecondarySource.QueryBodyTranslated)
		}

		buffer.Html("\n</pre>")
		if withLinks {
			buffer.Html("\n</a>")
		}
	}
	buffer.Html("\n</div>")
	buffer.Html("\n</div>\n")

	buffer.Html(`<div class="bottom_right" id="query-bottom-right">` + "\n")
	buffer.Html(`<div class="title-bar">Clickhouse response` + "\n" + `</div>`)
	buffer.Html(`<div class="debug-body">`)
	for _, v := range debugKeyValueSlice {
		if withLinks {
			buffer.Html(`<a href="/request-id/`).Text(v.id).Html(`">`)
		}
		buffer.Html("<p>UUID:").Text(v.id).Html(errorBanner(v.query)).Html("</p>\n")
		buffer.Html(`<pre Id="second_response`).Text(v.id).Html(`">`)
		if len(v.query.QueryTranslatedResults) > 0 {
			buffer.Text(string(v.query.QueryTranslatedResults))
		} else {
			buffer.Text("(empty, request was not sent to Clickhouse)")
		}
		buffer.Html("\n\nThere are more results ...")
		buffer.Html("\n</pre>")
		if withLinks {
			buffer.Html("\n</a>")
		}
	}
	buffer.Html("\n</div>")
	buffer.Html("\n</div>\n")

	return buffer.Bytes()
}

func errorBanner(debugInfo queryDebugInfo) string {
	result := ""
	if debugInfo.errorLogCount > 0 {
		result += fmt.Sprintf(` <span class="debug-error-log">%d errors</span>`, debugInfo.errorLogCount)
	}
	if debugInfo.warnLogCount > 0 {
		result += fmt.Sprintf(` <span class="debug-warn-log">%d warnings</span>`, debugInfo.warnLogCount)
	}
	return result
}

func (qmc *QuesmaManagementConsole) printPerformanceResult(buffer *builder.HtmlBuffer, q diag.TranslatedSQLQuery) {

	if qmc.cfg.ClickHouse.AdminUrl != nil {
		// ClickHouse web UI /play expects a base64-encoded query
		// in the URL:
		query := "select * from system.query_log where type='QueryFinish' and query_id = '" + q.QueryID + "'"
		base64QueryBody := base64.StdEncoding.EncodeToString([]byte(query))
		buffer.Html(`<a href="`).Text(qmc.cfg.ClickHouse.AdminUrl.String()).Text("/play#").Text(base64QueryBody).Html(`">`)
	}

	if q.Error != nil {
		errorMsg := q.Error.Error()
		errorMsg = strings.ReplaceAll(errorMsg, "\n", " ")
		buffer.Text(fmt.Sprintf("-- error: %s\n", errorMsg))
	}
	buffer.Text(fmt.Sprintf("-- time: %s, rows returned: %d, query_id: %s \n", q.Duration, q.RowsReturned, q.QueryID))
	if qmc.cfg.ClickHouse.AdminUrl != nil {
		buffer.Html("</a>")
	}

	if len(q.ExplainPlan) > 0 {
		buffer.Text("--  Slow query has been detected. Check logs for explain plan.\n")
	}
	if len(q.QueryTransformations) > 0 {
		buffer.Text(fmt.Sprintf("-- transformations: %s\n", strings.Join(q.QueryTransformations, ", ")))
	}
	if len(q.PerformedOptimizations) > 0 {
		buffer.Text(fmt.Sprintf("-- optimization: %s\n", strings.Join(q.PerformedOptimizations, ", ")))
	}
}
