package ui

import (
	"fmt"
	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/host"
	"github.com/shirou/gopsutil/v3/mem"
	"mitmproxy/quesma/quesma/mux"
	"mitmproxy/quesma/stats"
	"mitmproxy/quesma/stats/errorstats"
	"mitmproxy/quesma/util"
	"net/url"
	"runtime"
	"strings"
	"time"
)

func generateQueries(debugKeyValueSlice []DebugKeyValue, withLinks bool) []byte {
	var buffer HtmlBuffer

	buffer.Html("\n" + `<div class="left" Id="left">` + "\n")
	buffer.Html(`<div class="title-bar">Query`)
	buffer.Html("\n</div>\n")
	buffer.Html(`<div class="debug-body">`)
	for _, v := range debugKeyValueSlice {
		if withLinks {
			buffer.Html(`<a href="/request-Id/`).Text(v.Key).Html(`">`)
		}
		buffer.Html("<p>RequestID:").Text(v.Key).Html("</p>\n")
		buffer.Html(`<pre Id="query`).Text(v.Key).Html(`">`)
		buffer.Text(util.JsonPrettify(string(v.Value.IncomingQueryBody), true))
		buffer.Html("\n</pre>")
		if withLinks {
			buffer.Html("\n</a>")
		}
	}
	buffer.Html("\n</div>")
	buffer.Html("\n</div>\n")

	buffer.Html(`<div class="right" Id="right">` + "\n")
	buffer.Html(`<div class="title-bar">Elasticsearch response` + "\n" + `</div>`)
	buffer.Html(`<div class="debug-body">`)
	for _, v := range debugKeyValueSlice {
		if withLinks {
			buffer.Html(`<a href="/request-Id/`).Text(v.Key).Html(`">`)
		}
		buffer.Html("<p>ResponseID:").Text(v.Key).Html("</p>\n")
		buffer.Html(`<pre Id="response`).Text(v.Key).Html(`">`)
		buffer.Text(util.JsonPrettify(string(v.Value.QueryResp), true))
		buffer.Html("\n</pre>")
		if withLinks {
			buffer.Html("\n</a>")
		}
	}
	buffer.Html("\n</div>")
	buffer.Html("\n</div>\n")

	buffer.Html(`<div class="bottom_left" Id="bottom_left">` + "\n")
	buffer.Html(`<div class="title-bar">Clickhouse translated query` + "\n" + `</div>`)
	buffer.Html(`<div class="debug-body">`)
	for _, v := range debugKeyValueSlice {
		if withLinks {
			buffer.Html(`<a href="/request-Id/`).Text(v.Key).Html(`">`)
		}
		buffer.Html("<p>RequestID:").Text(v.Key).Html("</p>\n")
		buffer.Html(`<pre Id="second_query`).Text(v.Key).Html(`">`)
		buffer.Text(sqlPrettyPrint(v.Value.QueryBodyTranslated))
		buffer.Html("\n</pre>")
		if withLinks {
			buffer.Html("\n</a>")
		}
	}
	buffer.Html("\n</div>")
	buffer.Html("\n</div>\n")

	buffer.Html(`<div class="bottom_right" Id="bottom_right">` + "\n")
	buffer.Html(`<div class="title-bar">Clickhouse response` + "\n" + `</div>`)
	buffer.Html(`<div class="debug-body">`)
	for _, v := range debugKeyValueSlice {
		if withLinks {
			buffer.Html(`<a href="/request-Id/`).Text(v.Key).Html(`">`)
		}
		buffer.Html("<p>ResponseID:").Text(v.Key).Html("</p>\n")
		buffer.Html(`<pre Id="second_response`).Text(v.Key).Html(`">`)
		buffer.Text(util.JsonPrettify(string(v.Value.QueryTranslatedResults), true))
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

func dropFirstSegment(path string) string {
	segments := strings.SplitN(path, "/", 3)
	if len(segments) > 2 {
		return "/" + segments[2]
	}
	return path
}

func (qmc *QuesmaManagementConsole) generateRouterStatistics() []byte {
	var buffer HtmlBuffer

	matchedKeys, matched, unmatchedKeys, unmatched := mux.MatchStatistics().GroupByFirstSegment()

	buffer.Html("\n<h2>Matched URLs</h2>\n<ul>")
	for _, segment := range matchedKeys {
		paths := matched[segment]
		if len(paths) > 1 {
			buffer.Html("<li>").Text(segment).Html("</li>")

			buffer.Html("<ul>\n")
			for _, path := range paths {
				buffer.Html("<li><small>").Text(dropFirstSegment(path)).Html("</small></li>")
			}
			buffer.Html("</ul>\n")
		} else {
			buffer.Html("<li>").Text(paths[0]).Html("</li>\n")
		}
	}

	buffer.Html("</ul>\n")
	buffer.Html("\n<h2>Not matched URLs</h2>\n<ul>")
	for _, segment := range unmatchedKeys {
		paths := unmatched[segment]
		if len(paths) > 1 {
			buffer.Html("<li>").Text(segment).Html("</li>")

			buffer.Html("<ul>\n")
			for _, path := range paths {
				buffer.Html("<li><small>").Text(dropFirstSegment(path)).Html("</small></li>")
			}
			buffer.Html("</ul>\n")
		} else {
			buffer.Html("<li>").Text(paths[0]).Html("</li>\n")
		}
	}
	buffer.Html("</ul>\n")

	return buffer.Bytes()
}

func (qmc *QuesmaManagementConsole) generateStatistics() []byte {
	var buffer HtmlBuffer
	const maxTopValues = 5

	if !qmc.config.IngestStatistics {
		buffer.Html("<h2>Statistics are disabled.</h2>\n")
		buffer.Html("<p>&nbsp;You can enable them by changing ingest_statistics setting to true.</p>\n")
		return buffer.Bytes()
	}

	statistics := stats.GlobalStatistics

	for _, index := range statistics.SortedIndexNames() {
		buffer.Html("\n<h2> Stats for \"").Text(index.IndexName).
			Html(fmt.Sprintf("\" <small>from %d requests</small></h2>\n", index.Requests))

		buffer.Html("<table>\n")

		buffer.Html("<thead>\n")
		buffer.Html(`<tr>` + "\n")
		buffer.Html(`<th class="key">Key</th>` + "\n")
		buffer.Html(`<th class="key-count">Count</th>` + "\n")
		buffer.Html(`<th class="value">Value</th>` + "\n")
		buffer.Html(`<th class="value-count">Count</th>` + "\n")
		buffer.Html(`<th class="value-count">Percentage</th>` + "\n")
		buffer.Html(`<th class="types">Potential type</th>` + "\n")
		buffer.Html("</tr>\n")
		buffer.Html("</thead>\n")
		buffer.Html("<tbody>\n")

		for _, keyStats := range index.SortedKeyStatistics() {
			topValuesCount := maxTopValues
			if len(keyStats.Values) < maxTopValues {
				topValuesCount = len(keyStats.Values)
			}

			buffer.Html(`<tr class="group-divider">` + "\n")
			buffer.Html(fmt.Sprintf(`<td class="key" rowspan="%d">`, topValuesCount)).Text(keyStats.KeyName).Html("</td>\n")
			buffer.Html(fmt.Sprintf(`<td class="key-count" rowspan="%d">%d</td>`+"\n", topValuesCount, keyStats.Occurrences))

			for i, value := range keyStats.TopNValues(topValuesCount) {
				if i > 0 {
					buffer.Html("</tr>\n<tr>\n")
				}

				buffer.Html(`<td class="value">`).Text(value.ValueName).Html(`</td>`)
				buffer.Html(fmt.Sprintf(`<td class="value-count">%d</td>`, value.Occurrences))
				buffer.Html(fmt.Sprintf(`<td class="value-count">%.1f%%</td>`, 100*float32(value.Occurrences)/float32(keyStats.Occurrences)))
				buffer.Html(fmt.Sprintf(`<td class="types">%s</td>`, strings.Join(value.Types, ", ")))
			}
			buffer.Html("</tr>\n")
		}

		buffer.Html("</tbody>\n")

		buffer.Html("</table>\n")
	}

	return buffer.Bytes()
}

func secondsToTerseString(second uint64) string {
	return (time.Duration(second) * time.Second).String()
}

func statusToDiv(s healthCheckStatus) string {
	return fmt.Sprintf(`<div class="status %s" title="%s">%s</div>`, s.status, s.tooltip, s.message)
}

func (qmc *QuesmaManagementConsole) generateDashboardPanel() []byte {
	var buffer HtmlBuffer

	buffer.Html(`<div id="dashboard-kibana" class="component">`)
	buffer.Html(`<h3>Kibana</h3>`)
	buffer.Html(statusToDiv(qmc.checkKibana()))
	buffer.Html(`</div>`)

	buffer.Html(`<div id="dashboard-ingest" class="component">`)
	buffer.Html(`<h3>Ingest</h3>`)
	buffer.Html(statusToDiv(qmc.checkIngest()))
	buffer.Html(`</div>`)

	buffer.Html(`<div id="dashboard-elasticsearch" class="component">`)
	buffer.Html(`<h3>Elastic</h3><h3>search</h3>`)
	buffer.Html(statusToDiv(qmc.checkElasticsearch()))
	buffer.Html(`</div>`)

	buffer.Html(`<div id="dashboard-clickhouse" class="component">`)
	buffer.Html(`<h3>ClickHouse</h3>`)
	buffer.Html(statusToDiv(qmc.checkClickhouseHealth()))
	buffer.Html(`</div>`)

	buffer.Html(`<div id="dashboard-traffic" class="component">`)

	buffer.Html(`<div id="dashboard-quesma" class="component">`)
	buffer.Html(`<h3>Quesma</h3>`)

	cpuStr := ""
	c0, err0 := cpu.Percent(0, false)

	if err0 == nil {
		cpuStr = fmt.Sprintf("Host CPU: %.1f%%", c0[0])
	} else {
		cpuStr = fmt.Sprintf("Host CPU: N/A (error: %s)", err0.Error())
	}

	buffer.Html(fmt.Sprintf(`<div class="status">%s</div>`, cpuStr))

	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	memStr := fmt.Sprintf("Memory - used: %1.f MB", float64(m.Alloc)/1024.0/1024.0)
	if v, errV := mem.VirtualMemory(); errV == nil {
		total := float64(v.Total) / 1024.0 / 1024.0 / 1024.0
		memStr += fmt.Sprintf(", available: %.1f GB", total)
	}
	buffer.Html(fmt.Sprintf(`<div class="status">%s</div>`, memStr))

	duration := uint64(time.Since(qmc.startedAt).Seconds())

	buffer.Html(fmt.Sprintf(`<div class="status">Started: %s ago</div>`, secondsToTerseString(duration)))
	buffer.Html(fmt.Sprintf(`<div class="status">Mode: %s</div>`, qmc.config.Mode.String()))

	if h, errH := host.Info(); errH == nil {
		buffer.Html(fmt.Sprintf(`<div class="status">Host uptime: %s</div>`, secondsToTerseString(h.Uptime)))
	}
	buffer.Html(`</div>`)

	buffer.Html(`<div id="dashboard-errors" class="component">`)
	errors := errorstats.GlobalErrorStatistics.ReturnTopErrors(5)
	if len(errors) > 0 {
		buffer.Html(`<h3>Top errors:</h3>`)
		for _, e := range errors {
			buffer.Html(fmt.Sprintf(`<div class="status">%d: <a href="/error/%s">%s</a></div>`,
				e.Count, url.PathEscape(e.Reason), e.Reason))
		}
	} else {
		buffer.Html(`<h3>No errors</h3>`)
	}
	buffer.Html(`</div>`)
	buffer.Html(`</div>`)

	return buffer.Bytes()
}

func (qmc *QuesmaManagementConsole) generateDashboardTrafficText(typeName string) (string, string) {
	reqStats := qmc.requestsStore.GetRequestsStats(typeName)
	status := "green"
	if reqStats.ErrorRate > 0.20 {
		status = "red"
	}
	return status, fmt.Sprintf("%4.1f req/s, err:%5.1f%%, p99:%3dms",
		reqStats.RatePerMinute/60, reqStats.ErrorRate*100, reqStats.Duration99Percentile)
}

func (qmc *QuesmaManagementConsole) generateDashboardTrafficElement(typeName string, y int) string {
	status, text := qmc.generateDashboardTrafficText(typeName)
	return fmt.Sprintf(`<text x="400" y="%d" class="%s" xml:space="preserve">%s</text>`, y, status, text)
}

func (qmc *QuesmaManagementConsole) generateDashboardTrafficPanel() []byte {
	var buffer HtmlBuffer

	buffer.Html(`<svg width="100%" height="100%" viewBox="0 0 1000 1000">`)

	// Clickhouse -> Kibana
	if qmc.config.ReadsFromClickhouse() {
		buffer.Html(qmc.generateDashboardTrafficElement(RequestStatisticKibana2Clickhouse, 240))
	}

	// Elasticsearch -> Kibana
	if qmc.config.ReadsFromElasticsearch() {
		buffer.Html(qmc.generateDashboardTrafficElement(RequestStatisticKibana2Elasticsearch, 690))
	}

	// Ingest -> Clickhouse
	if qmc.config.WritesToClickhouse() {
		buffer.Html(qmc.generateDashboardTrafficElement(RequestStatisticIngest2Clickhouse, 340))
	}

	// Ingest -> Elasticsearch
	if qmc.config.WritesToElasticsearch() {
		buffer.Html(qmc.generateDashboardTrafficElement(RequestStatisticIngest2Elasticsearch, 790))
	}
	buffer.Html(`</svg>`)

	return buffer.Bytes()
}
