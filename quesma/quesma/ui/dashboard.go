// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package ui

import (
	"fmt"
	"github.com/QuesmaOrg/quesma/quesma/buildinfo"
	"github.com/QuesmaOrg/quesma/quesma/health"
	"github.com/QuesmaOrg/quesma/quesma/logger"
	"github.com/QuesmaOrg/quesma/quesma/quesma/ui/internal/builder"
	"github.com/QuesmaOrg/quesma/quesma/stats/errorstats"
	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/host"
	"github.com/shirou/gopsutil/v3/mem"
	"html"
	"net/url"
	"runtime"
	"strings"
	"time"
)

func (qmc *QuesmaManagementConsole) generateDashboard() []byte {
	buffer := newBufferWithHead()
	buffer.Write(qmc.generateTopNavigation("dashboard"))

	buffer.Html(`<main id="dashboard-main">` + "\n")

	// Unfortunately, we need tiny bit of javascript to pause the animation.
	buffer.Html(`
		<script type="text/javascript">
		var checkbox = document.getElementById("autorefresh");
		var dashboard = document.getElementById("dashboard-main");
		checkbox.addEventListener('change', function() {
			if (this.checked) {
				dashboard.classList.remove("paused");
			} else {
				dashboard.classList.add("paused");
			}
		});
		</script>
	`)

	buffer.Html(`<div id="svg-container">`)
	buffer.Html(`<svg width="100%" height="100%" viewBox="0 0 1000 1000" preserveAspectRatio="none">` + "\n")
	// One limitation is that, we don't update color of paths after initial draw.
	// They rarely change, so it's not a big deal for now.
	// Clickhouse -> Kibana
	if qmc.cfg.ReadsFromClickhouse() {
		status, _ := qmc.generateDashboardTrafficText(RequestStatisticKibana2Clickhouse)
		buffer.Html(fmt.Sprintf(`<path d="M 0 250 L 1000 250" fill="none" stroke="%s" />`, status))
	}
	// Elasticsearch -> Kibana
	if qmc.cfg.ReadsFromElasticsearch() {
		status, _ := qmc.generateDashboardTrafficText(RequestStatisticKibana2Elasticsearch)
		buffer.Html(fmt.Sprintf(`<path d="M 0 350 L 150 350 L 150 700 L 1000 700" fill="none" stroke="%s" />`, status))
	}

	// Ingest -> Clickhouse
	if qmc.cfg.WritesToClickhouse() {
		status, _ := qmc.generateDashboardTrafficText(RequestStatisticIngest2Clickhouse)
		buffer.Html(fmt.Sprintf(`<path d="M 1000 350 L 300 350 L 300 650 L 0 650" fill="none" stroke="%s" />`, status))
	}
	// Ingest -> Elasticsearch
	if qmc.cfg.WritesToElasticsearch() {
		status, _ := qmc.generateDashboardTrafficText(RequestStatisticIngest2Elasticsearch)
		buffer.Html(fmt.Sprintf(`<path d="M 1000 800 L 0 800" fill="none" stroke="%s" />`, status))
	}
	buffer.Html(`</svg>` + "\n")
	buffer.Write(qmc.generateDashboardTrafficPanel())
	buffer.Html(`</div>` + "\n")

	buffer.Html(`<div id="dashboard">` + "\n")
	buffer.Write(qmc.generateDashboardPanel())
	buffer.Html("</div>\n")
	buffer.Html("\n</main>\n\n")

	buffer.Html("\n</body>")
	buffer.Html("\n</html>")
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
	return fmt.Sprintf(
		`<div style="left: 40%%; top: %d%%" id="traffic-%s" hx-swap-oob="true" class="traffic-element %s">%s</div>`,
		y, typeName, status, text)
}

func (qmc *QuesmaManagementConsole) generateDashboardTrafficPanel() []byte {
	var buffer builder.HtmlBuffer

	// Clickhouse -> Kibana
	if qmc.cfg.ReadsFromClickhouse() {
		buffer.Html(qmc.generateDashboardTrafficElement(RequestStatisticKibana2Clickhouse, 21))
	}

	// Elasticsearch -> Kibana
	if qmc.cfg.ReadsFromElasticsearch() {
		buffer.Html(qmc.generateDashboardTrafficElement(RequestStatisticKibana2Elasticsearch, 66))
	}

	// Ingest -> Clickhouse
	if qmc.cfg.WritesToClickhouse() {
		buffer.Html(qmc.generateDashboardTrafficElement(RequestStatisticIngest2Clickhouse, 31))
	}

	// Ingest -> Elasticsearch
	if qmc.cfg.WritesToElasticsearch() {
		buffer.Html(qmc.generateDashboardTrafficElement(RequestStatisticIngest2Elasticsearch, 76))
	}

	return buffer.Bytes()
}

func secondsToTerseString(second uint64) string {
	return (time.Duration(second) * time.Second).String()
}

func statusToDiv(s health.Status) string {
	return fmt.Sprintf(`<span class="status %s" title="%s">%s</span>`, html.EscapeString(s.Status),
		html.EscapeString(s.Tooltip), html.EscapeString(s.Message))
}

func (qmc *QuesmaManagementConsole) generateDashboardPanel() []byte {
	var buffer builder.HtmlBuffer

	dashboardName := "<h3>Kibana</h3>"
	storeName := "<h3>Elasticsearch</h3>"
	if qmc.cfg.Elasticsearch.Url != nil && strings.Contains(qmc.cfg.Elasticsearch.Url.String(), "opensearch") {
		dashboardName = "<h3>OpenSearch</h3><h3>Dashboards</h3>"
		storeName = "<h3>OpenSearch</h3>"
	}

	clickhouseName := "<h3>ClickHouse</h3>"
	if qmc.cfg.Hydrolix.Url != nil {
		clickhouseName = "<h3>Hydrolix</h3>"
	}

	buffer.Html(`<div id="dashboard-kibana" class="component">`)
	if qmc.cfg.Elasticsearch.AdminUrl != nil {
		buffer.Html(`<a href="`).Text(qmc.cfg.Elasticsearch.AdminUrl.String()).Html(`">`)
	}
	buffer.Html(dashboardName)
	if qmc.cfg.Elasticsearch.AdminUrl != nil {
		buffer.Html(`</a>`)
	}
	buffer.Html(statusToDiv(qmc.checkKibana()))
	buffer.Html(`</div>`)

	buffer.Html(`<div id="dashboard-ingest" class="component">`)
	buffer.Html(`<h3>Ingest</h3>`)
	buffer.Html(statusToDiv(qmc.checkIngest()))
	buffer.Html(`</div>`)

	buffer.Html(`<div id="dashboard-elasticsearch" class="component">`)
	buffer.Html(storeName)
	buffer.Html(statusToDiv(qmc.checkElasticsearch()))
	buffer.Html(`</div>`)

	buffer.Html(`<div id="dashboard-clickhouse" class="component">`)
	if qmc.cfg.ClickHouse.AdminUrl != nil {
		buffer.Html(`<a href="`).Text(qmc.cfg.ClickHouse.AdminUrl.String()).Html(`">`)
	}
	buffer.Html(clickhouseName)
	if qmc.cfg.ClickHouse.AdminUrl != nil {
		buffer.Html(`</a>`)
	}
	buffer.Html(statusToDiv(qmc.checkClickhouseHealth()))
	buffer.Html(`</div>`)

	buffer.Html(`<div id="dashboard-traffic" class="component">`)

	buffer.Html(`<div id="dashboard-quesma" class="component">`)
	buffer.Html(`<h3>Quesma</h3>`)

	buffer.Write(qmc.maybePrintUpgradeAvailableBanner())

	buffer.Html(`<div class="status">Version: `)
	buffer.Text(buildinfo.Version)
	buffer.Html("</div>")

	cpuStr := ""
	c0, err0 := cpu.Percent(0, false)

	if err0 == nil {
		cpuStr = fmt.Sprintf("Host CPU: %.1f%%", c0[0])
	} else {
		cpuStr = fmt.Sprintf("Host CPU: N/A (error: %s)", err0.Error())
	}

	buffer.Html(`<div class="status">`).Text(cpuStr).Html(`</div>`)

	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	memStr := fmt.Sprintf("Memory used: %1.f MB", float64(m.Alloc)/1024.0/1024.0)
	if v, errV := mem.VirtualMemory(); errV == nil {
		total := float64(v.Total) / 1024.0 / 1024.0 / 1024.0
		memStr += fmt.Sprintf(", avail: %.1f GB", total)
	}
	buffer.Html(`<div class="status">`).Text(memStr).Html(`</div>`)

	duration := uint64(time.Since(qmc.startedAt).Seconds())

	buffer.Html(fmt.Sprintf(`<div class="status">Started: %s ago</div>`, secondsToTerseString(duration)))
	buffer.Html(fmt.Sprintf(`<div class="status">Transparent proxy: %t</div>`, qmc.cfg.TransparentProxy))

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

type latestVersionCheckResult struct {
	upgradeAvailable bool
	message          string
}

// maybePrintUpgradeAvailableBanner has time cap of 500ms to check for the latest version, if it takes longer than that,
// it will log an error message and don't render anything
func (qmc *QuesmaManagementConsole) maybePrintUpgradeAvailableBanner() []byte {
	if qmc.cfg.Logging.RemoteLogDrainUrl == nil {
		return nil
	}

	resultChan := make(chan latestVersionCheckResult, 1)
	go func() {
		upgradeAvailable, message := buildinfo.CheckForTheLatestVersion()
		resultChan <- latestVersionCheckResult{upgradeAvailable, message}
	}()
	buffer := builder.HtmlBuffer{}
	select {
	case result := <-resultChan:
		if result.upgradeAvailable {
			buffer.Html(`<div class="status" style="background-color: yellow; padding: 5px;">`)
			buffer.Text(result.message)
			buffer.Html("</div>")
		}
	case <-time.After(500 * time.Millisecond):
		logger.Error().Msg("Timeout while checking for the latest version.")
	}
	return buffer.Bytes()
}
