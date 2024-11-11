// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package ui

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"quesma/elasticsearch"
	"quesma/jsondiff"
	"quesma/logger"
	"quesma/quesma/ui/internal/builder"
	"strings"
	"time"
)

const abTestingPath = "/ab-testing-dashboard"

func (qmc *QuesmaManagementConsole) hasABTestingTable() bool {

	db := qmc.logManager.GetDB()

	sql := `SELECT count(*) FROM ab_testing_logs`

	row := db.QueryRow(sql)
	var count int
	err := row.Scan(&count)
	if err != nil {
		logger.Error().Err(err).Msg("Error checking for ab_testing_logs table")
		return false
	}

	return true
}

func (qmc *QuesmaManagementConsole) renderError(buff *builder.HtmlBuffer, err error) {

	buff.Html(`<div style="border: 10px solid red; padding: 5em; margin: 5em; color: red">`)
	buff.Html(`<h2>Error</h2>`)
	buff.Html(`<p>`)
	buff.Text(err.Error())
	buff.Html(`</p>`)
	buff.Html(`</div>`)

}

func (qmc *QuesmaManagementConsole) generateABTestingDashboard() []byte {

	buffer := newBufferWithHead()
	buffer.Write(qmc.generateTopNavigation("ab-testing-dashboard"))

	buffer.Html(`<main id="ab_testing_dashboard">`)

	explanation := `
This table compares results and performance of Kibana dashboards and its panels as seen by Quesma. 
Every panel query returning similar results is a success, 
load times are calculated into performance gain  as a percentage by comparing the average times of first and second backend connectors for successful responses.
If the performance gain is positive, it means that the second backend connector is faster than the first one.
	`

	buffer.Html(`<h2>Kibana dashboards compatibility report <span class="tooltip" data-tooltip="`)
	buffer.Text(explanation)
	buffer.Html(`">&#9432;</span></h2>`)

	if qmc.hasABTestingTable() {

		buffer.Html(`<form name="form1" hx-post="`)
		buffer.Text(abTestingPath)
		buffer.Html(`/report" hx-trigger="submit,load,change" hx-target="#report">`)
		buffer.Html(`<label for="kibana_url">Kibana URL</label>`)
		buffer.Html(`<input id="kibana_url" name="kibana_url" type="text"  value="http://localhost:5601"/>`)
		buffer.Html(`<br>`)
		buffer.Html(`<label for="order_by">Sort by</label>`)
		buffer.Html(`<select id="order_by" name="order_by">`)
		buffer.Html(`<option value="default">Default</option>`)
		buffer.Html(`<option value="response_similarity">Response similarity</option>`)
		buffer.Html(`<option value="performance_gain">Performance gain</option>`)
		buffer.Html(`<option value="count">Count</option>`)
		buffer.Html(`</select>`)
		buffer.Html(`<br>`)
		buffer.Html(`<button type="submit">Refresh</button>`)
		buffer.Html(`</form>`)
		buffer.Html(`<div id="report"></div>`)

		buffer.Html(`<div class="menu">`)
		buffer.Html("\n</div>")
	} else {
		buffer.Html(`<p>A/B Testing results are not available.</p>`)
	}

	buffer.Html("\n</main>\n\n")
	return buffer.Bytes()
}

type kibanaDashboard struct {
	name   string
	panels map[string]string
}

type resolvedDashboards struct {
	dashboards map[string]kibanaDashboard
}

func (d resolvedDashboards) dashboardName(dashboardId string) string {
	if dashboard, ok := d.dashboards[dashboardId]; ok {
		return dashboard.name
	}
	return dashboardId
}

func (d resolvedDashboards) panelName(dashboardId, panelId string) string {
	if dashboard, ok := d.dashboards[dashboardId]; ok {
		if name, ok := dashboard.panels[panelId]; ok {
			return name
		}
	}
	return panelId
}

func (qmc *QuesmaManagementConsole) readKibanaDashboards() (resolvedDashboards, error) {

	result := resolvedDashboards{
		dashboards: make(map[string]kibanaDashboard),
	}

	elasticQuery := `
{
  "_source": false,
  "fields": [
    "_id",
    "dashboard.title",
    "panelsJSON",
    "dashboard.panelsJSON"
    ],
    "query": {
        "bool": {
        	"filter": [
               {
        	    	"term": {
                    	 "type": "dashboard"
                	}
            	}
        	]
        }
    }
}
`
	client := elasticsearch.NewSimpleClient(&qmc.cfg.Elasticsearch)

	resp, err := client.Request(context.Background(), "POST", ".kibana_analytics/_search", []byte(elasticQuery))
	if err != nil {
		return result, err
	}

	if resp.StatusCode != 200 {
		return result, fmt.Errorf("unexpected HTTP status: %s", resp.Status)
	}

	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return result, err
	}

	type responseSchema struct {
		Hits struct {
			Hits []struct {
				Fields struct {
					Id     []string `json:"_id"`
					Title  []string `json:"dashboard.title"`
					Panels []string `json:"dashboard.panelsJSON"`
				} `json:"fields"`
			} `json:"hits"`
		} `json:"hits"`
	}

	type panelSchema struct {
		Type    string `json:"type"`
		PanelID string `json:"panelIndex"`
		Name    string `json:"title"`
	}

	var response responseSchema
	err = json.Unmarshal(data, &response)
	if err != nil {
		return result, err
	}

	for _, hit := range response.Hits.Hits {
		if len(hit.Fields.Id) == 0 {
			continue // no ID, skip
		}
		_id := hit.Fields.Id[0]

		var title string
		if len(hit.Fields.Title) > 0 {
			title = hit.Fields.Title[0]
		} else {
			title = _id
		}
		_id = strings.TrimPrefix(_id, "dashboard:")

		var panels string
		if len(hit.Fields.Panels) > 0 {
			panels = hit.Fields.Panels[0]
		} else {
			panels = "[]" // empty array, so we can unmarshal it
		}

		var panelsJson []panelSchema
		err := json.Unmarshal([]byte(panels), &panelsJson)
		if err != nil {
			return result, err
		}

		dashboard := kibanaDashboard{
			name:   title,
			panels: make(map[string]string),
		}

		for _, panel := range panelsJson {
			if panel.Name == "" {
				panel.Name = panel.PanelID
			}
			dashboard.panels[panel.PanelID] = panel.Name
		}
		result.dashboards[_id] = dashboard
	}

	return result, nil
}

func parseMismatches(mismatch string) ([]jsondiff.JSONMismatch, error) {
	var mismatches []jsondiff.JSONMismatch
	err := json.Unmarshal([]byte(mismatch), &mismatches)
	return mismatches, err
}

func formatJSON(in *string) string {
	if in == nil {
		return "n/a"
	}

	m := make(map[string]interface{})

	err := json.Unmarshal([]byte(*in), &m)
	if err != nil {
		return err.Error()
	}

	b, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err.Error()
	}
	return string(b)
}

type abTestingReportRow struct {
	dashboardId     string
	panelId         string
	dashboardUrl    string
	detailsUrl      string
	dashboardName   string
	panelName       string
	aName           string
	bName           string
	aTime           *float64
	bTime           *float64
	successRate     *float64
	performanceGain *float64
	count           int
}

func (qmc *QuesmaManagementConsole) abTestingReadReport(kibanaUrl, orderBy string) ([]abTestingReportRow, error) {

	kibanaDashboards, err := qmc.readKibanaDashboards()
	if err != nil {
		logger.Warn().Msgf("Error reading dashboards %v", err)
	}

	orderByToSQL := map[string]string{
		"default":             "dashboard_id, panel_id, a_name, b_name",
		"response_similarity": "response_similarity DESC, dashboard_id, panel_id, a_name, b_name",
		"performance_gain":    "performance_gain DESC,dashboard_id, panel_id, a_name, b_name",
		"count":               "count DESC,dashboard_id, panel_id, a_name, b_name",
	}

	orderBySQL, ok := orderByToSQL[orderBy]
	if !ok {
		orderBySQL = orderByToSQL["default"]
	}

	sql := `
WITH subresults AS (
SELECT
   kibana_dashboard_id , 
   kibana_dashboard_panel_id,
   response_a_name AS a_name,
   response_b_name AS b_name,
   response_mismatch_is_ok AS ok ,
   count(*) AS c,
   avg(response_a_time) AS a_time, 
   avg(response_b_time) AS b_time 
FROM
  ab_testing_logs GROUP BY 1,2,3,4,5
)

SELECT 
  kibana_dashboard_id AS dashboard_id,
  kibana_dashboard_panel_id  AS panel_id,
  a_name,
  b_name,
  (sumIf(c,ok)/ sum(c)) * 100 as response_similarity,
 
  sum(c) as count,
  avgIf(a_time,ok) as a_time,
	avgIf(b_time,ok) as b_time
FROM
  subresults 
GROUP BY  
 kibana_dashboard_id,kibana_dashboard_panel_id,a_name,b_name 
`

	sql = sql + " ORDER BY " + orderBySQL

	var result []abTestingReportRow

	db := qmc.logManager.GetDB()
	rows, err := db.Query(sql, orderBySQL)
	if err != nil {
		return nil, err
	}

	for rows.Next() {
		row := abTestingReportRow{}
		err := rows.Scan(&row.dashboardId, &row.panelId, &row.aName, &row.bName, &row.successRate, &row.count, &row.aTime, &row.bTime)
		if err != nil {
			return nil, err
		}

		row.dashboardUrl = fmt.Sprintf("%s/app/kibana#/dashboard/%s", kibanaUrl, row.dashboardId)
		row.detailsUrl = fmt.Sprintf("%s/panel?dashboard_id=%s&panel_id=%s", abTestingPath, row.dashboardId, row.panelId)
		row.dashboardName = kibanaDashboards.dashboardName(row.dashboardId)
		row.panelName = kibanaDashboards.panelName(row.dashboardId, row.panelId)

		if row.aTime != nil && row.bTime != nil {
			if *row.aTime == 0 {
				row.performanceGain = nil
			} else {
				gain := (*row.aTime - *row.bTime) / *row.aTime * 100
				row.performanceGain = &gain
			}
		}

		result = append(result, row)
	}

	if rows.Err() != nil {
		return nil, rows.Err()
	}

	return result, nil
}

func (qmc *QuesmaManagementConsole) generateABTestingReport(kibanaUrl, orderBy string) []byte {
	buffer := newBufferWithHead()

	buffer.Html("<table>\n")
	buffer.Html("<thead>\n")
	buffer.Html(`<tr>` + "\n")
	buffer.Html(`<th class="key">Dashboard</th>` + "\n")
	buffer.Html(`<th class="key">Panel</th>` + "\n")
	buffer.Html(`<th class="key">Count <br> <small>(since start)</small></th>` + "\n")
	buffer.Html(`<th class="key">Response similarity</th>` + "\n")
	buffer.Html(`<th class="key">Performance gain</th>` + "\n")
	buffer.Html(`<th class="key"></th>` + "\n")
	buffer.Html("</tr>\n")
	buffer.Html("</thead>\n")
	buffer.Html("<tbody>\n")

	rows, err := qmc.abTestingReadReport(kibanaUrl, orderBy)
	if err != nil {
		qmc.renderError(&buffer, err)
		return buffer.Bytes()
	}

	var lastDashboardId string
	for _, row := range rows {
		buffer.Html(`<tr>` + "\n")

		if lastDashboardId != row.dashboardId {
			buffer.Html(`<td>`)
			buffer.Html(`<a target="_blank" href="`).Text(row.dashboardUrl).Html(`">`).Text(row.dashboardName).Html(`</a>`)
			buffer.Html("<br>")
			buffer.Text(fmt.Sprintf("(%s vs %s)", row.aName, row.bName))
			buffer.Html(`</td>`)
			lastDashboardId = row.dashboardId
		} else {
			buffer.Html(`<td></td>`)
		}

		buffer.Html(`<td>`)
		buffer.Text(row.panelName)
		buffer.Html(`</td>`)

		buffer.Html(`<td>`)
		buffer.Text(fmt.Sprintf("%d", row.count))
		buffer.Html(`</td>`)

		buffer.Html(`<td>`)
		if row.successRate != nil {
			buffer.Text(fmt.Sprintf("%.01f%%", *row.successRate))
		} else {
			buffer.Text("n/a")
		}
		buffer.Html(`</td>`)

		buffer.Html(`<td>`)

		const minTime = 0.05 // 50ms, we don't show performance gain for queries faster than this
		const maxTime = 5.0  // if a query takes longer than this, we show the name of the slowest backend

		if row.performanceGain != nil {

			switch {

			case *row.aTime < minTime && *row.bTime < minTime:
				buffer.Text("both < 50ms")

			case *row.aTime > maxTime && *row.bTime < maxTime:
				buffer.Text(fmt.Sprintf("%s is over %0.02fs", row.aName, *row.aTime))

			case *row.bTime > maxTime && *row.aTime < maxTime:
				buffer.Text(fmt.Sprintf("%s is over %0.02fs", row.bName, *row.bTime))

			default:
				buffer.Html(`<span title="`)
				buffer.Html(fmt.Sprintf("%s=%.03fs, %s=%.03fs", row.aName, *row.aTime, row.bName, *row.bTime))
				buffer.Html(`">`)
				buffer.Text(fmt.Sprintf("%.01f%%", *row.performanceGain))
				buffer.Html(`</span>`)
			}

		} else {
			buffer.Text("n/a")
		}
		buffer.Html(`</td>`)

		buffer.Html("<td>")

		buffer.Html(`<a target="_blank" href="`)
		buffer.Text(row.detailsUrl)
		buffer.Html(`">`)
		buffer.Text("Details")
		buffer.Html(`</a>`)

		buffer.Html("</td>")
		buffer.Html("</tr>")
	}

	buffer.Html("</tbody>\n")
	buffer.Html("</table>\n")

	return buffer.Bytes()
}

type abTestingPanelDetailsRow struct {
	mismatch   string
	mismatchId string
	count      int
}

func (qmc *QuesmaManagementConsole) abTestingReadPanelDetails(dashboardId, panelId string) ([]abTestingPanelDetailsRow, error) {

	sql := `
		select  response_mismatch_mismatches, response_mismatch_sha1, count() as c
		from ab_testing_logs 
		where kibana_dashboard_id = ? and 
		      kibana_dashboard_panel_id = ? and 
		      response_mismatch_is_ok = false
		group  by 1,2
		order by c desc
		limit 100
`
	db := qmc.logManager.GetDB()

	rows, err := db.Query(sql, dashboardId, panelId)
	if err != nil {
		return nil, err
	}

	var result []abTestingPanelDetailsRow
	for rows.Next() {

		var mismatch string
		var count int
		var mismatchId string

		err := rows.Scan(&mismatch, &mismatchId, &count)
		if err != nil {
			return nil, err
		}

		r := abTestingPanelDetailsRow{
			mismatch:   mismatch,
			mismatchId: mismatchId,
			count:      count,
		}
		result = append(result, r)
	}

	if rows.Err() != nil {
		return nil, rows.Err()
	}

	return result, nil
}

func (qmc *QuesmaManagementConsole) renderABTestingMismatch(buffer *builder.HtmlBuffer, mismatch jsondiff.JSONMismatch) {

	buffer.Html(`<li>`)
	buffer.Html(`<p>`)
	buffer.Text(mismatch.Message)
	buffer.Text(" ")

	if mismatch.Path != "" {
		buffer.Html(`<code>`)
		buffer.Text(`(`)
		buffer.Text(mismatch.Path)
		buffer.Text(`)`)
		buffer.Html(`</code>`)
		{ // poor man's HTML indent
			buffer.Html(`<ul>`)

			buffer.Html(`<li>`)
			buffer.Html(`<code>`)
			buffer.Text("Expected: ")
			buffer.Text(mismatch.Expected)
			buffer.Html(`</code>`)
			buffer.Html(`</li>`)

			buffer.Html(`<li>`)
			buffer.Html(`<code>`)
			buffer.Text("Actual: ")
			buffer.Text(mismatch.Actual)
			buffer.Html(`</code>`)
			buffer.Html(`</li>`)

			buffer.Html(`</ul>`)
		}
	}
	buffer.Html(`</p>`)
	buffer.Html(`</li>`)

}

func (qmc *QuesmaManagementConsole) generateABPanelDetails(dashboardId, panelId string) []byte {
	buffer := newBufferWithHead()

	dashboards, err := qmc.readKibanaDashboards()
	dashboardName := dashboardId
	panelName := panelId

	if err == nil {
		dashboardName = dashboards.dashboardName(dashboardId)
		panelName = dashboards.panelName(dashboardId, panelId)
	} else {
		logger.Warn().Err(err).Msgf("Error reading dashboards %v", err)
	}

	buffer.Html(`<main id="ab_testing_dashboard">`)

	buffer.Html(`<h2>A/B Testing - Panel Details</h2>`)
	buffer.Html(`<h3>`)
	buffer.Text(fmt.Sprintf("Dashboard: %s", dashboardName))
	buffer.Html(`</h3>`)
	buffer.Html(`<h3>`)
	buffer.Text(fmt.Sprintf("Panel: %s", panelName))
	buffer.Html(`</h3>`)

	rows, err := qmc.abTestingReadPanelDetails(dashboardId, panelId)
	if err != nil {
		qmc.renderError(&buffer, err)
		return buffer.Bytes()
	}

	if len(rows) > 0 {
		buffer.Html("<table>")
		buffer.Html("<thead>")
		buffer.Html(`<tr>`)
		buffer.Html(`<th class="key">Mismatch</th>`)
		buffer.Html(`<th class="key">Count</th>`)
		buffer.Html(`<th class="key"></th>`)
		buffer.Html("</tr>")

		buffer.Html("</thead>\n")
		buffer.Html("<tbody>\n")

		for _, row := range rows {
			buffer.Html(`<tr>`)
			buffer.Html(`<td>`)

			mismatches, err := parseMismatches(row.mismatch)
			if err == nil {
				const limit = 10
				size := len(mismatches)
				if size > limit {
					mismatches = mismatches[:limit]
					mismatches = append(mismatches, jsondiff.JSONMismatch{
						Message: fmt.Sprintf("... and %d more", size-limit),
					})
				}

				buffer.Html(`<ol>`)
				for _, m := range mismatches {
					qmc.renderABTestingMismatch(&buffer, m)
				}
				buffer.Html(`</ol>`)

			} else {
				buffer.Text(row.mismatch)
			}
			buffer.Html(`</td>`)

			buffer.Html(`<td>`)
			buffer.Text(fmt.Sprintf("%d", row.count))
			buffer.Html(`</td>`)

			buffer.Html("<td>")
			buffer.Html(`<a href="`).Text(fmt.Sprintf("%s/mismatch?dashboard_id=%s&panel_id=%s&mismatch_id=%s", abTestingPath, dashboardId, panelId, row.mismatchId)).Html(`">`).Text("Requests").Html(`</a>`)
			buffer.Html("</td>")

			buffer.Html("</tr>\n")
		}

		buffer.Html("</tbody>\n")
		buffer.Html("</table>\n")
		buffer.Html("\n</main>\n\n")
	} else {
		buffer.Html(`<h3>No mismatches found</h3>`)

	}

	return buffer.Bytes()
}

type abTestingMismatchDetailsRow struct {
	timestamp   string
	requestId   string
	requestPath string
	opaqueId    string
}

func (qmc *QuesmaManagementConsole) abTestingReadMismatchDetails(dashboardId, panelId, mismatchHash string) ([]abTestingMismatchDetailsRow, error) {

	sql := `
		select "@timestamp", request_id, request_path, opaque_id
		from ab_testing_logs 
		where
		    kibana_dashboard_id = ? and 
		    kibana_dashboard_panel_id = ? and 
		    response_mismatch_sha1 = ?  

		order by 1 desc
		limit 100
`

	db := qmc.logManager.GetDB()

	rows, err := db.Query(sql, dashboardId, panelId, mismatchHash)
	if err != nil {
		return nil, err
	}

	var result []abTestingMismatchDetailsRow
	for rows.Next() {

		row := abTestingMismatchDetailsRow{}
		err := rows.Scan(&row.timestamp, &row.requestId, &row.requestPath, &row.opaqueId)
		if err != nil {
			return nil, err
		}
		result = append(result, row)

	}
	if rows.Err() != nil {
		return nil, rows.Err()
	}
	return result, nil
}

func (qmc *QuesmaManagementConsole) generateABMismatchDetails(dashboardId, panelId, mismatchHash string) []byte {
	buffer := newBufferWithHead()

	dashboards, err := qmc.readKibanaDashboards()
	dashboardName := dashboardId
	panelName := panelId

	if err == nil {
		dashboardName = dashboards.dashboardName(dashboardId)
		panelName = dashboards.panelName(dashboardId, panelId)
	} else {
		logger.Warn().Err(err).Msgf("Error reading dashboards %v", err)
	}

	buffer.Html(`<main id="ab_testing_dashboard">`)

	buffer.Html(`<h2>A/B Testing - Panel requests</h2>`)

	buffer.Html(`<h3>`)
	buffer.Text(fmt.Sprintf("Dashboard: %s", dashboardName))
	buffer.Html(`</h3>`)
	buffer.Html(`<h3>`)
	buffer.Text(fmt.Sprintf("Panel: %s", panelName))
	buffer.Html(`</h3>`)

	buffer.Html("<table>")
	buffer.Html("<thead>")
	buffer.Html(`<tr>`)
	buffer.Html(`<th class="key">Timestamp</th>`)
	buffer.Html(`<th class="key">Request ID</th>`)
	buffer.Html(`<th class="key">Request Path</th>`)
	buffer.Html(`<th class="key">Opaque ID</th>`)
	buffer.Html("</tr>")
	buffer.Html("</thead>")

	buffer.Html("<tbody>")

	rows, err := qmc.abTestingReadMismatchDetails(dashboardId, panelId, mismatchHash)
	if err != nil {
		qmc.renderError(&buffer, err)
		return buffer.Bytes()
	}

	for _, row := range rows {

		buffer.Html(`<tr>`)
		buffer.Html(`<td>`)
		buffer.Text(row.timestamp)
		buffer.Html(`</td>`)

		buffer.Html(`<td>`)
		buffer.Html(`<a href="`).Text(fmt.Sprintf("%s/request?request_id=%s", abTestingPath, row.requestId)).Html(`">`).Text(row.requestId).Html(`</a>`)

		buffer.Html(`</td>`)

		buffer.Html(`<td>`)
		buffer.Text(row.requestPath)
		buffer.Html(`</td>`)

		buffer.Html(`<td>`)
		buffer.Text(row.opaqueId)
		buffer.Html(`</td>`)

		buffer.Html("</tr>\n")
	}

	buffer.Html("</tbody>\n")
	buffer.Html("</table>\n")

	buffer.Html(`</main>`)
	return buffer.Bytes()
}

type abTestingTableRow struct {
	requestID                  *string
	requestPath                *string
	requestIndexName           *string
	requestBody                *string
	responseBTime              *float64
	responseBError             *string
	responseBName              *string
	responseBBody              *string
	quesmaHash                 *string
	kibanaDashboardID          *string
	opaqueID                   *string
	responseABody              *string
	responseATime              *float64
	responseAError             *string
	responseAName              *string
	timestamp                  time.Time
	responseMismatchSHA1       *string
	responseMismatchCount      *int64
	responseMismatchTopType    *string
	responseMismatchIsOK       *bool
	responseMismatchMismatches *string
	responseMismatchMessage    *string
	quesmaVersion              *string
	kibanaDashboardPanelID     *string
}

func (qmc *QuesmaManagementConsole) abTestingReadRow(requestId string) (abTestingTableRow, error) {
	sql := `SELECT
	 request_id, request_path, request_index_name,
		request_body, response_b_time, response_b_error, response_b_name, response_b_body,
		quesma_hash, kibana_dashboard_id, opaque_id, response_a_body, response_a_time,
		response_a_error, response_a_name, "@timestamp", response_mismatch_sha1,
		response_mismatch_count, response_mismatch_top_mismatch_type, response_mismatch_is_ok,
		response_mismatch_mismatches, response_mismatch_message, quesma_version,
		kibana_dashboard_panel_id 
	FROM ab_testing_logs 
	WHERE request_id = ?`

	db := qmc.logManager.GetDB()

	row := db.QueryRow(sql, requestId)

	rec := abTestingTableRow{}
	err := row.Scan(
		&rec.requestID, &rec.requestPath, &rec.requestIndexName,
		&rec.requestBody, &rec.responseBTime, &rec.responseBError, &rec.responseBName, &rec.responseBBody,
		&rec.quesmaHash, &rec.kibanaDashboardID, &rec.opaqueID, &rec.responseABody, &rec.responseATime,
		&rec.responseAError, &rec.responseAName, &rec.timestamp, &rec.responseMismatchSHA1,
		&rec.responseMismatchCount, &rec.responseMismatchTopType, &rec.responseMismatchIsOK,
		&rec.responseMismatchMismatches, &rec.responseMismatchMessage, &rec.quesmaVersion,
		&rec.kibanaDashboardPanelID)

	if err != nil {
		return rec, err
	}

	if row.Err() != nil {
		return rec, row.Err()
	}

	return rec, nil
}

func (qmc *QuesmaManagementConsole) generateABSingleRequest(requestId string) []byte {
	buffer := newBufferWithHead()
	buffer.Html(`<main id="ab_testing_dashboard">`)

	buffer.Html(`<h2>A/B Testing - Request Results </h2>`)

	fmtAny := func(value any) string {
		if value == nil {
			return "n/a"
		}

		switch v := value.(type) {
		case *string:
			return *v
		case *float64:
			return fmt.Sprintf("%f", *v)
		case *int64:
			return fmt.Sprintf("%d", *v)
		case *bool:
			return fmt.Sprintf("%t", *v)
		default:
			return fmt.Sprintf("%s", value)
		}
	}

	tableRow := func(label string, value any, pre bool) {

		buffer.Html(`<tr>`)
		buffer.Html(`<td width="20%">`)
		buffer.Text(label)
		buffer.Html(`</td>`)
		buffer.Html(`<td width="80%">`)
		if pre {
			buffer.Html(`<pre>`)
		}
		buffer.Text(fmtAny(value))
		if pre {
			buffer.Html(`</pre>`)
		}
		buffer.Html(`</td>`)
		buffer.Html("</tr>\n")

	}

	var dashboardName string
	var panelName string

	dashboards, err := qmc.readKibanaDashboards()
	if err != nil {
		logger.Warn().Err(err).Msgf("Error reading dashboards %v", err)
	}

	row, err := qmc.abTestingReadRow(requestId)

	if err == nil {

		if row.kibanaDashboardID != nil {

			dashboardName = dashboards.dashboardName(*row.kibanaDashboardID)
			if row.kibanaDashboardPanelID != nil {
				panelName = dashboards.panelName(*row.kibanaDashboardID, *row.kibanaDashboardPanelID)
			}
		}
	} else {
		logger.Warn().Err(err).Msgf("Error reading dashboards %v", err)
	}

	buffer.Html(`<table width="90%">`)
	tableRow("Request ID", row.requestID, true)
	tableRow("Timestamp", row.timestamp, true)
	tableRow("Kibana Dashboard ID", dashboardName, false)
	tableRow("Kibana Dashboard Panel ID", panelName, false)
	tableRow("Opaque ID", row.opaqueID, true)
	tableRow("Quesma Hash", row.quesmaHash, true)
	tableRow("Quesma Version", row.quesmaVersion, true)
	tableRow("Request Path", row.requestPath, true)
	tableRow("Request Index Name", row.requestIndexName, false)
	tableRow("Request Body", formatJSON(row.requestBody), true)
	buffer.Html(`</table>`)

	rowAB := func(label string, valueA any, valueB any, pre bool) {
		buffer.Html(`<tr>`)
		buffer.Html(`<td>`)
		buffer.Text(label)
		buffer.Html(`</td>`)
		buffer.Html(`<td>`)
		if pre {
			buffer.Html(`<pre>`)
		}
		buffer.Text(fmtAny(valueA))
		if pre {
			buffer.Html(`</pre>`)
		}
		buffer.Html(`</td>`)
		buffer.Html(`<td>`)
		if pre {
			buffer.Html(`<pre>`)
		}
		buffer.Text(fmtAny(valueB))
		if pre {
			buffer.Html(`</pre>`)
		}
		buffer.Html(`</td>`)
		buffer.Html("</tr>\n")
	}

	buffer.Html(`<h3>Response A vs Response B</h3>`)
	buffer.Html(`<table width="90%">`)
	buffer.Html(`<tr>`)
	buffer.Html(`<th width="10%">Label</th>`)
	buffer.Html(`<th width="45%">Response A</th>`)
	buffer.Html(`<th width="45%">Response B</th>`)
	buffer.Html("</tr>")

	rowAB("Name", row.responseAName, row.responseBName, false)
	rowAB("Time", row.responseATime, row.responseBTime, false)
	rowAB("Error", row.responseAError, row.responseBError, true)
	rowAB("Response Body", formatJSON(row.responseABody), formatJSON(row.responseBBody), true)
	buffer.Html(`</table>`)

	buffer.Html(`<h3>Difference</h3>`)
	if row.responseMismatchSHA1 != nil {
		mismaches, err := parseMismatches(*row.responseMismatchMismatches)
		if err != nil {
			buffer.Text(fmt.Sprintf("Error: %s", err))
		} else {
			buffer.Html(`<table width="90%">`)
			buffer.Html(`<tr>`)
			buffer.Html(`<th>Message</th>`)
			buffer.Html(`<th>Path</th>`)
			buffer.Html(`<th>Expected (`)
			buffer.Text(fmtAny(row.responseAName))
			buffer.Html(`)</th>`)
			buffer.Html(`<th>Actual (`)
			buffer.Text(fmtAny(row.responseBName))
			buffer.Html(`)</th>`)
			buffer.Html("</tr>")

			for _, m := range mismaches {
				buffer.Html(`<tr>`)
				buffer.Html(`<td>`)
				buffer.Text(m.Message)
				buffer.Html(`</td>`)
				buffer.Html(`<td>`)
				buffer.Text(m.Path)
				buffer.Html(`</td>`)
				buffer.Html(`<td>`)
				buffer.Text(m.Expected)
				buffer.Html(`</td>`)
				buffer.Html(`<td>`)
				buffer.Text(m.Actual)
				buffer.Html(`</td>`)
				buffer.Html("</tr>")
			}
		}
	}

	buffer.Html(`</main>`)
	return buffer.Bytes()
}
