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
	buffer.Html(`<h2>A/B Testing Dashboard</h2>`)

	if qmc.hasABTestingTable() {

		buffer.Html(`<form hx-post="/ab-testing-dashboard/report" hx-target="#report">`)
		buffer.Html(`<label for="kibana_url">Kibana URL</label>`)
		buffer.Html(`<input id="kibana_url" name="kibana_url" type="text" value="http://localhost:5601" />`)
		buffer.Html(`<button type="submit">Generate report</button>`)
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
		_id := hit.Fields.Id[0]
		title := hit.Fields.Title[0]
		_id = strings.TrimPrefix(_id, "dashboard:")

		panels := hit.Fields.Panels[0]

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
			} else {
				dashboard.panels[panel.PanelID] = panel.Name
			}
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

func (qmc *QuesmaManagementConsole) generateABTestingReport(kibanaUrl string) []byte {
	buffer := newBufferWithHead()

	buffer.Html(`<h2>AB Testing Report</h2>`)

	kibanaDashboards, err := qmc.readKibanaDashboards()
	if err != nil {
		logger.Warn().Msgf("Error reading dashboards %v", err)
	}

	sql := `
with subresults as (
select
   kibana_dashboard_id, 
   kibana_dashboard_panel_id,
   concat(response_a_name,' vs ',response_b_name) as name, 
   response_mismatch_is_ok as ok ,
   count(*) as c,
   avg(response_a_time) as a_time, 
   avg(response_b_time) as b_time 
from
  ab_testing_logs group by 1,2,3,4
)

select 
  kibana_dashboard_id,
  kibana_dashboard_panel_id,
  name as name,
  (sumIf(c,ok)/ sum(c)) * 100 as success_rate,
  (avgIf(a_time,ok)/ avgIf(b_time,ok)) *100  as time_ratio,
  sum(c) as count
from
  subresults 
group by 
 kibana_dashboard_id,kibana_dashboard_panel_id, name
order by 1,2,3 
`

	type reportRow struct {
		dashboardId   string
		panelId       string
		dashboardUrl  string
		detailsUrl    string
		dashboardName string
		panelName     string
		testName      string
		successRate   *float64
		timeRatio     *float64
		count         int
	}

	var report []reportRow

	db := qmc.logManager.GetDB()

	rows, err := db.Query(sql)
	if err != nil {
		qmc.renderError(&buffer, err)
		return buffer.Bytes()
	}

	for rows.Next() {
		row := reportRow{}
		err := rows.Scan(&row.dashboardId, &row.panelId, &row.testName, &row.successRate, &row.timeRatio, &row.count)
		if err != nil {
			qmc.renderError(&buffer, err)
			return buffer.Bytes()
		}

		row.dashboardUrl = fmt.Sprintf("%s/app/kibana#/dashboard/%s", kibanaUrl, row.dashboardId)

		row.detailsUrl = fmt.Sprintf("/ab-testing-dashboard/panel?dashboard_id=%s&panel_id=%s", row.dashboardId, row.panelId)

		row.dashboardName = kibanaDashboards.dashboardName(row.dashboardId)
		row.panelName = kibanaDashboards.panelName(row.dashboardId, row.panelId)

		report = append(report, row)
	}

	if rows.Err() != nil {
		qmc.renderError(&buffer, rows.Err())
		return buffer.Bytes()
	}

	buffer.Html("<table>\n")

	buffer.Html("<thead>\n")
	buffer.Html(`<tr>` + "\n")
	buffer.Html(`<th class="key">Dashboard</th>` + "\n")
	buffer.Html(`<th class="key">Panel</th>` + "\n")
	buffer.Html(`<th class="key">Count</th>` + "\n")
	buffer.Html(`<th class="key">Success rate</th>` + "\n")
	buffer.Html(`<th class="key">Time ratio</th>` + "\n")
	buffer.Html(`<th class="key"></th>` + "\n")
	buffer.Html("</tr>\n")
	buffer.Html("</thead>\n")

	buffer.Html("<tbody>\n")

	var lastDashboardId string
	for _, row := range report {
		buffer.Html(`<tr>` + "\n")

		if lastDashboardId != row.dashboardId {

			buffer.Html(`<td>`)
			buffer.Html(`<a target="_blank" href="`).Text(row.dashboardUrl).Html(`">`).Text(row.dashboardName).Html(`</a>`)
			buffer.Html("<br>")
			buffer.Text(fmt.Sprintf("(%s)", row.testName))
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
			buffer.Text(fmt.Sprintf("%f", *row.successRate))
		} else {
			buffer.Text("n/a")
		}
		buffer.Html(`</td>`)

		buffer.Html(`<td>`)
		if row.timeRatio != nil {
			buffer.Text(fmt.Sprintf("%f", *row.timeRatio))
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

		buffer.Html("</tr>\n")
	}

	buffer.Html("</tbody>\n")
	buffer.Html("</table>\n")

	return buffer.Bytes()
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
		qmc.renderError(&buffer, err)
		return buffer.Bytes()
	}

	type row struct {
		mismatch   string
		mismatchId string
		count      int
	}

	var tableRows []row
	for rows.Next() {

		var mismatch string
		var count int
		var mismatchId string

		err := rows.Scan(&mismatch, &mismatchId, &count)
		if err != nil {
			qmc.renderError(&buffer, err)
			return buffer.Bytes()
		}

		r := row{
			mismatch:   mismatch,
			mismatchId: mismatchId,
			count:      count,
		}
		tableRows = append(tableRows, r)
	}

	if rows.Err() != nil {
		qmc.renderError(&buffer, rows.Err())
		return buffer.Bytes()
	}

	if len(tableRows) > 0 {

		buffer.Html("<table>")
		buffer.Html("<thead>")
		buffer.Html(`<tr>`)
		buffer.Html(`<th class="key">Mismatch</th>`)
		buffer.Html(`<th class="key">Count</th>`)
		buffer.Html(`<th class="key"></th>`)
		buffer.Html("</tr>")

		buffer.Html("</thead>\n")
		buffer.Html("<tbody>\n")

		for _, row := range tableRows {

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
					buffer.Html(`<li>`)
					buffer.Html(`<p>`)
					buffer.Text(m.Message)
					buffer.Text(" ")

					if m.Path != "" {
						buffer.Html(`<code>`)
						buffer.Text(`(`)
						buffer.Text(m.Path)
						buffer.Text(`)`)
						buffer.Html(`</code>`)

						{
							buffer.Html(`<ul>`)
							buffer.Html(`<li>`)
							buffer.Html(`<code>`)
							buffer.Text("Actual: ")
							buffer.Text(m.Actual)
							buffer.Html(`</code>`)
							buffer.Html(`</li>`)

							buffer.Html(`<li>`)
							buffer.Html(`<code>`)
							buffer.Text("Expected: ")
							buffer.Text(m.Expected)
							buffer.Html(`</code>`)
							buffer.Html(`</li>`)
							buffer.Html(`</ul>`)
						}
					}

					buffer.Html(`</p>`)
					buffer.Html(`</li>`)
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
			buffer.Html(`<a href="`).Text(fmt.Sprintf("/ab-testing-dashboard/mismatch?dashboard_id=%s&panel_id=%s&mismatch_id=%s", dashboardId, panelId, row.mismatchId)).Html(`">`).Text("Requests").Html(`</a>`)
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

	type tableRow struct {
		timestamp   string
		requestId   string
		requestPath string
		opaqueId    string
	}

	db := qmc.logManager.GetDB()

	rows, err := db.Query(sql, dashboardId, panelId, mismatchHash)
	if err != nil {
		qmc.renderError(&buffer, err)
		return buffer.Bytes()
	}

	var allRows []tableRow
	for rows.Next() {

		row := tableRow{}
		err := rows.Scan(&row.timestamp, &row.requestId, &row.requestPath, &row.opaqueId)
		if err != nil {
			qmc.renderError(&buffer, err)
			return buffer.Bytes()
		}

		allRows = append(allRows, row)

	}
	if rows.Err() != nil {
		qmc.renderError(&buffer, rows.Err())
		return buffer.Bytes()
	}

	buffer.Html("<table>")
	buffer.Html("<thead>")
	buffer.Html(`<tr>`)
	buffer.Html(`<th class="key">Timestamp</th>`)
	buffer.Html(`<th class="key">Request ID</th>`)
	buffer.Html(`<th class="key">Request Path</th>`)
	buffer.Html(`<th class="key">Opaque ID</th>`)

	buffer.Html("</tr>")

	buffer.Html("</thead>\n")
	buffer.Html("<tbody>\n")

	for _, row := range allRows {

		buffer.Html(`<tr>`)
		buffer.Html(`<td>`)
		buffer.Text(row.timestamp)
		buffer.Html(`</td>`)

		buffer.Html(`<td>`)
		buffer.Html(`<a href="`).Text(fmt.Sprintf("/ab-testing-dashboard/request?request_id=%s", row.requestId)).Html(`">`).Text(row.requestId).Html(`</a>`)

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

func (qmc *QuesmaManagementConsole) generateABSingleRequest(requestId string) []byte {
	buffer := newBufferWithHead()
	buffer.Html(`<main id="ab_testing_dashboard">`)

	buffer.Html(`<h2>A/B Testing - Request Results </h2>`)

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

	type request struct {
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

	row := db.QueryRow(sql, requestId)

	rec := request{}
	err := row.Scan(
		&rec.requestID, &rec.requestPath, &rec.requestIndexName,
		&rec.requestBody, &rec.responseBTime, &rec.responseBError, &rec.responseBName, &rec.responseBBody,
		&rec.quesmaHash, &rec.kibanaDashboardID, &rec.opaqueID, &rec.responseABody, &rec.responseATime,
		&rec.responseAError, &rec.responseAName, &rec.timestamp, &rec.responseMismatchSHA1,
		&rec.responseMismatchCount, &rec.responseMismatchTopType, &rec.responseMismatchIsOK,
		&rec.responseMismatchMismatches, &rec.responseMismatchMessage, &rec.quesmaVersion,
		&rec.kibanaDashboardPanelID)

	if err != nil {
		qmc.renderError(&buffer, err)
		return buffer.Bytes()
	}

	if row.Err() != nil {
		qmc.renderError(&buffer, row.Err())
		return buffer.Bytes()
	}

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

	if err == nil {

		if rec.kibanaDashboardID != nil {

			dashboardName = dashboards.dashboardName(*rec.kibanaDashboardID)
			if rec.kibanaDashboardPanelID != nil {
				panelName = dashboards.panelName(*rec.kibanaDashboardID, *rec.kibanaDashboardPanelID)
			}
		}
	} else {
		logger.Warn().Err(err).Msgf("Error reading dashboards %v", err)
	}

	buffer.Html(`<table width="90%">`)
	tableRow("Request ID", rec.requestID, true)
	tableRow("Timestamp", rec.timestamp, true)
	tableRow("Kibana Dashboard ID", dashboardName, false)
	tableRow("Kibana Dashboard Panel ID", panelName, false)
	tableRow("Opaque ID", rec.opaqueID, true)
	tableRow("Quesma Hash", rec.quesmaHash, true)
	tableRow("Quesma Version", rec.quesmaVersion, true)
	tableRow("Request Path", rec.requestPath, true)
	tableRow("Request Index Name", rec.requestIndexName, false)
	tableRow("Request Body", formatJSON(rec.requestBody), true)
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

	rowAB("Name", rec.responseAName, rec.responseBName, false)
	rowAB("Time", rec.responseATime, rec.responseBTime, false)
	rowAB("Error", rec.responseAError, rec.responseBError, true)
	rowAB("Response Body", formatJSON(rec.responseABody), formatJSON(rec.responseBBody), true)
	buffer.Html(`</table>`)

	buffer.Html(`<h3>Difference</h3>`)
	if rec.responseMismatchSHA1 != nil {

		mismaches, err := parseMismatches(*rec.responseMismatchMismatches)
		if err != nil {
			buffer.Text(fmt.Sprintf("Error: %s", err))

		} else {

			buffer.Html(`<table width="90%">`)
			buffer.Html(`<tr>`)
			buffer.Html(`<th>Message</th>`)
			buffer.Html(`<th>Path</th>`)
			buffer.Html(`<th>Actual</th>`)
			buffer.Html(`<th>Expected</th>`)
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
				buffer.Text(m.Actual)
				buffer.Html(`</td>`)
				buffer.Html(`<td>`)
				buffer.Text(m.Expected)
				buffer.Html(`</td>`)
				buffer.Html("</tr>")
			}
		}
	}

	buffer.Html(`</main>`)
	return buffer.Bytes()
}
