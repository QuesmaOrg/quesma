// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package ui

import "fmt"

func (qmc *QuesmaManagementConsole) generateABTestingDashboard() []byte {

	buffer := newBufferWithHead()
	buffer.Write(generateTopNavigation("ab-testing-dashboard"))

	buffer.Html(`<main id="ab_testing_dashboard">`)

	buffer.Html(`<form hx-post="/ab-testing-dashboard/report" hx-target="#report">
		<label for="kibana_url">Kibana URL</label>
	    <input id="kibana_url" name="kibana_url" type="text" value="http://localhost:5601" />
    	<button type="submit">Submit</button>
	</form>`)

	buffer.Html(`<div id="report"></div>`)

	buffer.Html(`<div class="menu">`)
	buffer.Html("\n</div>")

	buffer.Html("\n</main>\n\n")
	return buffer.Bytes()
}

func (qmc *QuesmaManagementConsole) generateABTestingReport(kibanaUrl string) []byte {
	buffer := newBufferWithHead()

	buffer.Html(`<h2>AB Testing Report</h2>`)

	sql := `
with xx as (
select
   kibana_dashboard_id, 
   concat(response_a_name,' vs ',response_b_name) as name, 
   response_mismatch_is_ok as ok ,
   count(*) as c,
   avg(response_a_time) as a_time, 
   avg(response_b_time) as b_time 
from
  ab_testing_logs group by 1,2,3
)

select 
  kibana_dashboard_id,
  name as name,
  (sumIf(c,ok)/ sum(c)) * 100 as success_rate,
  (avg(a_time)/ avg(b_time)) *100  as time_ratio
from
  xx 
group by 
 kibana_dashboard_id,name
`

	type reportRow struct {
		dashboardId   string
		dashboardUrl  string
		dashboardName string
		testName      string
		successRate   float64
		timeRatio     float64
	}

	var report []reportRow

	db := qmc.logManager.GetDB()

	rows, err := db.Query(sql)
	if err != nil {
		buffer.Text(fmt.Sprintf("Error: %s", err))
		return buffer.Bytes()
	}

	for rows.Next() {
		row := reportRow{}
		err := rows.Scan(&row.dashboardId, &row.testName, &row.successRate, &row.timeRatio)
		if err != nil {
			buffer.Text(fmt.Sprintf("Error: %s", err))
			return buffer.Bytes()
		}

		row.dashboardUrl = fmt.Sprintf("%s/app/kibana#/dashboard/%s", kibanaUrl, row.dashboardId)
		row.dashboardName = fmt.Sprintf("Dashboard %s", row.dashboardId)

		report = append(report, row)
	}

	if rows.Err() != nil {
		buffer.Text(fmt.Sprintf("Error: %s", rows.Err()))
		return buffer.Bytes()
	}

	buffer.Html("<table>\n")

	buffer.Html("<thead>\n")
	buffer.Html(`<tr>` + "\n")
	buffer.Html(`<th class="key">Dashboard</th>` + "\n")
	buffer.Html(`<th class="key">Test name</th>` + "\n")
	buffer.Html(`<th class="key">Success rate</th>` + "\n")
	buffer.Html(`<th class="key">Time ratio</th>` + "\n")
	buffer.Html("</tr>\n")
	buffer.Html("</thead>\n")

	buffer.Html("<tbody>\n")

	td := func(s string) {
		buffer.Html(`<td>`).Text(s).Html(`</td>`)
	}

	for _, row := range report {
		buffer.Html(`<tr>` + "\n")

		buffer.Html(`<td>`)
		buffer.Html(`<a target="_blank" href="`).Text(row.dashboardUrl).Html(`">`).Text(row.dashboardName).Html(`</a>`)
		buffer.Html(`</td>`)

		td(row.testName)
		td(fmt.Sprintf("%f", row.successRate))
		td(fmt.Sprintf("%f", row.timeRatio))
		buffer.Html("</tr>\n")
	}

	buffer.Html("</tbody>\n")
	buffer.Html("</table>\n")

	return buffer.Bytes()
}
