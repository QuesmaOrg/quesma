// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package ui

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/k0kubun/pp"
	"io"
	"quesma/elasticsearch"
	"strings"
)

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

	type kibanaDashboard struct {
		name   string
		panels map[string]string
	}

	kibanaDashboardId2Name := make(map[string]kibanaDashboard)

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
		buffer.Text(fmt.Sprintf("Error: %s", err))
		return buffer.Bytes()
	}

	if resp.StatusCode != 200 {
		buffer.Text(fmt.Sprintf("Error: %s", resp.Body))
		return buffer.Bytes()
	}

	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		buffer.Text(fmt.Sprintf("Error: %s", err))
		return buffer.Bytes()
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
		buffer.Text(fmt.Sprintf("Error: %s", err))
		return buffer.Bytes()
	}

	for _, hit := range response.Hits.Hits {
		_id := hit.Fields.Id[0]
		title := hit.Fields.Title[0]
		_id = strings.TrimPrefix(_id, "dashboard:")

		panels := hit.Fields.Panels[0]

		var panelsJson []panelSchema
		err := json.Unmarshal([]byte(panels), &panelsJson)
		if err != nil {
			buffer.Text(fmt.Sprintf("Error: %s", err))
			return buffer.Bytes()
		}

		pp.Println(_id, title, panelsJson)

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

		kibanaDashboardId2Name[_id] = dashboard
	}

	sql := `
with xx as (
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
  xx 
group by 
 kibana_dashboard_id,kibana_dashboard_panel_id, name
order by 1,2,3 
`

	type reportRow struct {
		dashboardId   string
		panelId       string
		dashboardUrl  string
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
		buffer.Text(fmt.Sprintf("Error: %s", err))
		return buffer.Bytes()
	}

	for rows.Next() {
		row := reportRow{}
		err := rows.Scan(&row.dashboardId, &row.panelId, &row.testName, &row.successRate, &row.timeRatio, &row.count)
		if err != nil {
			buffer.Text(fmt.Sprintf("Error: %s", err))
			return buffer.Bytes()
		}

		row.dashboardUrl = fmt.Sprintf("%s/app/kibana#/dashboard/%s", kibanaUrl, row.dashboardId)

		if dashboard, ok := kibanaDashboardId2Name[row.dashboardId]; ok {
			row.dashboardName = dashboard.name

			if panelName, ok := dashboard.panels[row.panelId]; ok {
				row.panelName = panelName
			} else {
				row.panelName = row.panelId
			}

		} else {
			row.dashboardName = row.dashboardId
		}

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
	buffer.Html(`<th class="key">Panel</th>` + "\n")
	buffer.Html(`<th class="key">Test name</th>` + "\n")
	buffer.Html(`<th class="key">Count</th>` + "\n")
	buffer.Html(`<th class="key">Success rate</th>` + "\n")
	buffer.Html(`<th class="key">Time ratio</th>` + "\n")
	buffer.Html("</tr>\n")
	buffer.Html("</thead>\n")

	buffer.Html("<tbody>\n")

	var lastDashboardId string
	for _, row := range report {
		buffer.Html(`<tr>` + "\n")

		if lastDashboardId != row.dashboardId {

			buffer.Html(`<td>`)
			buffer.Html(`<a target="_blank" href="`).Text(row.dashboardUrl).Html(`">`).Text(row.dashboardName).Html(`</a>`)
			buffer.Html(`</td>`)
			lastDashboardId = row.dashboardId
		} else {
			buffer.Html(`<td></td>`)
		}

		buffer.Html(`<td>`)
		buffer.Html(`<a target="_blank" href="`).Text(row.dashboardUrl).Html(`">`).Text(row.panelName).Html(`</a>`)
		buffer.Html(`</td>`)

		buffer.Html(`<td>`)
		buffer.Text(row.testName)
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
		buffer.Html("</tr>\n")
	}

	buffer.Html("</tbody>\n")
	buffer.Html("</table>\n")

	return buffer.Bytes()
}
