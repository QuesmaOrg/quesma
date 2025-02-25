// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package ui

import (
	"fmt"
	"github.com/QuesmaOrg/quesma/quesma/ui/internal/builder"
	"net/url"
)

func generateSimpleTop(title string) []byte {
	var buffer builder.HtmlBuffer
	buffer.Html(`<div class="topnav">` + "\n")
	buffer.Html(`<div class="topnav-menu">` + "\n")
	buffer.Html(`<img src="/static/asset/quesma-logo-white-full.svg" alt="Quesma logo" class="quesma-logo" />` + "\n")
	buffer.Html(`<h3>`).Text(title).Html(`</h3>`)
	buffer.Html("\n</div>\n</div>\n\n")
	return buffer.Bytes()
}

func (qmc *QuesmaManagementConsole) generateTopNavigation(target string) []byte {
	var buffer builder.HtmlBuffer
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

	if target == "table_resolver" {
		buffer.Html(` class="active"`)
	}
	buffer.Html(`><a href="/table_resolver">Resolver</a></li>`)
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
	if target == "schemas" {
		buffer.Html(` class="active"`)
	}
	buffer.Html(`><a href="/schemas">Schemas</a></li>`)

	buffer.Html("<li")
	if target == "tables" {
		buffer.Html(` class="active"`)
	}
	buffer.Html(`><a href="/tables">Tables</a></li>`)

	buffer.Html("<li")
	if target == "phone-home" {
		buffer.Html(` class="active"`)
	}
	buffer.Html(`><a href="/telemetry">Telemetry</a></li>`)

	buffer.Html("<li")
	if target == "data-sources" {
		buffer.Html(` class="active"`)
	}
	buffer.Html(`><a href="/data-sources">Data sources</a></li>`)

	buffer.Html("<li")
	if target == "ab-testing-dashboard" {
		buffer.Html(` class="active"`)
	}
	buffer.Html(`><a title="Compatibility Report" href="`)
	buffer.Html(abTestingPath)
	buffer.Html(`">CR</a></li>`)

	if qmc.isAuthEnabled {
		buffer.Html(`<li><a href="/logout">Logout</a></li>`)
	}

	buffer.Html("\n</ul>\n")
	buffer.Html("\n</div>\n")

	if target != "tables" && target != "telemetry" && target != "table_resolver" && target != "ab-testing-dashboard" {
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

func newBufferWithHead() builder.HtmlBuffer {
	const bufferSize = 4 * 1024 // size of ui/head.html
	var buffer builder.HtmlBuffer
	buffer.Grow(bufferSize)
	head, err := uiFs.ReadFile("asset/head.html")
	buffer.Write(head)
	if err != nil {
		buffer.Text(err.Error())
	}
	buffer.Html("\n")
	return buffer
}
