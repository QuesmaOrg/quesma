// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package ui

import (
	"quesma/quesma/ui/internal/builder"
	"strings"
)

func (qmc *QuesmaManagementConsole) generateIndexRegistry() []byte {

	buffer := newBufferWithHead()
	buffer.Write(generateTopNavigation("index_registry"))

	buffer.Html(`<main id="index_registry">`)

	buffer.Html("<h1>Index Registry</h1>")

	buffer.Html("<h2>Ask Quesma</h2>")

	buffer.Html("Ask Quesma to resolve an index pattern:")
	buffer.Html(`<input class="form-control" type="search" `)
	buffer.Html(`name="prompt" placeholder="Type a index pattern here" `)
	buffer.Html(`hx-post="/index_registry/ask" `)
	buffer.Html(`hx-trigger="input changed delay:500ms, prompt" `)
	buffer.Html(`hx-target="#search-results" `)
	buffer.Html(`hx-indicator=".htmx-indicator">`)

	buffer.Html(`<div id="search-results"></div>`)

	buffer.Html(`<br>`)

	buffer.Html("<h2>Recent decisions</h2>")

	buffer.Html(`<table class="index-registry">`)
	buffer.Html(`<tr>`)
	buffer.Html(`<th>Index pattern</th>`)
	buffer.Html(`<th>Ingest</th>`)
	buffer.Html(`<th>Query</th>`)
	buffer.Html(`<tr>`)

	decisions := qmc.indexRegistry.RecentDecisions()

	for _, decision := range decisions {
		buffer.Html(`<tr>`)
		buffer.Html(`<td>`).Text(decision.Pattern).Html(`</td>`)
		buffer.Html(`<td>`)
		if decision.Ingest != nil {
			buffer.Text(decision.Ingest.String())
		} else {
			buffer.Text("n/a")
		}
		buffer.Html(`</td>`)

		buffer.Html(`<td>`)

		if decision.Query != nil {
			buffer.Text(decision.Query.String())
		} else {
			buffer.Text("n/a")

		}
		buffer.Html(`</td>`)

		buffer.Html(`</tr>`)
	}
	buffer.Html(`</table>`)

	buffer.Html(`<div class="menu">`)
	buffer.Html("\n</div>")

	buffer.Html("\n</main>\n\n")
	return buffer.Bytes()

}

func (qmc *QuesmaManagementConsole) generateIndexRegistryPrompt(prompt string) []byte {
	var buffer builder.HtmlBuffer

	prompt = strings.TrimSpace(prompt)

	if prompt == "" {
		return buffer.Bytes()
	}

	buffer.Html("<p>The answer for the pattern ")
	buffer.Html("<strong>")
	buffer.Text(prompt)
	buffer.Html("</strong>")
	buffer.Html(" is:")

	buffer.Html("<dl>")

	buffer.Html("<dt>Ingest path</dt>")
	buffer.Html("<dd>")
	buffer.Text(qmc.indexRegistry.ResolveIngest(prompt).String())
	buffer.Html("</dd>")
	buffer.Html("<dt>Query path</dt>")
	buffer.Html("<dd>")
	buffer.Text(qmc.indexRegistry.ResolveQuery(prompt).String())
	buffer.Html("</dd>")
	buffer.Html("</dl>")
	buffer.Html("</p>")

	return buffer.Bytes()
}
