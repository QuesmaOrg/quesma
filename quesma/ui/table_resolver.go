// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package ui

import (
	"github.com/QuesmaOrg/quesma/quesma/ui/internal/builder"
	"strings"
)

func (qmc *QuesmaManagementConsole) generateTableResolver() []byte {

	buffer := newBufferWithHead()
	buffer.Write(qmc.generateTopNavigation("table_resolver"))

	buffer.Html(`<main id="table_resolver">`)

	buffer.Html("<h1>Table Resolver</h1>")

	buffer.Html("<h2>Ask Quesma</h2>")

	buffer.Html("Ask Quesma to resolve an pattern:")
	buffer.Html(`<br>`)
	buffer.Html(`<textarea class="form-control" type="text" `)
	buffer.Html(`name="prompt" placeholder="Type a pattern here" `)
	buffer.Html(`hx-post="/table_resolver/ask" `)
	buffer.Html(`hx-trigger="input changed delay:500ms, prompt" `)
	buffer.Html(`hx-target="#search-results" `)
	buffer.Html(`hx-indicator=".htmx-indicator">`)
	buffer.Html("</textarea>")

	buffer.Html(`<div id="search-results"></div>`)

	buffer.Html(`<hr>`)

	buffer.Html("<h2>Recent decisions</h2>")

	pipelines := qmc.tableResolver.Pipelines()

	buffer.Html(`<table class="table_resolver">`)
	buffer.Html(`<tr>`)
	buffer.Html(`<th>Pattern</th>`)
	for _, pipeline := range pipelines {
		buffer.Html(`<th>`).Text(pipeline).Html(`</th>`)
	}
	buffer.Html(`</tr>`)

	decisions := qmc.tableResolver.RecentDecisions()

	for _, decision := range decisions {
		buffer.Html(`<tr>`)
		buffer.Html(`<td>`).Text(decision.Pattern).Html(`</td>`)

		for _, pipeline := range pipelines {
			buffer.Html(`<td>`)
			if decision.Decisions[pipeline] != nil {
				buffer.Text(decision.Decisions[pipeline].String())
			} else {
				buffer.Text("n/a")
			}
			buffer.Html(`</td>`)
		}

		buffer.Html(`</tr>`)
	}
	buffer.Html(`</table>`)

	buffer.Html(`<div class="menu">`)
	buffer.Html("\n</div>")

	buffer.Html("\n</main>\n\n")
	return buffer.Bytes()

}

func (qmc *QuesmaManagementConsole) generateTableResolverAnswer(prompt string) []byte {
	var buffer builder.HtmlBuffer

	prompt = strings.TrimSpace(prompt)

	if prompt == "" {
		return buffer.Bytes()
	}

	patterns := strings.Split(prompt, " ")

	pipelines := qmc.tableResolver.Pipelines()

	buffer.Html("<h4>Quesma's decision</h2>")

	buffer.Html(`<table class="table_resolver">`)
	buffer.Html(`<tr>`)
	buffer.Html(`<th>Pattern</th>`)
	for _, pipeline := range pipelines {
		buffer.Html(`<th>`).Text(pipeline).Html(`</th>`)
	}
	buffer.Html(`</tr>`)

	for _, pattern := range patterns {

		pattern = strings.TrimSpace(pattern)

		buffer.Html(`<tr>`)
		buffer.Html(`<td>`).Text(pattern).Html(`</td>`)

		for _, pipeline := range pipelines {
			decision := qmc.tableResolver.Resolve(pipeline, pattern)
			buffer.Html(`<td>`)
			if decision != nil {
				buffer.Text(decision.String())
			} else {
				buffer.Text("n/a")
			}
			buffer.Html(`</td>`)
		}
		buffer.Html(`</tr>`)
	}

	buffer.Html(`</table>`)

	return buffer.Bytes()
}
