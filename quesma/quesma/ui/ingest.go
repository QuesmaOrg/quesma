// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package ui

import (
	"fmt"
	"quesma/quesma/ui/internal/builder"
	"quesma/stats"
	"strings"
)

func (qmc *QuesmaManagementConsole) generateIngestStatistics() []byte {
	buffer := newBufferWithHead()
	buffer.Write(qmc.generateTopNavigation("statistics"))

	buffer.Html(`<main id="statistics">`)
	buffer.Write(qmc.generateStatistics())
	buffer.Html("\n</main>\n\n")

	buffer.Html(`<div class="menu">`)
	buffer.Html("\n<h2>Menu</h2>")

	buffer.Html(`<form action="/">&nbsp;<input class="btn" type="submit" value="Back to dashboard" /></form>`)

	buffer.Html("\n</div>")

	buffer.Html("\n</body>")
	buffer.Html("\n</html>")
	return buffer.Bytes()
}

func (qmc *QuesmaManagementConsole) generateSchemaNonCompliantStatistics(index *stats.IngestStatistics) []byte {
	var buffer builder.HtmlBuffer
	const maxTopValues = 5
	headerInjected := false
	for _, keyStats := range index.SortedKeyStatistics() {
		topInvalidValuesCount := maxTopValues
		if len(keyStats.NonSchemaValues) < maxTopValues {
			topInvalidValuesCount = len(keyStats.NonSchemaValues)
		}
		if topInvalidValuesCount == 0 {
			continue
		}
		if !headerInjected {
			buffer.Html("<table>\n")

			buffer.Html("<thead>\n")
			buffer.Html(`<tr>` + "\n")
			buffer.Html(`<th class="key">Key</th>` + "\n")
			buffer.Html(`<th class="key-count">Count</th>` + "\n")
			buffer.Html(`<th class="value">schema non compliant fields</th>` + "\n")
			buffer.Html(`<th class="value-count">Count</th>` + "\n")
			buffer.Html(`<th class="value-count">Percentage</th>` + "\n")
			buffer.Html(`<th class="types">Potential type</th>` + "\n")
			buffer.Html("</tr>\n")
			buffer.Html("</thead>\n")
			buffer.Html("<tbody>\n")
			headerInjected = true
		}
		buffer.Html(`<tr class="group-divider">` + "\n")
		buffer.Html(fmt.Sprintf(`<td class="key" rowspan="%d">`, topInvalidValuesCount)).Text(keyStats.KeyName).Html("</td>\n")
		buffer.Html(fmt.Sprintf(`<td class="key-count" rowspan="%d">%d</td>`+"\n", topInvalidValuesCount, keyStats.Occurrences))

		for i, value := range keyStats.TopNInvalidValues(topInvalidValuesCount) {
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
	if headerInjected {
		buffer.Html("</tbody>\n")
		buffer.Html("</table>\n")
	}

	return buffer.Bytes()
}

func (qmc *QuesmaManagementConsole) generateStatistics() []byte {
	var buffer builder.HtmlBuffer
	const maxTopValues = 5

	if !qmc.cfg.IngestStatistics {
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
			if topValuesCount == 0 {
				continue
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
		invalidValuesStatistics := qmc.generateSchemaNonCompliantStatistics(index)
		if len(invalidValuesStatistics) > 0 {
			buffer.Html("<br>\n")
			buffer.Html(fmt.Sprintf("<h3>%s schema non compliant fields</h3>\n", index.IndexName))
			buffer.Write(invalidValuesStatistics)
			buffer.Html("<br>\n")
		}
	}
	return buffer.Bytes()
}
