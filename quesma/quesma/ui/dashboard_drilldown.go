// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package ui

import (
	"fmt"
	"github.com/QuesmaOrg/quesma/quesma/quesma/ui/internal/builder"
	"github.com/QuesmaOrg/quesma/quesma/stats/errorstats"
	"net/url"
	"strconv"
)

func (qmc *QuesmaManagementConsole) generateErrorForReason(reason string) []byte {
	buffer := newBufferWithHead()
	buffer.Write(generateSimpleTop(fmt.Sprintf("Errors with reason '%s'", reason)))

	buffer.Html(`<main id="errors">`)

	errors := errorstats.GlobalErrorStatistics.ErrorReportsForReason(reason)
	buffer.Write(generateErrorMessage(errors))
	buffer.Html("\n</main>\n\n")

	buffer.Html(`<div class="menu">`)
	buffer.Html("\n<h2>Menu</h2>")

	buffer.Html(`<form action="/">&nbsp;<input class="btn" type="submit" value="Back to dashboard" /></form>`)
	errorTypes := errorstats.GlobalErrorStatistics.ReturnTopErrors(100)

	buffer.Html(`<br>`)
	buffer.Html(`<h3>Top error types</h3>` + "\n")
	buffer.Html(`<ul>` + "\n")
	for _, errorType := range errorTypes {
		buffer.Html(`<li>`)
		buffer.Html(`<a href="/error/`).Text(url.PathEscape(errorType.Reason)).Html(`">`)
		buffer.Text(strconv.Itoa(errorType.Count)).Text(": ").Text(errorType.Reason)
		buffer.Html(`</a></li>` + "\n")
	}
	buffer.Html(`</ul>` + "\n")

	buffer.Html("\n</div>")

	buffer.Html("\n</body>")
	buffer.Html("\n</html>")

	return buffer.Bytes()
}

func generateErrorMessage(errorReports []errorstats.ErrorReport) []byte {
	var buffer builder.HtmlBuffer
	buffer.Html("<table>\n")
	buffer.Html("<thead>\n")
	buffer.Html("<tr>\n")
	buffer.Html(`<th class="time">Time</th>`)
	buffer.Html(`<th class="request-id">Request id</th>`)
	buffer.Html(`<th class="message">Message</th>`)
	buffer.Html("</tr>\n")

	buffer.Html("</thead>\n")
	buffer.Html("<tbody>\n")

	for _, errorReport := range errorReports {
		buffer.Html("<tr>\n")

		// time
		buffer.Html(`<td class="time">`)
		time := errorReport.ReportedAt.Format("2006-01-02 15:04:05")
		buffer.Text(time)
		buffer.Html("</td>")

		// message
		buffer.Html(`<td class="request-id">`)
		if errorReport.RequestId != nil {
			buffer.Html(`<a href="/request-id/`).Text(url.PathEscape(*errorReport.RequestId)).Html(`">`)
			buffer.Text(*errorReport.RequestId)
			buffer.Html(`"</a>`)
		} else {
			buffer.Text("No request id")
		}
		buffer.Html("</td>")

		// message
		buffer.Html(`<td class="message">`)
		buffer.Text(errorReport.DebugMessage)
		buffer.Html("</td>")
	}

	buffer.Html("</tbody>\n")
	buffer.Html("</table>\n")
	return buffer.Bytes()
}
