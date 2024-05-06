package ui

import (
	"fmt"
	"mitmproxy/quesma/stats/errorstats"
)

func (qmc *QuesmaManagementConsole) generateErrorForReason(reason string) []byte {
	buffer := newBufferWithHead()
	buffer.Write(generateTopNavigation(fmt.Sprintf("Errors with reason '%s'", reason)))

	buffer.Html(`<main id="errors">`)
	errors := errorstats.GlobalErrorStatistics.ErrorReportsForReason(reason)
	// TODO: Make it nicer
	for _, errorReport := range errors {
		buffer.Html("<p>").Text(errorReport.ReportedAt.String() + " " + errorReport.DebugMessage).Html("</p>\n")
	}
	buffer.Html("\n</main>\n\n")

	buffer.Html(`<div class="menu">`)
	buffer.Html("\n<h2>Menu</h2>")

	buffer.Html(`<form action="/">&nbsp;<input class="btn" type="submit" value="Back to dashboard" /></form>`)
	// TODO: implement
	// buffer.Html(`<form action="/dashboard">&nbsp;<input class="btn" type="submit" value="See requests with errors" /></form>`)
	buffer.Html("\n</div>")

	buffer.Html("\n</body>")
	buffer.Html("\n</html>")

	return buffer.Bytes()
}
