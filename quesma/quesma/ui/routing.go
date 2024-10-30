// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package ui

import (
	"quesma/quesma/mux"
	"quesma/quesma/ui/internal/builder"
	"strings"
)

func (qmc *QuesmaManagementConsole) generateRouterStatisticsLiveTail() []byte {
	buffer := newBufferWithHead()
	buffer.Write(qmc.generateTopNavigation("routing-statistics"))

	buffer.Html(`<main id="routing-statistics">`)
	buffer.Write(qmc.generateRouterStatistics())
	buffer.Html("\n</main>\n\n")

	buffer.Html(`<div class="menu">`)
	buffer.Html("\n<h2>Menu</h2>")

	buffer.Html(`<form action="/">&nbsp;<input class="btn" type="submit" value="Back to dashboard" /></form>`)

	buffer.Html("\n</div>")

	buffer.Html("\n</body>")
	buffer.Html("\n</html>")
	return buffer.Bytes()
}

func dropFirstSegment(path string) string {
	segments := strings.SplitN(path, "/", 3)
	if len(segments) > 2 {
		return "/" + segments[2]
	}
	return path
}

func (qmc *QuesmaManagementConsole) generateRouterStatistics() []byte {
	var buffer builder.HtmlBuffer

	matchedKeys, matched, unmatchedKeys, unmatched := mux.MatchStatistics().GroupByFirstSegment()

	buffer.Html("\n<h2>Matched URLs</h2>\n<ul>")
	for _, segment := range matchedKeys {
		paths := matched[segment]
		if len(paths) > 1 {
			buffer.Html("<li>").Text(segment).Html("</li>")

			buffer.Html("<ul>\n")
			for _, path := range paths {
				buffer.Html("<li><small>").Text(dropFirstSegment(path)).Html("</small></li>")
			}
			buffer.Html("</ul>\n")
		} else {
			buffer.Html("<li>").Text(paths[0]).Html("</li>\n")
		}
	}

	buffer.Html("</ul>\n")
	buffer.Html("\n<h2>Not matched URLs</h2>\n<ul>")
	for _, segment := range unmatchedKeys {
		paths := unmatched[segment]
		if len(paths) > 1 {
			buffer.Html("<li>").Text(segment).Html("</li>")

			buffer.Html("<ul>\n")
			for _, path := range paths {
				buffer.Html("<li><small>").Text(dropFirstSegment(path)).Html("</small></li>")
			}
			buffer.Html("</ul>\n")
		} else {
			buffer.Html("<li>").Text(paths[0]).Html("</li>\n")
		}
	}
	buffer.Html("</ul>\n")

	return buffer.Bytes()
}
