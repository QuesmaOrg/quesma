package ui

import (
	"fmt"
	"mitmproxy/quesma/quesma/ui/internal/buffer"
	"slices"
	"strings"
)

func (qmc *QuesmaManagementConsole) generateDatasourcesPage() []byte {
	buffer := newBufferWithHead()
	buffer.Write(generateTopNavigation("data-sources"))

	buffer.Html(`<main id="data-sources">`)
	buffer.Write(qmc.generateDatasources())

	buffer.Html("\n</main>\n\n")

	buffer.Html(`<div class="menu">`)
	buffer.Html("\n<h2>Menu</h2>")

	buffer.Html(`<form action="/live">&nbsp;<input class="btn" type="submit" value="Back to live tail" /></form>`)

	buffer.Html("\n</div>")

	buffer.Html("\n</body>")
	buffer.Html("\n</html>")
	return buffer.Bytes()
}

func (qmc *QuesmaManagementConsole) generateDatasources() []byte {
	var buffer buffer.HtmlBuffer
	buffer.Html(`<h2>Data sources</h2>`)

	buffer.Html(`<h3>Clickhouse</h3>`)

	buffer.Html(`<ul>`)

	tableNames := []string{}
	for tableName := range qmc.config.IndexConfig {
		tableNames = append(tableNames, tableName)
	}
	slices.Sort(tableNames)
	tables := qmc.logManager.GetTableDefinitions()
	slices.Sort(tableNames)
	for _, tableName := range tableNames {
		if _, exist := tables.Load(tableName); exist {
			buffer.Html(fmt.Sprintf(`<li>%s (table exists)</li>`, tableName))
		} else {
			buffer.Html(fmt.Sprintf(`<li>%s</li>`, tableName))
		}
	}
	buffer.Html(`</ul>`)

	buffer.Html(`<h3>Elasticsearch</h3>`)

	buffer.Html(`<ul>`)

	qmc.indexManagement.Start()
	indexNames := []string{}
	internalIndexNames := []string{}
	for indexName := range qmc.indexManagement.GetSourceNames() {
		if strings.HasPrefix(indexName, ".") {
			internalIndexNames = append(internalIndexNames, indexName)
		} else {
			indexNames = append(indexNames, indexName)
		}
	}

	slices.Sort(indexNames)
	slices.Sort(internalIndexNames)
	for _, indexName := range indexNames {
		buffer.Html(fmt.Sprintf(`<li>%s</li>`, indexName))
	}

	if len(internalIndexNames) > 0 {
		buffer.Html(`<ul>`)

		for _, indexName := range internalIndexNames {
			buffer.Html(fmt.Sprintf(`<li><small>%s</small></li>`, indexName))
		}
		buffer.Html(`</ul>`)
	}

	buffer.Html(`</ul>`)
	return buffer.Bytes()
}
