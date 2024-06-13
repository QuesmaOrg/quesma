package ui

import (
	"fmt"
	"mitmproxy/quesma/util"
)

func (qmc *QuesmaManagementConsole) generateSchemas() []byte {
	buffer := newBufferWithHead()
	buffer.Write(generateTopNavigation("schemas"))
	buffer.Html(`<main id="schemas">`)

	schemas := qmc.schemasProvider.AllSchemas()
	for i, schemaName := range util.MapKeysSorted(schemas) {
		schema := schemas[schemaName]
		buffer.Html("\n<table>")

		id := fmt.Sprintf("schema-table-%d", i)

		buffer.Html(`<tr class="tableName"`)
		buffer.Html(fmt.Sprintf(` id="%s"`, id))
		buffer.Html(`>`)
		buffer.Html(`<th colspan=2><h2>`)
		buffer.Html(`Index Name: `)
		buffer.Text(schemaName.AsString())

		buffer.Html(`</h2></th>`)
		buffer.Html(`</tr>`)
		buffer.Html(`<tr>`)
		buffer.Html(`<th>`)
		buffer.Html(`Name`)
		buffer.Html(`</th>`)
		buffer.Html(`<th>`)
		buffer.Html(`Type`)
		buffer.Html(`</th>`)
		buffer.Html(`</tr>`)

		for _, fieldName := range util.MapKeysSorted(schema.Fields) {
			field := schema.Fields[fieldName]
			buffer.Html(`<tr>`)
			buffer.Html(`<td>`)
			buffer.Text(fieldName.AsString())
			buffer.Html(`</td>`)
			buffer.Html(`<td>`)
			buffer.Text(fmt.Sprintf("%s %s", field.Type.Name, field.Type.Properties))
			buffer.Html(`</td>`)
			buffer.Html(`</tr>`)
		}
	}

	buffer.Html("\n</body>")
	buffer.Html("\n</html>")
	return buffer.Bytes()
}
