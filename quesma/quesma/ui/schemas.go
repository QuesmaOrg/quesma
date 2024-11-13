// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package ui

import (
	"fmt"
	"quesma/util"
)

func (qmc *QuesmaManagementConsole) generateSchemas() []byte {
	buffer := newBufferWithHead()
	buffer.Write(qmc.generateTopNavigation("schemas"))
	buffer.Html(`<main id="schemas">`)

	schemas := qmc.schemasProvider.AllSchemas()
	for i, schemaName := range util.MapKeysSorted(schemas) {
		schema := schemas[schemaName]
		buffer.Html("\n<table>")

		id := fmt.Sprintf("schema-table-%d", i)

		buffer.Html(`<tr class="tableName"`)
		buffer.Html(fmt.Sprintf(` id="%s"`, id))
		buffer.Html(`>`)
		buffer.Html(`<th colspan=3><h2>`)
		buffer.Html(`Index Name: `)
		buffer.Text(schemaName.AsString())

		buffer.Html(`</h2></th>`)
		buffer.Html(`</tr>`)
		buffer.Html(`<tr>`)
		buffer.Html(`<th>Public Name</th>`)
		buffer.Html(`<th>Internal Name</th>`)
		buffer.Html(`<th>Type</th>`)
		buffer.Html(`</tr>`)

		for _, fieldName := range util.MapKeysSorted(schema.Fields) {
			field := schema.Fields[fieldName]
			buffer.Html(`<tr>`)
			buffer.Html(`<td>`)
			buffer.Text(fieldName.AsString())
			buffer.Html(`</td>`)
			buffer.Html(`<td>`)
			buffer.Text(field.InternalPropertyName.AsString())
			buffer.Html(`</td>`)
			buffer.Html(`<td>`)
			buffer.Text(fmt.Sprintf("%s %s", field.Type.Name, field.Type.Properties))
			buffer.Html(`</td>`)
			buffer.Html(`</tr>`)
		}

		if len(schema.Aliases) > 0 {
			buffer.Html(`<th colspan=3><h4>Aliases</h4></th>`)

			for _, aliasFieldName := range util.MapKeysSorted(schema.Aliases) {
				targetFieldName := schema.Aliases[aliasFieldName]
				buffer.Html(`<tr>`)
				buffer.Html(`<td>`)
				buffer.Text(fmt.Sprintf("%s->%s", aliasFieldName.AsString(), targetFieldName.AsString()))
				buffer.Html(`</td>`)
				buffer.Html(`<td>`)
				buffer.Text("-")
				buffer.Html(`</td>`)
				buffer.Html(`<td>`)
				field := schema.Fields[targetFieldName]
				buffer.Text(fmt.Sprintf("%s %s", field.Type.Name, field.Type.Properties))
				buffer.Html(`</td>`)
			}
		}
	}

	buffer.Html("\n</main>\n\n")

	buffer.Html(`<div class="menu">`)
	buffer.Html("\n<h2>Menu</h2>")

	buffer.Html(`<form action="/">&nbsp;<input class="btn" type="submit" value="Back to dashboard" /></form>`)

	buffer.Html("\n</div>")

	buffer.Html("\n</body>")
	buffer.Html("\n</html>")
	return buffer.Bytes()
}
