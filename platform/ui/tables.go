// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package ui

import (
	"errors"
	"fmt"
	"github.com/QuesmaOrg/quesma/platform/clickhouse"
	"github.com/QuesmaOrg/quesma/platform/common_table"
	"github.com/QuesmaOrg/quesma/platform/end_user_errors"
	"github.com/QuesmaOrg/quesma/platform/util"
	"sort"
	"strings"
)

func (qmc *QuesmaManagementConsole) generateQuesmaAllLogs() []byte {
	buffer := newBufferWithHead()
	buffer.Write(qmc.generateTopNavigation("tables"))
	buffer.Html(`<main id="quesma_all_logs">`)

	var schema clickhouse.TableMap
	var hasSchema bool
	var err error
	var schemaError error
	if qmc.logManager != nil {
		schema, err = qmc.logManager.GetTableDefinitions()
		if err != nil {
			schemaError = err
		} else {
			hasSchema = true
		}
	}

	if hasSchema {
		tableNames := schema.Keys()

		sort.Strings(tableNames)

		for i, v := range tableNames {
			if v == common_table.TableName {
				// Remove element by value
				tableNames = append(tableNames[:i], tableNames[i+1:]...)
			}
		}

		commonTable, ok := schema.Load(common_table.TableName)
		if !ok {
			buffer.Html(fmt.Sprintf("<p>Table %s is not available.</p>", common_table.TableName))

		} else {

			allColumnNamesMap := make(map[string]struct{})

			var filteredTableNames []string

			for _, tableName := range tableNames {

				indexConf, ok := qmc.cfg.IndexConfig[tableName]
				if !ok {
					continue
				}

				if (!indexConf.IsClickhouseQueryEnabled() && !indexConf.IsClickhouseIngestEnabled()) || !indexConf.UseCommonTable {
					continue
				}

				table, ok := schema.Load(tableName)
				if !ok {
					continue
				}

				if !table.VirtualTable {
					continue
				}

				for k := range table.Cols {
					allColumnNamesMap[k] = struct{}{}
				}
				filteredTableNames = append(filteredTableNames, tableName)
			}
			tableNames = filteredTableNames

			tableNames = append([]string{common_table.TableName}, tableNames...)
			for k := range commonTable.Cols {
				allColumnNamesMap[k] = struct{}{}
			}

			allColumnNames := make([]string, 0, len(allColumnNamesMap))

			for k := range allColumnNamesMap {
				allColumnNames = append(allColumnNames, k)
			}
			sort.Strings(allColumnNames)

			buffer.Html("\n<table>")

			buffer.Html("<tr>")
			buffer.Html("<th>Column Name</th>")

			for _, tableName := range tableNames {
				buffer.Html("<th class=catchAll>")
				buffer.Text(tableName)
				buffer.Html("</th>")
			}
			buffer.Html("</tr>")

			for _, columnName := range allColumnNames {
				commonTableCol, existsInCommonTable := commonTable.Cols[columnName]

				buffer.Html("<tr>")
				buffer.Html("<td>")
				buffer.Text(columnName)
				buffer.Html("</td>")

				for _, tableName := range tableNames {
					table, ok := schema.Load(tableName)
					if !ok {
						buffer.Html("<td>-</td>")
						continue
					}

					if tableName == common_table.TableName {
						if existsInCommonTable {
							buffer.Html("<td>")
							buffer.Text(commonTableCol.Type.StringWithNullable())
							buffer.Html("</td>")
						} else {
							buffer.Html("<td>MISSING</td>")
						}
					} else {
						colType, ok := table.Cols[columnName]
						if ok {
							buffer.Html("<td>")

							if existsInCommonTable {
								if commonTableCol.Type.StringWithNullable() != colType.Type.StringWithNullable() {
									buffer.Text(colType.Type.StringWithNullable())
								} else {
									buffer.Text("✔")
								}
							} else {
								buffer.Text(colType.Type.StringWithNullable())

							}
							buffer.Html("</td>")
						} else {
							buffer.Html("<td></td>")
						}
					}
				}
				buffer.Html("</tr>")
			}

			buffer.Html("</table>")
		}

	} else {
		buffer.Text("Schema is not available.")
		buffer.Text(fmt.Sprintf("Error: %s", schemaError))
	}

	buffer.Html("\n</main>\n\n")
	buffer.Html("\n</body>")
	buffer.Html("\n</html>")
	return buffer.Bytes()
}

func (qmc *QuesmaManagementConsole) generateTables() []byte {
	type menuEntry struct {
		label  string
		target string
	}

	var menuEntries []menuEntry

	type tableColumn struct {
		name        string
		typeName    string
		isAttribute bool
		warning     *string
		comment     string
	}

	buffer := newBufferWithHead()
	buffer.Write(qmc.generateTopNavigation("tables"))
	buffer.Html(`<main id="tables">`)

	var schema clickhouse.TableMap
	var hasSchema bool
	var err error
	var schemaError error
	if qmc.logManager != nil {
		schema, err = qmc.logManager.GetTableDefinitions()
		if err != nil {
			schemaError = err
		} else {
			hasSchema = true
		}
	}

	if hasSchema {

		// Not sure if we should read directly from the TableMap or we should use the Snapshot of it.
		// Let's leave it as is for now.

		tableNames := schema.Keys()
		sort.Strings(tableNames)

		buffer.Html("\n<table>")

		for i, tableName := range tableNames {
			table, ok := schema.Load(tableName)
			if !ok {
				continue
			}

			id := fmt.Sprintf("schema-table-%d", i)
			var menu menuEntry
			menu.label = table.Name
			menu.target = fmt.Sprintf("#%s", id)
			menuEntries = append(menuEntries, menu)

			buffer.Html(`<tr class="tableName"`)
			buffer.Html(fmt.Sprintf(` id="%s"`, id))
			buffer.Html(`>`)
			buffer.Html(`<th colspan=3><h2>`)
			buffer.Html(`Table: `)
			buffer.Text(table.Name)

			buffer.Html(`</h2>`)
			if table.Comment != "" {
				buffer.Html(`<h3>`)
				buffer.Text(" (")
				buffer.Text(table.Comment)
				buffer.Text(")")
				buffer.Html(`</h3>`)
			}

			if table.Name == common_table.TableName {
				buffer.Html(`<h3><a href="/tables/common_table_stats" >table statistics</a></h3>`)
			}

			buffer.Html(`</th>`)
			buffer.Html(`</tr>`)

			buffer.Html(`<tr>`)
			buffer.Html(`<th>`)
			buffer.Html(`Name`)
			buffer.Html(`</th>`)
			buffer.Html(`<th>`)
			buffer.Html(`Type`)
			buffer.Html(`</th>`)
			buffer.Html(`<th>`)
			buffer.Html(`Comment`)
			buffer.Html(`</th>`)
			buffer.Html(`</tr>`)

			var columnNames []string
			var columnMap = make(map[string]tableColumn)

			// standard columns, visible for the user
			for k := range table.Cols {
				c := tableColumn{}

				c.name = k
				if table.Cols[k].Type != nil {
					c.typeName = table.Cols[k].Type.StringWithNullable()
				} else {
					c.typeName = "n/a"
				}

				c.isAttribute = false
				c.comment = table.Cols[k].Comment

				columnNames = append(columnNames, k)
				columnMap[k] = c
			}

			for alias, target := range qmc.cfg.AliasFields(table.Name) {

				// check for collisions
				if field, collide := columnMap[alias]; collide {
					field.warning = util.Pointer("alias declared with the same name")
					columnMap[alias] = field
					continue
				}

				// check if target exists
				c := tableColumn{}
				c.name = alias
				if aliasedField, ok := columnMap[target]; ok {
					c.typeName = fmt.Sprintf("alias of '%s', %s", target, aliasedField.typeName)
					c.isAttribute = aliasedField.isAttribute
				} else {
					c.warning = util.Pointer("alias points to non-existing field '" + target + "'")
					c.typeName = "dangling alias"
				}

				columnNames = append(columnNames, alias)
				columnMap[alias] = c
			}

			// columns added by Quesma, not visible for the user
			//
			// this part is based on addOurFieldsToCreateTableQuery in log_manager.go
			attributes := table.Config.GetAttributes()
			if len(attributes) > 0 {
				for _, a := range attributes {
					_, ok := table.Cols[a.KeysArrayName]
					if !ok {
						c := tableColumn{}
						c.name = a.KeysArrayName
						c.typeName = clickhouse.CompoundType{Name: "Array", BaseType: clickhouse.NewBaseType("String")}.StringWithNullable()
						c.isAttribute = true
						columnNames = append(columnNames, c.name)
						columnMap[c.name] = c
					}
					_, ok = table.Cols[a.ValuesArrayName]
					if !ok {
						c := tableColumn{}
						c.name = a.ValuesArrayName
						c.typeName = clickhouse.CompoundType{Name: "Array", BaseType: a.Type}.StringWithNullable()
						c.isAttribute = true
						columnNames = append(columnNames, c.name)
						columnMap[c.name] = c
					}
				}
			}

			sort.Strings(columnNames)

			for _, columnName := range columnNames {
				column, ok := columnMap[columnName]
				if !ok {
					continue
				}

				buffer.Html(`<tr class="`)

				if column.isAttribute {
					buffer.Html(`columnAttribute `)
				}
				if column.warning != nil {
					buffer.Html(`columnWarning `)
				}
				buffer.Html(`column`)

				buffer.Html(`">`)
				buffer.Html(`<td class="columnName">`)

				buffer.Text(column.name)
				buffer.Html(`</td>`)
				buffer.Html(`<td class="columnType">`)

				buffer.Text(column.typeName)

				if column.warning != nil {
					buffer.Html(` <span class="columnWarningText">WARNING: `)
					buffer.Text(*column.warning)
					buffer.Html(`</span>`)
				}
				buffer.Html(`</td>`)

				buffer.Html(`<td class="columnComment">`)
				if column.comment != "" {
					buffer.Text(column.comment)
				} else {
					buffer.Html(`&mdash;`) // something that doesn't look like a comment
				}

				buffer.Html(`</tr>`)
			}

			buffer.Html("<tr>")
			buffer.Html(`<td colspan=3 class="create-table-query">`)
			query := table.CreateTableQuery

			// indent first line
			query = strings.Replace(query, "(", "(\n", 1)
			query = strings.ReplaceAll(query, "), ", "),\n")
			query = strings.ReplaceAll(query, ")`, ", ")`,\n")
			query = strings.ReplaceAll(query, "', ", "',\n")

			query = strings.ReplaceAll(query, " ENGINE", "\nENGINE")
			query = strings.ReplaceAll(query, " SETTINGS", "\nSETTINGS")
			query = strings.ReplaceAll(query, " PARTITION BY", "\nPARTITION BY")
			query = strings.ReplaceAll(query, " ORDER BY", "\nORDER BY")
			query = strings.ReplaceAll(query, " PRIMARY KEY", "\nPRIMARY KEY")
			query = strings.ReplaceAll(query, " SAMPLE BY", "\nSAMPLE BY")
			query = strings.ReplaceAll(query, " TTL", "\nTTL")

			buffer.Html("<details><summary><b>Click to show CREATE TABLE query</b></summary><pre>")
			buffer.Text(query)
			buffer.Html("</pre></details>")
			buffer.Html(`</td>`)
			buffer.Html(`</tr>`)
		}

		buffer.Html("\n</table>")

	} else {
		details := ""
		if schemaError != nil {

			var endUserError *end_user_errors.EndUserError
			if errors.As(err, &endUserError) {
				details = fmt.Sprintf("Error: %s", endUserError.EndUserErrorMessage())
			}
			buffer.Html(`<p>Schema is not available.</p>`)
			if details != "" {
				buffer.Html(`<p>`)
				buffer.Text(details)
				buffer.Html(`</p>`)
			}
		} else {
			buffer.Html(`<p>Schema is not available</p>`)
		}
	}

	buffer.Html("\n<table>")
	buffer.Html(`<tr class="tableName" id="quesma-config">`)
	buffer.Html(`<th colspan=4><h2>`)
	buffer.Html(`Quesma Config`)
	buffer.Html(`</h2></th>`)
	buffer.Html(`</tr>`)

	buffer.Html(`<tr>`)
	buffer.Html(`<th>`)
	buffer.Html(`Name Pattern`)
	buffer.Html(`</th>`)
	buffer.Html(`<th>`)
	buffer.Html(`Query targets`)
	buffer.Html(`</th>`)
	buffer.Html(`<th>`)
	buffer.Html(`Ingest targets`)
	buffer.Html(`</th>`)

	buffer.Html(`</tr>`)

	for indexName, cfg := range qmc.cfg.IndexConfig {
		buffer.Html(`<tr>`)
		buffer.Html(`<td>`)
		buffer.Text(indexName)
		buffer.Html(`</td>`)

		// TODO: these are not the backend connector names, but config.ElasticsearchTarget and config.ClickhouseTarget
		// constants
		buffer.Html(`<td>`)
		buffer.Text(strings.Join(cfg.QueryTarget, ", "))
		buffer.Html(`</td>`)
		buffer.Html(`<td>`)
		buffer.Text(strings.Join(cfg.IngestTarget, ", "))
		buffer.Html(`</td>`)

		buffer.Html(`</tr>`)
	}

	buffer.Html("\n</table>")

	buffer.Html("\n</main>\n\n")

	buffer.Html(`<div class="menu">`)
	buffer.Html("\n<h2>Menu</h2>")

	buffer.Html(`<h3>Admin actions</h3>`)
	buffer.Html(`<ul>`)

	buffer.Html(`<li><button hx-post="/tables/reload" hx-target="body">Reload Tables</button></li>`)

	buffer.Html(`</ul>`)

	buffer.Html(`<h3>Tables:</h3>`)

	buffer.Html("<ol>")

	for _, menu := range menuEntries {
		buffer.Html(`<li><a href="`)
		buffer.Text(menu.target)
		buffer.Html(`">`)
		buffer.Text(menu.label)
		buffer.Html(`</a></li>`)
	}

	buffer.Html("</ol>")

	buffer.Html(`<h3><a href="#quesma-config">Jump to Quesma Config</a></h3>`)

	buffer.Html("\n</div>")

	buffer.Html("\n</body>")
	buffer.Html("\n</html>")
	return buffer.Bytes()
}
