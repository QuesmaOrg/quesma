// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package qpl_experiment

import (
	"fmt"
	"strings"
)

func ParseQPL(query string) (Pipeline, error) {

	query = strings.TrimSpace(query)
	query = strings.TrimSuffix(query, ";")

	res := Pipeline{}

	currentDatabase := "clickhouse"

	currentSQL := "select * from input"

	startedSQL := false

	finishSQL := func() {
		if startedSQL {
			res = append(res, &SQLTVF{Query: currentSQL, Database: currentDatabase})
			currentSQL = "select * from input"
			startedSQL = false
		}
	}

	startSQL := func() {
		startedSQL = true
	}

	commands := strings.Split(query, "|>")

	for _, command := range commands {
		command := strings.TrimSpace(command)
		fields := strings.Fields(command)
		if len(fields) > 0 {
			c := strings.ToLower(fields[0])

			switch c {
			case "_":
				currentSQL = command[1:]
				finishSQL()

			case "from":
				startSQL()
				currentSQL = "SELECT * " + command

			case "select":
				startSQL()

				currentSQL = fmt.Sprintf("%s FROM (%s) sub", command, currentSQL)

			case "where", "group", "order":
				startSQL()

				currentSQL = fmt.Sprintf("SELECT * FROM (%s) sub %s", currentSQL, command)

			case "limit":
				startSQL()

				currentSQL = fmt.Sprintf("SELECT * FROM (%s) sub %s", currentSQL, command)

			case "aggregate":
				startSQL()

				rest := command[9:]

				ary := strings.Split(rest, "GROUP BY")

				switch len(ary) {
				//
				//case 1:
				//	currentSQL = fmt.Sprintf("SELECT %s FROM (%s) GROUP BY %s", ary[0], currentSQL)

				case 2:
					currentSQL = fmt.Sprintf("SELECT %s, %s  FROM (%s) sub GROUP BY %s", ary[0], ary[1], currentSQL, ary[1])

				default:
					return nil, fmt.Errorf("aggregate parsing failed")
				}

			case "use":

				finishSQL()
				currentDatabase = fields[1]
				currentSQL = "SELECT * FROM input"

			case "print":
				finishSQL()
				res = append(res, &PrintTVF{})

			case "to":
				finishSQL()
				res = append(res, &ToSqlTVF{database: currentDatabase, name: fields[1]})

			case "shell":
				finishSQL()
				res = append(res, &ShellTVF{cmd: command[6:]})

			case "???":
				startSQL()

				currentSQL = fmt.Sprintf("SELECT * FROM (%s) sub %s", currentSQL, "LIMIT 100")

				finishSQL()
				res = append(res, &ChatGPTTVF{Prompt: command[3:]})
			}

		} else {
			continue
		}

	}

	finishSQL()

	return res, nil
}
