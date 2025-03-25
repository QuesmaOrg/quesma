// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package pipe_syntax

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/ip2location/ip2location-go/v9"
	"strings"
	"time"

	"github.com/QuesmaOrg/quesma/platform/parsers/sql/parser/transforms"
	"github.com/huandu/go-clone"

	lexer_core "github.com/QuesmaOrg/quesma/platform/parsers/sql/lexer/core"
	"github.com/QuesmaOrg/quesma/platform/parsers/sql/parser/core"
)

func ExpandEnrichments(node core.Node, conn *sql.DB) {
	TransformPipeNodes(node, func(pipeNode *PipeNode) core.Node {
		for i, pipe := range pipeNode.Pipes {
			pipeNodeList, ok := pipe.(core.NodeListNode)
			if !ok {
				continue
			}
			if len(pipeNodeList.Nodes) < 5 {
				continue
			}

			// Verify we have a "CALL" operator.
			tokenNode, ok := pipeNodeList.Nodes[2].(core.TokenNode)
			if !ok || strings.ToUpper(tokenNode.Token.RawValue) != "CALL" {
				continue
			}

			// Determine the macro type from the 5th token.
			macroToken, ok := pipeNodeList.Nodes[4].(core.TokenNode)
			if !ok {
				continue
			}
			macroType := strings.ToUpper(macroToken.Token.RawValue)

			if macroType == "ENRICH_IP" {
				// Parse out the tokens following "CALL ENRICH_IP":
				// Expected form: |> CALL ENRICH_IP <ip_column>
				var ipColumn []core.Node
				for j := 5; j < len(pipeNodeList.Nodes); j++ {
					ipColumn = append(ipColumn, pipeNodeList.Nodes[j])
				}

				copiedNode := clone.Clone(pipeNode).(*PipeNode)
				copiedNode.Pipes = copiedNode.Pipes[:i]
				{
					newNodes := []core.Node{
						core.TokenNode{Token: lexer_core.Token{RawValue: "|>"}},
						core.TokenNode{Token: lexer_core.Token{RawValue: " "}},
						core.TokenNode{Token: lexer_core.Token{RawValue: "AGGREGATE"}},
						core.TokenNode{Token: lexer_core.Token{RawValue: " "}},
					}
					newNodes = append(newNodes, ipColumn...)
					newNodes = append(newNodes,
						core.TokenNode{Token: lexer_core.Token{RawValue: " "}},
						core.TokenNode{Token: lexer_core.Token{RawValue: "GROUP BY"}},
						core.TokenNode{Token: lexer_core.Token{RawValue: " "}},
					)
					newNodes = append(newNodes, ipColumn...)
					copiedNode.Pipes = append(copiedNode.Pipes, core.NodeListNode{Nodes: newNodes})
				}
				copiedNode.Pipes = append(copiedNode.Pipes, core.NodeListNode{Nodes: []core.Node{
					core.TokenNode{Token: lexer_core.Token{RawValue: "|>"}},
					core.TokenNode{Token: lexer_core.Token{RawValue: " "}},
					core.TokenNode{Token: lexer_core.Token{RawValue: "LIMIT"}},
					core.TokenNode{Token: lexer_core.Token{RawValue: " "}},
					core.TokenNode{Token: lexer_core.Token{RawValue: "100"}},
				}})
				copiedNode2 := &core.NodeListNode{Nodes: []core.Node{copiedNode}}
				Transpile(copiedNode2)
				fmt.Println(transforms.ConcatTokenNodes(copiedNode2))
				fmt.Println("------")

				// Execute the query and print the result to stdout
				queryStr := transforms.ConcatTokenNodes(copiedNode2)
				fmt.Println("Executing query:", queryStr)

				// Create a context with timeout
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()

				// Execute the query
				result, err := conn.QueryContext(ctx, queryStr)
				if err != nil {
					fmt.Println("Error executing query:", err)
				} else {
					defer result.Close()

					// Get column names
					columns, err := result.Columns()
					if err != nil {
						fmt.Println("Error getting columns:", err)
					} else {
						// Prepare values holders
						values := make([]interface{}, len(columns))
						valuePtrs := make([]interface{}, len(columns))
						for i := range columns {
							valuePtrs[i] = &values[i]
						}

						// Collect first column values into a string array
						firstColumnValues := []string{}
						for result.Next() {
							err := result.Scan(valuePtrs...)
							if err != nil {
								fmt.Println("Error scanning row:", err)
								break
							}

							// Add first column value to array
							if values[0] != nil {
								firstColumnValues = append(firstColumnValues, fmt.Sprintf("%v", values[0]))
							} else {
								firstColumnValues = append(firstColumnValues, "NULL")
							}
						}

						fmt.Println("First column values:", firstColumnValues)
						fmt.Println("Total rows:", len(firstColumnValues))

						// Attempt to enrich IP addresses with country information
						if len(firstColumnValues) > 0 {
							fmt.Println("Enriching IP addresses with country information...")

							// Try to open the IP2Location database
							db, err := ip2location.OpenDB("/root/quesma-logexplorer-app/IP2LOCATION-LITE-DB11.BIN")
							if err != nil {
								fmt.Println("Error opening IP2Location database:", err)
							} else {
								defer db.Close()

								// Create a map to store IP to country mappings
								ipToCountry := make(map[string]string)

								// Process each IP address
								for _, ip := range firstColumnValues {
									if ip == "NULL" || ip == "" {
										ipToCountry[ip] = "Unknown"
										continue
									}

									// Look up the IP address
									results, err := db.Get_all(ip)
									if err != nil {
										fmt.Printf("Error looking up IP %s: %v\n", ip, err)
										ipToCountry[ip] = "Unknown"
									} else {
										ipToCountry[ip] = results.Country_long
										fmt.Printf("IP: %s -> Country: %s\n", ip, results.Country_long)
									}
								}

								fmt.Println("IP enrichment complete. Found countries for", len(ipToCountry), "IPs")

								// For each unique IP, insert a record into quesma_enrich table
								for ip, country := range ipToCountry {
									if ip != "NULL" && ip != "" && country != "Unknown" {
										// Insert or update the enrichment data
										_, err := conn.Exec(
											"INSERT INTO quesma_enrich (key, value, enrich_type) VALUES (?, ?, 'ip')",
											ip, country,
										)
										if err != nil {
											fmt.Printf("Error inserting enrichment for IP %s: %v\n", ip, err)
										}
									}
								}
							}
						}
					}
				}

				// Build two new pipes:
				// |> LEFT JOIN quesma_enrich ON quesma_enrich.key = <ip_column> AND enrich_type = 'ip'
				// |> EXTEND quesma_enrich.value AS ip_country
				newNodes := []core.Node{
					core.TokenNode{Token: lexer_core.Token{RawValue: "|>"}},
					core.TokenNode{Token: lexer_core.Token{RawValue: " "}},
					core.TokenNode{Token: lexer_core.Token{RawValue: "LEFT JOIN"}},
					core.TokenNode{Token: lexer_core.Token{RawValue: " "}},
					core.TokenNode{Token: lexer_core.Token{RawValue: "quesma_enrich"}},
					core.TokenNode{Token: lexer_core.Token{RawValue: " "}},
					core.TokenNode{Token: lexer_core.Token{RawValue: "ON"}},
					core.TokenNode{Token: lexer_core.Token{RawValue: " "}},
					core.TokenNode{Token: lexer_core.Token{RawValue: "quesma_enrich.key"}},
					core.TokenNode{Token: lexer_core.Token{RawValue: " "}},
					core.TokenNode{Token: lexer_core.Token{RawValue: "="}},
					core.TokenNode{Token: lexer_core.Token{RawValue: " "}},
				}
				newNodes = append(newNodes, ipColumn...)
				newNodes = append(newNodes,
					core.TokenNode{Token: lexer_core.Token{RawValue: " "}},
					core.TokenNode{Token: lexer_core.Token{RawValue: "AND"}},
					core.TokenNode{Token: lexer_core.Token{RawValue: " "}},
					core.TokenNode{Token: lexer_core.Token{RawValue: "enrich_type"}},
					core.TokenNode{Token: lexer_core.Token{RawValue: " "}},
					core.TokenNode{Token: lexer_core.Token{RawValue: "="}},
					core.TokenNode{Token: lexer_core.Token{RawValue: " "}},
					core.TokenNode{Token: lexer_core.Token{RawValue: "'ip'"}},
				)

				// Create second pipe for EXTEND
				extendNodes := []core.Node{
					core.TokenNode{Token: lexer_core.Token{RawValue: "|>"}},
					core.TokenNode{Token: lexer_core.Token{RawValue: " "}},
					core.TokenNode{Token: lexer_core.Token{RawValue: "EXTEND"}},
					core.TokenNode{Token: lexer_core.Token{RawValue: " "}},
					core.TokenNode{Token: lexer_core.Token{RawValue: "quesma_enrich.value"}},
					core.TokenNode{Token: lexer_core.Token{RawValue: " "}},
					core.TokenNode{Token: lexer_core.Token{RawValue: "AS"}},
					core.TokenNode{Token: lexer_core.Token{RawValue: " "}},
					core.TokenNode{Token: lexer_core.Token{RawValue: "ip_country"}},
				}

				// FIXME: iteration probably breaks after adding new pipes!

				// Replace the old macro pipe with the two new pipes
				pipeNode.Pipes[i] = core.NodeListNode{Nodes: newNodes}
				pipeNode.Pipes = append(pipeNode.Pipes[:i+1], append([]core.Node{core.NodeListNode{Nodes: extendNodes}}, pipeNode.Pipes[i+1:]...)...)
			} else {
				// Enrichment not recognized; continue.
				continue
			}
		}
		return pipeNode
	})
}
