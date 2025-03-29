// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package pipe_syntax

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/goccy/go-json"

	"github.com/ip2location/ip2location-go/v9"

	"github.com/QuesmaOrg/quesma/platform/parsers/sql/parser/transforms"
	"github.com/huandu/go-clone"

	lexer_core "github.com/QuesmaOrg/quesma/platform/parsers/sql/lexer/core"
	"github.com/QuesmaOrg/quesma/platform/parsers/sql/parser/core"
)

type (
	ChatGPTRequest struct {
		Model    string           `json:"model"`
		Messages []ChatGPTMessage `json:"messages"`
	}
	ChatGPTMessage struct {
		Role    string `json:"role"`
		Content string `json:"content"`
	}
	ChatGPTResponse struct {
		Choices []struct {
			Message ChatGPTMessage `json:"message"`
		} `json:"choices"`
	}
)

func llmCall(request string, input string) (string, error) {
	modelName := "openai/gpt-4o-mini-2024-07-18"
	apiEndpoint := "https://openrouter.ai/api/v1/chat/completions"

	systemPrompt := fmt.Sprintf("You are a helpful assistant. You are helping a user, which uses you through a CALL ENRICH_LLM() operator. For example, the user is trying to enrich a table with a new column, like a country description based on the existing country code column.\nExample: Request: description of country.\nInput: PL\nYour output: Poland is a country in central Europe.\n \nExample: Request: area of country.\nInput: France\nYour output: 551,695 square kilometers.\n Provide a direct answer, without any additional information or small-talk. DO NOT respond with a full sentence. The response should be very concise - it should fit in 100 characters ideally. For example, if a user asks you for country population, just give him a number, not a full sentence 'The population of X is Y' - just give Y!")

	userPrompt := fmt.Sprintf("Request: %s.\nInput: %s", request, input)

	apiKey := os.Getenv("OPENAI_API_KEY")

	requestBody := ChatGPTRequest{
		Model: modelName,
		Messages: []ChatGPTMessage{
			{Role: "system", Content: systemPrompt},
			{Role: "user", Content: userPrompt},
		},
	}

	requestBodyBytes, err := json.Marshal(requestBody)
	if err != nil {
		return "", fmt.Errorf("error marshalling request body: %w", err)
	}

	req, err := http.NewRequest("POST", apiEndpoint, bytes.NewBuffer(requestBodyBytes))
	if err != nil {
		return "", fmt.Errorf("error creating HTTP request: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+apiKey)
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("error making HTTP request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("received non-OK HTTP status: %d", resp.StatusCode)
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("error reading response body: %w", err)
	}

	var chatResponse ChatGPTResponse
	if err := json.Unmarshal(body, &chatResponse); err != nil {
		return "", fmt.Errorf("error unmarshalling chat response: %w", err)
	}

	if len(chatResponse.Choices) == 0 {
		return "", fmt.Errorf("chat response contained no choices")
	}

	return chatResponse.Choices[0].Message.Content, nil
}

func ExpandEnrichments(node core.Node, conn *sql.DB) {
	TransformPipeNodes(node, func(pipeNode *PipeNode) core.Node {
		for i := 0; i < len(pipeNode.Pipes); i++ {
			pipeNodeList, ok := pipeNode.Pipes[i].(core.NodeListNode)
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
				// Build two new pipes:
				// 1.
				// |> LEFT JOIN quesma_enrich ON quesma_enrich.key = <ip_column> AND enrich_type = 'ip'
				// |> EXTEND quesma_enrich.value AS ip_country
				// 2.
				// second pipe for EXTEND
				ipPipe, extendPipe := enrichIpMacro(pipeNodeList, clone.Clone(pipeNode).(*PipeNode), i, conn)

				// FIXME: iteration probably breaks after adding new pipes!

				// Replace the old macro pipe with the two new pipes
				pipeNode.Pipes[i] = core.NodeListNode{Nodes: ipPipe}
				pipeNode.Pipes = append(pipeNode.Pipes[:i+1], append([]core.Node{core.NodeListNode{Nodes: extendPipe}}, pipeNode.Pipes[i+1:]...)...)
			} else if macroType == "ENRICH_LLM" {
				// Parse out the tokens following "CALL ENRICH_LLM":
				// Expected form: |> CALL ENRICH_LLM <prompt> , <input_column>
				var promptNodes []core.Node
				var inputColumn []core.Node
				commaFound := false
				for j := 5; j < len(pipeNodeList.Nodes); j++ {
					if token, ok := pipeNodeList.Nodes[j].(core.TokenNode); ok && token.Token.RawValue == "," {
						commaFound = true
						continue
					}
					if !commaFound {
						promptNodes = append(promptNodes, pipeNodeList.Nodes[j])
					} else {
						inputColumn = append(inputColumn, pipeNodeList.Nodes[j])
					}
				}
				if len(promptNodes) == 0 || len(inputColumn) == 0 {
					continue
				}

				// Build the aggregated query for enrichment using inputColumn
				copiedNode := clone.Clone(pipeNode).(*PipeNode)
				copiedNode.Pipes = copiedNode.Pipes[:i]
				{
					newNodes := []core.Node{
						core.TokenNode{Token: lexer_core.Token{RawValue: "|>"}},
						core.TokenNode{Token: lexer_core.Token{RawValue: " "}},
						core.TokenNode{Token: lexer_core.Token{RawValue: "AGGREGATE"}},
						core.TokenNode{Token: lexer_core.Token{RawValue: " "}},
					}
					newNodes = append(newNodes, inputColumn...)
					newNodes = append(newNodes,
						core.TokenNode{Token: lexer_core.Token{RawValue: " "}},
						core.TokenNode{Token: lexer_core.Token{RawValue: "GROUP BY"}},
						core.TokenNode{Token: lexer_core.Token{RawValue: " "}},
					)
					newNodes = append(newNodes, inputColumn...)
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

							if values[0] != nil {
								firstColumnValues = append(firstColumnValues, fmt.Sprintf("%v", values[0]))
							} else {
								firstColumnValues = append(firstColumnValues, "NULL")
							}
						}

						fmt.Println("First column values:", firstColumnValues)
						fmt.Println("Total rows:", len(firstColumnValues))

						// Enrich using LLM calls in parallel
						if len(firstColumnValues) > 0 {
							promptStr := transforms.ConcatTokenNodes(&core.NodeListNode{Nodes: promptNodes})
							fmt.Println("Enriching using LLM with prompt:", promptStr)

							var wg sync.WaitGroup
							var mu sync.Mutex
							llmResults := make(map[string]string)

							// Create a semaphore channel to limit parallel calls to 20.
							limiter := make(chan struct{}, 20)

							for _, val := range firstColumnValues {
								if val == "NULL" || val == "" {
									mu.Lock()
									llmResults[val] = "Unknown"
									mu.Unlock()
									continue
								}
								wg.Add(1)
								limiter <- struct{}{} // Acquire a slot
								go func(input string) {
									defer wg.Done()
									defer func() { <-limiter }() // Release the slot

									res, err := llmCall(promptStr, input)
									if err != nil {
										fmt.Printf("Error calling llm for input %s: %v\n", input, err)
										res = "Error"
									}
									mu.Lock()
									llmResults[input] = res
									mu.Unlock()
								}(val)
							}
							wg.Wait()

							fmt.Println("LLM enrichment complete. Received responses for", len(llmResults), "inputs")

							// Create enrichment table if not exists
							_, err := conn.Exec("CREATE TABLE IF NOT EXISTS quesma_enrich\n(\n    `enrich_type` LowCardinality(String),\n    `key` String,\n    `value` Nullable(String)\n)\nENGINE = MergeTree\nORDER BY (`enrich_type`, `key`)")
							if err != nil {
								fmt.Printf("Error creating quesma_enrich table: %v\n", err)
							}

							// For each unique input, insert the LLM enrichment result into the table
							for input, response := range llmResults {
								if input != "NULL" && input != "" && response != "Error" {
									// First delete any existing entry for this input
									_, err := conn.Exec(
										"DELETE FROM quesma_enrich WHERE enrich_type = 'llm' AND key = ?",
										input,
									)
									if err != nil {
										fmt.Printf("Error deleting existing enrichment for input %s: %v\n", input, err)
									}
									// Then insert the new enrichment result
									_, err = conn.Exec(
										"INSERT INTO quesma_enrich (key, value, enrich_type) VALUES (?, ?, 'llm')",
										input, response,
									)
									if err != nil {
										fmt.Printf("Error inserting enrichment for input %s: %v\n", input, err)
									}
								}
							}
						}
					}
				}

				// Build two new pipes:
				// |> LEFT JOIN quesma_enrich ON quesma_enrich.key = <input_column> AND enrich_type = 'llm'
				// |> EXTEND quesma_enrich.value AS llm_result
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
				newNodes = append(newNodes, inputColumn...)
				newNodes = append(newNodes,
					core.TokenNode{Token: lexer_core.Token{RawValue: " "}},
					core.TokenNode{Token: lexer_core.Token{RawValue: "AND"}},
					core.TokenNode{Token: lexer_core.Token{RawValue: " "}},
					core.TokenNode{Token: lexer_core.Token{RawValue: "enrich_type"}},
					core.TokenNode{Token: lexer_core.Token{RawValue: " "}},
					core.TokenNode{Token: lexer_core.Token{RawValue: "="}},
					core.TokenNode{Token: lexer_core.Token{RawValue: " "}},
					core.TokenNode{Token: lexer_core.Token{RawValue: "'llm'"}},
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
					core.TokenNode{Token: lexer_core.Token{RawValue: "llm_result"}},
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

func enrichIpMacro(pipeNodeList core.NodeListNode, copiedNode *PipeNode, i int, conn *sql.DB) (ipPipe, extendPipe core.Pipe) {
	// Parse out the tokens following "CALL ENRICH_IP":
	// Expected form: |> CALL ENRICH_IP <ip_column>
	var (
		ipColumn                   core.Pipe
		columns, firstColumnValues []string
		values, valuePtrs          []interface{}
		ipToCountry                map[string]string
		db                         *ip2location.DB
		r_                         sql.Result
	)
	_ = r_

	for j := 5; j < len(pipeNodeList.Nodes); j++ {
		core.Add(ipColumn, pipeNodeList.Nodes[j])
	}

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
		goto END
	}

	defer result.Close()

	// Get column names
	columns, err = result.Columns()
	if err != nil {
		fmt.Println("Error getting columns:", err)
		goto END
	}

	// Prepare values holders
	values = make([]interface{}, len(columns))
	valuePtrs = make([]interface{}, len(columns))
	for j := range columns {
		valuePtrs[j] = &values[j]
	}

	// Collect first column values into a string array
	for result.Next() {
		err = result.Scan(valuePtrs...)
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

	if len(firstColumnValues) == 0 {
		goto END
	}

	// Attempt to enrich IP addresses with country information
	fmt.Println("Enriching IP addresses with country information...")

	// Try to open the IP2Location database
	db, err = ip2location.OpenDB("/root/quesma-logexplorer-app/IP2LOCATION-LITE-DB11.BIN")
	if err != nil {
		fmt.Println("Error opening IP2Location database:", err)
		goto END
	}

	defer db.Close()

	// Create a map to store IP to country mappings
	ipToCountry = make(map[string]string)

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

	r_, err = conn.Exec("CREATE TABLE IF NOT EXISTS quesma_enrich\n(\n    `enrich_type` LowCardinality(String),\n    `key` String,\n    `value` Nullable(String)\n)\nENGINE = MergeTree\nORDER BY (`enrich_type`, `key`)")
	if err != nil {
		fmt.Printf("Error creating quesma_enrich table: %v\n", err)
	}

	// For each unique IP, insert a record into quesma_enrich table
	for ip, country := range ipToCountry {
		if ip != "NULL" && ip != "" && country != "Unknown" {
			// Insert or update the enrichment data
			// First delete any existing entry for this IP
			_, err := conn.Exec(
				"DELETE FROM quesma_enrich WHERE enrich_type = 'ip' AND key = ?",
				ip,
			)
			if err != nil {
				fmt.Printf("Error deleting existing enrichment for IP %s: %v\n", ip, err)
			}

			// Then insert the new entry
			_, err = conn.Exec(
				"INSERT INTO quesma_enrich (key, value, enrich_type) VALUES (?, ?, 'ip')",
				ip, country,
			)
			if err != nil {
				fmt.Printf("Error inserting enrichment for IP %s: %v\n", ip, err)
			}
		}
	}

END:
	return buildIpPipe(ipColumn), buildExtendPipe()
}

func buildIpPipe(ipColumn []core.Node) core.Pipe {
	pipe := core.NewPipe(
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
	)
	core.Add(pipe, ipColumn...)
	core.Add(pipe,
		core.TokenNode{Token: lexer_core.Token{RawValue: " "}},
		core.TokenNode{Token: lexer_core.Token{RawValue: "AND"}},
		core.TokenNode{Token: lexer_core.Token{RawValue: " "}},
		core.TokenNode{Token: lexer_core.Token{RawValue: "enrich_type"}},
		core.TokenNode{Token: lexer_core.Token{RawValue: " "}},
		core.TokenNode{Token: lexer_core.Token{RawValue: "="}},
		core.TokenNode{Token: lexer_core.Token{RawValue: " "}},
		core.TokenNode{Token: lexer_core.Token{RawValue: "'ip'"}},
	)

	return pipe
}

func buildExtendPipe() core.Pipe {
	return core.NewPipe(
		core.TokenNode{Token: lexer_core.Token{RawValue: "|>"}},
		core.TokenNode{Token: lexer_core.Token{RawValue: " "}},
		core.TokenNode{Token: lexer_core.Token{RawValue: "EXTEND"}},
		core.TokenNode{Token: lexer_core.Token{RawValue: " "}},
		core.TokenNode{Token: lexer_core.Token{RawValue: "quesma_enrich.value"}},
		core.TokenNode{Token: lexer_core.Token{RawValue: " "}},
		core.TokenNode{Token: lexer_core.Token{RawValue: "AS"}},
		core.TokenNode{Token: lexer_core.Token{RawValue: " "}},
		core.TokenNode{Token: lexer_core.Token{RawValue: "ip_country"}},
	)
}
