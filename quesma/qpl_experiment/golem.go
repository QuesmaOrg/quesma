// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package qpl_experiment

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
)

const apiURL = "https://api.openai.com/v1/chat/completions"

type ChatGPTRequest struct {
	Model    string           `json:"model"`
	Messages []ChatGPTMessage `json:"messages"`
}

type ChatGPTMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type ChatGPTResponse struct {
	Choices []struct {
		Message ChatGPTMessage `json:"message"`
	} `json:"choices"`
}

type ChatGPTTVF struct {
	Prompt string
}

func (c ChatGPTTVF) Fn(t Table) (Table, error) {

	apiKey := os.Getenv("OPENAI_API_KEY") // Make sure your API key is set as an environment variable
	if apiKey == "" {
		fmt.Println("Error: OPENAI_API_KEY environment variable not set")
		return t, fmt.Errorf("Error: OPENAI_API_KEY environment variable not set")
	}

	csvInput, err := WriteTableToString(t)

	prompt := fmt.Sprintf(`
Transform the following CSV:
%s 

Request: %s

Please return the transformed CSV only.
`, csvInput, c.Prompt)

	fmt.Println("Prompt:", prompt)

	// Construct the request
	requestBody := ChatGPTRequest{
		Model: "gpt-3.5-turbo", // You can also use "gpt-3.5-turbo"
		Messages: []ChatGPTMessage{
			{Role: "system", Content: "You are a helpful assistant."},
			{Role: "user", Content: prompt},
		},
	}

	// Serialize the request body

	requestBodyBytes, err := json.Marshal(requestBody)
	if err != nil {
		return EmptyTable(), fmt.Errorf("Error marshalling request: %w", err)
	}

	// Create HTTP request
	req, err := http.NewRequest("POST", apiURL, bytes.NewBuffer(requestBodyBytes))
	if err != nil {
		fmt.Println("Error creating HTTP request:", err)
		return EmptyTable(), fmt.Errorf("Error creating HTTP request: %w", err)
	}

	// Set headers
	req.Header.Set("Authorization", "Bearer "+apiKey)
	req.Header.Set("Content-Type", "application/json")

	// Send the request
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return EmptyTable(), fmt.Errorf("Error sending request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Println("Error response from API:", resp.Status)
		return t, fmt.Errorf("Error response from API: %w", resp.Status)
	}

	// Read the response
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Println("Error reading response:", err)
		return EmptyTable(), fmt.Errorf("Error reading response: %w", err)
	}

	// Parse the response
	var chatResponse ChatGPTResponse
	if err := json.Unmarshal(body, &chatResponse); err != nil {
		fmt.Println("Error unmarshalling response:", err)
		return EmptyTable(), fmt.Errorf("Error unmarshalling response: %w", err)
	}

	// Output the response content
	if len(chatResponse.Choices) > 0 {

		response := chatResponse.Choices[0].Message.Content

		log.Println("ChatGPT Response:", response)

		out, err := ReadTableFromString(response)
		if err != nil {
			return EmptyTable(), fmt.Errorf("Error reading table from response: %w", err)
		}

		return out, nil
	} else {
		log.Println("No response received")
	}

	return t, nil

}
