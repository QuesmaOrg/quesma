// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package main

import (
	"bytes"
	"fmt"

	"io"
	"log"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
)

// Kibana configuration

func importDashboards() {
	// Get list of NDJSON files in the directory
	files, err := filepath.Glob(filepath.Join(ndjsonFolder, "*.ndjson"))
	if err != nil {
		log.Fatalf("Error reading directory: %v", err)
	}

	if len(files) == 0 {
		log.Println("No NDJSON files found to import.")
		return
	}

	// Import each NDJSON file
	for _, file := range files {
		fmt.Printf("Importing file: %s\n", file)
		err := importNDJSON(file)
		if err != nil {
			log.Printf("Failed to import %s: %v\n", file, err)
		} else {
			fmt.Printf("Successfully imported: %s\n", file)
		}
	}
}

// importNDJSON uploads a given NDJSON file to Kibana's Saved Objects API
func importNDJSON(filePath string) error {
	// Open the NDJSON file
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("error opening file: %v", err)
	}
	defer file.Close()

	// Create a buffer and a multipart writer
	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)

	// Add the NDJSON file as form data
	part, err := writer.CreateFormFile("file", filepath.Base(filePath))
	if err != nil {
		return fmt.Errorf("failed to create form file: %v", err)
	}
	_, err = io.Copy(part, file)
	if err != nil {
		return fmt.Errorf("failed to write file to form: %v", err)
	}

	// Close the multipart writer to finalize the body
	writer.Close()

	// Create the Kibana API request
	url := fmt.Sprintf("%s/s/%s/api/saved_objects/_import?overwrite=true", kibanaURL, kibanaSpace)
	req, err := http.NewRequest("POST", url, body)
	if err != nil {
		return fmt.Errorf("failed to create request: %v", err)
	}

	// Set required headers
	req.Header.Set("kbn-xsrf", "true") // Required for Kibana API
	req.Header.Set("Content-Type", writer.FormDataContentType())

	// If using an API key for authentication
	if apiKey != "" {
		req.Header.Set("Authorization", "ApiKey "+apiKey)
	}

	// Send the request
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send request: %v", err)
	}
	defer resp.Body.Close()

	// Read response
	respBody := new(bytes.Buffer)
	_, err = io.Copy(respBody, resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response: %v", err)
	}

	// Check response status
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("Kibana API error: %s", respBody.String())
	}

	fmt.Println("Response:", respBody.String())
	return nil
}
