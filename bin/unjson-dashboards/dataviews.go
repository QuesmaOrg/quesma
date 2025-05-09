// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
)

func createDataViews() {

	indexes, err := readIndexMappings()
	if err != nil {
		log.Fatalf("Error reading index mappings: %v", err)
	}

	for _, index := range indexes {

		var timestampField string
		for field, mapping := range index.Properties {

			if mapping.Type == "date" {
				timestampField = field
				break
			}
		}

		err = CreateDataView(index.Pattern, timestampField)
		if err != nil {
			log.Printf("Failed to create Data View: %v", err)
		}

	}

}

type DataViewRequest struct {
	Attributes struct {
		Title         string `json:"title"`                   // Index pattern name
		TimeFieldName string `json:"timeFieldName,omitempty"` // Optional time field
	} `json:"attributes"`
}

// CreateDataView creates a new Data View (Index Pattern) in Kibana
func CreateDataView(dataViewName string, timeField string) error {
	// Construct the API URL
	url := "http://localhost:5601/api/saved_objects/index-pattern"

	// Create the request payload
	payload := DataViewRequest{}
	payload.Attributes.Title = dataViewName
	if timeField != "" {
		payload.Attributes.TimeFieldName = timeField
	}

	// Convert to JSON
	jsonData, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal JSON: %v", err)
	}

	// Create HTTP request
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("failed to create request: %v", err)
	}

	// Set headers
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("kbn-xsrf", "true") // Required to bypass CSRF protection

	// Execute the request
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send request: %v", err)
	}
	defer resp.Body.Close()

	// Check response status
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("failed to create Data View, status: %d", resp.StatusCode)
	}

	log.Printf("✅ Successfully created Data View: %s", dataViewName)
	return nil
}
