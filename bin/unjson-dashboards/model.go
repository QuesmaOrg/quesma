// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
)

type IndexMappingsField struct {
	Type         string   `json:"type"`
	SampleValues []string `json:"_sample_values,omitempty"`
}

type IndexMappings struct {
	Name       string                        `json:"_name"`
	Pattern    string                        `json:"_pattern"`
	Properties map[string]IndexMappingsField `json:"properties"`
}

func readIndexMappings() ([]IndexMappings, error) {
	indexes := make([]IndexMappings, 0)

	inputDir := "mappings"
	entries, err := os.ReadDir(inputDir)
	if err != nil {
		log.Fatal(err)
	}

	for _, entry := range entries {

		if entry.IsDir() {
			continue
		}

		if !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}

		file := fmt.Sprintf("%s/%s", inputDir, entry.Name())
		log.Println("Reading file:", file)

		bytes, err := os.ReadFile(file)
		if err != nil {
			log.Println("Failed to read file:", err)
			continue
		}

		var mappings IndexMappings
		if err := json.Unmarshal(bytes, &mappings); err != nil {
			log.Println("Failed to parse JSON:", err)
			continue
		}

		indexes = append(indexes, mappings)
	}
	return indexes, nil
}
