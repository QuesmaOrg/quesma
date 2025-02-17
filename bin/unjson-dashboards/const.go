// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package main

const (
	kibanaURL             = "http://localhost:5601" // Update this with your Kibana URL
	kibanaSpace           = "default"               // Change if using a specific space
	apiKey                = ""                      // Add API key if required
	ndjsonFolder          = "dashboards"            // Directory containing NDJSON files
	dashboardListEndpoint = "/api/saved_objects/_find?type=dashboard&per_page=10000"
)
