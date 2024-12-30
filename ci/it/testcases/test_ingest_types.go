// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

// This file contains integration tests for different ingest functionalities.
// This is a good place to add regression tests for ingest bugs.

package testcases

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/assert"
	"log"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
	"text/tabwriter"
)

type IngestTypesTestcase struct {
	IntegrationTestcaseBase
}

func NewIngestTypesTestcase() *IngestTypesTestcase {
	return &IngestTypesTestcase{
		IntegrationTestcaseBase: IntegrationTestcaseBase{
			ConfigTemplate: "quesma-ingest-types.yml.template",
		},
	}
}

func (a *IngestTypesTestcase) SetupContainers(ctx context.Context) error {
	containers, err := setupAllContainersWithCh(ctx, a.ConfigTemplate)
	a.Containers = containers
	return err
}

func (a *IngestTypesTestcase) RunTests(ctx context.Context, t *testing.T) error {

	t.Run("test supported types", func(t *testing.T) { a.testSupportedTypesInDefaultSetup(ctx, t) })
	return nil
}

// Struct to parse only the `fields` tree
type Hit struct {
	Fields map[string][]any `json:"fields"`
	Source map[string]any   `json:"_source"`
}

type HitsWrapper struct {
	Hits []Hit `json:"hits"`
}

type Response struct {
	Hits HitsWrapper `json:"hits"`
}

func ParseResponse(t *testing.T, body []byte) map[string]any {
	var response Response
	err := json.Unmarshal([]byte(body), &response)
	if err != nil {
		log.Fatalf("Error parsing JSON: %v", err)
	}

	// Extract and print the `fields` tree
	for _, hit := range response.Hits.Hits {
		return hit.Source
	}
	return nil
}

func (a *IngestTypesTestcase) testSupportedTypesInDefaultSetup(ctx context.Context, t *testing.T) {

	// Struct to parse only the `fields` tree
	type Hit struct {
		Fields map[string][]string `json:"fields"`
	}

	type HitsWrapper struct {
		Hits []Hit `json:"hits"`
	}

	type Response struct {
		Hits HitsWrapper `json:"hits"`
	}

	types := []struct {
		name        string
		ingestValue string
		description string
		supported   bool
		skipReason  string
	}{
		{
			name:        "binary",
			ingestValue: `"U29tZSBiaW5hcnkgZGF0YQ=="`,
			description: "Binary value encoded as a Base64 string.",
			supported:   true,
		},
		{
			name:        "boolean",
			ingestValue: "true",
			description: "Represents `true` and `false` values.",
			supported:   true,
		},
		{
			name:        "keyword",
			ingestValue: `"example_keyword"`,
			description: "Used for structured content like tags, keywords, or identifiers.",
			supported:   true,
		},
		{
			name:        "constant_keyword",
			ingestValue: `"fixed_value"`,
			description: "A keyword field for a single constant value across all documents.",
			supported:   true,
		},
		{
			name:        "wildcard",
			ingestValue: `"example*wildcard"`,
			description: "Optimized for wildcard search patterns.",
			supported:   true,
		},
		{
			name:        "long",
			ingestValue: "1234",
			description: "64-bit integer value.",
			supported:   true,
		},
		{
			name:        "double",
			ingestValue: "3.14159",
			description: "Double-precision 64-bit IEEE 754 floating point.",
			supported:   true,
		},
		{
			name:        "date",
			ingestValue: `"2024-12-19"`,
			description: "Date value in ISO 8601 format.",
			supported:   true,
		},
		{
			name:        "date_nanos",
			ingestValue: `"2024-12-19 13:21:53.123 +0000 UTC"`,
			description: "Date value with nanosecond precision.",
			supported:   true,
		},
		{
			name:        "object",
			ingestValue: `{"name": "John", "age": 30}`,
			description: "JSON object containing multiple fields.",
			supported:   false,
		},
		{
			name:        "flattened",
			ingestValue: `{"key1": "value1", "key2": "value2"}`,
			description: "Entire JSON object as a single field value.",
			supported:   false,
		},
		{
			name:        "nested",
			ingestValue: `[{"first": "John", "last": "Smith"}, {"first": "Alice", "last": "White"}]`,
			description: "Array of JSON objects preserving the relationship between subfields.",
			supported:   false,
		},
		{
			name:        "ip",
			ingestValue: `"192.168.1.1"`,
			description: "IPv4 or IPv6 address.",
			supported:   true,
		},
		{
			name:        "version",
			ingestValue: `"1.2.3"`,
			description: "Software version following Semantic Versioning.",
			supported:   true,
		},
		{
			name:        "text",
			ingestValue: `"This is a full-text field."`,
			description: "Analyzed, unstructured text for full-text search.",
			supported:   true,
		},
		{
			name:        "annotated-text",
			ingestValue: `"This is <entity>annotated</entity> text."`,
			description: "Text containing special markup for identifying named entities.",
			supported:   true,
		},
		{
			name:        "completion",
			ingestValue: `"autocomplete suggestion"`,
			description: "Used for auto-complete suggestions.",
			supported:   true,
		},
		{
			name:        "search_as_you_type",
			ingestValue: `"search as you type"`,
			description: "Text-like type for as-you-type completion.",
			supported:   true,
		},
		{
			name:        "dense_vector",
			ingestValue: `[0.1,0.2,0.3]`,
			description: "Array of float values representing a dense vector.",
			supported:   true,
		},
		{
			name:        "geo_point",
			ingestValue: `{"lat": 52.2297, "lon": 21.0122}`,
			description: "Latitude and longitude point.",
			supported:   false,
		},
		{
			name:        "geo_shape",
			ingestValue: `{"type": "polygon", "coordinates": [[[21.0, 52.0], [21.1, 52.0], [21.1, 52.1], [21.0, 52.1], [21.0, 52.0]]]}`,
			description: "Complex shapes like polygons.",
			supported:   false,
		},
		{
			name:        "integer_range",
			ingestValue: `{"gte": 10, "lte": 20}`,
			description: "Range of 32-bit integer values.",
			supported:   false,
		},
		{
			name:        "float_range",
			ingestValue: `{"gte": 1.5, "lte": 10.0}`,
			description: "Range of 32-bit floating-point values.",
			supported:   false,
		},
		{
			name:        "long_range",
			ingestValue: `{"gte": 1000000000, "lte": 2000000000}`,
			description: "Range of 64-bit integer values.",
			supported:   false,
		},
		{
			name:        "double_range",
			ingestValue: `{"gte": 2.5, "lte": 20.5}`,
			description: "Range of 64-bit double-precision floating-point values.",
			supported:   false,
		},
		{
			name:        "date_range",
			ingestValue: `{"gte": "2024-01-01", "lte": "2024-12-31"}`,
			description: "Range of date values, specified in ISO 8601 format.",
			supported:   false,
		},
		{
			name:        "ip_range",
			ingestValue: `{"gte": "192.168.0.0", "lte": "192.168.0.255"}`,
			description: "Range of IPv4 or IPv6 addresses.",
			supported:   false,
		},
	}

	type result struct {
		name              string
		claimedSupport    bool
		currentSupport    bool
		putMappingSuccess bool
		ingestSuccess     bool
		querySuccess      bool
		errors            []string
		dbStorage         string
	}

	var results []*result

	for _, typ := range types {
		t.Run(typ.name, func(t *testing.T) {

			r := &result{
				name:           typ.name,
				claimedSupport: typ.supported,
			}

			addError := func(s string) {
				r.errors = append(r.errors, s)
			}

			checkIfStatusOK := func(op string, resp *http.Response) bool {
				if resp.StatusCode != http.StatusOK {
					addError(fmt.Sprintf("failed HTTP request %s got status %d", op, resp.StatusCode))
					return false
				}
				return true
			}

			results = append(results, r)

			indexName := "types_test_" + typ.name
			fieldName := "field_" + typ.name

			resp, _ := a.RequestToQuesma(ctx, t, "PUT", "/"+indexName, []byte(`
{
	"mappings": {
		"properties": {
			"`+fieldName+`": {
				"type": "`+typ.name+`"
			},
		}
	},
	"settings": {
		"index": {}
	}
}`))

			r.putMappingSuccess = checkIfStatusOK("PUT mapping", resp)

			resp, _ = a.RequestToQuesma(ctx, t, "POST", fmt.Sprintf("/%s/_doc", indexName), []byte(`
{
	"`+fieldName+`": `+typ.ingestValue+`
}`))
			r.ingestSuccess = checkIfStatusOK("POST document", resp)

			resp, bytes := a.RequestToQuesma(ctx, t, "GET", "/"+indexName+"/_search", []byte(`
{ "query": { "match_all": {} } }
`))
			assert.Equal(t, http.StatusOK, resp.StatusCode)

			r.querySuccess = true

			source := ParseResponse(t, bytes)
			if source == nil {
				r.querySuccess = false
				addError("failed to parse quesma response")
			} else {

				if typ.skipReason != "" {
					t.Skip(typ.skipReason)
				}

				// We perform a strict comparison of the field value here.

				// TODO: we should compare flattened ingest value as well. Quesma doesn't "unflattening" the 'object' types.
				// In some cases it works (for kibana). Is some it doesn't e.g. geo type.

				fieldValue, ok := source[fieldName]
				if !ok {

					prefix := fieldName + "."
					var fields []string
					for k, _ := range source {
						if strings.HasPrefix(k, prefix) {
							fields = append(fields, k)
						}
					}

					if len(fields) > 0 {
						r.querySuccess = false
						addError(fmt.Sprintf("field %s not found in response, but found fields: %v", fieldName, fields))
					} else {
						addError(fmt.Sprintf("field %s not found in response", fieldName))
						r.querySuccess = false
					}
				} else {

					var fieldValueAsString string
					switch v := fieldValue.(type) {

					case string:
						fieldValueAsString = strconv.Quote(v)
					case float64:
						fieldValueAsString = strconv.FormatFloat(v, 'f', -1, 64)

					default:
						data, err := json.Marshal(v)
						if err != nil {

						}
						fieldValueAsString = string(data)
					}

					if fieldValueAsString != typ.ingestValue {
						r.querySuccess = false
						addError(fmt.Sprintf("field %s has unexpected value %v", fieldName, fieldValueAsString))
					}

				}
			}

			columns, err := a.FetchClickHouseColumns(ctx, indexName)
			columName := strings.ReplaceAll(fieldName, "-", "_")
			if err != nil {
				t.Fatalf("failed to fetch 'quesma_common_table' columns: %v", err)
			} else {
				if dbType, ok := columns[columName]; ok {
					r.dbStorage = "single column: " + dbType
				} else {
					r.dbStorage = "n/a"
					prefix := columName + "_"

					var cols []string
					for k, _ := range columns {
						if strings.HasPrefix(k, prefix) {
							cols = append(cols, k)
						}
					}
					if len(cols) > 0 {
						r.dbStorage = "columns: " + strings.Join(cols, ", ")
					}
				}
			}

			r.currentSupport = len(r.errors) == 0

			switch {

			case !r.claimedSupport && r.currentSupport:
				t.Log("Type not supported but works. What a surprise!")

			case r.claimedSupport && r.currentSupport:
				t.Log("Type supported and works. All good.")

			case !r.claimedSupport && !r.currentSupport:
				t.Skip("Type not supported and it doesn't work. Not great. Not terrible.")

			case r.claimedSupport && !r.currentSupport:
				t.Errorf("Type '%s' should be supported but is not: %v", r.name, r.errors)
			}

		})
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].name < results[j].name
	})

	fmt.Println("")
	// Create a new tabwriter
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)

	// Print table header
	fmt.Fprintf(w, "Name\tSupport\tCurrent Support\tPut Mapping\tIngest\tQuery\tStored as\t\n")
	fmt.Fprintf(w, "----\t-------\t---------------\t-----------\t------\t-----\t---------\t\n")

	// Print rows
	for _, res := range results {
		fmt.Fprintf(w, "%s\t%v\t%v\t%v\t%v\t%v\t%v\t\n",
			res.name, res.claimedSupport, res.currentSupport, res.putMappingSuccess, res.ingestSuccess, res.querySuccess, res.dbStorage)
	}

	// Flush the writer to output
	w.Flush()

	fmt.Println("")

	var failedTypes []string

	for _, r := range results {

		if r.claimedSupport && !r.currentSupport {
			failedTypes = append(failedTypes, r.name)
		}
		if len(r.errors) > 0 {
			fmt.Println("Type: ", r.name)
			fmt.Println("Errors: ", strings.Join(r.errors, ", "))
		}
	}
}
