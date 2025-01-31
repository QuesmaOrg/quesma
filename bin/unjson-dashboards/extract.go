// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"github.com/k0kubun/pp"
	"io"
	"log"
	"os"
	"path"
	"slices"
	"sort"
	"strings"
)

type jsonMap = map[string]any

var filename = "" // copy ndjson to this directory and enter filename here, e.g. "file.ndjson"

type fieldAttrsResult struct {
	title      string
	name       string
	fieldAttrs jsonMap
}

func (f *fieldAttrsResult) Equal(other *fieldAttrsResult) bool {
	if f.title != other.title || f.name != other.name || len(f.fieldAttrs) != len(other.fieldAttrs) {
		return false
	}
	for k := range f.fieldAttrs { // we only compare keys
		if _, ok := other.fieldAttrs[k]; !ok {
			return false
		}
	}
	return true
}

var fieldAttrsResults []fieldAttrsResult

var allFieldValues = make(map[string]map[string]bool)

var keysWithNestedJsonsAsStrings = []string{ /*"optionsJSON", */ "panelsJSON", "fieldAttrs"}
var interestingKeys = []string{"attributes", "match_phrase", "text", "formula", "query", "field", "sourceField", "index_pattern", "fieldAttrs", "title", "name"}

var formulas = make(map[string]struct{})
var sourceFields = make(map[string]struct{})
var queries = make(map[string]struct{})
var errors = make([]string, 0)

func scanOneFile() error {
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		var j jsonMap
		if err = json.Unmarshal([]byte(line), &j); err != nil {
			return err
		}
		if err = processJson(j); err != nil {
			return err
		}

		fieldValues := guessValues([]byte(line))

		for field, values := range fieldValues {

			if allFieldValues[field] == nil {
				allFieldValues[field] = make(map[string]bool)
			}

			for value, _ := range values {
				allFieldValues[field][value] = true
			}
		}
	}
	return nil
}

func parseNdJson(s string) []jsonMap {
	var jsons []jsonMap
	var printDebug = false
	d := json.NewDecoder(strings.NewReader(s))
	for {
		// Decode one JSON document.
		var v any
		err := d.Decode(&v)

		if err != nil {
			// io.EOF is expected at end of stream.
			if err != io.EOF {
				fmt.Println("Error decoding JSON: ", err)
			}
			break
		}

		// Do something with the value.
		// fmt.Println(v)
		//for k, v := range v.([]any) {
		//	fmt.Println(k, v)
		//}
		switch vv := v.(type) {
		case jsonMap:
			if printDebug {
				fmt.Println("parseNdJson jsonMap:", vv)
			}
			jsons = append(jsons, vv)
		case []any:
			if printDebug {
				pp.Println("parseNdJson []any", vv)
			}
			for _, vvv := range vv {
				if j, ok := vvv.(jsonMap); ok {
					jsons = append(jsons, j)
				}
			}
		}
	}
	return jsons
}

func processJson(j jsonMap) error {
	fieldAttrs, hasFieldAttrs := j["fieldAttrs"]
	title, hasTitle := j["title"].(string)
	name, hasName := j["name"].(string)

	if hasFieldAttrs {
		/*
			if hasTitle || hasName {
				pp.Printf("====== VIP processJson ======\n===, title: %v, name: %v, fieldAttrs:\n%v", title, name, fieldAttrs)
			} else {
				pp.Println("====== VIP processJson, fieldAttrs:", fieldAttrs)
			}

		*/
		var dict map[string]interface{}
		err := json.Unmarshal([]byte(fieldAttrs.(string)), &dict)
		if err != nil {
			panic(err)
		}
		var keys []string
		for k := range dict {
			keys = append(keys, k)
		}
		slices.Sort(keys)

		/*
			for _, k := range keys {
				fmt.Println("-", k, ",", dict[k])
			}
		*/

		thisTableResult := fieldAttrsResult{fieldAttrs: dict}
		if hasTitle {
			thisTableResult.title = title
		}
		if hasName {
			thisTableResult.name = name
		}
		if len(fieldAttrsResults) == 0 || !fieldAttrsResults[len(fieldAttrsResults)-1].Equal(&thisTableResult) {
			fieldAttrsResults = append(fieldAttrsResults, thisTableResult)
		}
	}

	for k, v := range j {

		if slices.Contains(keysWithNestedJsonsAsStrings, k) {
			//fmt.Println("--- processJson, in keysWithNestedJsonsAsStrings, key:", k, "val:", v)
			var ndJson []jsonMap
			if k == "panelsJSON" {
				ndJson = parseNdJson(v.(string))
			} else {
				ndJson = parseNdJson(v.(string))
			}
			for _, js := range ndJson {
				// pp.Println(js)
				if err := processJson(js); err != nil {
					return err
				}
			}
		}
		if slices.Contains(interestingKeys, k) {
			dataType := j["dataType"]
			if _, ok := v.(string); ok {
				if v.(string) != "" {
					processInteresting(k, v, dataType)
				}
			} else {
				processInteresting(k, v, dataType)
			}
		}
		if nestedJson, ok := v.(jsonMap); ok {
			// fmt.Println(k, "hoho")
			if err := processJson(nestedJson); err != nil {
				return err
			}
		} else if va, ok := v.([]any); ok {
			for vaa := range va {
				err := processJson(jsonMap{"k_nested": vaa})
				if err != nil {
					return err
				}
			}
		} else {
			switch v.(type) {
			case string, int, float64, bool:
			default:
				//pp.Printf("ERROR: %s %T %v\n", k, v, v)
				errors = append(errors, fmt.Sprintf("ERROR: %s %T %v", k, v, v))
			}
		}
	}
	return nil
}

func processInteresting(key string, value, dataType any) {
	switch key {
	case "sourceField":
		processSourceField(key, value, dataType)
	case "formula", "text":
		processFormula(key, value, dataType)
	case "query":
		processQuery(key, value, dataType)
	case "attributes":
		//fmt.Println("processJson from attributes")
		processJson(value.(jsonMap))
	default:
		//fmt.Println("processInteresting, default case, key:", key, value, dataType)
	}
}

func processSourceField(key string, value, dataType any) {
	if _, ok := value.(string); !ok {
		//pp.Println(key, value, dataType)
		errors = append(errors, fmt.Sprintf("sourceField is not a string: %v", value))
		return
	}

	dtIsNil := dataType == nil
	_, dtIsStr := dataType.(string)
	if !dtIsStr && !dtIsNil {
		//pp.Println(key, value, dataType)
		errors = append(errors, fmt.Sprintf("dataType is not a string: %v", dataType))
		return
	}
	dtString := ""
	if dtIsStr && dataType.(string) != "" {
		dtString = "|" + dataType.(string)
	}
	sourceFields[value.(string)+dtString] = struct{}{}
}

func processFormula(key string, value, dataType any) {
	if _, ok := value.(string); !ok {
		//pp.Println("processFormula key:", key, "value:", value, "dataType:", dataType)
		errors = append(errors, fmt.Sprintf("formula is not a string: %v", value))
		return
	}
	if dataType != nil {
		//pp.Println("processFormula key:", key, "value:", value, "dataType:", dataType)
		errors = append(errors, fmt.Sprintf("dataType is not nil: %v", dataType))
		return
	}
	formulas[value.(string)] = struct{}{}
}

func processQuery(key string, value, dataType any) {
	//pp.Println("--- processQuery, key:", key)
	if valueAsMap, ok := value.(jsonMap); ok {
		// we skip <=> len == 2, and there are 2 keys: 'query' == "", and 'language'
		weCanSkip := len(valueAsMap) == 2
		if query, exists := valueAsMap["query"]; !exists || query.(string) != "" {
			weCanSkip = false
		}
		if _, exists := valueAsMap["language"]; !exists {
			weCanSkip = false
		}
		if weCanSkip {
			return
		}
		//pp.Println("processQuery key:", key, "value:", value, "dataType:", dataType)
		errors = append(errors, fmt.Sprintf("query is a map: %v", value))
	}

	if dataType != nil {
		//pp.Println("processQuery key:", key, "value:", value, "dataType:", dataType)
		errors = append(errors, fmt.Sprintf("dataType is not nil: %v", dataType))
		return
	}

	if _, ok := value.(string); !ok {
		//pp.Println("processQuery key:", key, "value:", value, "dataType:", dataType)
		errors = append(errors, fmt.Sprintf("query is not a string: %v", value))
		return
	}

	queries[value.(string)] = struct{}{}
}

func printSourceFields() {
	if len(sourceFields) > 0 {
		//pp.Println("sourceFields:")
		for k := range sourceFields {
			sf := strings.Split(k, "|")
			switch len(sf) {
			case 1:
				fmt.Printf("  * %s\n", k)
			case 2:
				fmt.Printf("  * %s\t%s\n", sf[0], sf[1])
			default:
				pp.Println("ERROR\n")
			}
		}
	}
}

func printFormulas() {
	if len(formulas) > 0 {
		pp.Println("formulas:")
		for k := range formulas {
			fmt.Printf("  * %s\n", k)
		}
	}
}

func printQueries() {
	if len(formulas) > 0 {
		pp.Println("queries:")
		for k := range formulas {
			fmt.Printf("  * %s\n", k)
		}
	}
}

func printFieldAttrsResults(printAlsoValue bool) {
	fmt.Println("Printing fieldAttrs results:")
	if len(fieldAttrsResults) > 0 {
		pp.Println("fieldAttrsResults:, len:", len(fieldAttrsResults), "First 10:")

		var a []fieldAttrsResult
		if len(fieldAttrsResults) > 10 {
			a = fieldAttrsResults[:10]
		} else {
			a = fieldAttrsResults
		}

		for i, res := range a {
			fmt.Printf("%d. name: %v title: %v\n", i+1, res.name, res.title)

			var keys []string
			for k := range res.fieldAttrs {
				keys = append(keys, k)
			}
			slices.Sort(keys)

			for _, k := range keys {
				Type := "String"

				if k == "@timestamp" || k == "timestamp" || k == "date_from" || k == "date_to" {
					Type = "DateTime64(3)"
				}

				if printAlsoValue {
					fmt.Printf(`- "%s": %v`, k, res.fieldAttrs[k])
				} else {
					fmt.Printf(`"%s" Nullable(%s),`, k, Type)
				}
				fmt.Println()
			}
			fmt.Printf("\n\n")
		}
	}
}

type extractedIndexField struct {
	name     string
	dataType string
	values   []string
}
type extractedIndex struct {
	name    string
	pattern string
	fields  map[string]*extractedIndexField
}

var extractedIndexes = make(map[string]*extractedIndex)

func guessElasticsearchType(field string) string {
	lowerField := strings.ToLower(field)

	switch {
	case strings.Contains(lowerField, "timestamp") || strings.Contains(lowerField, "date"):
		return "date"
	case strings.HasSuffix(lowerField, "id") || strings.HasSuffix(lowerField, "count") || strings.HasSuffix(lowerField, "age") || strings.HasSuffix(lowerField, "code"):
		return "long"
	case strings.Contains(lowerField, "price") || strings.Contains(lowerField, "score") || strings.Contains(lowerField, "percent") || strings.Contains(lowerField, "ratio"):
		return "double"
	case strings.HasPrefix(lowerField, "is_") || strings.HasPrefix(lowerField, "has_") || strings.Contains(lowerField, "enabled") || strings.Contains(lowerField, "valid"):
		return "boolean"
	case strings.Contains(lowerField, "latitude") || strings.Contains(lowerField, "longitude"):
		return "float"
	case strings.Contains(lowerField, "message") || strings.Contains(lowerField, "description"):
		return "text"
	default:
		return "keyword"
	}
}

func process() {

	log.Println("results: ", len(fieldAttrsResults))

	if len(fieldAttrsResults) > 0 {

		for _, res := range fieldAttrsResults {
			//fmt.Printf("%d. name: %v title: %v\n", i+1, res.name, res.title)

			if res.name == "" {
				continue
			}

			name := res.name

			if strings.HasSuffix(name, "*") {
				name = strings.TrimSuffix(name, "*")
				name = name + "-1"
			}

			idx, ok := extractedIndexes[name]
			if !ok {
				idx = &extractedIndex{name: name, fields: make(map[string]*extractedIndexField)}
				idx.pattern = res.name
				extractedIndexes[name] = idx
			}

			var keys []string
			for k := range res.fieldAttrs {
				keys = append(keys, k)
			}
			slices.Sort(keys)

			for _, k := range keys {

				if _, ok := idx.fields[k]; ok {
					continue
				}

				elasticType := guessElasticsearchType(k)

				idx.fields[k] = &extractedIndexField{name: k, dataType: elasticType}

				for value := range allFieldValues[k] {
					idx.fields[k].values = append(idx.fields[k].values, value)
				}

			}
		}
	}
}

func toMappings(idx extractedIndex) IndexMappings {
	m := IndexMappings{Properties: make(map[string]IndexMappingsField), Name: idx.name, Pattern: idx.pattern}
	for _, field := range idx.fields {
		m.Properties[field.name] = IndexMappingsField{Type: field.dataType, SampleValues: field.values}
	}
	return m
}

func extractMappings() {

	inputDir := "dashboards"

	log.Println("Starting...")
	entries, err := os.ReadDir(inputDir)
	if err != nil {
		log.Fatal(err)
	}

	for _, entry := range entries {

		if entry.IsDir() {
			continue
		}

		if !strings.HasSuffix(entry.Name(), ".ndjson") {
			continue
		}

		fieldAttrsResults = make([]fieldAttrsResult, 0)
		filename = path.Join(inputDir, entry.Name())

		log.Println("Processing", entry.Name())

		if err := scanOneFile(); err != nil {
			fmt.Println(err)
		}
		process()

	}

	fieldNames := make(map[string]bool)
	for _, idx := range extractedIndexes {
		for _, field := range idx.fields {
			fieldNames[field.name] = true
		}
	}

	fieldNamesAsSlice := make([]string, 0, len(fieldNames))
	for k := range fieldNames {
		fieldNamesAsSlice = append(fieldNamesAsSlice, k)
	}
	sort.Strings(fieldNamesAsSlice)
	fmt.Println("Fields:\n", fieldNamesAsSlice)

	//printSourceFields()
	//printFormulas()
	//printQueries()
	//printFieldAttrsResults(true)
	//printFieldAttrsResults(false)

	count := 0
	for _, idx := range extractedIndexes {
		log.Println(count, "-", idx.name, len(idx.fields))

		count++
		mappings := toMappings(*idx)
		b, err := json.MarshalIndent(mappings, "", "  ")
		if err != nil {
			log.Fatal(err)
		}
		out := fmt.Sprintf("%s/%s.json", "mappings", idx.name)
		err = os.WriteFile(out, b, 0644)
		if err != nil {
			log.Println("Error writing file:", err)
		}
	}

	if len(errors) > 0 {
		//pp.Println("Errors:", errors)
	} else {
		fmt.Println("Done, no error! :)")
	}

}
