package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"github.com/k0kubun/pp"
	"io"
	"os"
	"slices"
	"strings"
)

type jsonMap = map[string]any

const filename = "SIP_export.ndjson"

//const filename = "S1AP_export.ndjson"

//const filename = "S6a_export.ndjson"

var keysWithNestedJsonsAsStrings = []string{ /*"optionsJSON", */ "panelsJSON", "fieldAttrs"}
var interestingKeys = []string{"attributes", "match_phrase", "text", "formula", "query", "field", "sourceField", "index_pattern"}

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
	}
	return nil
}

func parseNdJson(s string, printDebug bool) []jsonMap {
	var jsons []jsonMap
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
	for k, v := range j {
		if strings.HasPrefix(k, "a") {
			pp.Println(k)
		}
		if slices.Contains(keysWithNestedJsonsAsStrings, k) {
			fmt.Println("--- processJson, in keysWithNestedJsonsAsStrings, key:", k, "val:", v)
			var ndJson []jsonMap
			if k == "panelsJSON" {
				ndJson = parseNdJson(v.(string), true)
			} else {
				ndJson = parseNdJson(v.(string), false)
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
				pp.Printf("ERROR: %s %T %v\n", k, v, v)
				errors = append(errors, fmt.Sprintf("ERROR: %s %T %v", k, v, v))
			}
		}
	}
	return nil
}

var formulas = make(map[string]struct{})
var sourceFields = make(map[string]struct{})
var queries = make(map[string]struct{})
var errors = make([]string, 0)

func processInteresting(key string, value, dataType any) {
	switch key {
	case "sourceField":
		processSourceField(key, value, dataType)
	case "formula", "text":
		processFormula(key, value, dataType)
	case "query":
		processQuery(key, value, dataType)
	case "attributes":
		fmt.Println("processJson from attributes")
		processJson(value.(jsonMap))
	default:
		fmt.Println("processInteresting, default case, key:", key, value, dataType)
	}
}

func processSourceField(key string, value, dataType any) {
	if _, ok := value.(string); !ok {
		pp.Println(key, value, dataType)
		errors = append(errors, fmt.Sprintf("sourceField is not a string: %v", value))
		return
	}

	dtIsNil := dataType == nil
	_, dtIsStr := dataType.(string)
	if !dtIsStr && !dtIsNil {
		pp.Println(key, value, dataType)
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
		pp.Println("processFormula key:", key, "value:", value, "dataType:", dataType)
		errors = append(errors, fmt.Sprintf("formula is not a string: %v", value))
		return
	}
	if dataType != nil {
		pp.Println("processFormula key:", key, "value:", value, "dataType:", dataType)
		errors = append(errors, fmt.Sprintf("dataType is not nil: %v", dataType))
		return
	}
	formulas[value.(string)] = struct{}{}
}

func processQuery(key string, value, dataType any) {
	pp.Println("--- processQuery, key:", key)
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
		pp.Println("processQuery key:", key, "value:", value, "dataType:", dataType)
		errors = append(errors, fmt.Sprintf("query is a map: %v", value))
	}

	if dataType != nil {
		pp.Println("processQuery key:", key, "value:", value, "dataType:", dataType)
		errors = append(errors, fmt.Sprintf("dataType is not nil: %v", dataType))
		return
	}

	if _, ok := value.(string); !ok {
		pp.Println("processQuery key:", key, "value:", value, "dataType:", dataType)
		errors = append(errors, fmt.Sprintf("query is not a string: %v", value))
		return
	}

	queries[value.(string)] = struct{}{}
}

func printSourceFields() {
	if len(sourceFields) > 0 {
		pp.Println("sourceFields:")
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

func main() {
	if err := scanOneFile(); err != nil {
		fmt.Println(err)
	} else if len(errors) > 0 {
		fmt.Println("Errors:", errors)
	} else {
		fmt.Println("Done, no error! :)")
		printSourceFields()
		printFormulas()
		printQueries()
	}
}
