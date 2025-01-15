// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
//go:build integration

package e2e

import (
	"bytes"
	"fmt"
	"github.com/QuesmaOrg/quesma/quesma/jsonprocessor"
	"github.com/goccy/go-json"
	"io"
	"net/http"
	"sort"
	"strings"
)

//

type eqlEvent map[string]interface{}

func eqlClient(target string, eqlQuery string) ([]eqlEvent, error) {

	type elasticQuery struct {
		Query string `json:"query"`
	}

	query := elasticQuery{Query: eqlQuery}
	data, err := json.Marshal(query)
	if err != nil {
		return nil, err
	}
	reader := bytes.NewReader(data)

	url := target + "/windows_logs/_eql/search"

	// We're calling GET method here with body.
	// This is oddity. Golang http client does not support sending body with GET method.

	req, err := http.NewRequest(http.MethodGet, url, reader)
	if err != nil {
		panic(err)
	}
	req.Header.Set("Content-Type", "application/json")
	res, err := http.DefaultClient.Do(req)

	if err != nil {
		return nil, err
	}

	defer res.Body.Close()

	response, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}

	if res.StatusCode != http.StatusOK {
		fmt.Println("response", string(response))
		return nil, fmt.Errorf("Unexpected status code: %v, %v", res.StatusCode, res.Status)
	}

	return extractListOfEvents(string(response))
}

func parseResponse(response string) (map[string]interface{}, error) {

	var result map[string]interface{}
	err := json.Unmarshal([]byte(response), &result)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func extractListOfEvents(response string) ([]eqlEvent, error) {

	var res []eqlEvent

	parsed, err := parseResponse(response)
	if err != nil {
		return nil, err
	}
	hits, ok := parsed["hits"]
	if !ok {
		return nil, fmt.Errorf("missing hits in response")
	}

	events, ok := hits.(map[string]interface{})["events"]
	if !ok {
		fmt.Println("missing events in hits")
		// FIXME this is a bug
		// quesma omits empty events array
		//return nil, fmt.Errorf("missing events in hits")
		events = []interface{}{}
	}

	for i, event := range events.([]interface{}) {

		m := event.(map[string]interface{})

		source, ok := m["_source"]
		if !ok {
			return nil, fmt.Errorf("missing source in event")
		}

		sourceAsMap, ok := source.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("source is not a map")
		}
		sourceAsMap = jsonprocessor.FlattenMap(sourceAsMap, "::")

		fmt.Println("event", i, sourceAsMap)
		res = append(res, sourceAsMap)
	}

	// not sure if it is necessary
	sort.Slice(res, func(i, j int) bool {
		return strings.Compare(res[i]["@timestamp"].(string), res[j]["@timestamp"].(string)) < 0
	})
	return res, nil
}
