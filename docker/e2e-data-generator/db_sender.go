// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"strings"
)

const (
	clickhouseUrl = "http://clickhouse:8123/"
	elasticUrl    = "http://elasticsearch_direct:9200/"
)

var httpClient = http.Client{}

// clickhouse

func sendCommandToClickhouse(cmd string) {
	req, err := http.NewRequest("POST", clickhouseUrl, bytes.NewBuffer([]byte(cmd)))
	if err != nil {
		panic(err)
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		response, err := io.ReadAll(resp.Body)
		if err != nil {
			panic(err)
		}
		panic(fmt.Sprintf("Status code: %d, Clickhouse error: %s", resp.StatusCode, string(response)))
	}
}

func insertToClickhouse(tableName string, rows []string) {
	sendCommandToClickhouse(fmt.Sprintf("INSERT INTO %s VALUES %s", tableName, strings.Join(rows, ",")))
}

// elastic

func createIndexElastic(indexName string) {
	req, err := http.NewRequest("PUT", elasticUrl+indexName, nil)
	if err != nil {
		panic(err)
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()

	response, err := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		panic("Elasticsearch error: " + string(response))
	}
}

func insertToElastic(indexName string, rows []string) {
	infoLine := fmt.Sprintf(`{"index": {"_index": "%s"}}`+"\n", indexName)
	bodybuilder := strings.Builder{}
	for _, row := range rows {
		bodybuilder.WriteString(infoLine)
		bodybuilder.WriteString(row + "\n")
	}

	req, err := http.NewRequest(
		"POST", fmt.Sprintf("%s_doc/_bulk", elasticUrl), strings.NewReader(bodybuilder.String()),
	)
	if err != nil {
		panic(err)
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := httpClient.Do(req)
	if err != nil {
		panic(err)
	}
	_ = resp.Body.Close()
}
