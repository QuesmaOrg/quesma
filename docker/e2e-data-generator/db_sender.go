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
	if err != nil || resp.StatusCode != 200 {
		panic(err)
	}
	_ = resp.Body.Close()
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
	response, err := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 && !(resp.StatusCode == 400 && strings.Contains(string(response), "resource_already_exists_exception")) {
		panic("Elasticsearch error: " + string(response))
	}

	_ = resp.Body.Close()
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
