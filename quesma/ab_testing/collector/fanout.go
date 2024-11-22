// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package collector

import (
	"bytes"
	"context"
	"github.com/goccy/go-json"
	"fmt"
	"net/http"
	"quesma/ingest"
	"quesma/logger"
	"quesma/quesma/types"
)

type elasticSearchFanout struct {
	url        string
	indexName  string
	errorCount int
}

func (t *elasticSearchFanout) name() string {
	return "elasticSearchFanout"
}

func (t *elasticSearchFanout) process(in EnrichedResults) (out EnrichedResults, drop bool, err error) {

	// TODO collect and send in bulk every 10 seconds or 1000 records for example

	logBytes := []byte{}

	bulkJson := fmt.Sprintf("{\"index\":{\"_index\":\"%s\"}}\n", t.indexName)

	logBytes = append(logBytes, []byte(bulkJson)...)
	logBytes = append(logBytes, []byte("\n")...)

	logLine, err := json.Marshal(in)
	if err != nil {
		logger.Error().Msgf("failed to marshal A/B results line: %v", err)
		return in, false, err
	}

	logBytes = append(logBytes, logLine...)
	logBytes = append(logBytes, []byte("\n")...)

	if resp, err := http.Post(t.url+"/_bulk", "application/json", bytes.NewBuffer(logBytes)); err != nil {
		t.errorCount += +1
		return in, false, fmt.Errorf("failed to send A/B results: %v", err)
	} else {
		if err := resp.Body.Close(); err != nil {
			t.errorCount += +1
			return in, false, fmt.Errorf("failed to close response body: %v", err)
		}
		if resp.StatusCode != http.StatusOK {
			t.errorCount += +1
			return in, false, fmt.Errorf("failed to send A/B results: %v", resp.Status)
		}
	}

	// Elasticsearch logic here
	return in, false, nil
}

type internalIngestFanout struct {
	indexName       string
	ingestProcessor ingest.Ingester
}

func (t *internalIngestFanout) name() string {
	return "internalIngestFanout"
}

func (t *internalIngestFanout) process(in EnrichedResults) (out EnrichedResults, drop bool, err error) {

	asBytes, err := json.Marshal(in)
	if err != nil {
		logger.Error().Msgf("failed to marshal A/B results line: %v", err)
		return in, false, err
	}

	asJson, err := types.ParseJSON(string(asBytes))

	if err != nil {
		logger.Error().Msgf("failed to parse A/B results line: %v", err)
		return
	}

	err = t.ingestProcessor.Ingest(context.Background(), t.indexName, []types.JSON{asJson})

	return in, false, err
}

var _ = &internalIngestFanout{}
var _ = &elasticSearchFanout{}
