package repository

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/k0kubun/pp"
	"math/rand"
	"net/http"
	"quesma/logger"
)

type ppPrintFanout struct {
}

func (t *ppPrintFanout) process(in Data) (out Data, drop bool, err error) {
	pp.Println("A/B Testing FANOUT", in)
	return in, false, nil
}

type elasticSearchFanout struct {
	url        string
	indexName  string
	errorCount int
}

func (t *elasticSearchFanout) process(in Data) (out Data, drop bool, err error) {

	// add some fail rate
	if rand.Float64() < 0.1 {
		return in, true, nil
	}

	// add real bulk logic here

	logBytes := []byte{}

	bulkJson := fmt.Sprintf("{\"index\":{\"_index\":\"%s\"}}\n", t.indexName)

	logBytes = append(logBytes, []byte(bulkJson)...)
	logBytes = append(logBytes, []byte("\n")...)

	logLine, err := json.Marshal(in)
	if err != nil {
		logger.Error().Msgf("Failed to marshal A/B results line: %v", err)
		return in, false, err
	}

	logBytes = append(logBytes, logLine...)
	logBytes = append(logBytes, []byte("\n")...)

	if resp, err := http.Post(t.url+"/_bulk", "application/json", bytes.NewBuffer(logBytes)); err != nil {
		t.errorCount += +1
		return in, false, fmt.Errorf("Failed to send A/B results: %v", err)
	} else {
		if err := resp.Body.Close(); err != nil {
			t.errorCount += +1
			return in, false, fmt.Errorf("Failed to close response body: %v", err)
		}
		if resp.StatusCode != http.StatusOK {
			t.errorCount += +1
			return in, false, fmt.Errorf("Failed to send A/B results: %v", resp.Status)
		}
	}

	// Elasticsearch logic here
	return in, false, nil
}
