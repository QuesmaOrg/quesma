// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package quesma

import (
	"context"
	"fmt"
	"github.com/QuesmaOrg/quesma/quesma/logger"
	"github.com/QuesmaOrg/quesma/quesma/quesma/config"
	"github.com/stretchr/testify/assert"
	"net/http"
	"testing"
	"time"
)

func TestShouldExposePprof(t *testing.T) {

	t.Skip("FIXME @pivovarit: this test is flaky, it should be fixed")

	quesma := NewQuesmaTcpProxy(&config.QuesmaConfiguration{
		PublicTcpPort: 8080,
		Elasticsearch: config.ElasticsearchConfiguration{Url: &config.Url{}},
	}, nil, make(<-chan logger.LogWithLevel), false)
	quesma.Start()
	waitForHealthyQuesma(t)
	t.Cleanup(func() {
		quesma.Close(context.Background())
	})
	response, err := http.Get("http://localhost:9999/debug/pprof/")
	if err != nil {

		t.Fatal("could not reach /debug/pprof:", err)
	}

	assert.Equal(t, 200, response.StatusCode)
}

func waitForHealthyQuesma(t *testing.T) {
	for i := 0; i < 10; i++ {
		resp, err := http.Get("http://localhost:9999/_quesma/health")
		if err != nil {
			fmt.Println("Error, retrying:", err)
			time.Sleep(1 * time.Second)
			continue
		}
		defer resp.Body.Close()

		if resp.StatusCode == http.StatusOK {
			return
		}

		if i == 9 {
			t.Fatal("quesma failed healthcheck", err)
		}
	}
}
