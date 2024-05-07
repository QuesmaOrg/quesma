package quesma

import (
	"context"
	"github.com/stretchr/testify/assert"
	"mitmproxy/quesma/quesma/config"
	"mitmproxy/quesma/telemetry"
	"mitmproxy/quesma/tracing"
	"net/http"
	"testing"
)

func TestShouldExposePprof(t *testing.T) {
	quesma := NewQuesmaTcpProxy(telemetry.NoopPhoneHomeAgent(), config.QuesmaConfiguration{
		PublicTcpPort: 8080,
		Elasticsearch: config.ElasticsearchConfiguration{Url: &config.Url{}},
	}, make(<-chan tracing.LogWithLevel), false)
	quesma.Start()
	t.Cleanup(func() {
		quesma.Close(context.Background())
	})
	response, err := http.Get("http://localhost:9999/debug/pprof/")
	if err != nil {
		t.Fatal("could not reach /debug/pprof:", err)
	}

	assert.Equal(t, 200, response.StatusCode)
}
