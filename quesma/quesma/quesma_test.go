package quesma

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func sendRequest(url string, client *http.Client) (string, error) {
	req, err := http.NewRequest("GET", fmt.Sprintf("http://%s", url), http.NoBody)
	if err != nil {
		return "", err
	}
	req.SetBasicAuth("", "")
	req.Header.Set("Accept", "*/*")
	log.Println("Sending request " + "http://" + url)
	resp, err := client.Do(req)

	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	// read response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	return string(body), nil
}

func runReceiver(serverMux *http.ServeMux, addr string) {
	go func() {
		receiver := &http.Server{Addr: addr, Handler: serverMux}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		serverMux.HandleFunc("/shutdown", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(200)
			_, _ = w.Write([]byte("shutdown receiver"))
			cancel()
		})
		go func() {
			if err := receiver.ListenAndServe(); err != nil {
				log.Println("Receiver ListenAndServe:", err)
			}
		}()

		<-ctx.Done()
		_ = receiver.Shutdown(ctx)
	}()
}

const QUESMA_URL = "localhost:8080"
const ELASTIC_URL = "localhost:9201"
const CLICKHOUSE_URL = "localhost:8081"

func TestSuccessRequests(t *testing.T) {
	var Receiver1Response = "ReceiverBody1"

	serverMux1 := http.NewServeMux()
	serverMux1.HandleFunc("/Hello", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write([]byte(Receiver1Response))
	})
	runReceiver(serverMux1, ELASTIC_URL)

	serverMux2 := http.NewServeMux()
	serverMux2.HandleFunc("/Hello", func(w http.ResponseWriter, r *http.Request) {
		req := fmt.Sprintf("%s%s", r.Host, r.URL.Path)
		// check whether request path is equal origin
		// which should request send to quesma
		assert.Equal(t, req, QUESMA_URL+"/Hello", "should be the same")
	})
	runReceiver(serverMux2, CLICKHOUSE_URL)

	instance := New(nil, nil, ELASTIC_URL, "8080", "8081")
	go func() {
		instance.WaitForReadyToListen()
		log.Println("quesma ready to listen")
		client := &http.Client{Transport: &http.Transport{DisableCompression: true}}
		body, err := sendRequest(QUESMA_URL+"/Hello", client)
		assert.Equal(t, body, Receiver1Response)

		// TODO Wait for request processing completion, this is not reliable
		time.Sleep(time.Second * 1)

		_, _ = net.Dial("tcp", QUESMA_URL)
		instance.finishChannel <- struct{}{}
		close(instance.finishChannel)
		require.NoError(t, err)
		body, err = sendRequest(ELASTIC_URL+"/shutdown", client)
		require.NoError(t, err)
		assert.Equal(t, body, "shutdown receiver")
		body, err = sendRequest(CLICKHOUSE_URL+"/shutdown", client)
		require.NoError(t, err)
		assert.Equal(t, body, "shutdown receiver")
	}()
	instance.Start()
}
