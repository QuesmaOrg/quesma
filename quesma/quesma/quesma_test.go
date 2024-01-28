package quesma

import (
	"context"
	"fmt"
	"io"
	"log"
	"mitmproxy/quesma/quesma/config"
	"net/http"
	"net/url"
	"sync"
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

func runReceiver(serverMux *http.ServeMux, shutdownWG *sync.WaitGroup, addr string) {
	go func() {
		receiver := &http.Server{Addr: addr, Handler: serverMux}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		serverMux.HandleFunc("/shutdown", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(200)
			_, _ = w.Write([]byte("shutdown receiver"))
			log.Println("Shutdown receiver:" + addr)
			cancel()
		})
		go func() {
			log.Println("Calling receiver.ListenAndServe()...")
			if err := receiver.ListenAndServe(); err != nil {
				log.Println("Receiver ListenAndServe:", err)
				shutdownWG.Done()
			}
		}()

		<-ctx.Done()
		_ = receiver.Shutdown(ctx)
	}()
}

const (
	QuesmaUrl  = "localhost:8080"
	ElasticUrl = "localhost:9201"
)

func TestSuccessRequests(t *testing.T) {
	if testing.Short() {
		t.Skip("this test takes ~25sec to complete, skipping in short mode")
	}

	var Receiver1Response = "ReceiverBody1"
	var wg sync.WaitGroup
	wg.Add(1)

	serverMux1 := http.NewServeMux()
	serverMux1.HandleFunc("/Hello", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		_, err := w.Write([]byte(Receiver1Response))
		require.NoError(t, err)
	})
	runReceiver(serverMux1, &wg, ElasticUrl)

	instance := New(nil, ElasticUrl, "8080", "8081", config.QuesmaConfiguration{Mode: config.DualWriteQueryElastic})

	go func() {
		listener, err := instance.listen()
		require.NoError(t, err)
		// below call will block on accept
		// and wait just for exactly one request
		// if more needed, duplicate of run in a loop
		in, err := listener.Accept()
		require.NoError(t, err)
		instance.handleRequest(in)
	}()

	log.Println("quesma ready to listen")
	client := &http.Client{Transport: &http.Transport{DisableCompression: true}}
	waitForHealthy()
	body, err := sendRequest(QuesmaUrl+"/Hello", client)
	assert.Equal(t, Receiver1Response, body)
	require.NoError(t, err)

	body, err = sendRequest(ElasticUrl+"/shutdown", client)
	require.NoError(t, err)
	assert.Equal(t, "shutdown receiver", body)
	wg.Wait()
}

func waitForHealthy() {
	fmt.Println("waiting for http server...")
	client := &http.Client{Transport: &http.Transport{DisableCompression: true}}
	retries := 0
	for retries < 5 {
		time.Sleep(5 * time.Second)
		request := http.Request{URL: &url.URL{Scheme: "http", Host: QuesmaUrl, Fragment: HealthPath}, Method: "GET"}
		body, err := client.Do(&request)
		if err == nil && body.StatusCode == 200 {
			return
		}
		retries++
	}
}
