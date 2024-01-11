package quesma

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"testing"

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
			if err := receiver.ListenAndServe(); err != nil {
				log.Println("Receiver ListenAndServe:", err)
				shutdownWG.Done()
			}
		}()

		<-ctx.Done()
		_ = receiver.Shutdown(ctx)
	}()
}

const QUESMA_URL = "localhost:8080"
const ELASTIC_URL = "localhost:9201"

func TestSuccessRequests(t *testing.T) {
	var Receiver1Response = "ReceiverBody1"
	var wg sync.WaitGroup
	wg.Add(1)

	serverMux1 := http.NewServeMux()
	serverMux1.HandleFunc("/Hello", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		_, err := w.Write([]byte(Receiver1Response))
		require.NoError(t, err)
	})
	runReceiver(serverMux1, &wg, ELASTIC_URL)

	instance := New(nil, nil, ELASTIC_URL, "8080", "8081")

	go func() {
		listener, err := instance.listenTCP()
		require.NoError(t, err)
		go instance.listenHTTP()
		// below call will block on accept
		// and wait just for exactly one request
		// if more needed, duplicate of run in a loop
		in, err := listener.Accept()
		require.NoError(t, err)
		instance.handleRequest(in)
	}()

	log.Println("quesma ready to listen")
	client := &http.Client{Transport: &http.Transport{DisableCompression: true}}

	body, err := sendRequest(QUESMA_URL+"/Hello", client)
	assert.Equal(t, Receiver1Response, body)
	require.NoError(t, err)

	body, err = sendRequest(ELASTIC_URL+"/shutdown", client)
	require.NoError(t, err)
	assert.Equal(t, "shutdown receiver", body)
	wg.Wait()

}
