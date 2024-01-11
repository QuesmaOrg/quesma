package quesma

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

func sendRequest(addr string, client *http.Client, url string) error {
	req, err := http.NewRequest("GET", fmt.Sprintf("http://%s/%s", addr, url), http.NoBody)
	if err != nil {
		return err
	}
	req.SetBasicAuth("", "")
	req.Header.Set("Accept", "*/*")

	resp, err := client.Do(req)

	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// read response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	// print response body
	log.Println(string(body))
	return nil
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

func TestSuccessRequests(t *testing.T) {
	serverMux1 := http.NewServeMux()
	serverMux1.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write([]byte("ReceiverBody"))
	})
	runReceiver(serverMux1, "localhost:9201")

	serverMux2 := http.NewServeMux()
	serverMux2.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write([]byte("ReceiverBody"))
	})
	runReceiver(serverMux2, "localhost:8081")
	instance := New(nil, nil, "localhost:8081", "8080", "8081")
	go func() {
		instance.WaitForReadyToListen()
		log.Println("quesma ready to listen")
		client := &http.Client{Transport: &http.Transport{DisableCompression: true}}
		err := sendRequest("localhost:9201", client, "/")
		_, _ = net.Dial("tcp", "localhost:8080")
		instance.finishChannel <- struct{}{}
		close(instance.finishChannel)
		require.NoError(t, err)
		err = sendRequest("localhost:9201", client, "/shutdown")
		require.NoError(t, err)
		_ = sendRequest("localhost:8081", client, "/shutdown")
	}()
	instance.Start()
}
