package quesma

import (
	"context"
	"github.com/stretchr/testify/require"
	"log"
	"mitmproxy/quesma/network"
	"mitmproxy/quesma/quesma/config"
	"net/http"
	"sync"
	"testing"
)

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

	instance := New(nil, ElasticUrl, "8081", config.QuesmaConfiguration{Mode: config.DualWriteQueryElastic, PublicTcpPort: network.Port(8080)}, make(<-chan string, 50000))
	_ = instance
	// TODO we have rewrite this test according to new architecture

	go func() {
		listener, err := instance.processor.(*dualWriteHttpProxy).listen()
		require.NoError(t, err)
		// below call will block on accept
		// and wait just for exactly one request
		// if more needed, duplicate of run in a loop
		in, err := listener.Accept()
		require.NoError(t, err)
		instance.handleRequest(in)
	}()
}
