package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"mitmproxy/quesma/clickhouse"
	"net"
	"net/http"
	"os"
	"strings"
)

const tcpPortEnv = "TCP_PORT"
const targetEnv = "ELASTIC_URL"

const internalHttpPort = "8081"

var tcpPort = os.Getenv(tcpPortEnv)

type Quesma struct {
	server       *http.Server
	logManager   *clickhouse.LogManager
	tableManager *clickhouse.TableManager
	targetUrl    string
}

func New(tableManager *clickhouse.TableManager, logManager *clickhouse.LogManager, target string) *Quesma {
	return &Quesma{
		tableManager: tableManager,
		logManager:   logManager,
		targetUrl:    target,
		server: &http.Server{
			Addr: ":" + internalHttpPort,
			Handler: http.HandlerFunc(func(writer http.ResponseWriter, r *http.Request) {
				if r.Method == "POST" {
					body, err := io.ReadAll(r.Body)
					if err != nil {
						log.Fatal(err)
					}
					r.Body = io.NopCloser(bytes.NewBuffer(body))
					go dualWrite(r.RequestURI, string(body), logManager)
				}
			}),
		},
	}
}

func (q *Quesma) close(ctx context.Context) {
	defer q.logManager.Close()
	if err := q.server.Shutdown(ctx); err != nil {
		log.Fatal("Error during server shutdown:", err)
	}
}

func (q *Quesma) start() {
	q.tableManager.Migrate()
	go func() {
		if err := q.server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatal("Error starting server:", err)
		}
	}()

	fmt.Printf("listening TCP at %s\n", tcpPort)
	listener, err := net.Listen("tcp", ":"+tcpPort)
	if err != nil {
		println(err)
	}

	go func() {
		for {
			in, err := listener.Accept()
			log.Println("New connection from: ", in.RemoteAddr())
			if err != nil {
				log.Println("error accepting connection", err)
				continue
			}
			go func() {
				defer in.Close()
				elkConnection, err := net.Dial("tcp", q.targetUrl)
				if err != nil {
					log.Println("error dialing primary addr", err)
					return
				}
				internalHttpServerConnection, err := net.Dial("tcp", ":"+internalHttpPort)
				if err != nil {
					log.Println("error dialing secondary addr", err)
					return
				}
				defer elkConnection.Close()
				defer internalHttpServerConnection.Close()

				signal := make(chan struct{}, 2)
				go copy(signal, io.MultiWriter(elkConnection, internalHttpServerConnection), in)
				go copy(signal, in, elkConnection)
				<-signal
				log.Println("Connection complete", in.RemoteAddr())
			}()
		}
	}()

}

func dualWrite(url string, body string, lm *clickhouse.LogManager) {
	if strings.Contains(url, "/_bulk") {
		fmt.Printf("%s --> clickhouse\n", url)
		for _, op := range strings.Fields(body) {
			fmt.Printf("  --> clickhouse, body: %s\n", op)
		}
	} else if strings.Contains(url, "/logs-generic-default/_doc") {
		lm.Insert(body)
		fmt.Printf("%s --> clickhouse, body: %s\n", url, body)
	} else {
		fmt.Printf("%s --> pass-through\n", url)
	}
}
