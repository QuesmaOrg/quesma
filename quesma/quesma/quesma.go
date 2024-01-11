package quesma

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
	"strconv"
	"strings"
)

type Quesma struct {
	server       *http.Server
	logManager   *clickhouse.LogManager
	tableManager *clickhouse.TableManager
	targetUrl    string
	tcpPort      string
	httpPort     string
}

func New(tableManager *clickhouse.TableManager,
	logManager *clickhouse.LogManager, target string, tcpPort string, httpPort string) *Quesma {
	q := &Quesma{
		tableManager: tableManager,
		logManager:   logManager,
		targetUrl:    target,
		tcpPort:      tcpPort,
		httpPort:     httpPort,
		server: &http.Server{
			Addr: ":" + httpPort,
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
	return q
}

func (q *Quesma) Close(ctx context.Context) {
	if q.logManager != nil {
		defer q.logManager.Close()
	}
	if err := q.server.Shutdown(ctx); err != nil {
		log.Fatal("Error during server shutdown:", err)
	}
}

func (q *Quesma) listenTCP() (net.Listener, error) {
	fmt.Printf("listening TCP at %s\n", q.tcpPort)
	listener, err := net.Listen("tcp", ":"+q.tcpPort)
	if err != nil {
		return nil, err
	}
	return listener, nil
}

func (q *Quesma) handleRequest(in net.Conn) {
	defer in.Close()
	elkConnection, err := net.Dial("tcp", q.targetUrl)
	log.Println("elkConnection:" + q.targetUrl)
	if err != nil {
		log.Println("error dialing primary addr", err)
		return
	}

	internalHttpServerConnection, err := net.Dial("tcp", ":"+q.httpPort)
	log.Println("internalHttpServerConnection:" + q.httpPort)
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
}

func (q *Quesma) listenHTTP() {
	if err := q.server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal("Error starting server:", err)
	}
}

func (q *Quesma) Start() {
	if q.tableManager != nil {
		q.tableManager.Migrate()
	}
	go q.listenHTTP()
	listener, err := q.listenTCP()
	if err != nil {
		log.Println(err)
		return
	}

	for {
		in, err := listener.Accept()
		log.Println("New connection from: ", in.RemoteAddr())
		if err != nil {
			log.Println("error accepting connection", err)
			continue
		}
		go q.handleRequest(in)
	}
}

func dualWrite(url string, body string, lm *clickhouse.LogManager) {
	// to make logs more readable by truncating very long request bodies
	firstNChars := func(s string, n int) string {
		if len(s) > n {
			return s[:n]
		}
		return s
	}

	if strings.Contains(url, "bulk") || strings.Contains(url, "/_doc") {
		fmt.Printf("%s  --> clickhouse, body: %s\n", url, firstNChars(body, 34))
		jsons := strings.Split(body, "\n")
		for i, singleJson := range jsons {
			if len(singleJson) == 0 {
				continue
			}
			tableName := url
			if len(jsons) > 1 {
				tableName += "_" + strconv.Itoa(i+1)
			}
			// very unnecessary trying to create tables with every request
			// We can improve this later if needed
			err := lm.CreateTable(tableName, singleJson)
			if err != nil {
				log.Fatal(err)
			}
			err = lm.Insert(tableName, singleJson)
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
			}
		}
	} else {
		fmt.Printf("%s --> pass-through\n", url)
	}
}
