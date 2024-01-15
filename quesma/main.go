package main

import (
	"context"
	"mitmproxy/quesma/clickhouse"
	"mitmproxy/quesma/quesma"
	"os"
	"os/signal"
	"syscall"
	"time"
)

const targetEnv = "ELASTIC_URL"
const tcpPortEnv = "TCP_PORT"
const internalHttpPort = "8081"

var tcpPort = os.Getenv(tcpPortEnv)

func main() {
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)

	lm := clickhouse.NewLogManager(
		clickhouse.PredefinedTableSchemas,
		clickhouse.NewRuntimeSchemas,
	)
	instance := quesma.New(lm, os.Getenv(targetEnv), tcpPort, internalHttpPort)
	instance.Start()

	<-sig
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	instance.Close(ctx)
}
