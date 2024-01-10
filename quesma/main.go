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

func main() {
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)

	instance := quesma.New(clickhouse.NewTableManager(), clickhouse.NewLogManager(), os.Getenv(targetEnv))
	instance.Start()

	<-sig
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	instance.Close(ctx)
}
