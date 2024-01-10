package main

import (
	"context"
	"mitmproxy/quesma/clickhouse"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)

	quesma := New(clickhouse.NewTableManager(), clickhouse.NewLogManager(), os.Getenv(targetEnv))
	quesma.start()

	<-sig
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	quesma.close(ctx)
}
