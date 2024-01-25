package main

import (
	"context"
	"log"
	"mitmproxy/quesma/clickhouse"
	"mitmproxy/quesma/quesma"
	"mitmproxy/quesma/quesma/config"
	"os"
	"os/signal"
	"syscall"
	"time"
)

const banner = `
               ________                                       
               \_____  \  __ __   ____   ______ _____ _____   
                /  / \  \|  |  \_/ __ \ /  ___//     \\__  \  
               /   \_/.  \  |  /\  ___/ \___ \|  Y Y  \/ __ \_
               \_____\ \_/____/  \___  >____  >__|_|  (____  /
                      \__>           \/     \/      \/     \/ 
`

const (
	targetEnv        = "ELASTIC_URL"
	tcpPortEnv       = "TCP_PORT"
	internalHttpPort = "8081"
)

var tcpPort = os.Getenv(tcpPortEnv)

func main() {
	println(banner)
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)

	lm := clickhouse.NewLogManager(
		clickhouse.PredefinedTableSchemas,
		clickhouse.NewRuntimeSchemas,
	)

	var config = config.Load()
	log.Printf("loaded config: %+v\n", config)

	instance := quesma.New(lm, os.Getenv(targetEnv), tcpPort, internalHttpPort, config)
	instance.Start()

	<-sig
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	instance.Close(ctx)
}
