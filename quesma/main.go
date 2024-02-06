package main

import (
	"context"
	"fmt"
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

	var cfg = config.Load()
	log.Printf("loaded config: %+v\n", cfg)

	instance := constructQuesma(cfg, lm)
	instance.Start()

	<-sig
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	instance.Close(ctx)
}

func constructQuesma(cfg config.QuesmaConfiguration, lm *clickhouse.LogManager) *quesma.Quesma {
	target := os.Getenv(targetEnv)

	switch cfg.Mode {
	case config.Proxy:
		return quesma.NewQuesmaTcpProxy(target, tcpPort, cfg, false)
	case config.ProxyInspect:
		return quesma.NewQuesmaTcpProxy(target, tcpPort, cfg, true)
	case config.DualWriteQueryElastic, config.DualWriteQueryClickhouse, config.DualWriteQueryClickhouseVerify, config.DualWriteQueryClickhouseFallback:
		return quesma.NewHttpProxy(lm, target, tcpPort, internalHttpPort, cfg)
	case config.ClickHouse:
		return quesma.NewHttpClickhouseAdapter(lm, target, tcpPort, internalHttpPort, cfg)
	}
	panic(fmt.Sprintf("unknown operation mode: %d\n", cfg.Mode))
}
