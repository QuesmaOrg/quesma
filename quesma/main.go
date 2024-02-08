package main

import (
	"context"
	"mitmproxy/quesma/clickhouse"
	"mitmproxy/quesma/logger"
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
	internalHttpPort = "8081"
)

func main() {
	println(banner)
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)

	var cfg = config.Load()

	qmcLogChannel := logger.InitLogger(cfg)
	defer logger.StdLogFile.Close()
	defer logger.ErrLogFile.Close()

	lm := clickhouse.NewLogManager(
		clickhouse.PredefinedTableSchemas,
		clickhouse.NewRuntimeSchemas,
	)

	logger.Info().Msgf("loaded config: %+v", cfg)

	instance := constructQuesma(cfg, lm, qmcLogChannel)
	instance.Start()

	<-sig
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	instance.Close(ctx)
}

func constructQuesma(cfg config.QuesmaConfiguration, lm *clickhouse.LogManager, logChan <-chan string) *quesma.Quesma {
	target := os.Getenv(targetEnv)

	switch cfg.Mode {
	case config.Proxy:
		return quesma.NewQuesmaTcpProxy(target, cfg, logChan, false)
	case config.ProxyInspect:
		return quesma.NewQuesmaTcpProxy(target, cfg, logChan, true)
	case config.DualWriteQueryElastic, config.DualWriteQueryClickhouse, config.DualWriteQueryClickhouseVerify, config.DualWriteQueryClickhouseFallback:
		return quesma.NewHttpProxy(lm, target, internalHttpPort, cfg, logChan)
	case config.ClickHouse:
		return quesma.NewHttpClickhouseAdapter(lm, target, internalHttpPort, cfg, logChan)
	}
	logger.Panic().Msgf("unknown operation mode: %d", cfg.Mode)
	panic("unreachable")
}
