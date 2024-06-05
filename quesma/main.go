package main

import (
	"context"
	"fmt"
	"log"
	"mitmproxy/quesma/buildinfo"
	"mitmproxy/quesma/clickhouse"
	"mitmproxy/quesma/concurrent"
	"mitmproxy/quesma/elasticsearch"
	"mitmproxy/quesma/feature"
	"mitmproxy/quesma/logger"
	"mitmproxy/quesma/quesma"
	"mitmproxy/quesma/quesma/config"
	"mitmproxy/quesma/telemetry"
	"mitmproxy/quesma/tracing"
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

func main() {
	println(banner)
	fmt.Printf("Quesma build info: version=[%s], build hash=[%s], build date=[%s] license key=[%s]\n",
		buildinfo.Version, buildinfo.BuildHash, buildinfo.BuildDate, config.MaskLicenseKey(buildinfo.LicenseKey))

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	doneCh := make(chan struct{})
	var cfg = config.Load()

	if err := cfg.Validate(); err != nil {
		log.Fatalf("error validating configuration: %v", err)
	}

	asyncQueryTraceLogger := &tracing.AsyncTraceLogger{AsyncQueryTrace: concurrent.NewMap[string, tracing.TraceCtx]()}

	qmcLogChannel := logger.InitLogger(cfg, sig, doneCh, asyncQueryTraceLogger)
	defer logger.StdLogFile.Close()
	defer logger.ErrLogFile.Close()

	asyncQueryTraceEvictor := quesma.AsyncQueryTraceLoggerEvictor{AsyncQueryTrace: asyncQueryTraceLogger.AsyncQueryTrace}
	asyncQueryTraceEvictor.Start()

	var connectionPool = clickhouse.InitDBConnectionPool(cfg)

	phoneHomeAgent := telemetry.NewPhoneHomeAgent(cfg, connectionPool)
	phoneHomeAgent.Start()

	lm := clickhouse.NewEmptyLogManager(cfg, connectionPool, phoneHomeAgent)
	im := elasticsearch.NewIndexManagement(cfg.Elasticsearch.Url.String())

	logger.Info().Msgf("loaded config: %s", cfg.String())

	instance := constructQuesma(cfg, lm, im, phoneHomeAgent, qmcLogChannel)
	instance.Start()

	<-doneCh

	logger.Info().Msgf("Quesma quiting")

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	feature.NotSupportedLogger.Stop()
	phoneHomeAgent.Stop(ctx)
	lm.Stop()
	asyncQueryTraceEvictor.Stop()
	instance.Close(ctx)

}

func constructQuesma(cfg config.QuesmaConfiguration, lm *clickhouse.LogManager, im elasticsearch.IndexManagement, phoneHomeAgent telemetry.PhoneHomeAgent, logChan <-chan tracing.LogWithLevel) *quesma.Quesma {

	switch cfg.Mode {
	case config.Proxy:
		return quesma.NewQuesmaTcpProxy(phoneHomeAgent, cfg, logChan, false)
	case config.ProxyInspect:
		return quesma.NewQuesmaTcpProxy(phoneHomeAgent, cfg, logChan, true)
	case config.DualWriteQueryElastic, config.DualWriteQueryClickhouse, config.DualWriteQueryClickhouseVerify, config.DualWriteQueryClickhouseFallback:
		return quesma.NewHttpProxy(phoneHomeAgent, lm, im, cfg, logChan)
	}
	logger.Panic().Msgf("unknown operation mode: %s", cfg.Mode.String())
	panic("unreachable")
}
