// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"quesma/ab_testing"
	"quesma/ab_testing/sender"
	"quesma/buildinfo"
	"quesma/clickhouse"
	"quesma/common_table"
	"quesma/connectors"
	"quesma/elasticsearch"
	"quesma/feature"
	"quesma/ingest"
	"quesma/licensing"
	"quesma/logger"
	"quesma/persistence"
	"quesma/quesma"
	"quesma/quesma/async_search_storage"
	"quesma/quesma/config"
	"quesma/quesma/ui"
	"quesma/schema"
	"quesma/table_resolver"
	"quesma/telemetry"
	"quesma/tracing"
	"runtime"
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

const EnableConcurrencyProfiling = false

// Example of how to use the v2 module api in main function
//func main() {
//	q1 := buildQueryOnlyQuesma()
//	q1.Start()
//	stop := make(chan os.Signal, 1)
//	<-stop
//	q1.Stop(context.Background())
//}

func main() {
	if EnableConcurrencyProfiling {
		runtime.SetBlockProfileRate(1)
		runtime.SetMutexProfileFraction(1)
	}

	println(banner)
	fmt.Printf("Quesma build info: version=[%s], build hash=[%s], build date=[%s]\n",
		buildinfo.Version, buildinfo.BuildHash, buildinfo.BuildDate)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	doneCh := make(chan struct{})

	var newConfiguration = config.LoadV2Config()
	var cfg = newConfiguration.TranslateToLegacyConfig()

	if err := cfg.Validate(); err != nil {
		log.Fatalf("error validating configuration: %v", err)
	}

	var asyncQueryTraceLogger *tracing.AsyncTraceLogger

	licenseMod := licensing.Init(&cfg)
	qmcLogChannel := logger.InitLogger(logger.Configuration{
		FileLogging:       cfg.Logging.FileLogging,
		Path:              cfg.Logging.Path,
		RemoteLogDrainUrl: cfg.Logging.RemoteLogDrainUrl.ToUrl(),
		Level:             *cfg.Logging.Level,
		ClientId:          licenseMod.License.ClientID,
	}, sig, doneCh, asyncQueryTraceLogger)
	defer logger.StdLogFile.Close()
	defer logger.ErrLogFile.Close()
	go func() {
		if upgradeAvailable, message := buildinfo.CheckForTheLatestVersion(); upgradeAvailable {
			logger.Warn().Msg(message)
		}
	}()

	if asyncQueryTraceLogger != nil {
		asyncQueryTraceEvictor := async_search_storage.AsyncQueryTraceLoggerEvictor{AsyncQueryTrace: asyncQueryTraceLogger.AsyncQueryTrace}
		asyncQueryTraceEvictor.Start()
		defer asyncQueryTraceEvictor.Stop()
	}

	var connectionPool = clickhouse.InitDBConnectionPool(&cfg)

	phoneHomeAgent := telemetry.NewPhoneHomeAgent(&cfg, connectionPool, licenseMod.License.ClientID)
	phoneHomeAgent.Start()

	virtualTableStorage := persistence.NewElasticJSONDatabase(cfg.Elasticsearch, common_table.VirtualTableElasticIndexName)
	tableDisco := clickhouse.NewTableDiscovery(&cfg, connectionPool, virtualTableStorage)
	schemaRegistry := schema.NewSchemaRegistry(clickhouse.TableDiscoveryTableProviderAdapter{TableDiscovery: tableDisco}, &cfg, clickhouse.SchemaTypeAdapter{})

	im := elasticsearch.NewIndexManagement(cfg.Elasticsearch)

	connManager := connectors.NewConnectorManager(&cfg, connectionPool, phoneHomeAgent, tableDisco)
	lm := connManager.GetConnector()

	// TODO index configuration for ingest and query is the same for now
	tableResolver := table_resolver.NewTableResolver(cfg, tableDisco, im)
	tableResolver.Start()

	var ingestProcessor *ingest.IngestProcessor

	if cfg.EnableIngest {
		if cfg.CreateCommonTable {
			// Ensure common table exists. This table have to be created before ingest processor starts
			common_table.EnsureCommonTableExists(connectionPool)
		}

		ingestProcessor = ingest.NewIngestProcessor(&cfg, connectionPool, phoneHomeAgent, tableDisco, schemaRegistry, virtualTableStorage, tableResolver)
	} else {
		logger.Info().Msg("Ingest processor is disabled.")
	}

	logger.Info().Msgf("loaded config: %s", cfg.String())

	quesmaManagementConsole := ui.NewQuesmaManagementConsole(&cfg, lm, im, qmcLogChannel, phoneHomeAgent, schemaRegistry, tableResolver) //FIXME no ingest processor here just for now

	abTestingController := sender.NewSenderCoordinator(&cfg, ingestProcessor)
	abTestingController.Start()

	instance := constructQuesma(&cfg, tableDisco, lm, ingestProcessor, im, schemaRegistry, phoneHomeAgent, quesmaManagementConsole, qmcLogChannel, abTestingController.GetSender(), tableResolver)
	instance.Start()

	<-doneCh

	logger.Info().Msgf("Quesma quiting")

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	feature.NotSupportedLogger.Stop()
	phoneHomeAgent.Stop(ctx)
	lm.Stop()
	abTestingController.Stop()
	tableResolver.Stop()
	instance.Close(ctx)

}

func constructQuesma(cfg *config.QuesmaConfiguration, sl clickhouse.TableDiscovery, lm *clickhouse.LogManager, ip *ingest.IngestProcessor, im elasticsearch.IndexManagement, schemaRegistry schema.Registry, phoneHomeAgent telemetry.PhoneHomeAgent, quesmaManagementConsole *ui.QuesmaManagementConsole, logChan <-chan logger.LogWithLevel, abResultsrepository ab_testing.Sender, indexRegistry table_resolver.TableResolver) *quesma.Quesma {
	if cfg.TransparentProxy {
		return quesma.NewQuesmaTcpProxy(cfg, quesmaManagementConsole, logChan, false)
	} else {
		const quesma_v2 = false
		return quesma.NewHttpProxy(phoneHomeAgent, lm, ip, sl, im, schemaRegistry, cfg, quesmaManagementConsole, abResultsrepository, indexRegistry, quesma_v2)
	}
}
