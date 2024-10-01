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
	"quesma/quesma/config"
	"quesma/quesma/ui"
	"quesma/schema"
	"quesma/telemetry"
	"quesma/tracing"
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
		asyncQueryTraceEvictor := quesma.AsyncQueryTraceLoggerEvictor{AsyncQueryTrace: asyncQueryTraceLogger.AsyncQueryTrace}
		asyncQueryTraceEvictor.Start()
		defer asyncQueryTraceEvictor.Stop()
	}

	var connectionPool = clickhouse.InitDBConnectionPool(&cfg)

	phoneHomeAgent := telemetry.NewPhoneHomeAgent(&cfg, connectionPool, licenseMod.License.ClientID)
	phoneHomeAgent.Start()

	virtualTableStorage := persistence.NewElasticJSONDatabase(cfg.Elasticsearch, common_table.VirtualTableElasticIndexName)
	tableDisco := clickhouse.NewTableDiscovery(&cfg, connectionPool, virtualTableStorage)
	schemaRegistry := schema.NewSchemaRegistry(clickhouse.TableDiscoveryTableProviderAdapter{TableDiscovery: tableDisco}, &cfg, clickhouse.SchemaTypeAdapter{})

	connManager := connectors.NewConnectorManager(&cfg, connectionPool, phoneHomeAgent, tableDisco)
	lm := connManager.GetConnector()

	var ingestProcessor *ingest.IngestProcessor

	if cfg.EnableIngest {
		if cfg.CreateCommonTable {
			// Ensure common table exists. This table have to be created before ingest processor starts
			common_table.EnsureCommonTableExists(connectionPool)
		}

		ingestProcessor = ingest.NewEmptyIngestProcessor(&cfg, connectionPool, phoneHomeAgent, tableDisco, schemaRegistry, virtualTableStorage)
	} else {
		logger.Info().Msg("Ingest processor is disabled.")
	}

	im := elasticsearch.NewIndexManagement(cfg.Elasticsearch.Url.String())

	logger.Info().Msgf("loaded config: %s", cfg.String())

	quesmaManagementConsole := ui.NewQuesmaManagementConsole(&cfg, lm, im, qmcLogChannel, phoneHomeAgent, schemaRegistry) //FIXME no ingest processor here just for now

	abTestingController := sender.NewSenderCoordinator(&cfg)
	abTestingController.Start()

	instance := constructQuesma(&cfg, tableDisco, lm, ingestProcessor, im, schemaRegistry, phoneHomeAgent, quesmaManagementConsole, qmcLogChannel, abTestingController.GetSender())
	instance.Start()

	<-doneCh

	logger.Info().Msgf("Quesma quiting")

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	feature.NotSupportedLogger.Stop()
	phoneHomeAgent.Stop(ctx)
	lm.Stop()
	abTestingController.Stop()

	instance.Close(ctx)

}

func constructQuesma(cfg *config.QuesmaConfiguration, sl clickhouse.TableDiscovery, lm *clickhouse.LogManager, ip *ingest.IngestProcessor, im elasticsearch.IndexManagement, schemaRegistry schema.Registry, phoneHomeAgent telemetry.PhoneHomeAgent, quesmaManagementConsole *ui.QuesmaManagementConsole, logChan <-chan logger.LogWithLevel, abResultsrepository ab_testing.Sender) *quesma.Quesma {
	if cfg.TransparentProxy {
		return quesma.NewQuesmaTcpProxy(phoneHomeAgent, cfg, quesmaManagementConsole, logChan, true)
	} else {
		return quesma.NewHttpProxy(phoneHomeAgent, lm, ip, sl, im, schemaRegistry, cfg, quesmaManagementConsole, logChan, abResultsrepository)
	}
}
