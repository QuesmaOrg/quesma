// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package main

import (
	"context"
	"fmt"
	"github.com/QuesmaOrg/quesma/platform/ab_testing"
	"github.com/QuesmaOrg/quesma/platform/ab_testing/sender"
	"github.com/QuesmaOrg/quesma/platform/buildinfo"
	"github.com/QuesmaOrg/quesma/platform/clickhouse"
	"github.com/QuesmaOrg/quesma/platform/common_table"
	"github.com/QuesmaOrg/quesma/platform/config"
	"github.com/QuesmaOrg/quesma/platform/connectors"
	"github.com/QuesmaOrg/quesma/platform/database_common"
	"github.com/QuesmaOrg/quesma/platform/elasticsearch"
	"github.com/QuesmaOrg/quesma/platform/elasticsearch/feature"
	"github.com/QuesmaOrg/quesma/platform/ingest"
	"github.com/QuesmaOrg/quesma/platform/licensing"
	"github.com/QuesmaOrg/quesma/platform/logger"
	"github.com/QuesmaOrg/quesma/platform/persistence"
	"github.com/QuesmaOrg/quesma/platform/recovery"
	"github.com/QuesmaOrg/quesma/platform/schema"
	"github.com/QuesmaOrg/quesma/platform/table_resolver"
	"github.com/QuesmaOrg/quesma/platform/telemetry"
	"github.com/QuesmaOrg/quesma/platform/ui"
	quesma_api "github.com/QuesmaOrg/quesma/platform/v2/core"
	"log"
	"os"
	"os/signal"
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

func main() {
	defer recovery.LogPanic()

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

	var newConfiguration, configErr = config.LoadV2Config()
	if configErr != nil {
		return // We log error in LoadV2Config
	}
	var cfg = newConfiguration.TranslateToLegacyConfig()

	if err := cfg.Validate(); err != nil {
		log.Fatalf("error validating configuration: %v", err)
	}

	licenseMod := licensing.Init(&cfg)
	qmcLogChannel := logger.InitLogger(logger.Configuration{
		FileLogging:       cfg.Logging.FileLogging,
		Path:              cfg.Logging.Path,
		RemoteLogDrainUrl: cfg.Logging.RemoteLogDrainUrl.ToUrl(),
		Level:             *cfg.Logging.Level,
		ClientId:          licenseMod.License.ClientID,
	}, sig, doneCh)
	defer logger.StdLogFile.Close()
	defer logger.ErrLogFile.Close()
	go func() {
		if upgradeAvailable, message := buildinfo.CheckForTheLatestVersion(); upgradeAvailable {
			logger.Warn().Msg(message)
		}
	}()

	var connectionPool = clickhouse.InitDBConnectionPool(&cfg)
	//var connectionPool = doris.InitDBConnectionPool(&cfg)

	phoneHomeAgent := telemetry.NewPhoneHomeAgent(&cfg, connectionPool, licenseMod.License.ClientID)
	phoneHomeAgent.Start()

	virtualTableStorage := persistence.NewElasticJSONDatabase(cfg.Elasticsearch, common_table.VirtualTableElasticIndexName)
	tableDisco := database_common.NewTableDiscovery(&cfg, connectionPool, virtualTableStorage)
	schemaRegistry := schema.NewSchemaRegistry(database_common.TableDiscoveryTableProviderAdapter{TableDiscovery: tableDisco}, &cfg, clickhouse.NewClickhouseSchemaTypeAdapter(cfg.DefaultStringColumnType))
	//schemaRegistry := schema.NewSchemaRegistry(database_common.TableDiscoveryTableProviderAdapter{TableDiscovery: tableDisco}, &cfg, doris.NewDorisSchemaTypeAdapter(cfg.DefaultStringColumnType))
	schemaRegistry.Start()

	im := elasticsearch.NewIndexManagement(cfg.Elasticsearch)
	im.Start()

	connManager := connectors.NewConnectorManager(&cfg, connectionPool, phoneHomeAgent, tableDisco)
	lm := connManager.GetConnector()

	// TODO index configuration for ingest and query is the same for now
	tableResolver := table_resolver.NewTableResolver(cfg, tableDisco, im)
	tableResolver.Start()

	var ingestProcessor *ingest.IngestProcessor

	if cfg.EnableIngest {
		if cfg.CreateCommonTable {
			// Ensure common table exists. This table have to be created before ingest processor starts
			common_table.EnsureCommonTableExists(connectionPool, cfg.ClusterName)
		}
		sqlLowerer := ingest.NewSqlLowerer(virtualTableStorage)
		hydrolixLowerer := ingest.NewHydrolixLowerer(virtualTableStorage)
		ingestProcessor = ingest.NewIngestProcessor(&cfg, connectionPool, phoneHomeAgent, tableDisco, schemaRegistry, sqlLowerer, tableResolver)
		ingestProcessor.RegisterLowerer(sqlLowerer, quesma_api.ClickHouseSQLBackend)
		ingestProcessor.RegisterLowerer(hydrolixLowerer, quesma_api.HydrolixSQLBackend)
	} else {
		logger.Info().Msg("Ingest processor is disabled.")
	}

	logger.Info().Msgf("loaded config: %s", cfg.String())

	quesmaManagementConsole := ui.NewQuesmaManagementConsole(&cfg, lm, qmcLogChannel, phoneHomeAgent, schemaRegistry, tableResolver)

	abTestingController := sender.NewSenderCoordinator(&cfg, ingestProcessor)
	abTestingController.Start()

	instance := constructQuesma(&cfg, tableDisco, lm, ingestProcessor, schemaRegistry, phoneHomeAgent, quesmaManagementConsole, qmcLogChannel, abTestingController.GetSender(), tableResolver)
	instance.Start()

	<-doneCh

	logger.Info().Msgf("Quesma quiting")

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	schemaRegistry.Stop()
	feature.NotSupportedLogger.Stop()
	phoneHomeAgent.Stop(ctx)
	lm.Stop()
	abTestingController.Stop()
	tableResolver.Stop()
	instance.Close(ctx)

}

func constructQuesma(cfg *config.QuesmaConfiguration, sl database_common.TableDiscovery, lm *database_common.LogManager, ip *ingest.IngestProcessor, schemaRegistry schema.Registry, phoneHomeAgent telemetry.PhoneHomeAgent, quesmaManagementConsole *ui.QuesmaManagementConsole, logChan <-chan logger.LogWithLevel, abResultsrepository ab_testing.Sender, indexRegistry table_resolver.TableResolver) *Quesma {
	if cfg.TransparentProxy {
		return NewQuesmaTcpProxy(cfg, quesmaManagementConsole, logChan, false)
	} else {
		return NewHttpProxy(phoneHomeAgent, lm, ip, sl, schemaRegistry, cfg, quesmaManagementConsole, abResultsrepository, indexRegistry)
	}
}
