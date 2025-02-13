// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package main

import (
	"context"
	"fmt"
	"github.com/QuesmaOrg/quesma/quesma/ab_testing"
	"github.com/QuesmaOrg/quesma/quesma/ab_testing/sender"
	"github.com/QuesmaOrg/quesma/quesma/backend_connectors"
	"github.com/QuesmaOrg/quesma/quesma/buildinfo"
	"github.com/QuesmaOrg/quesma/quesma/clickhouse"
	"github.com/QuesmaOrg/quesma/quesma/common_table"
	"github.com/QuesmaOrg/quesma/quesma/connectors"
	"github.com/QuesmaOrg/quesma/quesma/elasticsearch"
	"github.com/QuesmaOrg/quesma/quesma/elasticsearch/feature"
	"github.com/QuesmaOrg/quesma/quesma/frontend_connectors"
	"github.com/QuesmaOrg/quesma/quesma/ingest"
	"github.com/QuesmaOrg/quesma/quesma/licensing"
	"github.com/QuesmaOrg/quesma/quesma/logger"
	"github.com/QuesmaOrg/quesma/quesma/persistence"
	"github.com/QuesmaOrg/quesma/quesma/processors"
	"github.com/QuesmaOrg/quesma/quesma/quesma"
	"github.com/QuesmaOrg/quesma/quesma/quesma/config"
	"github.com/QuesmaOrg/quesma/quesma/quesma/ui"
	"github.com/QuesmaOrg/quesma/quesma/schema"
	"github.com/QuesmaOrg/quesma/quesma/table_resolver"
	"github.com/QuesmaOrg/quesma/quesma/telemetry"
	quesma_api "github.com/QuesmaOrg/quesma/quesma/v2/core"
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

// Example of how to use the v2 module api in main function
//func main() {
//	q1 := BuildNewQuesma() // Back working on ingest for a while
//	//q1 := buildQueryOnlyQuesma()
//	q1.Start()
//	stop := make(chan os.Signal, 1)
//	<-stop
//	q1.Stop(context.Background())
//}

func main() {
	// TODO: Experimental feature, move to the configuration after architecture v2
	const mysql_passthrough_experiment = false
	if mysql_passthrough_experiment {
		launchMySqlPassthrough()
		return
	}

	// TODO: Experimental feature, move to the configuration after architecture v2
	const mysql_vitess_experiment = false
	if mysql_vitess_experiment {
		launchMySqlVitess()
		return
	}

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

	phoneHomeAgent := telemetry.NewPhoneHomeAgent(&cfg, connectionPool, licenseMod.License.ClientID)
	phoneHomeAgent.Start()

	virtualTableStorage := persistence.NewElasticJSONDatabase(cfg.Elasticsearch, common_table.VirtualTableElasticIndexName)
	tableDisco := clickhouse.NewTableDiscovery(&cfg, connectionPool, virtualTableStorage)
	schemaRegistry := schema.NewSchemaRegistry(clickhouse.TableDiscoveryTableProviderAdapter{TableDiscovery: tableDisco}, &cfg, clickhouse.SchemaTypeAdapter{})
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

		ingestProcessor = ingest.NewIngestProcessor(&cfg, connectionPool, phoneHomeAgent, tableDisco, schemaRegistry, virtualTableStorage, tableResolver)
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

func launchMySqlVitess() {
	var frontendConn, err = frontend_connectors.NewVitessMySqlConnector(":13306")
	if err != nil {
		panic(err)
	}
	var vitessProcessor quesma_api.Processor = processors.NewVitessMySqlProcessor()
	frontendConn.SetHandlers([]quesma_api.Processor{vitessProcessor})
	var mySqlBackendConn = backend_connectors.NewMySqlBackendConnector("root:my-secret-pw@tcp(localhost:3306)/exampledb3")
	var mySqlPipeline quesma_api.PipelineBuilder = quesma_api.NewPipeline()
	mySqlPipeline.AddProcessor(vitessProcessor)
	mySqlPipeline.AddFrontendConnector(frontendConn)
	mySqlPipeline.AddBackendConnector(mySqlBackendConn)
	var quesmaBuilder quesma_api.QuesmaBuilder = quesma_api.NewQuesma(quesma_api.EmptyDependencies())
	quesmaBuilder.AddPipeline(mySqlPipeline)
	qb, err := quesmaBuilder.Build()
	if err != nil {
		panic(err)
	}
	qb.Start()
	stop := make(chan os.Signal, 1)
	<-stop
	qb.Stop(context.Background())
}

func launchMySqlPassthrough() {
	var frontendConn = frontend_connectors.NewTCPConnector(":13306")
	var tcpProcessor quesma_api.Processor = processors.NewTcpMySqlPassthroughProcessor()
	var tcpMySqlHandler = frontend_connectors.TcpMySqlConnectionHandler{}
	frontendConn.AddConnectionHandler(&tcpMySqlHandler)
	var mySqlPipeline quesma_api.PipelineBuilder = quesma_api.NewPipeline()
	mySqlPipeline.AddProcessor(tcpProcessor)
	mySqlPipeline.AddFrontendConnector(frontendConn)
	var quesmaBuilder quesma_api.QuesmaBuilder = quesma_api.NewQuesma(quesma_api.EmptyDependencies())
	backendConn, err := backend_connectors.NewTcpBackendConnector("localhost:3306")
	if err != nil {
		panic(err)
	}
	mySqlPipeline.AddBackendConnector(backendConn)
	quesmaBuilder.AddPipeline(mySqlPipeline)
	qb, err := quesmaBuilder.Build()
	if err != nil {
		panic(err)
	}
	qb.Start()
	stop := make(chan os.Signal, 1)
	<-stop
	qb.Stop(context.Background())
}

func constructQuesma(cfg *config.QuesmaConfiguration, sl clickhouse.TableDiscovery, lm *clickhouse.LogManager, ip *ingest.IngestProcessor, schemaRegistry schema.Registry, phoneHomeAgent telemetry.PhoneHomeAgent, quesmaManagementConsole *ui.QuesmaManagementConsole, logChan <-chan logger.LogWithLevel, abResultsrepository ab_testing.Sender, indexRegistry table_resolver.TableResolver) *quesma.Quesma {
	if cfg.TransparentProxy {
		return quesma.NewQuesmaTcpProxy(cfg, quesmaManagementConsole, logChan, false)
	} else {
		return quesma.NewHttpProxy(phoneHomeAgent, lm, ip, sl, schemaRegistry, cfg, quesmaManagementConsole, abResultsrepository, indexRegistry)
	}
}
