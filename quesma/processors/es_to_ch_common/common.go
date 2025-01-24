// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package es_to_ch_common

import (
	"github.com/QuesmaOrg/quesma/quesma/ab_testing/sender"
	"github.com/QuesmaOrg/quesma/quesma/clickhouse"
	"github.com/QuesmaOrg/quesma/quesma/common_table"
	"github.com/QuesmaOrg/quesma/quesma/ingest"
	"github.com/QuesmaOrg/quesma/quesma/persistence"
	"github.com/QuesmaOrg/quesma/quesma/quesma/config"
	"github.com/QuesmaOrg/quesma/quesma/quesma/ui"
	"github.com/QuesmaOrg/quesma/quesma/schema"
	"github.com/QuesmaOrg/quesma/quesma/table_resolver"
	quesma_api "github.com/QuesmaOrg/quesma/quesma/v2/core"
	"github.com/ucarion/urlpath"
	"net/http"
)

// Shared code for Elasticsearch to Clickhouse Query/Ingest processors

const (
	IndexPattern = "index_pattern"
	PathPattern  = "path_pattern"
	Id           = "id"

	// Maybe to be removed, it's a dumb fallback handler
	Bypass = "true"
)

// Copied from `quesma/v2/core/routes/paths.go` to temporarily avoid import cycle
const (
	GlobalSearchPath          = "/_search"
	IndexSearchPath           = "/:index/_search"
	IndexAsyncSearchPath      = "/:index/_async_search"
	IndexCountPath            = "/:index/_count"
	IndexDocPath              = "/:index/_doc"
	IndexRefreshPath          = "/:index/_refresh"
	IndexBulkPath             = "/:index/_bulk"
	IndexMappingPath          = "/:index/_mapping"
	FieldCapsPath             = "/:index/_field_caps"
	TermsEnumPath             = "/:index/_terms_enum"
	EQLSearch                 = "/:index/_eql/search"
	ResolveIndexPath          = "/_resolve/index/:index"
	ClusterHealthPath         = "/_cluster/health"
	BulkPath                  = "/_bulk"
	AsyncSearchIdPrefix       = "/_async_search/"
	AsyncSearchIdPath         = "/_async_search/:id"
	AsyncSearchStatusPath     = "/_async_search/status/:id"
	KibanaInternalPrefix      = "/.kibana_"
	IndexPath                 = "/:index"
	ExecutePainlessScriptPath = "/_scripts/painless/_execute" // This path is used on the Kibana side to evaluate painless scripts when adding a new scripted field.

	// Quesma internal paths

	QuesmaTableResolverPath = "/:index/_quesma_table_resolver"
)

const (
	// RealSourceHeader is a header to determine what exactly processor has calld
	RealSourceHeader        = "X-Backend-Called"
	RealSourceClickHouse    = "CLICKHOUSE"
	RealSourceElasticsearch = "ELASTICSEARCH"
	RealSourceQuesma        = "NONE"  // if response is just processor's own rendered content, no DB is called
	RealSourceMixed         = "MIXED" // e.g. in case of _resolve API
)

// SetPathPattern sets the path pattern matched at the frontend connector level in the metadata
// Therefore, the processor doesn't have to iterate again over route list to determine desired action
func SetPathPattern(req *quesma_api.Request, pathPattern string) *quesma_api.Result {
	metadata := quesma_api.MakeNewMetadata()
	metadata[PathPattern] = pathPattern
	return &quesma_api.Result{Meta: metadata, GenericResult: req.OriginalRequest}
}

// GetParamFromRequestURI extracts a parameter from the request URI,
// e.g. for request URI=`/myIndexName/1337`, path=/:index/:id param=index
// it will return `myIndexName`
func GetParamFromRequestURI(request *http.Request, path string, param string) string {
	if request.URL == nil {
		return ""
	}
	expectedUrl := urlpath.New(path)
	if match, ok := expectedUrl.Match(request.URL.Path); !ok {
		return ""
	} else {
		return match.Params[param]
	}
}

// LegacyQuesmaDependencies is a struct that holds dependencies for Quesma MVP processors
type LegacyQuesmaDependencies struct {
	quesma_api.DependenciesImpl
	OldQuesmaConfig     *config.QuesmaConfiguration
	ConnectionPool      quesma_api.BackendConnector
	VirtualTableStorage persistence.ElasticJSONDatabase
	TableDiscovery      clickhouse.TableDiscovery
	SchemaRegistry      schema.Registry
	TableResolver       table_resolver.TableResolver
	Adminconsole        *ui.QuesmaManagementConsole
	AbTestingController *sender.SenderCoordinator
	IngestProcessor     *ingest.IngestProcessor
	LogManager          clickhouse.LogManagerIFace
}

func newLegacyQuesmaDependencies(
	baseDependencies quesma_api.DependenciesImpl,
	oldQuesmaConfig *config.QuesmaConfiguration,
	connectionPool quesma_api.BackendConnector,
	virtualTableStorage persistence.ElasticJSONDatabase,
	tableDiscovery clickhouse.TableDiscovery,
	schemaRegistry schema.Registry,
	tableResolver table_resolver.TableResolver,
	abTestingController *sender.SenderCoordinator,
	ingestProcessor *ingest.IngestProcessor,
	logManager clickhouse.LogManagerIFace,
) *LegacyQuesmaDependencies {
	return &LegacyQuesmaDependencies{
		DependenciesImpl:    baseDependencies,
		OldQuesmaConfig:     oldQuesmaConfig,
		ConnectionPool:      connectionPool,
		VirtualTableStorage: virtualTableStorage,
		TableDiscovery:      tableDiscovery,
		SchemaRegistry:      schemaRegistry,
		TableResolver:       tableResolver,
		AbTestingController: abTestingController,
		IngestProcessor:     ingestProcessor,
		LogManager:          logManager,
	}
}

func InitializeLegacyQuesmaDependencies(baseDeps *quesma_api.DependenciesImpl, oldQuesmaConfig *config.QuesmaConfiguration) *LegacyQuesmaDependencies {
	connectionPool := clickhouse.InitDBConnectionPool(oldQuesmaConfig)
	virtualTableStorage := persistence.NewElasticJSONDatabase(oldQuesmaConfig.Elasticsearch, common_table.VirtualTableElasticIndexName)
	tableDisco := clickhouse.NewTableDiscovery(oldQuesmaConfig, connectionPool, virtualTableStorage)
	schemaRegistry := schema.NewSchemaRegistry(clickhouse.TableDiscoveryTableProviderAdapter{TableDiscovery: tableDisco}, oldQuesmaConfig, clickhouse.SchemaTypeAdapter{})
	schemaRegistry.Start()
	dummyTableResolver := table_resolver.NewDummyTableResolver(oldQuesmaConfig.IndexConfig, oldQuesmaConfig.UseCommonTableForWildcard)
	phoneHomeAgent := baseDeps.PhoneHomeAgent()

	ingestProcessor := ingest.NewIngestProcessor(
		oldQuesmaConfig,
		connectionPool,
		phoneHomeAgent,
		tableDisco,
		schemaRegistry,
		virtualTableStorage,
		dummyTableResolver,
	)
	ingestProcessor.Start()

	abTestingController := sender.NewSenderCoordinator(oldQuesmaConfig, ingestProcessor)
	abTestingController.Start()

	logManager := clickhouse.NewEmptyLogManager(oldQuesmaConfig, connectionPool, phoneHomeAgent, tableDisco)
	logManager.Start()

	legacyDependencies := newLegacyQuesmaDependencies(*baseDeps, oldQuesmaConfig, connectionPool, *virtualTableStorage, tableDisco, schemaRegistry, dummyTableResolver, abTestingController, ingestProcessor, logManager)
	return legacyDependencies
}
