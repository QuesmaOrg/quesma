// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package quesma

import (
	"context"
	"quesma/ab_testing"
	"quesma/clickhouse"
	"quesma/elasticsearch"
	"quesma/ingest"
	"quesma/logger"
	"quesma/proxy"
	"quesma/quesma/config"
	"quesma/quesma/recovery"
	"quesma/quesma/ui"
	"quesma/schema"
	"quesma/table_resolver"
	"quesma/telemetry"
	"quesma/util"
)

type (
	Quesma struct {
		processor               engine
		publicTcpPort           util.Port
		quesmaManagementConsole *ui.QuesmaManagementConsole
		config                  *config.QuesmaConfiguration
		telemetryAgent          telemetry.PhoneHomeAgent
	}
	engine interface {
		Ingest()
		Stop(ctx context.Context)
	}
)

func (q *Quesma) Close(ctx context.Context) {
	q.processor.Stop(ctx)
}

func (q *Quesma) Start() {
	defer recovery.LogPanic()
	logger.Info().Msgf("starting quesma, transparent proxy mode: %t", q.config.TransparentProxy)

	go q.processor.Ingest()
	go q.quesmaManagementConsole.Run()
}

func NewQuesmaTcpProxy(phoneHomeAgent telemetry.PhoneHomeAgent, config *config.QuesmaConfiguration, quesmaManagementConsole *ui.QuesmaManagementConsole, logChan <-chan logger.LogWithLevel, inspect bool) *Quesma {
	return &Quesma{
		processor:               proxy.NewTcpProxy(config.PublicTcpPort, config.Elasticsearch.Url.Host, inspect),
		publicTcpPort:           config.PublicTcpPort,
		quesmaManagementConsole: quesmaManagementConsole,
		config:                  config,
	}
}

func NewHttpProxy(phoneHomeAgent telemetry.PhoneHomeAgent,
	logManager *clickhouse.LogManager, ingestProcessor *ingest.IngestProcessor,
	schemaLoader clickhouse.TableDiscovery,
	indexManager elasticsearch.IndexManagement,
	schemaRegistry schema.Registry, config *config.QuesmaConfiguration,
	quesmaManagementConsole *ui.QuesmaManagementConsole,
	abResultsRepository ab_testing.Sender, resolver table_resolver.TableResolver) *Quesma {

	return &Quesma{
		telemetryAgent: phoneHomeAgent,
		processor: newDualWriteProxy(schemaLoader, logManager, indexManager,
			schemaRegistry, config, quesmaManagementConsole, phoneHomeAgent,
			ingestProcessor, resolver, abResultsRepository),
		publicTcpPort:           config.PublicTcpPort,
		quesmaManagementConsole: quesmaManagementConsole,
		config:                  config,
	}
}
