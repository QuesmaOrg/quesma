// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package main

import (
	"context"
	"github.com/QuesmaOrg/quesma/platform/ab_testing"
	"github.com/QuesmaOrg/quesma/platform/clickhouse"
	"github.com/QuesmaOrg/quesma/platform/config"
	"github.com/QuesmaOrg/quesma/platform/ingest"
	"github.com/QuesmaOrg/quesma/platform/logger"
	"github.com/QuesmaOrg/quesma/platform/recovery"
	"github.com/QuesmaOrg/quesma/platform/schema"
	"github.com/QuesmaOrg/quesma/platform/table_resolver"
	"github.com/QuesmaOrg/quesma/platform/telemetry"
	"github.com/QuesmaOrg/quesma/platform/ui"
	"github.com/QuesmaOrg/quesma/platform/util"
	quesma_v2 "github.com/QuesmaOrg/quesma/platform/v2/core"
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

func NewHttpProxy(phoneHomeAgent telemetry.PhoneHomeAgent,
	logManager *clickhouse.LogManager, ingestProcessor *ingest.IngestProcessor,
	schemaLoader clickhouse.TableDiscovery,
	schemaRegistry schema.Registry, config *config.QuesmaConfiguration,
	quesmaManagementConsole *ui.QuesmaManagementConsole,
	abResultsRepository ab_testing.Sender, resolver table_resolver.TableResolver) *Quesma {

	dependencies := quesma_v2.NewDependencies()
	dependencies.SetPhoneHomeAgent(phoneHomeAgent)
	dependencies.SetDebugInfoCollector(quesmaManagementConsole)
	dependencies.SetLogger(logger.GlobalLogger()) // FIXME: we're using global logger here, create

	return &Quesma{
		telemetryAgent: phoneHomeAgent,
		processor: newDualWriteProxyV2(dependencies, schemaLoader, logManager,
			schemaRegistry, config,
			ingestProcessor, resolver, abResultsRepository),
		publicTcpPort:           config.PublicTcpPort,
		quesmaManagementConsole: quesmaManagementConsole,
		config:                  config,
	}
}
