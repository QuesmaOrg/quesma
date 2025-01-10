// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package main

import (
	"log"
	"quesma/backend_connectors"
	"quesma/frontend_connectors"
	"quesma/processors/es_to_ch_ingest"
	"quesma/processors/es_to_ch_query"
	"quesma/quesma/config"
	quesma_api "quesma_v2/core"
)

// BuildNewQuesma creates a new quesma instance with both Ingest And Query Processors, unused yet
func BuildNewQuesma() quesma_api.QuesmaBuilder {

	var quesmaBuilder quesma_api.QuesmaBuilder = quesma_api.NewQuesma(quesma_api.EmptyDependencies())

	queryFrontendConnector := frontend_connectors.NewElasticsearchQueryFrontendConnector(
		":8080",
		&config.QuesmaConfiguration{
			DisableAuth: true,
			Elasticsearch: config.ElasticsearchConfiguration{
				Url:      &config.Url{Host: "localhost:9200", Scheme: "http"},
				User:     "",
				Password: "",
			},
		})

	var queryPipeline quesma_api.PipelineBuilder = quesma_api.NewPipeline()
	queryPipeline.AddFrontendConnector(queryFrontendConnector)

	queryProcessor := es_to_ch_query.NewElasticsearchToClickHouseQueryProcessor(
		config.QuesmaProcessorConfig{
			UseCommonTable: false,
			IndexConfig: map[string]config.IndexConfiguration{
				"test_index":   {},
				"test_index_2": {},
				"tab1": {
					UseCommonTable: true,
				},
				"tab2": {
					UseCommonTable: true,
				},
				"kibana_sample_data_ecommerce": {
					QueryTarget: []string{config.ClickhouseTarget}, // table_discovery2.go:230 explains why this is needed
				},
				"*": {
					QueryTarget: []string{config.ElasticsearchTarget},
				},
			},
		},
	)

	ingestFrontendConnector := frontend_connectors.NewElasticsearchIngestFrontendConnector(":8080")

	var ingestPipeline quesma_api.PipelineBuilder = quesma_api.NewPipeline()
	ingestPipeline.AddFrontendConnector(ingestFrontendConnector)

	ingestProcessor := es_to_ch_ingest.NewElasticsearchToClickHouseIngestProcessor(
		config.QuesmaProcessorConfig{
			UseCommonTable: false,
			IndexConfig: map[string]config.IndexConfiguration{
				"test_index":   {},
				"test_index_2": {},
				"tab1": {
					UseCommonTable: true,
				},
				"tab2": {
					UseCommonTable: true,
				},
				"*": {
					IngestTarget: []string{config.ElasticsearchTarget},
				},
			},
		},
	)

	ingestPipeline.AddProcessor(ingestProcessor)
	quesmaBuilder.AddPipeline(ingestPipeline)

	queryPipeline.AddProcessor(queryProcessor)
	quesmaBuilder.AddPipeline(queryPipeline)

	clickHouseBackendConnector := backend_connectors.NewClickHouseBackendConnector("clickhouse://localhost:9000")
	elasticsearchBackendConnector := backend_connectors.NewElasticsearchBackendConnector(
		config.ElasticsearchConfiguration{
			Url:      &config.Url{Host: "localhost:9200", Scheme: "http"},
			User:     "elastic",
			Password: "quesmaquesma",
		})
	queryPipeline.AddBackendConnector(clickHouseBackendConnector)
	queryPipeline.AddBackendConnector(elasticsearchBackendConnector)

	ingestPipeline.AddBackendConnector(clickHouseBackendConnector)
	ingestPipeline.AddBackendConnector(elasticsearchBackendConnector)

	quesmaInstance, err := quesmaBuilder.Build()
	if err != nil {
		log.Fatalf("error building quesma instance: %v", err)
	}
	return quesmaInstance
}

// buildIngestOnlyQuesma is for now a helper function to help establishing the way of v2 module api import
func buildIngestOnlyQuesma() quesma_api.QuesmaBuilder {
	var quesmaBuilder quesma_api.QuesmaBuilder = quesma_api.NewQuesma(quesma_api.EmptyDependencies())

	ingestFrontendConnector := frontend_connectors.NewElasticsearchIngestFrontendConnector(":8080")

	var ingestPipeline quesma_api.PipelineBuilder = quesma_api.NewPipeline()
	ingestPipeline.AddFrontendConnector(ingestFrontendConnector)

	ingestProcessor := es_to_ch_ingest.NewElasticsearchToClickHouseIngestProcessor(
		config.QuesmaProcessorConfig{
			UseCommonTable: false,
			IndexConfig: map[string]config.IndexConfiguration{
				"test_index":   {},
				"test_index_2": {},
				"tab1": {
					UseCommonTable: true,
				},
				"tab2": {
					UseCommonTable: true,
				},
				"*": {
					IngestTarget: []string{config.ElasticsearchTarget},
				},
			},
		},
	)
	ingestPipeline.AddProcessor(ingestProcessor)
	quesmaBuilder.AddPipeline(ingestPipeline)

	clickHouseBackendConnector := backend_connectors.NewClickHouseBackendConnector("clickhouse://localhost:9000")
	elasticsearchBackendConnector := backend_connectors.NewElasticsearchBackendConnector(
		config.ElasticsearchConfiguration{
			Url:      &config.Url{Host: "localhost:9200", Scheme: "https"},
			User:     "elastic",
			Password: "quesmaquesma",
		})
	ingestPipeline.AddBackendConnector(clickHouseBackendConnector)
	ingestPipeline.AddBackendConnector(elasticsearchBackendConnector)

	quesmaInstance, err := quesmaBuilder.Build()
	if err != nil {
		log.Fatalf("error building quesma instance: %v", err)
	}
	return quesmaInstance
}

// buildQueryOnlyQuesma is for now a helper function to help establishing the way of v2 module api import
//func buildQueryOnlyQuesma() quesma_api.QuesmaBuilder {
//	var quesmaBuilder quesma_api.QuesmaBuilder = quesma_api.NewQuesma(quesma_api.EmptyDependencies())
//	queryFrontendConnector := frontend_connectors.NewElasticsearchQueryFrontendConnector(
//		":8080",
//		&config.QuesmaConfiguration{
//			DisableAuth: true,
//			Elasticsearch: config.ElasticsearchConfiguration{
//				Url:      &config.Url{Host: "localhost:9200", Scheme: "http"},
//				User:     "",
//				Password: "",
//			},
//		})
//
//	var queryPipeline quesma_api.PipelineBuilder = quesma_api.NewPipeline()
//	queryPipeline.AddFrontendConnector(queryFrontendConnector)
//
//	queryProcessor := es_to_ch_query.NewElasticsearchToClickHouseQueryProcessor(
//		config.QuesmaProcessorConfig{
//			UseCommonTable: false,
//			IndexConfig: map[string]config.IndexConfiguration{
//				"test_index":   {},
//				"test_index_2": {},
//				"tab1": {
//					UseCommonTable: true,
//				},
//				"tab2": {
//					UseCommonTable: true,
//				},
//				"kibana_sample_data_ecommerce": {
//					QueryTarget: []string{config.ClickhouseTarget}, // table_discovery2.go:230 explains why this is needed
//				},
//				"*": {
//					QueryTarget: []string{config.ElasticsearchTarget},
//				},
//			},
//		},
//	)
//	queryPipeline.AddProcessor(queryProcessor)
//	quesmaBuilder.AddPipeline(queryPipeline)
//
//	clickHouseBackendConnector := backend_connectors.NewClickHouseBackendConnector("clickhouse://localhost:9000")
//	elasticsearchBackendConnector := backend_connectors.NewElasticsearchBackendConnector(
//		config.ElasticsearchConfiguration{
//			Url:      &config.Url{Host: "localhost:9200", Scheme: "http"},
//			User:     "elastic",
//			Password: "quesmaquesma",
//		})
//	queryPipeline.AddBackendConnector(clickHouseBackendConnector)
//	queryPipeline.AddBackendConnector(elasticsearchBackendConnector)
//
//	quesmaInstance, err := quesmaBuilder.Build()
//	if err != nil {
//		log.Fatalf("error building quesma instance: %v", err)
//	}
//	return quesmaInstance
//}
