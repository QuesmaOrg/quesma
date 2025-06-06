// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package config

import (
	"fmt"
	"github.com/hashicorp/go-multierror"
	"slices"
	"sort"
)

func (c *QuesmaConfiguration) translateAndAddSinglePipeline(confNew *QuesmaNewConfiguration, errAcc error) {
	processor := confNew.GetProcessorByName(confNew.Pipelines[0].Processors[0])
	procType := processor.Type
	if procType == QuesmaV1ProcessorNoOp {
		c.TransparentProxy = true
		return
	} else if procType != QuesmaV1ProcessorQuery {
		errAcc = multierror.Append(errAcc, fmt.Errorf("unsupported processor %s in single pipeline", procType))
		return
	}

	queryProcessor := processor

	c.IndexConfig = make(map[string]IndexConfiguration)

	// Handle default index configuration
	defaultConfig := queryProcessor.Config.IndexConfig[DefaultWildcardIndexName]
	targets, errTarget := confNew.getTargetsExtendedConfig(defaultConfig.Target)
	if errTarget != nil {
		errAcc = multierror.Append(errAcc, errTarget)
	}

	for _, target := range targets {
		if targetType, found := confNew.getTargetType(target.target); found {
			if !slices.Contains(defaultConfig.QueryTarget, targetType) {
				defaultConfig.QueryTarget = append(defaultConfig.QueryTarget, targetType)
			}
		} else {
			errAcc = multierror.Append(errAcc, fmt.Errorf("invalid target %s in configuration of %s", target, DefaultWildcardIndexName))
		}
		if val, exists := target.properties["useCommonTable"]; exists {
			c.CreateCommonTable = val == "true"
			c.UseCommonTableForWildcard = val == "true"
		} else {
			// inherit setting from the processor level
			c.CreateCommonTable = queryProcessor.Config.UseCommonTable
			c.UseCommonTableForWildcard = queryProcessor.Config.UseCommonTable
		}
	}

	if defaultConfig.UseCommonTable {
		// We set both flags to true here
		// as creating common table depends on the first one
		c.CreateCommonTable = true
		c.UseCommonTableForWildcard = true
	}
	if defaultConfig.SchemaOverrides != nil {
		errAcc = multierror.Append(errAcc, fmt.Errorf("schema overrides of default index ('%s') are not currently supported (only supported in configuration of a specific index)", DefaultWildcardIndexName))
	}
	if len(defaultConfig.QueryTarget) > 1 {
		errAcc = multierror.Append(errAcc, fmt.Errorf("the target configuration of default index ('%s') of query processor is not currently supported", DefaultWildcardIndexName))
	}
	c.DefaultIngestTarget = []string{}
	c.DefaultQueryTarget = defaultConfig.QueryTarget
	c.AutodiscoveryEnabled = slices.Contains(c.DefaultQueryTarget, ClickhouseTarget)

	// safe to call per validation earlier
	if targts, ok := queryProcessor.Config.IndexConfig[DefaultWildcardIndexName].Target.([]interface{}); ok {
		conn := confNew.GetBackendConnectorByName(targts[0].(string))
		queryProcessor.Config.DefaultTargetConnectorType = conn.Type
		for indexName, indexCfg := range queryProcessor.Config.IndexConfig {
			var targetType string
			indexTarget, ok2 := indexCfg.Target.([]interface{})
			if len(indexTarget) > 0 && ok2 {
				tgt := indexTarget[0]
				var indexTargetType *BackendConnector
				if targetMap, ok := tgt.(map[string]interface{}); ok {
					for name := range targetMap {
						indexTargetType = confNew.GetBackendConnectorByName(name)
						break
					}
				} else {
					indexTargetType = confNew.GetBackendConnectorByName(indexTarget[0].(string))
				}

				if indexTargetType != nil && indexTargetType.Type == "elasticsearch" {
					targetType = "elasticsearch"
				} else { // clickhouse-os, hydrolix included
					targetType = "clickhouse"
				}
				indexCfg.QueryTarget = []string{targetType}
				queryProcessor.Config.IndexConfig[indexName] = indexCfg
			}
		}
		queryProcessor.Config.DefaultTargetConnectorType = conn.Type
		_ = confNew.updateProcessorConfig(queryProcessor.Name, queryProcessor.Config)
	}
	if defaultQueryConfig, ok := queryProcessor.Config.IndexConfig[DefaultWildcardIndexName]; ok {
		c.DefaultQueryOptimizers = defaultQueryConfig.Optimizers
	} else {
		c.DefaultQueryOptimizers = nil
	}
	delete(queryProcessor.Config.IndexConfig, DefaultWildcardIndexName)

	for indexName, indexConfig := range queryProcessor.Config.IndexConfig {
		processedConfig := indexConfig
		targets, errTarget := confNew.getTargetsExtendedConfig(indexConfig.Target)
		if errTarget != nil {
			errAcc = multierror.Append(errAcc, errTarget)
		}
		for _, target := range targets {
			if targetType, found := confNew.getTargetType(target.target); found {
				if !slices.Contains(processedConfig.QueryTarget, targetType) {
					processedConfig.QueryTarget = append(processedConfig.QueryTarget, targetType)
				}
			} else {
				errAcc = multierror.Append(errAcc, fmt.Errorf("invalid target %s in configuration of index %s", target, indexName))
			}
			if val, exists := target.properties["useCommonTable"]; exists {
				processedConfig.UseCommonTable = val == "true"
			} else {
				// inherit setting from the processor level
				processedConfig.UseCommonTable = queryProcessor.Config.UseCommonTable
			}
			if val, exists := target.properties["tableName"]; exists {
				processedConfig.Override = val.(string)
			}

		}
		if len(processedConfig.QueryTarget) == 2 && !((processedConfig.QueryTarget[0] == ClickhouseTarget && processedConfig.QueryTarget[1] == ElasticsearchTarget) ||
			(processedConfig.QueryTarget[0] == ElasticsearchTarget && processedConfig.QueryTarget[1] == ClickhouseTarget)) {
			errAcc = multierror.Append(errAcc, fmt.Errorf("index %s has invalid dual query target configuration", indexName))
			continue
		}

		if len(processedConfig.QueryTarget) == 2 {
			// Turn on A/B testing
			if processedConfig.Optimizers == nil {
				processedConfig.Optimizers = make(map[string]OptimizerConfiguration)
			}
			processedConfig.Optimizers[ElasticABOptimizerName] = OptimizerConfiguration{
				Disabled:   false,
				Properties: map[string]string{},
			}
		}

		c.IndexConfig[indexName] = processedConfig
	}
}

func (c *QuesmaConfiguration) translateAndAddDualPipeline(confNew *QuesmaNewConfiguration,
	errAcc, relationalDBErr error, relDBConn *RelationalDbConfiguration) {

	fc1 := confNew.GetFrontendConnectorByName(confNew.Pipelines[0].FrontendConnectors[0])
	var queryPipeline, ingestPipeline Pipeline
	if fc1.Type == ElasticsearchFrontendQueryConnectorName {
		queryPipeline, ingestPipeline = confNew.Pipelines[0], confNew.Pipelines[1]
	} else {
		queryPipeline, ingestPipeline = confNew.Pipelines[1], confNew.Pipelines[0]
	}
	queryProcessor, ingestProcessor := confNew.GetProcessorByName(queryPipeline.Processors[0]), confNew.GetProcessorByName(ingestPipeline.Processors[0])

	if queryProcessor.Type == QuesmaV1ProcessorNoOp && ingestProcessor.Type == QuesmaV1ProcessorNoOp {
		c.TransparentProxy = true
		return
	}

	c.IndexConfig = make(map[string]IndexConfiguration)

	// Handle default index configuration
	defaultConfig := queryProcessor.Config.IndexConfig[DefaultWildcardIndexName]
	targets, errTarget := confNew.getTargetsExtendedConfig(defaultConfig.Target)
	if errTarget != nil {
		errAcc = multierror.Append(errAcc, errTarget)
	}

	for _, target := range targets {
		if targetType, found := confNew.getTargetType(target.target); found {
			if !slices.Contains(defaultConfig.QueryTarget, targetType) {
				defaultConfig.QueryTarget = append(defaultConfig.QueryTarget, targetType)
			}
		} else {
			errAcc = multierror.Append(errAcc, fmt.Errorf("invalid target %s in configuration of %s", target, DefaultWildcardIndexName))
		}
		if val, exists := target.properties["useCommonTable"]; exists {
			c.CreateCommonTable = val == "true"
			c.UseCommonTableForWildcard = val == "true"
		} else {
			// inherit setting from the processor level
			c.CreateCommonTable = queryProcessor.Config.UseCommonTable
			c.UseCommonTableForWildcard = queryProcessor.Config.UseCommonTable
		}
	}
	if defaultConfig.SchemaOverrides != nil {
		errAcc = multierror.Append(errAcc, fmt.Errorf("schema overrides of default index ('%s') are not currently supported (only supported in configuration of a specific index)", DefaultWildcardIndexName))
	}
	if defaultConfig.UseCommonTable {
		// We set both flags to true here
		// as creating common table depends on the first one
		c.CreateCommonTable = true
		c.UseCommonTableForWildcard = true
	}

	ingestProcessorDefaultIndexConfig := ingestProcessor.Config.IndexConfig[DefaultWildcardIndexName]
	targets, errTarget = confNew.getTargetsExtendedConfig(ingestProcessorDefaultIndexConfig.Target)
	if errTarget != nil {
		errAcc = multierror.Append(errAcc, errTarget)
	}
	for _, target := range targets {
		if targetType, found := confNew.getTargetType(target.target); found {
			if !slices.Contains(defaultConfig.IngestTarget, targetType) {
				defaultConfig.IngestTarget = append(defaultConfig.IngestTarget, targetType)
			}
		} else {
			errAcc = multierror.Append(errAcc, fmt.Errorf("invalid target %s in configuration of %s", target, DefaultWildcardIndexName))
		}
		if val, exists := target.properties["useCommonTable"]; exists {
			c.CreateCommonTable = val == "true"
			c.UseCommonTableForWildcard = val == "true"
		} else {
			// inherit setting from the processor level
			c.CreateCommonTable = ingestProcessor.Config.UseCommonTable
			c.UseCommonTableForWildcard = ingestProcessor.Config.UseCommonTable
		}
	}
	if ingestProcessorDefaultIndexConfig.SchemaOverrides != nil {
		errAcc = multierror.Append(errAcc, fmt.Errorf("schema overrides of default index ('%s') are not currently supported (only supported in configuration of a specific index)", DefaultWildcardIndexName))
	}
	if ingestProcessorDefaultIndexConfig.UseCommonTable {
		// We set both flags to true here
		// as creating common table depends on the first one
		c.CreateCommonTable = true
		c.UseCommonTableForWildcard = true
	}

	if len(defaultConfig.QueryTarget) > 1 {
		errAcc = multierror.Append(errAcc, fmt.Errorf("the target configuration of default index ('%s') of query processor is not currently supported", DefaultWildcardIndexName))
	}

	if defaultConfig.UseCommonTable != ingestProcessorDefaultIndexConfig.UseCommonTable {
		errAcc = multierror.Append(errAcc, fmt.Errorf("the target configuration of default index ('%s') of query processor and ingest processor should consistently use quesma common table property", DefaultWildcardIndexName))
	}

	// No restrictions for ingest target!
	c.DefaultIngestTarget = defaultConfig.IngestTarget
	c.DefaultQueryTarget = defaultConfig.QueryTarget
	c.AutodiscoveryEnabled = slices.Contains(c.DefaultQueryTarget, ClickhouseTarget)

	// we're calling this here because we don't allow having one ingest-only pipeline.
	if relationalDBErr == nil && relDBConn != nil {
		c.ClusterName = relDBConn.ClusterName
	}

	// safe to call per validation earlier
	if targts, ok := queryProcessor.Config.IndexConfig[DefaultWildcardIndexName].Target.([]interface{}); ok {
		conn := confNew.GetBackendConnectorByName(targts[0].(string))
		queryProcessor.Config.DefaultTargetConnectorType = conn.Type
		for indexName, indexCfg := range queryProcessor.Config.IndexConfig {
			var targetType string
			indexTarget, ok2 := indexCfg.Target.([]interface{})
			if len(indexTarget) > 0 && ok2 {
				tgt := indexTarget[0]
				var indexTargetType *BackendConnector
				if targetMap, ok := tgt.(map[string]interface{}); ok {
					for name := range targetMap {
						indexTargetType = confNew.GetBackendConnectorByName(name)
						break
					}
				} else {
					indexTargetType = confNew.GetBackendConnectorByName(indexTarget[0].(string))
				}

				if indexTargetType != nil && indexTargetType.Type == "elasticsearch" {
					targetType = "elasticsearch"
				} else { // clickhouse-os, hydrolix included
					targetType = "clickhouse"
				}
				indexCfg.QueryTarget = []string{targetType}
				queryProcessor.Config.IndexConfig[indexName] = indexCfg
			}
		}
		_ = confNew.updateProcessorConfig(queryProcessor.Name, queryProcessor.Config)
	}
	if defaultQueryConfig, ok := queryProcessor.Config.IndexConfig[DefaultWildcardIndexName]; ok {
		c.DefaultQueryOptimizers = defaultQueryConfig.Optimizers
		c.DefaultPartitioningStrategy = queryProcessor.Config.IndexConfig[DefaultWildcardIndexName].PartitioningStrategy
	} else {
		c.DefaultQueryOptimizers = nil
	}
	delete(queryProcessor.Config.IndexConfig, DefaultWildcardIndexName)

	for indexName, indexConfig := range queryProcessor.Config.IndexConfig {
		processedConfig := indexConfig

		processedConfig.IngestTarget = defaultConfig.IngestTarget
		targets, errTarget = confNew.getTargetsExtendedConfig(indexConfig.Target)
		if errTarget != nil {
			errAcc = multierror.Append(errAcc, errTarget)
		}
		for _, target := range targets {
			if targetType, found := confNew.getTargetType(target.target); found {
				if !slices.Contains(processedConfig.QueryTarget, targetType) {
					processedConfig.QueryTarget = append(processedConfig.QueryTarget, targetType)
				}
			} else {
				errAcc = multierror.Append(errAcc, fmt.Errorf("invalid target %s in configuration of index %s", target, indexName))
			}
			if val, exists := target.properties["useCommonTable"]; exists {
				processedConfig.UseCommonTable = val == true
			} else {
				// inherit setting from the processor level
				processedConfig.UseCommonTable = queryProcessor.Config.UseCommonTable
			}
			if val, exists := target.properties["tableName"]; exists {
				processedConfig.Override = val.(string)
			}
		}
		if len(processedConfig.QueryTarget) == 2 && !((processedConfig.QueryTarget[0] == ClickhouseTarget && processedConfig.QueryTarget[1] == ElasticsearchTarget) ||
			(processedConfig.QueryTarget[0] == ElasticsearchTarget && processedConfig.QueryTarget[1] == ClickhouseTarget)) {
			errAcc = multierror.Append(errAcc, fmt.Errorf("index %s has invalid dual query target configuration", indexName))
			continue
		}

		if len(processedConfig.QueryTarget) == 2 {
			// Turn on A/B testing
			if processedConfig.Optimizers == nil {
				processedConfig.Optimizers = make(map[string]OptimizerConfiguration)
			}
			processedConfig.Optimizers[ElasticABOptimizerName] = OptimizerConfiguration{
				Disabled:   false,
				Properties: map[string]string{},
			}
		}

		c.IndexConfig[indexName] = processedConfig
	}

	c.EnableIngest = true
	c.IngestStatistics = confNew.IngestStatistics

	if defaultIngestConfig, ok := ingestProcessor.Config.IndexConfig[DefaultWildcardIndexName]; ok {
		c.DefaultIngestOptimizers = defaultIngestConfig.Optimizers
	} else {
		c.DefaultIngestOptimizers = nil
	}

	if ingestProcessor.Config.IndexNameRewriteRules != nil {

		if len(ingestProcessor.Config.IndexNameRewriteRules) > 0 {

			var names []string
			for name := range ingestProcessor.Config.IndexNameRewriteRules {
				names = append(names, name)
			}

			sort.Strings(names)

			var orderedRules []IndexNameRewriteRule
			for _, name := range names {
				if rule, ok := ingestProcessor.Config.IndexNameRewriteRules[name]; ok {
					orderedRules = append(orderedRules, rule)
				}
			}
			c.IndexNameRewriteRules = orderedRules
		}
	}

	// safe to call per validation earlier
	if targts, ok := ingestProcessor.Config.IndexConfig[DefaultWildcardIndexName].Target.([]interface{}); ok {
		conn := confNew.GetBackendConnectorByName(targts[0].(string))
		ingestProcessor.Config.DefaultTargetConnectorType = conn.Type
		// In order to maintain compat with v1 code we have to fill QueryTarget and IngestTarget for each index
		for indexName, indexCfg := range ingestProcessor.Config.IndexConfig {
			var targetType string
			indexTarget, ok2 := indexCfg.Target.([]interface{})
			if len(indexTarget) > 0 && ok2 {
				tgt := indexTarget[0]
				var indexTargetType *BackendConnector
				if targetMap, ok := tgt.(map[string]interface{}); ok {
					for name := range targetMap {
						indexTargetType = confNew.GetBackendConnectorByName(name)
						break
					}
				} else {
					indexTargetType = confNew.GetBackendConnectorByName(indexTarget[0].(string))
				}

				if indexTargetType != nil && indexTargetType.Type == "elasticsearch" {
					targetType = "elasticsearch"
				} else { // clickhouse-os, hydrolix included
					targetType = "clickhouse"
				}
				indexCfg.IngestTarget = []string{targetType}
				ingestProcessor.Config.IndexConfig[indexName] = indexCfg
			}
		}
		confNew.updateProcessorConfig(ingestProcessor.Name, ingestProcessor.Config)
	}
	delete(ingestProcessor.Config.IndexConfig, DefaultWildcardIndexName)

	for indexName, indexConfig := range ingestProcessor.Config.IndexConfig {
		processedConfig, found := c.IndexConfig[indexName]
		if !found {
			// Index is only configured in ingest processor, not in query processor,
			// use the ingest processor's configuration as the base (similarly as in the previous loop)
			processedConfig = indexConfig
			processedConfig.QueryTarget = defaultConfig.QueryTarget
		}

		processedConfig.IngestTarget = make([]string, 0) // reset previously set defaultConfig.IngestTarget
		targets, errTarget = confNew.getTargetsExtendedConfig(indexConfig.Target)
		if errTarget != nil {
			errAcc = multierror.Append(errAcc, errTarget)
		}
		for _, target := range targets {
			if targetType, found := confNew.getTargetType(target.target); found {
				if !slices.Contains(processedConfig.IngestTarget, targetType) {
					processedConfig.IngestTarget = append(processedConfig.IngestTarget, targetType)
				}
			} else {
				errAcc = multierror.Append(errAcc, fmt.Errorf("invalid target %s in configuration of index %s", target, indexName))
			}
			if val, exists := target.properties["useCommonTable"]; exists {
				processedConfig.UseCommonTable = val == true
			} else {
				// inherit setting from the processor level
				processedConfig.UseCommonTable = ingestProcessor.Config.UseCommonTable
			}
			if val, exists := target.properties["tableName"]; exists {
				processedConfig.Override = val.(string)
			}
		}

		// copy ingest optimizers to the destination
		if indexConfig.Optimizers != nil {
			if processedConfig.Optimizers == nil {
				processedConfig.Optimizers = make(map[string]OptimizerConfiguration)
			}
			for optName, optConf := range indexConfig.Optimizers {
				processedConfig.Optimizers[optName] = optConf
			}
		}

		c.IndexConfig[indexName] = processedConfig
	}
}
