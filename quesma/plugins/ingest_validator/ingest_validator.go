// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package ingest_validator

import (
	"quesma/clickhouse"
	"quesma/logger"
	"quesma/plugins"
	"quesma/quesma/config"
	"quesma/quesma/types"
	"quesma/schema"
	"quesma/util"
	"reflect"
)

type IngestValidator struct {
	cfg            config.QuesmaConfiguration
	schemaRegistry schema.Registry
	table          string
	tableMap       clickhouse.TableMap
}

func (iv *IngestValidator) Transform(document types.JSON) (types.JSON, error) {
	clickhouseTable, ok := iv.tableMap.Load(iv.table)
	if !ok {
		logger.Error().Msgf("Table %s not found", iv.table)
		return document, nil
	}

	for k, v := range document {
		if v != nil {
			column := clickhouseTable.Cols[k]
			if column != nil {
				kind, _ := util.KindFromString(column.Type.String())
				if kind != reflect.TypeOf(v).Kind() {
					//logger.Error().Msgf("Field %s has wrong type %s, expected %s", k, getTypeName(v), clickhouseTable.Cols[k].Type.String())
					return document, nil
				}
			}
		}
	}
	return document, nil
}

func (iv *IngestValidator) ApplyIngestTransformers(table string, cfg config.QuesmaConfiguration, schema schema.Registry, tableMap clickhouse.TableMap, transformers []plugins.IngestTransformer) []plugins.IngestTransformer {
	transformers = append(transformers, &IngestValidator{cfg: cfg, schemaRegistry: schema, table: table, tableMap: tableMap})
	return transformers
}

func (iv *IngestValidator) ApplyQueryTransformers(table string, cfg config.QuesmaConfiguration, schema schema.Registry, transformers []plugins.QueryTransformer) []plugins.QueryTransformer {
	return transformers
}

func (iv *IngestValidator) ApplyResultTransformers(table string, cfg config.QuesmaConfiguration, schema schema.Registry, transformers []plugins.ResultTransformer) []plugins.ResultTransformer {
	return transformers
}

func (iv *IngestValidator) ApplyFieldCapsTransformers(table string, cfg config.QuesmaConfiguration, schema schema.Registry, transformers []plugins.FieldCapsTransformer) []plugins.FieldCapsTransformer {
	return transformers
}

func (iv *IngestValidator) GetTableColumnFormatter(table string, cfg config.QuesmaConfiguration, schema schema.Registry) plugins.TableColumNameFormatter {
	return nil
}
