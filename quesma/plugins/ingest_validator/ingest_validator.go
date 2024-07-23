// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package ingest_validator

import (
	"quesma/logger"
	"quesma/plugins"
	"quesma/quesma/config"
	"quesma/quesma/types"
	"quesma/schema"
	"reflect"
)

type IngestValidator struct {
	cfg            config.QuesmaConfiguration
	schemaRegistry schema.Registry
	table          string
}

func isLong(f float64) bool {
	return f == float64(int64(f))
}

func isUnsignedLong(f float64) bool {
	if f < 0 {
		return false
	}
	return f == float64(uint64(f))
}

func getTypeName(v interface{}) string {
	t := reflect.TypeOf(v).String()
	switch t {
	case "string":
		return "text"
	case "bool":
		return "boolean"
	case "float64":
		if isLong(v.(float64)) {
			return "long"
		} else if isUnsignedLong(v.(float64)) {
			return "unsigned_long"
		} else {
			return "float"
		}
	}
	return t
}

func (iv *IngestValidator) Transform(document types.JSON) (types.JSON, error) {
	if iv.schemaRegistry == nil {
		return document, nil
	}
	schemaInstance, exists := iv.schemaRegistry.FindSchema(schema.TableName(iv.table))
	if !exists {
		logger.Error().Msgf("Schema fot table %s not found", iv.table)
		return document, nil
	}

	for k, v := range document {
		field, exists := schemaInstance.ResolveField(k)
		if exists {
			if field.Type.Name != getTypeName(v) {
				logger.Error().Msgf("Field %s has wrong type %s, expected %s", k, getTypeName(v), field.Type.Name)
				return document, nil
			}
		}
	}

	return document, nil

}

func (iv *IngestValidator) ApplyIngestTransformers(table string, cfg config.QuesmaConfiguration, schema schema.Registry, transformers []plugins.IngestTransformer) []plugins.IngestTransformer {
	transformers = append(transformers, &IngestValidator{cfg: cfg, schemaRegistry: schema, table: table})
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
