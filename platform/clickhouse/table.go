// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package clickhouse

import (
	"context"
	"fmt"
	"github.com/QuesmaOrg/quesma/platform/config"
	"github.com/QuesmaOrg/quesma/platform/logger"
	"github.com/QuesmaOrg/quesma/platform/model"
	"github.com/QuesmaOrg/quesma/platform/util"
	"strconv"
	"strings"
)

type Table struct {
	Name         string
	DatabaseName string `default:""`
	ClusterName  string `default:""`
	Cols         map[string]*Column
	Config       *ChTableConfig
	Created      bool // do we need to create it during first insert
	Indexes      []IndexStatement
	aliases      map[string]string //deprecated
	// TODO: we should use aliases directly from configuration, not store them here
	Comment          string // this human-readable comment
	CreateTableQuery string

	DiscoveredTimestampFieldName *string

	VirtualTable bool
}

func (t *Table) createTableOurFieldsString() []string {
	rows := make([]string, 0)
	if t.Config.HasTimestamp {
		_, ok := t.Cols[timestampFieldName]
		if !ok {
			defaultStr := ""
			if t.Config.TimestampDefaultsNow {
				defaultStr = " DEFAULT now64()"
			}
			rows = append(rows, fmt.Sprintf("%s\"%s\" DateTime64(3)%s", util.Indent(1), timestampFieldName, defaultStr))
		}
	}
	if len(t.Config.Attributes) > 0 {
		for _, a := range t.Config.Attributes {
			_, ok := t.Cols[a.MapValueName]
			if !ok {
				rows = append(rows, fmt.Sprintf("%s\"%s\" Map(String,String)", util.Indent(1), a.MapValueName))
			}
			_, ok = t.Cols[a.MapMetadataName]
			if !ok {
				rows = append(rows, fmt.Sprintf("%s\"%s\" Map(String,String)", util.Indent(1), a.MapMetadataName))
			}

		}
	}
	return rows
}

func (t *Table) CreateTableString() string {
	var onClusterClause string
	if t.ClusterName != "" {
		onClusterClause = " ON CLUSTER " + strconv.Quote(t.ClusterName)
	}
	s := "CREATE TABLE IF NOT EXISTS " + t.FullTableName() + onClusterClause + " (\n"
	rows := make([]string, 0)
	for _, col := range t.Cols {
		rows = append(rows, col.createTableString(1))
	}
	rows = append(rows, t.createTableOurFieldsString()...)
	for _, index := range t.Indexes {
		rows = append(rows, util.Indent(1)+index.Statement())
	}
	return s + strings.Join(rows, ",\n") + "\n)\n" + t.Config.CreateTablePostFieldsString()
}

// FullTableName returns full table name with database name if it's not empty.
// Format: ["database".]"table" as it seems to work for all cases in Clickhouse.
// Use this in Clickhouse queries, e.g. in FROM clause.
func (t *Table) FullTableName() string {
	if t.DatabaseName != "" {
		return strconv.Quote(t.DatabaseName) + "." + strconv.Quote(t.Name)
	} else {
		return strconv.Quote(t.Name)
	}
}

// FullTableNameUnquoted returns full table name with database name if it's not empty
// Format: [database.]table
// Used e.g. to add that information to query response.
func (t *Table) FullTableNameUnquoted() string {
	if t.DatabaseName != "" {
		return fmt.Sprintf("%s.%s", t.DatabaseName, t.Name)
	} else {
		return t.Name
	}
}

// GetDateTimeType returns type of a field (currently DateTime/DateTime64), if it's a DateTime type. Invalid otherwise.
// Timestamp from config defaults to DateTime64.
// We don't warn the log message e.g. in e.g. in sum/avg/etc. aggregations, where date is (very) unexpected or impossible.
func (t *Table) GetDateTimeType(ctx context.Context, fieldName string, dateInSchemaExpected bool) DateTimeType {
	if col, ok := t.Cols[fieldName]; ok {
		typeName := col.Type.String()
		// hasPrefix, not equal, because we can have DateTime64(3) and we want to catch it
		if strings.HasPrefix(typeName, "DateTime64") {
			return DateTime64
		}
		if strings.HasPrefix(typeName, "DateTime") {
			return DateTime
		}
	}
	if t.Config.HasTimestamp && fieldName == timestampFieldName {
		return DateTime64
	}
	if dateInSchemaExpected {
		logger.WarnWithCtx(ctx).Msgf("datetime field '%s' not found in table '%s'", fieldName, t.Name)
	}
	return Invalid
}

func (t *Table) GetDateTimeTypeFromExpr(ctx context.Context, expr model.Expr) DateTimeType {
	const dateInSchemaExpected = true
	if ref, ok := expr.(model.ColumnRef); ok {
		return t.GetDateTimeType(ctx, ref.ColumnName, dateInSchemaExpected)
	}
	logger.WarnWithCtx(ctx).Msgf("datetime field '%v' not found in table '%s'", expr, t.Name)
	return Invalid
}

// ApplyIndexConfig applies full text search and alias configuration to the table
func (t *Table) ApplyIndexConfig(configuration *config.QuesmaConfiguration) {

	t.aliases = make(map[string]string)
	if indexConf, ok := configuration.IndexConfig[t.Name]; ok {
		if indexConf.SchemaOverrides != nil {
			for fieldName, fieldConf := range indexConf.SchemaOverrides.Fields {
				if fieldConf.Type == config.TypeAlias {
					t.aliases[fieldName.AsString()] = fieldConf.TargetColumnName
					if _, ok := t.Cols[fieldConf.TargetColumnName]; !ok {
						logger.Warn().Msgf("target column '%s' for field '%s' not found in table '%s'",
							fieldConf.TargetColumnName, fieldName.AsString(), t.Name)
					}
				}
			}
		}
	}
}

func (t *Table) HasColumn(ctx context.Context, fieldName string) bool {
	return t.Cols[fieldName] != nil
}

func (t *Table) AliasFields(ctx context.Context) []*Column {
	aliasFields := make([]*Column, 0)
	for key, val := range t.aliases {
		col := t.Cols[val]
		if col == nil {
			logger.ErrorWithCtx(ctx).Msgf("alias '%s' for field '%s' not found in table '%s'", val, key, t.Name)
			continue
		}
		aliasFields = append(aliasFields, &Column{
			Name:      key,
			Type:      col.Type,
			Modifiers: col.Modifiers,
		})
	}
	return aliasFields
}

func (t *Table) Aliases() map[string]string {
	return t.aliases
}

func (t *Table) GetAttributesList() []Attribute {
	return t.Config.Attributes
}

// TODO Won't work with tuples, e.g. trying to access via tupleName.tupleField will return NotExists,
// instead of some other response. Fix this when needed (we seem to not need tuples right now)
func (t *Table) GetFieldInfo(ctx context.Context, fieldName string) FieldInfo {
	col, ok := t.Cols[fieldName]
	if !ok {
		return NotExists
	}
	if col.isArray() {
		return ExistsAndIsArray
	}
	return ExistsAndIsBaseType
}
