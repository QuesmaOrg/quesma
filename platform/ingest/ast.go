// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package ingest

import (
	"fmt"
	"github.com/QuesmaOrg/quesma/platform/util"
	"strings"
)

type ColumnStatement struct {
	ColumnName         string
	ColumnType         string
	Comment            string
	PropertyName       string
	AdditionalMetadata string
}

type CreateTableStatement struct {
	Name       string
	Cluster    string // Optional: ON CLUSTER
	Columns    []ColumnStatement
	Indexes    string // Optional: INDEXES
	Comment    string
	PostClause string // e.g. ENGINE, ORDER BY, etc.
}

type AlterStatementType int

const (
	AddColumn AlterStatementType = iota
	CommentColumn
)

type AlterStatement struct {
	Type       AlterStatementType
	TableName  string
	OnCluster  string
	ColumnName string
	ColumnType string // used only for AddColumn
	Comment    string // used only for CommentColumn
}

type InsertStatement struct {
	TableName    string
	InsertValues string // expected to be JSONEachRow-compatible content
}

func (ct CreateTableStatement) ToSQL() string {
	if ct.Name == "" {
		return ""
	}
	var b strings.Builder

	if ct.Cluster != "" {
		b.WriteString(fmt.Sprintf(`CREATE TABLE IF NOT EXISTS "%s" ON CLUSTER "%s"`+" \n(\n\n", ct.Name, ct.Cluster))
	} else {
		b.WriteString(fmt.Sprintf(`CREATE TABLE IF NOT EXISTS "%s"`, ct.Name))
	}

	first := true

	if len(ct.Columns) > 0 {
		b.WriteString(" \n(\n\n")

	}

	for _, column := range ct.Columns {
		if first {
			first = false
		} else {
			b.WriteString(",\n")
		}
		b.WriteString(util.Indent(1))
		b.WriteString(fmt.Sprintf("\"%s\" %s", column.ColumnName, column.ColumnType))
		if column.Comment != "" {
			b.WriteString(fmt.Sprintf(" COMMENT '%s'", column.Comment))
		}
		if column.AdditionalMetadata != "" {
			b.WriteString(fmt.Sprintf(" %s", column.AdditionalMetadata))
		}
	}

	b.WriteString(ct.Indexes)

	if len(ct.Columns) > 0 {
		b.WriteString("\n)\n")
	}

	if ct.PostClause != "" {
		b.WriteString(ct.PostClause + "\n")
	}
	if ct.Comment != "" {
		b.WriteString(fmt.Sprintf("COMMENT '%s'", ct.Comment))
	}

	return b.String()
}

func (stmt AlterStatement) ToSql() string {
	var onCluster string
	if stmt.OnCluster != "" {
		onCluster = fmt.Sprintf(` ON CLUSTER "%s"`, stmt.OnCluster)
	}

	switch stmt.Type {
	case AddColumn:
		return fmt.Sprintf(
			`ALTER TABLE "%s"%s ADD COLUMN IF NOT EXISTS "%s" %s`,
			stmt.TableName, onCluster, stmt.ColumnName, stmt.ColumnType,
		)
	case CommentColumn:
		return fmt.Sprintf(
			`ALTER TABLE "%s"%s COMMENT COLUMN "%s" '%s'`,
			stmt.TableName, onCluster, stmt.ColumnName, stmt.Comment,
		)
	default:
		panic(fmt.Sprintf("unsupported AlterStatementType: %v", stmt.Type))
	}
}

func (s InsertStatement) ToSQL() string {
	return fmt.Sprintf(`INSERT INTO "%s" FORMAT JSONEachRow %s`, s.TableName, s.InsertValues)
}
