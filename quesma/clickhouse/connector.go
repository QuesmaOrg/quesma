package clickhouse

import (
	"context"
	"fmt"
	"mitmproxy/quesma/quesma"
	"mitmproxy/quesma/quesma/config"
	"mitmproxy/quesma/schema"
)

type (
	connectorClickhouse struct {
		schemaLoader *schemaLoader
		cfg          config.QuesmaConfiguration
	}

	connectorIndexClickhouse struct {
		clickhouseTable *Table
	}

	connectorFieldClickhouse struct {
		name   string
		column *Column
	}

	newComputedFieldClickhouse struct {
		expression string
		origField  connectorFieldClickhouse
	}

	newWhereClauseClickhouse struct {
		field    connectorFieldClickhouse
		operator string
		value    string
	}

	newQueryBuilder struct {
		context context.Context
		index   *connectorIndexClickhouse

		selectedFields []schema.SearchField
		whereClauses   []schema.WhereClause
		groupByField   []schema.SearchField
		orderByField   []schema.SearchField
		orderAsc       bool

		limit       int
		sampleLimit int
		isDistinct  bool
	}
)

func NewConnectorClickhouse(schemaLoader *schemaLoader, cfg config.QuesmaConfiguration) schema.SearchConnector {
	connector := connectorClickhouse{
		schemaLoader: schemaLoader,
		cfg:          cfg,
	}
	return &connector
}

func (c *connectorClickhouse) ResolveIndex(indexName string) (schema.SearchIndex, error) {
	if table, ok := c.schemaLoader.TableDefinitions().Load(indexName); ok {
		index := connectorIndexClickhouse{
			clickhouseTable: table,
		}
		return &index, nil
	} else {
		return nil, fmt.Errorf("table '%s' not found", indexName)
	}
}

func (c connectorIndexClickhouse) ResolveField(fieldName string) (schema.SearchField, error) {
	resolvedName := fieldName
	if name, ok := c.clickhouseTable.aliases[fieldName]; ok {
		resolvedName = name
	}
	if column, ok := c.clickhouseTable.Cols[resolvedName]; ok {
		field := connectorFieldClickhouse{
			name:   fieldName,
			column: column,
		}
		return &field, nil
	} else {
		return nil, fmt.Errorf("field '%s' not found", fieldName)
	}
}

func (c connectorIndexClickhouse) AvailableFields() (result []schema.SearchField) {
	for _, col := range c.clickhouseTable.Cols {
		if col == nil {
			continue
		}

		// TODO: Remove internal and hidden

		result = append(result, &connectorFieldClickhouse{
			name:   col.Name,
			column: col,
		})
	}

	for _, alias := range c.clickhouseTable.AliasList() {
		if col, ok := c.clickhouseTable.Cols[alias.TargetFieldName]; ok {
			result = append(result, &connectorFieldClickhouse{
				name:   alias.SourceFieldName,
				column: col,
			})
		}
	}
	return
}

func (c connectorIndexClickhouse) NewQueryBuilder(ctx context.Context) schema.SearchQueryBuilder {
	return newQueryBuilder{
		context: ctx,
		index:   &c,
	}
}

func (c connectorIndexClickhouse) DebugName() string {
	return c.clickhouseTable.Name
}

func (c connectorFieldClickhouse) Name() string {
	return c.name
}

func (c connectorFieldClickhouse) IsSearchable() bool {
	return c.column.Type.isString()
}

func (c connectorFieldClickhouse) IsAggregatable() bool {
	return quesma.IsAggregatable(c.column.Type.String())
}

func (c connectorFieldClickhouse) IsMetadataField() bool {
	return false // Add Them
}

func (c connectorFieldClickhouse) GetNestedField() (result []schema.SearchField) {
	return
}

func (c connectorFieldClickhouse) NewComputedField(expression string) (schema.SearchField, error) {
	//TODO implement me
	panic("implement me")
}

func (c connectorFieldClickhouse) NewWhereClause(operator string, value interface{}) (schema.WhereClause, error) {
	//TODO implement me
	panic("implement me")
}

func (c connectorFieldClickhouse) DebugName() string {
	return fmt.Sprintf("field '%s' mapped to '%s' c.name + ", c.name, c.column.Name)
}

func (n newQueryBuilder) AddSelectStar() {
	//TODO implement me
	panic("implement me")
}

func (n newQueryBuilder) AddSelect(field schema.SearchField) {
	n.selectedFields = append(n.selectedFields, field)
}

func (n newQueryBuilder) AddWhere(clause schema.WhereClause) {
	n.whereClauses = append(n.whereClauses, clause)
}

func (n newQueryBuilder) AddWhereFullTextSearch(query string) {
	for _, field := range n.index.AvailableFields() {
		clickhouseField, _ := field.(connectorFieldClickhouse)
		if field.IsSearchable() {
			n.whereClauses = append(n.whereClauses, &newWhereClauseClickhouse{
				field:    clickhouseField,
				operator: "MATCH",
				value:    query,
			})
		}
	}
}

func (n newQueryBuilder) AddGroupBy(field schema.SearchField) {
	//TODO implement me
	panic("implement me")
}

func (n newQueryBuilder) AddOrderBy(field schema.SearchField, asc bool) {
	//TODO implement me
	panic("implement me")
}

func (n newQueryBuilder) SetSampleLimit(limit int) {
	//TODO implement me
	panic("implement me")
}

func (n newQueryBuilder) SetLimit(limit int) {
	n.limit = limit
}

func (n newQueryBuilder) SetDistinct(isDistinct bool) {
	n.isDistinct = isDistinct
}

func (n newQueryBuilder) IsValid() bool {
	//TODO implement me
	panic("implement me")
}

func (n newQueryBuilder) FindNonExistingFields() []schema.SearchField {
	//TODO implement me
	panic("implement me")
}

func (n newQueryBuilder) BuildSQL() (string, error) {
	//TODO implement me
	panic("implement me")
}

func (n newWhereClauseClickhouse) NewOrClause(clauses []schema.WhereClause) schema.WhereClause {
	//TODO implement me
	panic("implement me")
}

func (n newWhereClauseClickhouse) NewAndClause(clauses []schema.WhereClause) schema.WhereClause {
	//TODO implement me
	panic("implement me")
}

func (n newWhereClauseClickhouse) NewNotClause(clause schema.WhereClause) schema.WhereClause {
	//TODO implement me
	panic("implement me")
}

func (n newWhereClauseClickhouse) DebugName() string {
	//TODO implement me
	panic("implement me")
}
