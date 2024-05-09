package schema

import "context"

type (
	SearchConnector interface {
		ResolveIndex(indexName string) (SearchIndex, error)
	}

	SearchIndex interface {
		ResolveField(fieldName string) (SearchField, error)
		AvailableFields() []SearchField

		NewQueryBuilder(context context.Context) SearchQueryBuilder

		DebugName() string
	}

	SearchField interface {
		// E.g. returned in field caps
		Name() string
		IsSearchable() bool
		IsAggregatable() bool
		IsMetadataField() bool

		// GetNestedField returned nested fields if they are present
		GetNestedField() []SearchField

		// NewComputedField Expression can be count(?), sum(?), avg(?), etc.
		NewComputedField(expression string) (SearchField, error)
		// NewWhereClause We may break it down to more methods
		NewWhereClause(operator string, value interface{}) (WhereClause, error)

		DebugName() string
	}

	WhereClause interface {
		NewOrClause(clauses []WhereClause) WhereClause
		NewAndClause(clauses []WhereClause) WhereClause
		NewNotClause(clause WhereClause) WhereClause

		DebugName() string
	}

	SearchQueryBuilder interface {
		AddSelectStar()
		AddSelect(field SearchField)

		AddWhere(clause WhereClause)
		AddWhereFullTextSearch(query string)

		AddGroupBy(field SearchField)
		AddOrderBy(field SearchField, asc bool)

		SetSampleLimit(limit int)
		SetLimit(limit int)
		SetDistinct(isDistinct bool)

		IsValid() bool
		FindNonExistingFields() []SearchField
		BuildSQL() (string, error)
	}
)
