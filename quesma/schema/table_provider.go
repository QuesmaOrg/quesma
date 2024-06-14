package schema

type (
	Table struct {
		Columns map[string]Column
	}
	Column struct {
		Name string
		Type string
	}
	TableProvider interface {
		TableDefinitions() map[string]Table
	}
)
