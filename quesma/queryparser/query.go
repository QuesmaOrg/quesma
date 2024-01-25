package queryparser

type Query struct {
	Sql       string
	TableName string
	CanParse  bool
}
