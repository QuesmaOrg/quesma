package main

// Every process is a database.

type DatabaseLet interface {
	Query(query JSON) ([]JSON, error)
}

type Ingester interface {
	Ingest(document JSON) error
}

// Every data is a document

type JSON map[string]interface{}
