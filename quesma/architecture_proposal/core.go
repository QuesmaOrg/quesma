package main

// Every process is a database.

type DatabaseLet interface {
	Query(query Document) ([]Document, error)
}

type Ingester interface {
	Ingest(document Document) error
}

// Every data is a document

type Document map[string]interface{}
