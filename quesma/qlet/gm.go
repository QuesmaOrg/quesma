package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"time"
)

// Everything is a database.

type Document map[string]interface{}

// Ingest

type Ingester interface {
	Ingest(document Document) error
}

// Query

type Querier interface {
	Query(query string) (Documents, error)
}

type Documents interface {
	Next() bool
	Document() Document
}

// ---------------------

//

func (d Document) String() string {
	out, err := json.Marshal(d)
	if err != nil {
		return fmt.Sprintf("error: %v", err)
	}
	return string(out)
}

//

type NullIngester struct {
}

func (n *NullIngester) Ingest(document Document) error {
	return nil
}

type ConsoleIngester struct {
}

func (c *ConsoleIngester) Ingest(document Document) error {
	fmt.Println(document.String())
	return nil
}

type Timestamper struct {
	Out Ingester
}

func (t *Timestamper) Ingest(document Document) error {
	document["@timestamp"] = time.Now().Format(time.RFC3339)
	return t.Out.Ingest(document)
}

func Print(ingest Ingester, m string, a ...any) {

	if ingest == nil {
		return
	}

	doc := make(Document)

	doc["message"] = fmt.Sprintf(m, a...)

	err := ingest.Ingest(doc)
	if err != nil {
		log.Println(err)
	}
}

// ----------

type StaticDocuments struct {
	count int
	Docs  []Document
}

func (s *StaticDocuments) Next() bool {
	if s.count < len(s.Docs) {
		return true
	}
	return false
}

func (s *StaticDocuments) Document() Document {
	doc := s.Docs[s.count]
	s.count++
	return doc
}

// ----

type ZeroQuerier struct {
}

func (z *ZeroQuerier) Query(query string) (Documents, error) {
	return &StaticDocuments{Docs: []Document{}}, nil
}

/// -------------------------------

type StaticQuerier struct {
	Docs []Document
}

func (s *StaticQuerier) Query(query string) (Documents, error) {
	return &StaticDocuments{Docs: s.Docs}, nil
}

//

type TraceQuerier struct {
	Source Querier
	Out    Ingester
}

func (t *TraceQuerier) Query(query string) (Documents, error) {

	Print(t.Out, "query: %s", query)

	docs, err := t.Source.Query(query)

	if err != nil {
		Print(t.Out, "error: %v", err)
	}

	if err != nil {
		return docs, err
	}

	var count int
	for docs.Next() {
		doc := docs.Document()
		Print(t.Out, "doc: %d: %v", count, doc)
		count++
	}

	return docs, err
}

// --

type DbQuerier struct {
	db     *sql.DB
	logger Ingester
}

func (d *DbQuerier) Query(query string) (Documents, error) {
	rows, err := d.db.Query(query)
	if err != nil {
		return nil, err
	}

	cols, err := rows.Columns()
	if err != nil {
		fmt.Println("cols error:")
		fmt.Println(err)
		return nil, err
	}

	var docs []Document

	for rows.Next() {
		doc := make(Document)

		row := make([]any, len(cols))
		for i := range row {
			row[i] = new(interface{})
		}
		err = rows.Scan(row...)
		if err != nil {
			return nil, err
		}

		for i, col := range cols {
			doc[col] = *row[i].(*interface{})
		}

		docs = append(docs, doc)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return &StaticDocuments{Docs: docs}, nil
}

//

type HttpConnector struct {
	mux *http.ServeMux

	Source Querier
}

type HTTPPayload struct {
	Method  string
	Path    string
	Payload string
}

func (h *HttpConnector) ServeHTTP(w http.ResponseWriter, r *http.Request) {

	defer r.Body.Close()
	b, err := ioutil.ReadAll(r.Body)

	internalError := func(err error) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
	}

	if err != nil {
		internalError(err)
		return
	}

	payload := HTTPPayload{
		Method:  r.Method,
		Path:    r.URL.Path,
		Payload: string(b),
	}

	query, err := json.Marshal(payload)
	if err != nil {
		internalError(err)
		return
	}

	docs, err := h.Source.Query(string(query))
	if err != nil {
		internalError(err)
		return
	}

	for docs.Next() {
		doc := docs.Document()
		out := doc.String()

		w.Write([]byte(out))
	}
}

func (h *HttpConnector) ListenAndServe(addr string) error {

	h.mux = http.NewServeMux()

	h.mux.Handle("/", h)

	go http.ListenAndServe(addr, h.mux)

	return nil

}

func main() {

	sig := make(chan os.Signal, 1)

	console := &ConsoleIngester{}
	logger := &Timestamper{Out: console}

	options := clickhouse.Options{Addr: []string{"localhost:9000"}}
	db := clickhouse.OpenDB(&options)

	dbQuerier := &DbQuerier{db: db, logger: logger}

	docs, err := dbQuerier.Query("SELECT name FROM system.tables")

	if err != nil {
		log.Println(err)
		return
	}

	var docToServe Document
	for docs.Next() {

		doc := docs.Document()
		Print(logger, "doc: %v", doc)
		docToServe = doc
	}

	static := &StaticQuerier{Docs: []Document{docToServe}}

	httpConnector := &HttpConnector{Source: static}

	httpConnector.ListenAndServe(":9090")

	fmt.Println("waiting for signal...")

	<-sig

}
