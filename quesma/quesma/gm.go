package quesma

import (
	"database/sql"
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"time"
)

// Everything is a database.

type Document map[string]interface{}

type Ingester interface {
	Ingest(document Document) error
}

type Documents interface {
	Next() bool
	Document() Document
}

type Querier interface {
	Query(query string) (Documents, error)
}

// ---------------------

type ConsoleIngester struct {
}

func (c *ConsoleIngester) Ingest(document Document) error {
	out, err := json.Marshal(document)
	if err != nil {
		return err
	}
	log.Println(string(out))
	return nil
}

type Timestamper struct {
	Out Ingester
}

func (t *Timestamper) Ingest(document Document) error {
	document["@timestamp"] = time.Now().Format(time.RFC3339)
	return t.Out.Ingest(document)
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

/// -------------------------------

type StaticQuerier struct {
	Docs []Document
}

func (s *StaticQuerier) Query(query string) (Documents, error) {
	return &StaticDocuments{Docs: s.Docs}, nil
}

// --
type DbQuerier struct {
	db sql.DB
}

func (d *DbQuerier) Query(query string) (Documents, error) {
	rows, err := d.db.Query(query)
	if err != nil {
		return nil, err
	}

	var docs []Document

	for rows.Next() {
		var doc Document
		err := rows.Scan(&doc)
		if err != nil {
			return nil, err
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

	Out Querier
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

	docs, err := h.Out.Query(string(query))
	if err != nil {
		internalError(err)
		return
	}

	for docs.Next() {
		doc := docs.Document()
		out, err := json.Marshal(doc)
		if err != nil {
			internalError(err)
			return
		}
		w.Write(out)
	}
}
