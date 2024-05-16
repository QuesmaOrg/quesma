package main

import (
	"encoding/json"
	"fmt"
)

func NewDocument() Document {
	return make(Document)
}

func (d Document) String() string {
	out, err := json.Marshal(d)
	if err != nil {
		return fmt.Sprintf("error: %v", err)
	}
	return string(out)
}

// --------------------------

// ---------------------
//

// Aux  implementation

type DocumentLetFunc func(Document) ([]Document, error)

func (f DocumentLetFunc) Query(query Document) ([]Document, error) {
	return f(query)
}

// ---

var emptyDocuments = make([]Document, 0)

type EmptyDatabaseLet struct{}

func (e *EmptyDatabaseLet) Query(query Document) ([]Document, error) {
	return emptyDocuments, nil
}

type ErrorDatabaseLet struct {
	Err error
}

func (e *ErrorDatabaseLet) Query(query Document) ([]Document, error) {
	return nil, e.Err
}

// -----

type StaticDocuments struct {
	Documents []Document
}

func (s *StaticDocuments) Query(query Document) ([]Document, error) {
	return s.Documents, nil
}

//

// Document transformer

/// ---------

// actual implementations

// ---

// ----------

/// -------------------------------

//

type PanicBarrier struct {
	Source DatabaseLet
}

func (p *PanicBarrier) Query(query Document) ([]Document, error) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("panic:", r)
		}
	}()

	return p.Source.Query(query)
}

type Panic struct {
}

func (p *Panic) Query(query Document) ([]Document, error) {
	panic("panic")
	return nil, nil
}

type Tracer struct {
	Source DatabaseLet
}

func (t *Tracer) Query(query Document) ([]Document, error) {

	trace := make(Document)

	trace["query"] = query

	docs, err := t.Source.Query(query)
	trace["docs"] = docs
	trace["error"] = err

	Print("TRACE %s", trace.String())
	if err != nil {
		return nil, err
	}

	return docs, err
}

// --
