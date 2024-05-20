package main

import (
	"encoding/json"
	"fmt"
)

func NewJSON() JSON {
	return make(JSON)
}

func (d JSON) String() string {
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

type DocumentLetFunc func(JSON) ([]JSON, error)

func (f DocumentLetFunc) Query(query JSON) ([]JSON, error) {
	return f(query)
}

// ---

var emptyList = make([]JSON, 0)

type EmptyDatabaseLet struct{}

func (e *EmptyDatabaseLet) Query(query JSON) ([]JSON, error) {
	return emptyList, nil
}

type ErrorDatabaseLet struct {
	Err error
}

func (e *ErrorDatabaseLet) Query(query JSON) ([]JSON, error) {
	return nil, e.Err
}

// -----

type StaticDocuments struct {
	Contents []JSON
}

func (s *StaticDocuments) Query(query JSON) ([]JSON, error) {
	return s.Contents, nil
}

//

// JSON transformer

/// ---------

// actual implementations

// ---

// ----------

/// -------------------------------

//

type PanicBarrier struct {
	Source DatabaseLet
}

func (p *PanicBarrier) Query(query JSON) ([]JSON, error) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("panic:", r)
		}
	}()

	return p.Source.Query(query)
}

type Panic struct {
}

func (p *Panic) Query(query JSON) ([]JSON, error) {
	panic("panic")
	return nil, nil
}

type Tracer struct {
	Source DatabaseLet
}

func (t *Tracer) Query(query JSON) ([]JSON, error) {

	trace := make(JSON)

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
