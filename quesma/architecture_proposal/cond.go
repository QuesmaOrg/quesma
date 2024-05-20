package main

import "fmt"

type Dispatcher struct {
	Sources       map[string]DatabaseLet
	DispatchField string
}

func (d *Dispatcher) Query(query JSON) ([]JSON, error) {

	field, ok := query[d.DispatchField]
	if !ok {
		return nil, fmt.Errorf("missing dispatch field: %s", d.DispatchField)
	}

	source, ok := d.Sources[field.(string)]
	if !ok {
		return nil, fmt.Errorf("no source for field: %s", field)
	}

	return source.Query(query)

}

// -----------------

type If struct {
	condition func() bool
	True      DatabaseLet
	False     DatabaseLet
}

func (i *If) Query(query JSON) ([]JSON, error) {
	if i.condition() {
		return i.True.Query(query)
	}
	return i.False.Query(query)
}
