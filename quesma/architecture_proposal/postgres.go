package main

import (
	"context"
	"fmt"
	wire "github.com/jeroenrinzema/psql-wire"
	"github.com/lib/pq/oid"
	"log"
)

type postgreSqlServer struct {
	Source DatabaseLet
}

func (p *postgreSqlServer) startAndListen(addr string) {

	go func() {
		wire.ListenAndServe(addr, p.handler)
	}()
}

func cellValue(a interface{}) string {

	switch a := a.(type) {
	case *int:
		if a != nil {
			return fmt.Sprintf("%d", *a)
		} else {
			return "nil"
		}

	case *string:
		if a != nil {
			return *a
		} else {
			return "nil"
		}
	case *int64:
		if a != nil {
			return fmt.Sprintf("%d", *a)
		} else {
			return "nil"
		}

	case *interface{}:
		if a != nil {
			return fmt.Sprintf("%v", *a)
		} else {
			return "nil"
		}
	default:
		return fmt.Sprintf("%v", a)
	}

}

func (p *postgreSqlServer) handler(ctx context.Context, query string) (wire.PreparedStatements, error) {
	log.Println("incoming SQL query:", query)

	documentQuery := NewDocument()
	documentQuery["query"] = query

	docs, err := p.Source.Query(documentQuery)

	if err != nil {
		return nil, err
	}

	var columns []string

	if len(docs) == 0 {
		return nil, fmt.Errorf("no rows returned")
	}

	for key := range docs[0] {
		columns = append(columns, key)
	}

	pgColumns := make([]wire.Column, len(columns))

	for i, column := range columns {
		pgColumns[i] = wire.Column{
			Name:  column,
			Oid:   oid.T_text,
			Width: 256,
		}
	}

	handle := func(ctx context.Context, writer wire.DataWriter, parameters []wire.Parameter) error {

		for n, doc := range docs {

			fmt.Println("postgresql doc:", n, doc)

			row := make([]any, len(columns))
			for i, column := range columns {
				row[i] = cellValue(doc[column])
			}

			fmt.Println("postgresql row:", row)
			err := writer.Row(row)

			if err != nil {
				return err
			}
		}

		return writer.Complete(query)
	}

	return wire.Prepared(wire.NewStatement(handle, wire.WithColumns(pgColumns))), nil
}
