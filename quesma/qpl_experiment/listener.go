// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package qpl_experiment

import (
	"context"
	wire "github.com/jeroenrinzema/psql-wire"
	"github.com/lib/pq/oid"
	"log"
)

type postgreSqlServer struct {
}

func (p *postgreSqlServer) startAndListen(addr string) {

	log.Println("Starting PostgresSQL server")

	server, err := wire.NewServer(p.handler)
	if err != nil {
		log.Fatalf("failed to create server: %v", err)
	}

	server.Version = "17.0"

	server.ListenAndServe(addr)

}

func (p *postgreSqlServer) handler(ctx context.Context, query string) (wire.PreparedStatements, error) {
	log.Println("incoming Query Query:", query)

	input := AlmostEmptyTable()

	fn, err := ParseQPL(query)

	if err != nil {
		return nil, err
	}

	result, err := fn.Fn(input)
	if err != nil {
		return nil, err
	}

	pgColumns := make([]wire.Column, len(result.Names))

	for i, column := range result.Names {
		pgColumns[i] = wire.Column{
			Name:  column,
			Oid:   oid.T_text,
			Width: 256,
		}
	}

	handle := func(ctx context.Context, writer wire.DataWriter, parameters []wire.Parameter) error {

		for n := range result.Rows {
			row := make([]any, len(result.Names))
			for i, column := range result.Rows[n] {
				row[i] = column
			}
			err := writer.Row(row)
			if err != nil {
				return err
			}
		}

		return writer.Complete(query)
	}

	return wire.Prepared(wire.NewStatement(handle, wire.WithColumns(pgColumns))), nil
}
