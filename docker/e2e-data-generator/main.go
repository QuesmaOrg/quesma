// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package main

import (
	"fmt"
	"math/rand"
	"quesma/e2e-data-generator/tables"
	"sync"
	"time"
)

var TABLES = []table{tables.E2eTable1{}}

type table interface {
	GenerateCreateTableString() string
	GenerateOneRow(r *rand.Rand) (clickhouse, elastic string)
	Name() string
	RowsNr() int
}

func generateOneTable(table table, r *rand.Rand) {
	clickhouseRows := make([]string, 0, table.RowsNr())
	elasticRows := make([]string, 0, table.RowsNr())
	aa := false
	for range table.RowsNr() {
		clickhouse, elastic := table.GenerateOneRow(r)
		if !aa {
			fmt.Println(clickhouse, elastic)
			aa = true
		}
		clickhouseRows = append(clickhouseRows, clickhouse)
		elasticRows = append(elasticRows, elastic)
	}

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		sendCommandToClickhouse(table.GenerateCreateTableString())
		fmt.Printf("Clickhouse table %s created\n", table.Name())

		insertToClickhouse(table.Name(), clickhouseRows)
		fmt.Printf("Data of %d random rows inserted into Clickhouse's %s\n", table.RowsNr(), table.Name())
	}()
	go func() {
		defer wg.Done()
		createIndexElastic(table.Name())
		fmt.Printf("Elastic index %s created\n", table.Name())

		insertToElastic(table.Name(), elasticRows)
		fmt.Printf("Data of %d random rows inserted into Elastic's %s\n", table.RowsNr(), table.Name())
	}()

	wg.Wait()
}

func main() {
	seed := time.Now().UnixNano() // change to some constant to reproduce the same data
	r := rand.New(rand.NewSource(seed))
	fmt.Println("E2E data generator started, seed:", seed)
	for _, t := range TABLES {
		generateOneTable(t, r)
	}
}
