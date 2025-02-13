// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package qpl_experiment

import (
	"os"
	"text/tabwriter"
)

type PrintTVF struct {
}

func (p *PrintTVF) Fn(tabular Table) (Table, error) {

	w := tabwriter.NewWriter(os.Stdout, 1, 1, 1, ' ', 0)

	for i, name := range tabular.Names {
		if i == len(tabular.Names)-1 {
			w.Write([]byte(name))
		} else {
			w.Write([]byte(name + "\t"))
		}
	}
	w.Write([]byte("\n"))

	for _, row := range tabular.Rows {

		for i, cell := range row {
			if i == len(row)-1 {
				w.Write([]byte(cell))
			} else {
				w.Write([]byte(cell + "\t"))
			}
		}
		w.Write([]byte("\n"))

	}
	w.Flush()

	return tabular, nil
}
