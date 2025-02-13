// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package qpl_experiment

type Row []string
type Table struct {
	Names Row
	Rows  []Row
}

func EmptyTable() Table {
	return Table{}
}

func AlmostEmptyTable() Table {
	return Table{
		Names: Row{"id"},
		Rows:  []Row{{"1"}},
	}
}

type TableValueFunction interface {
	Fn(t Table) (Table, error)
}

type Pipeline []TableValueFunction

func (p Pipeline) Fn(tabular Table) (Table, error) {

	var err error
	for _, fn := range p {
		tabular, err = fn.Fn(tabular)
		if err != nil {
			return tabular, err
		}
	}

	return tabular, nil
}
