// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package controller

import (
	"github.com/k0kubun/pp"
	"quesma/ab_testing"
)

type facade struct {
	delegate ab_testing.ResultsRepository
}

func NewFacade(repository ab_testing.ResultsRepository) ab_testing.ResultsRepository {
	return &facade{
		delegate: repository,
	}
}

func (d *facade) Store(data ab_testing.Result) {

	pp.Println("XXXX Facade Store", data)

	if d.delegate != nil {
		d.delegate.Store(data)
	}
}
