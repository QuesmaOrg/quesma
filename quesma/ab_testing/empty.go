// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package ab_testing

type emptyResultsRepository struct{}

func (e *emptyResultsRepository) Store(result Result) {
	// do nothing
}

func NewEmptyResultsRepository() ResultsRepository {
	return &emptyResultsRepository{}
}
