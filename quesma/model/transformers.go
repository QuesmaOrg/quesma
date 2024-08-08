// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package model

type QueryTransformer interface {
	Transform(query []*Query) ([]*Query, error)
}
