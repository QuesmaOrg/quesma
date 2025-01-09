// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package es_to_ch_ingest

import (
	"github.com/QuesmaOrg/quesma/quesma/quesma/functionality/bulk"
	"github.com/QuesmaOrg/quesma/quesma/quesma/types"
)

type (
	// BulkRequestEntry is redeclared here as its using private fields
	// and the whole point of this experiment is not to mess too much with the v1 code
	BulkRequestEntry struct {
		operation string
		index     string
		document  types.JSON
		response  *bulk.BulkItem
	}
)
