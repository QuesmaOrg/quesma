// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package persistence

import "fmt"

type EvictorInterface interface {
	SelectToEvict(documents []*document, sizeNeeded int64) (indexesToEvict []string, bytesEvicted int64)
}

// It's only 1 implementation, which looks well suited for ElasticSearch.
// It can be implemented differently.
type Evictor struct{}

func (e *Evictor) SelectToEvict(documents []*document, sizeNeeded int64) (indexesToEvict []string, bytesEvicted int64) {
	if sizeNeeded <= 0 {
		return // check if it's empty array or nil
	}
	fmt.Println("kk dbg SelectToEvict() sizeNeeded:", sizeNeeded)

	countByIndex := make(map[string]int)
	countByIndexMarkedAsDeleted := make(map[string]int)

	for _, doc := range documents {
		countByIndex[doc.Index]++
		if doc.MarkedAsDeleted {
			countByIndexMarkedAsDeleted[doc.Index]++
		}
	}

	for index, markedAsDeletedCnt := range countByIndexMarkedAsDeleted {
		if countByIndex[index] == markedAsDeletedCnt {
			indexesToEvict = append(indexesToEvict, index)
		}
	}

	for _, doc := range documents {
		if countByIndex[doc.Index] == countByIndexMarkedAsDeleted[doc.Index] {
			bytesEvicted += doc.SizeInBytes
		}
	}

	return
}
