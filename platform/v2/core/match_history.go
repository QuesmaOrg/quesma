// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package quesma_api

import (
	"github.com/QuesmaOrg/quesma/platform/v2/core/routes"
	"slices"
	"strings"
	"sync"
)

type (
	Statistics struct {
		Matched   []string
		Unmatched []string
	}
	routerStatisticsAccumulator struct {
		mu        *sync.Mutex
		matched   map[string]bool
		unmatched map[string]bool
	}
)

// TODO make it bounded and use RWMutex
var routerStatistics = routerStatisticsAccumulator{
	mu:        &sync.Mutex{},
	matched:   make(map[string]bool),
	unmatched: make(map[string]bool)}

func MatchStatistics() Statistics {
	return routerStatistics.snapshot()
}

const maskStars = "*****"

func normalizeUrl(url string) string {
	if strings.HasPrefix(url, routes.AsyncSearchIdPrefix) {
		return routes.AsyncSearchIdPrefix + maskStars
	}
	if strings.HasPrefix(url, routes.KibanaInternalPrefix) {
		return routes.KibanaInternalPrefix + maskStars
	}
	return url
}

func (a *routerStatisticsAccumulator) addMatched(url string) {
	a.withLock(func() {
		a.matched[normalizeUrl(url)] = true
	})
}

func (a *routerStatisticsAccumulator) addUnmatched(url string) {
	a.withLock(func() {
		a.unmatched[normalizeUrl(url)] = true
	})
}

func (a *routerStatisticsAccumulator) snapshot() Statistics {
	var matched []string
	var nonmatched []string

	a.withLock(func() {
		for k := range a.unmatched {
			nonmatched = append(nonmatched, k)
		}

		for k := range a.matched {
			matched = append(matched, k)
		}
	})

	slices.Sort(matched)
	slices.Sort(nonmatched)
	return Statistics{matched, nonmatched}
}

func (a *routerStatisticsAccumulator) withLock(action func()) {
	a.mu.Lock()
	action()
	a.mu.Unlock()
}

func (s Statistics) GroupByFirstSegment() (sortedMatchedKeys []string, matched map[string][]string, sortedUnmatchedKeys []string, unmatched map[string][]string) {
	matched = make(map[string][]string)
	unmatched = make(map[string][]string)

	for _, url := range s.Matched {
		segments := strings.Split(url, "/")
		if len(segments) > 1 {
			matched["/"+segments[1]] = append(matched["/"+segments[1]], url)
		}
	}

	for _, url := range s.Unmatched {
		segments := strings.Split(url, "/")
		if len(segments) > 1 {
			unmatched["/"+segments[1]] = append(unmatched["/"+segments[1]], url)
		}
	}

	for _, paths := range matched {
		slices.Sort(paths)
	}

	for _, paths := range unmatched {
		slices.Sort(paths)
	}

	for k := range matched {
		sortedMatchedKeys = append(sortedMatchedKeys, k)
	}
	for k := range unmatched {
		sortedUnmatchedKeys = append(sortedUnmatchedKeys, k)
	}

	slices.Sort(sortedMatchedKeys)
	slices.Sort(sortedUnmatchedKeys)

	return sortedMatchedKeys, matched, sortedUnmatchedKeys, unmatched
}
