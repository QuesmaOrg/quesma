// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package stats

import (
	"fmt"
	"github.com/QuesmaOrg/quesma/platform/types"
	"github.com/QuesmaOrg/quesma/platform/util"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

var mu sync.Mutex
var GlobalStatistics = &Statistics{}

const (
	// we gather statistics only for the first 10000 requests
	STATISTICS_LIMIT = 10000
)

type (
	Statistics       map[string]*IngestStatistics
	IngestStatistics struct {
		IndexName string
		Requests  int64
		Keys      map[string]*KeyStatistics
	}
	KeyStatistics struct {
		KeyName         string
		Occurrences     int64
		Values          map[string]*ValueStatistics
		NonSchemaValues map[string]*ValueStatistics
	}
	ValueStatistics struct {
		ValueName   string
		Occurrences int64
		Types       []string
	}
)

func (s *Statistics) String() string {
	var result strings.Builder

	mu.Lock()
	defer mu.Unlock()

	for indexName, ingestStats := range *s {
		result.WriteString(fmt.Sprintf("Index: %s\n", indexName))
		result.WriteString(fmt.Sprintf("  Requests: %d\n", ingestStats.Requests))

		for keyName, keyStats := range ingestStats.Keys {
			result.WriteString(fmt.Sprintf("  %s\n", keyName))
			result.WriteString(fmt.Sprintf("    Occurrences: %d\n", keyStats.Occurrences))

			for value, count := range keyStats.Values {
				result.WriteString(fmt.Sprintf("    Value: %s, Count: %+v\n", value, count))
			}
		}
	}

	return result.String()
}

func New() *Statistics {
	statistics := make(Statistics)
	return &statistics
}

func (s *Statistics) getValueStatisticsPtr(keyStatistics *KeyStatistics, nonSchemaFields bool) *map[string]*ValueStatistics {
	switch nonSchemaFields {
	case true:
		return &keyStatistics.NonSchemaValues
	default:
		return &keyStatistics.Values
	}
}

func (s *Statistics) process(index string,
	jsonData types.JSON, nonSchemaFields bool, nestedSeparator string) {

	flatJson := util.FlattenMap(jsonData, nestedSeparator)

	statistics, ok := (*s)[index]
	if !ok {
		statistics = &IngestStatistics{IndexName: index, Keys: make(map[string]*KeyStatistics)}
		(*s)[index] = statistics
	}
	// TODO as proper eviction strategy requires some time
	// to be implemented, we limit the number of requests for now
	if statistics.Requests >= STATISTICS_LIMIT {
		return
	}

	for key, value := range flatJson {
		keyStatistics, ok := statistics.Keys[key]
		if !ok {
			keyStatistics = &KeyStatistics{KeyName: key, Values: make(map[string]*ValueStatistics),
				NonSchemaValues: make(map[string]*ValueStatistics)}
			statistics.Keys[key] = keyStatistics
		}

		if !nonSchemaFields {
			keyStatistics.Occurrences++
		}
		valueString := fmt.Sprintf("%v", value)
		valuesPtr := s.getValueStatisticsPtr(keyStatistics, nonSchemaFields)
		valueStatistics, ok := (*valuesPtr)[valueString]
		if !ok {
			valueStatistics = &ValueStatistics{ValueName: valueString}
			(*valuesPtr)[valueString] = valueStatistics
		}
		(*valuesPtr)[valueString].Occurrences++
		valueStatistics.Types = typesOf(valueString)
	}
}

func (s *Statistics) Process(ingestStatsEnabled bool, index string, jsonData types.JSON, nestedSeparator string) {

	mu.Lock()
	defer mu.Unlock()

	if ingestStatsEnabled {
		s.process(index, jsonData, false, nestedSeparator)
	}
	if statistics, ok := (*s)[index]; ok && statistics.Requests < STATISTICS_LIMIT {
		statistics.Requests++
	}
}

func (s *Statistics) UpdateNonSchemaValues(ingestStatsEnabled bool, index string, jsonData types.JSON, nestedSeparator string) {

	mu.Lock()
	defer mu.Unlock()

	if ingestStatsEnabled {
		s.process(index, jsonData, true, nestedSeparator)
	}
}

func (s *Statistics) GetIngestStatistics(indexName string) (*IngestStatistics, error) {
	mu.Lock()
	defer mu.Unlock()
	if stats, ok := (*s)[indexName]; ok {
		return stats, nil
	} else {
		return nil, fmt.Errorf("index %s not found", indexName)
	}
}

func (s *Statistics) SortedIndexNames() (result []*IngestStatistics) {
	mu.Lock()
	for _, value := range *s {
		result = append(result, value)
	}
	mu.Unlock()

	sort.Slice(result, func(i, j int) bool {
		return result[i].IndexName < result[j].IndexName
	})

	return result
}

func (is *IngestStatistics) SortedKeyStatistics() (result []*KeyStatistics) {
	mu.Lock()
	for _, value := range is.Keys {
		result = append(result, value)
	}
	mu.Unlock()

	sort.Slice(result, func(i, j int) bool {
		return result[i].KeyName < result[j].KeyName
	})

	return result
}

func topNValuesHelper(n int, values map[string]*ValueStatistics) (result []*ValueStatistics) {
	mu.Lock()
	for _, value := range values {
		result = append(result, value)
	}
	mu.Unlock()

	sort.Slice(result, func(i, j int) bool {
		if result[i].Occurrences == result[j].Occurrences {
			return result[i].ValueName < result[j].ValueName
		}

		return result[i].Occurrences > result[j].Occurrences
	})

	return result[:n]
}

func (vs *KeyStatistics) TopNValues(n int) (result []*ValueStatistics) {
	return topNValuesHelper(n, vs.Values)
}

func (vs *KeyStatistics) TopNInvalidValues(n int) (result []*ValueStatistics) {
	return topNValuesHelper(n, vs.NonSchemaValues)
}

func typesOf(str string) (types []string) {
	if isBool(str) {
		types = append(types, "bool")
	}

	if isInt(str) {
		types = append(types, "int")
	}

	if isFloat(str) {
		types = append(types, "float")
	}

	if isDate(str) {
		types = append(types, "date")
	}

	return append(types, "string")
}

func isInt(str string) bool {
	_, err := strconv.Atoi(str)
	return err == nil
}

func isBool(str string) bool {
	switch strings.ToLower(str) {
	case "true", "false":
		return true
	default:
		return false
	}
}

func isFloat(str string) bool {
	_, err := strconv.ParseFloat(str, 64)
	return err == nil
}

func isDate(str string) bool {
	dateLayouts := []string{
		"2006-01-02",
		"01/02/2006",
		"02-Jan-2006",
		"January 02, 2006",
		"Jan 02, 2006",
		"02-January-2006",
		"2006-01-02 15:04:05",
		"01/02/2006 15:04:05",
		"02-Jan-2006 15:04:05",
		"January 02, 2006 15:04:05",
		"Jan 02, 2006 15:04:05",
		"02-January-2006 15:04:05",
		"2006-01-02T15:04:05-0700",
		"2006-01-02T15:04:05.999Z",
	}

	for _, layout := range dateLayouts {
		if _, err := time.Parse(layout, str); err == nil {
			return true
		}
	}

	return false
}
