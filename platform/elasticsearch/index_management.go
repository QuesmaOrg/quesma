// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package elasticsearch

import (
	"context"
	"github.com/QuesmaOrg/quesma/platform/config"
	"github.com/QuesmaOrg/quesma/platform/logger"
	"github.com/QuesmaOrg/quesma/platform/recovery"
	"github.com/QuesmaOrg/quesma/platform/util"
	"strings"
	"sync/atomic"
	"time"
)

type (
	IndexManagement interface {
		startable
		ReloadIndices()
		GetSources() Sources
		GetSourceNames() map[string]bool
		GetSourceNamesMatching(indexPattern string) map[string]bool
		Resolve(indexPattern string) (Sources, bool, error)
	}
	indexManagement struct {
		ElasticsearchUrl string
		indexResolver    IndexResolver
		sources          atomic.Pointer[Sources]
		ctx              context.Context
		cancel           context.CancelFunc
	}
	startable interface {
		Start()
		Stop()
	}
)

func NewIndexManagement(elasticsearch config.ElasticsearchConfiguration) IndexManagement {
	return &indexManagement{
		ElasticsearchUrl: elasticsearch.Url.String(),
		indexResolver:    NewIndexResolver(elasticsearch),
	}
}

func (im *indexManagement) Resolve(indexPattern string) (Sources, bool, error) {
	return im.indexResolver.Resolve(indexPattern)
}

func (im *indexManagement) ReloadIndices() {
	sources, _, err := im.indexResolver.Resolve("*")
	if err != nil {
		logger.Error().Msgf("Failed to reload indices: %v", err)
		return
	}
	im.sources.Store(&sources)
}

func (im *indexManagement) GetSources() Sources {
	if s := im.sources.Load(); s != nil {
		return *s
	} else {
		logger.Warn().Msg("Indices are not yet loaded, returning empty sources.")
		return Sources{}
	}
}

func (im *indexManagement) GetSourceNames() map[string]bool {
	names := make(map[string]bool)
	sources := im.GetSources()
	for _, stream := range sources.DataStreams {
		names[stream.Name] = true
	}
	for _, index := range sources.Indices {
		names[index.Name] = true
	}
	for _, alias := range sources.Aliases {
		names[alias.Name] = true
	}
	for key := range names {
		if strings.TrimSpace(key) == "" {
			delete(names, key)
		}
	}
	return names
}

func (im *indexManagement) GetSourceNamesMatching(indexPattern string) map[string]bool {
	all := im.GetSourceNames()
	filtered := make(map[string]bool)

	if indexPattern == "*" || indexPattern == "_all" || indexPattern == "" {
		return all
	} else {
		for key := range all {
			if config.MatchName(indexPattern, key) {
				filtered[key] = true
			}
		}
	}
	return filtered
}

func (im *indexManagement) Start() {
	im.ReloadIndices()
	im.ctx, im.cancel = context.WithCancel(context.Background())

	go func() {
		defer recovery.LogPanic()
		for {
			select {
			case <-im.ctx.Done():
				logger.Debug().Msg("closing elasticsearch index management")
				return
			case <-time.After(1 * time.Minute): // TODO make it configurable
				im.ReloadIndices()
			}
		}
	}()
}

func (im *indexManagement) Stop() {
	im.cancel()
}

func NewFixedIndexManagement(indexes ...string) IndexManagement {
	return stubIndexManagement{indexes: indexes}
}

type stubIndexManagement struct {
	indexes []string
}

func (s stubIndexManagement) Start()         {}
func (s stubIndexManagement) Stop()          {}
func (s stubIndexManagement) ReloadIndices() {}
func (s stubIndexManagement) GetSources() Sources {
	var dataStreams = []DataStream{}
	for _, index := range s.indexes {
		dataStreams = append(dataStreams, DataStream{Name: index})
	}
	return Sources{DataStreams: dataStreams}
}
func (s stubIndexManagement) Resolve(_ string) (Sources, bool, error) {
	return Sources{}, true, nil
}

func (s stubIndexManagement) GetSourceNames() map[string]bool {
	var result = make(map[string]bool)
	for _, index := range s.indexes {
		result[index] = true
	}
	return result
}

func (s stubIndexManagement) GetSourceNamesMatching(indexPattern string) map[string]bool {
	var result = make(map[string]bool)
	for _, index := range s.indexes {
		if matches, err := util.IndexPatternMatches(indexPattern, index); err == nil && matches {
			result[index] = true
		} else {
			logger.Error().Err(err)
		}
	}
	return result
}
