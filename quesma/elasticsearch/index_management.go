package elasticsearch

import (
	"context"
	"mitmproxy/quesma/logger"
	"mitmproxy/quesma/quesma/config"
	"mitmproxy/quesma/quesma/recovery"
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

func NewIndexManagement(elasticsearchUrl string) IndexManagement {
	return &indexManagement{
		ElasticsearchUrl: elasticsearchUrl,
		indexResolver:    NewIndexResolver(elasticsearchUrl),
	}
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
	return *im.sources.Load()
}

func (im *indexManagement) GetSourceNames() map[string]bool {
	names := make(map[string]bool)
	sources := *im.sources.Load()
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
		recovery.LogPanic()
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
