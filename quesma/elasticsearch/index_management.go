package elasticsearch

import (
	"context"
	"mitmproxy/quesma/logger"
	"mitmproxy/quesma/quesma/recovery"
	"regexp"
	"strings"
	"sync/atomic"
	"time"
)

type (
	IndexManagement interface {
		startable
		ReloadIndices()
		GetSources() Sources
		GetSourceNames() map[string]interface{}
		GetSourceNamesMatching(indexPattern string) map[string]interface{}
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

func (im *indexManagement) GetSourceNames() map[string]interface{} {
	names := make(map[string]interface{})
	sources := *im.sources.Load()
	for _, stream := range sources.DataStreams {
		names[stream.Name] = struct{}{}
	}
	for _, index := range sources.Indices {
		names[index.Name] = struct{}{}
	}
	for _, alias := range sources.Aliases {
		names[alias.Name] = struct{}{}
	}
	return names
}

func (im *indexManagement) GetSourceNamesMatching(indexPattern string) map[string]interface{} {
	all := im.GetSourceNames()
	filtered := make(map[string]interface{})

	if indexPattern == "*" || indexPattern == "_all" || indexPattern == "" {
		return all
	} else {
		for key := range all {
			if matches(indexPattern, key) {
				filtered[key] = struct{}{}
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

func matches(indexName string, indexNamePattern string) bool {
	r, err := regexp.Compile(strings.Replace(indexNamePattern, "*", ".*", -1))
	if err != nil {
		logger.Error().Msgf("invalid index name pattern [%s]: %s", indexNamePattern, err)
		return false
	}

	return r.MatchString(indexName)
}
