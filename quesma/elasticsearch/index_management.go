package elasticsearch

import (
	"context"
	"mitmproxy/quesma/logger"
	"mitmproxy/quesma/quesma/recovery"
	"sync/atomic"
	"time"
)

type (
	IndexManagement interface {
		startable
		ReloadIndices()
		GetIndices() Sources
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

func (im *indexManagement) GetIndices() Sources {
	return *im.sources.Load()
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
