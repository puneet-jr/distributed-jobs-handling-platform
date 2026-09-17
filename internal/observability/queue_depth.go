package observability

import (
	"context"
	"fmt"
	"log/slot"
	"time"

	"github.com/redis/go-redis/v9"
)

type QueueDepthCollector struct {
	client *redis.Client
	stream string
	interval time.Duration
	metrics *Metrics
}

func NewQueueDepthCollector(
	client *redis.Client,
	stream string,
	interval time.Duration,
	metrics *Metrics,
) (*QueueDepthCollector, error) {
	if client == nil {
		return nil, fmt.Errorf("redis client is required")
	}
	if stream == "" {
		return nil, fmt.Errorf("stream name is required")
	}
	return &QueueDepthCollector {
	client: client,
	stream : stream,
	interval: interval,
	metrics: metrics,
}, nil
}

func ( c*QueueDepthCollector) Run(ctx context.Context, logger *slog.Logger) {
	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()

	for {
		if err := c.collect(ctx); err != nil && logger != nil {
			logger.Error("failed to collect queue depth","error",err)
		}
		c.metrics.QueueDepth.Set(float64(depth))
		return nil
}
