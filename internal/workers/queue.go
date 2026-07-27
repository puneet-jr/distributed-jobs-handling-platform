package workers

import (
	"context"
	"time"
)

// ID is the Redis stream message ID. We need it for ACK.
// JobID is the Postgres job ID. We use it to load the real job.
// Type helps route the job to the correct handler.
type Message struct {
	ID    string
	JobID string
	Type  string
}

type QueueConsumer interface {
	Read(ctx context.Context, consumer string, count int) ([]Message, error)
	Ack(ctx context.Context, messageID string) error
}

type PendingReclaimer interface {
	Reclaim(ctx context.Context, consumer string, minIdle time.Duration, count int) ([]Message, error)
}
