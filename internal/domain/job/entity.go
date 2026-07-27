package job

import (
	"context"
	"time"
)

type Job struct {
	ID             string
	Type           string
	Status         Status
	Priority       int
	Payload        []byte
	RetryCount     int
	MaxRetries     int
	IdempotencyKey string
	ErrorMessage   *string
	WorkerID       *string
	CreatedAt      time.Time
	NextRunAt      *time.Time
	StartedAt      *time.Time
	CompletedAt    *time.Time
}

type QueueMessage struct {
	JobID string
	Type  string
}

type Queue interface {
	Enqueue(ctx context.Context, msg QueueMessage) error
}
