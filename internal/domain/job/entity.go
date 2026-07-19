package job

import "time"

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
	StartedAt      *time.Time
	CompletedAt    *time.Time
}
