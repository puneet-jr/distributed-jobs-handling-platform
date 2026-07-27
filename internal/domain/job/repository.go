package job

import (
	"context"
	"time"
)

type Repository interface {
	Create(ctx context.Context, job *Job) error
	GetByID(ctx context.Context, id string) (*Job, error)
	GetByIdempotencyKey(ctx context.Context, key string) (*Job, error)
	UpdateStatus(ctx context.Context, id string, status Status, workerID *string, errMsg *string) error
	UpdateStatusIfCurrent(ctx context.Context, id string, from []Status, to Status, workerID *string, errMsg *string) error
	IncrementRetryCount(ctx context.Context, id string, errMsg string) error
	ClaimRunnable(ctx context.Context, id string, workerID string) error
	ScheduleRetry(ctx context.Context, id string, workerID string, errMsg string, nextRunAt time.Time) error
	ReclaimStaleRunning(ctx context.Context, staleBefore time.Time, limit int) ([]Job, error)
	ListRunnableRetries(ctx context.Context, limit int) ([]Job, error)
	List(ctx context.Context, limit, offset int) ([]Job, error)
}
