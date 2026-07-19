package job

import "context"

type Repository interface {
	Create(ctx context.Context, job *Job) error
	GetByID(ctx context.Context, id string) (*Job, error)
	GetByIdempotencyKey(ctx context.Context, key string) (*Job, error)
	UpdateStatus(ctx context.Context, id string, status Status, workerID *string, errMsg *string) error
	IncrementRetryCount(ctx context.Context, id string, errMsg string) error
	List(ctx context.Context, limit, offset int) ([]Job, error)
}
