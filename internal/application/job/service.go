package job

import (
	"context"
	"errors"
	"math"
	"time"

	domainjob "distributed-job-platform/internal/domain/job"
	"github.com/google/uuid"
)

var ErrAlreadyClaimed = errors.New("job is not claimable")

type Service struct {
	repo  domainjob.Repository
	queue domainjob.Queue
}

func NewService(repo domainjob.Repository, queue domainjob.Queue) *Service {
	return &Service{
		repo:  repo,
		queue: queue,
	}
}

func (s *Service) Create(ctx context.Context, in CreateJobRequest) (*CreateJobResponse, error) {
	if in.Type == "" {
		return nil, errors.New("type is required")
	}
	if len(in.Payload) == 0 {
		return nil, errors.New("payload is required")
	}

	if in.IdempotencyKey != "" {
		existing, err := s.repo.GetByIdempotencyKey(ctx, in.IdempotencyKey)
		if err == nil && existing != nil {
			return &CreateJobResponse{
				JobID:  existing.ID,
				Status: existing.Status,
			}, nil
		}
		if err != nil && !errors.Is(err, domainjob.ErrJobNotFound) {
			return nil, err
		}
	}

	now := time.Now().UTC()
	job := &domainjob.Job{
		ID:             uuid.NewString(),
		Type:           in.Type,
		Status:         domainjob.StatusPending,
		Priority:       in.Priority,
		Payload:        in.Payload,
		RetryCount:     0,
		MaxRetries:     5,
		IdempotencyKey: in.IdempotencyKey,
		CreatedAt:      now,
	}

	if err := s.repo.Create(ctx, job); err != nil {
		if errors.Is(err, domainjob.ErrDuplicateIdempotencyKey) && in.IdempotencyKey != "" {
			existing, lookupErr := s.repo.GetByIdempotencyKey(ctx, in.IdempotencyKey)
			if lookupErr != nil {
				return nil, lookupErr
			}

			return &CreateJobResponse{
				JobID:  existing.ID,
				Status: existing.Status,
			}, nil
		}

		return nil, err
	}

	if s.queue == nil {
		return nil, errors.New("job queue is required")
	}

	if err := s.queue.Enqueue(ctx, domainjob.QueueMessage{
		JobID: job.ID,
		Type:  job.Type,
	}); err != nil {
		return nil, err
	}

	return &CreateJobResponse{
		JobID:  job.ID,
		Status: job.Status,
	}, nil
}

func (s *Service) GetByID(ctx context.Context, id string) (*GetJobResponse, error) {
	job, err := s.repo.GetByID(ctx, id)
	if err != nil {
		return nil, err
	}

	return &GetJobResponse{
		ID:           job.ID,
		Type:         job.Type,
		Status:       job.Status,
		Priority:     job.Priority,
		RetryCount:   job.RetryCount,
		ErrorMessage: job.ErrorMessage,
	}, nil
}

func (s *Service) GetDomainJobByID(ctx context.Context, id string) (*domainjob.Job, error) {
	return s.repo.GetByID(ctx, id)
}

func (s *Service) MarkRunning(
	ctx context.Context,
	id string,
	workerID string,
) error {
	if workerID == "" {
		return errors.New("worker id is required")
	}

	if err := s.repo.ClaimRunnable(ctx, id, workerID); err != nil {
		if errors.Is(err, domainjob.ErrInvalidStatusTransition) {
			return ErrAlreadyClaimed
		}
		return err
	}
	return nil
}

func (s *Service) MarkCompleted(ctx context.Context, id string, workerID string) error {
	if workerID == "" {
		return errors.New("worker id is required")
	}

	return s.repo.UpdateStatusIfCurrent(
		ctx,
		id,
		[]domainjob.Status{
			domainjob.StatusRunning,
		},
		domainjob.StatusCompleted,
		&workerID,
		nil,
	)
}

func (s *Service) MarkFailed(
	ctx context.Context,
	id string,
	workerID string,
	errMsg string,
) error {
	if workerID == "" {
		return errors.New("worker id is required")
	}
	if errMsg == "" {
		errMsg = "job failed"
	}

	return s.repo.UpdateStatusIfCurrent(
		ctx,
		id,
		[]domainjob.Status{
			domainjob.StatusRunning,
			domainjob.StatusRetrying,
		},
		domainjob.StatusFailed,
		&workerID,
		&errMsg,
	)
}

func (s *Service) MarkRetrying(
	ctx context.Context,
	id string,
	workerID string,
	errMsg string,
) error {
	if workerID == "" {
		return errors.New("worker id is required")
	}
	if errMsg == "" {
		errMsg = "job retry scheduled"
	}

	job, err := s.repo.GetByID(ctx, id)
	if err != nil {
		return err
	}

	if job.RetryCount >= job.MaxRetries {
		return s.MarkFailed(ctx, id, workerID, errMsg)
	}

	nextRunAt := time.Now().UTC().Add(retryBackoff(job.RetryCount + 1))
	return s.repo.ScheduleRetry(ctx, id, workerID, errMsg, nextRunAt)
}

func (s *Service) Cancel(ctx context.Context, id string) error {
	return s.repo.UpdateStatusIfCurrent(
		ctx,
		id,
		[]domainjob.Status{
			domainjob.StatusPending,
			domainjob.StatusRetrying,
			domainjob.StatusRunning,
		},
		domainjob.StatusCancelled,
		nil,
		nil,
	)
}

func (s *Service) RequeueRunnableRetries(ctx context.Context, limit int) (int, error) {
	if s.queue == nil {
		return 0, errors.New("job queue is required")
	}
	if limit <= 0 {
		limit = 100
	}

	jobs, err := s.repo.ListRunnableRetries(ctx, limit)
	if err != nil {
		return 0, err
	}

	for _, job := range jobs {
		if err := s.queue.Enqueue(ctx, domainjob.QueueMessage{JobID: job.ID, Type: job.Type}); err != nil {
			return 0, err
		}
	}

	return len(jobs), nil
}

func (s *Service) ReclaimStaleRunning(ctx context.Context, staleBefore time.Time, limit int) (int, error) {
	if s.queue == nil {
		return 0, errors.New("job queue is required")
	}
	if limit <= 0 {
		limit = 100
	}

	jobs, err := s.repo.ReclaimStaleRunning(ctx, staleBefore, limit)
	if err != nil {
		return 0, err
	}

	for _, job := range jobs {
		if err := s.queue.Enqueue(ctx, domainjob.QueueMessage{JobID: job.ID, Type: job.Type}); err != nil {
			return 0, err
		}
	}

	return len(jobs), nil
}

func retryBackoff(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	seconds := math.Pow(2, float64(attempt-1))
	backoff := time.Duration(seconds) * time.Second
	if backoff > 5*time.Minute {
		return 5 * time.Minute
	}
	return backoff
}
