package job

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	domainjob "github.com/your-org/distributed-job-platform/internal/domain/job"
)

type Service struct {
	repo  domainjob.Repository
	queue Queue
}

func NewService(repo domainjob.Repository, queue Queue) *Service {
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
	
	// Why enqueue AFTER database insert?
	// Workers must never receive a job that does not exist in Postgres.
	// The safe order is: persist first, then publish the job ID.
	if err := s.queue.Enqueue(ctx, QueueMessage{
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

func (s *Service) GetByIdD(ctx context.Context, id string) (*GetJobResponse, error) {
	job, err := s.repo.GetByID(ctx,id)

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

func (s *Service) MarkRunning(
	ctx context.Context,
	id string,
	workerID string,
) error {
	if workerID == "" {
		return errors.New("worker id is required")
	}

	return s.repo.UpdateStatusIfCurrent(
		ctx,
		id,
		[]domainjob.Status{
			domainjob.StatusPending,
			domainjob.StatusRetrying,
		},
		domainjob.StatusRunning,
		&workerID,
		nil,
	)
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

	if err := s.repo.IncrementRetryCount(ctx, id, errMsg); err != nil {
		return err
	}

	return s.repo.UpdateStatusIfCurrent(
		ctx,
		id,
		[]domainjob.Status{
			domainjob.StatusRetrying,
		},
		domainjob.StatusPending,
		&workerID,
		&errMsg,
	)
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

