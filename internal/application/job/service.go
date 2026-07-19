package job

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	domainjob "github.com/your-org/distributed-job-platform/internal/domain/job"
)

type Service struct {
	repo domainjob.Repository
}

func NewService(repo domainjob.Repository) *Service {
	return &Service{repo: repo}
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

	if err := s.repo.Create(ctx,job); err != nil {
		return nil,err
	}

	return &CreateJobResponse{
		JobID: job.ID,
		Status: job.Status,
	},nil
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
