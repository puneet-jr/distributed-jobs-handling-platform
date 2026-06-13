package repository

import (
	"context"
	"distributed-job-platform/internal/jobs/model"
)

type JobRespository interface {
	Insert(ctx context.Context,job *model.Job) error
	GetByID(ctx context.Context, id string) (*model.Job, error)
	List(ctx context.Context)([]*model.Job, error)
    UpdateStatus(ctx context.Context, id string, status model.JobStatus) error
}