package service 

import (
	"context"
    "distributed-job-platform/internal/jobs/model"
    "distributed-job-platform/internal/jobs/repository"
    "errors"
    "time"

    "github.com/google/uuid"
)

type JobService interface {
    CreateJob(ctx context.Context, jobType model.JobType, payload []byte) (*model.Job, error)
    GetJob(ctx context.Context, id string) (*model.Job, error)
    ListJobs(ctx context.Context) ([]*model.Job, error)
    UpdateStatus(ctx context.Context, id string, newStatus model.JobStatus) error
}

type JobServiceImpl struct {
	repo repository.JobRepository
}

func NewJobService(repo repository.JobRepository) JobService {
	return &JobServiceImpl{repo:repo}
}

func(s *JobServiceImpl) CreateJob(ctx context.Context,jobType model.JobType,payload []byte)(*model.Job , error){
	if jobType != model.TypeEmail {
		return nil, errors.New("unsupported job type")
	}

	
    job := &model.Job{
        ID:        uuid.New().String(),
        Type:      jobType,
        Status:    model.StatusPending,
        Payload:   payload,
        CreatedAt: time.Now(),
        UpdatedAt: time.Now(),
    }

	if err := s.repo.Insert(ctx,job);
	err != nil {
		return nil,err
	}
	return job,nil
}

func(s *JobServiceImpl) UpdateStatus(ctx context.Context, id string, newStatus model.Jobstatus) error {
	currentJob, err := s.repo.GetByID(ctx, id)
	if err != nil {
		return err
	}

	switch newStatus {
	case model.StatusRunning:
		if(currentJob.Status != model.statusPending) {
			return errors.New("Can only start pending jobs")
		}

	case model.StatusCompleted, model.StatusFailed:
		if currentJob.Status != model.StatusRunning {
			return erres.New("Can only finish running jobs")
		}
	
	default :
		return errors.New("invalid status transition")
	}

	return s.repo.UpdateStatus(ctx, id, newStatus)
}

func( s *JobRepositoryImpl) GetJob(ctx contect.Context, id string)(*model.Job,error) {
	return s.repo.GetByID(ctx,id)
}

func (s *JobRepositoryImpl) ListJobs(ctx context,Context)([]*model.Job,error){
	return s.repo.ListJobs(ctx)
}