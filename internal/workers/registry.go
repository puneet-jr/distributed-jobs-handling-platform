package workers

import (
	"context"
	"fmt"

	domainjob "distributed-job-platform/internal/domain/job"
)

type JobHandler interface {
	Handle(ctx context.Context, job domainjob.Job) error
}

type HandlerRegistry map[string]JobHandler

func (r HandlerRegistry) Get(jobType string) (JobHandler, error) {
	handler, ok := r[jobType]
	if !ok {
		return nil, fmt.Errorf("no handler registered for job type %q", jobType)
	}

	return handler, nil
}
