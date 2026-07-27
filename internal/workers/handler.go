package workers

import (
	"context"
	"fmt"

	
	domainjob "distributed-job-platform/internal/domain/job" 
)


// The worker runtime does not know how to send email or generate PDFs.
// It only knows how to find the right handler and call Handle.

type JobHandler interface {
	Handle(ctx context.Context, job domainjob.Job) error
}


type HandlerRegistry map[string]JobHandler

func(r HandlerRegistry) Get(jobType string) (JobHandler, error) {
	handler, ok := r[jobType]
	if !ok {
		return nil, fmt.Errorf("no handler registred for job type %q",jobType)
	}
	return handler, nil
}

