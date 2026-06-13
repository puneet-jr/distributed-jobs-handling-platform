package executor

import (
	"fmt"
	"distributed-job-platform/internal/jobs/model"
)

type ExecutorRegistry struct {
	executors map[model.JobType]JobExecutor
}

func NewExecutorRegistry() *ExecutorRegistry{
	return &ExecutorRegistry{
		executors: make(map[model.JobType]JobExecutor),
	}
}

func (r *ExecutorRegistry) Register(jobType model.JobType, exec JobExecutor){
	r.executors[jobType] = exec
}

func(r *ExecutorRegistry) Get(jobType model.JobType)(JobExecutor, error) {
	exec, exists := r.executors[jobType] 

	if !exists {
		return nil, fmt.Errorf("No executor registered for job type: %s",jobType)
	}
	return exec, nil
}
