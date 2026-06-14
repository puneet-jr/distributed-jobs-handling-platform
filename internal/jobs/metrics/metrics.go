package metrics

import (
	"distributed-job-platform/internal/jobs/model"
	"log"
	"time"
)

// Recorder abstracts the observability backend ( Prometheus, Datalog etc)

type Recorder interface{
	RecorderJobStarted(jobType model.JobType)
	RecorderJobCompleted(jobType model.JobType, duration time.Duration)
	// what is transient?
	RecotderJobFailed(jobType model.JobType, duration time.Duration, isTransient bool)
}

// LogMetrics is a simple implementation for development.
// In production, replace this with a Prometheus Counter/Histogram wrapper.
type LogMetrics struct{}

func NewLogMetrics() *LogMetrics { return &LogMetrics{} }

func (m *LogMetrics) RecordJobStarted(jobType model.JobType) {
	log.Printf("[Metrics] Job Started | Type: %s", jobType)
}

func (m *LogMetrics) RecordJobCompleted(jobType model.JobType, duration time.Duration) {
	log.Printf("[Metrics] Job Completed | Type: %s | Duration: %v", jobType, duration)
}

func (m *LogMetrics) RecordJobFailed(jobType model.JobType, duration time.Duration, isTransient bool) {
	log.Printf("[Metrics] Job Failed | Type: %s | Duration: %v | Transient: %v", jobType, duration, isTransient)
}
