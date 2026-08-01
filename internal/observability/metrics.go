// internal/observability/metrics.go
package observability

import (
	"github.com/prometheus/client_golang/prometheus"
)

type Metrics struct {
	JobsCreated      *prometheus.CounterVec
	JobsStarted      *prometheus.CounterVec
	JobsCompleted    *prometheus.CounterVec
	JobsFailed       *prometheus.CounterVec
	JobsRetried      *prometheus.CounterVec
	HandlerErrors    *prometheus.CounterVec
	WorkerPollErrors *prometheus.CounterVec
	WorkerActiveJobs *prometheus.GaugeVec
	JobProcessing    *prometheus.HistogramVec
	JobWait          *prometheus.HistogramVec
}

func NewMetrics(reg *prometheus.Registry) *Metrics {
	m := &Metrics{
		JobsCreated: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "jobs_created_total",
			Help: "Total jobs created.",
		}, []string{"job_type"}),

		JobsStarted: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "jobs_started_total",
			Help: "Total jobs started by workers.",
		}, []string{"job_type", "worker_id"}),

		JobsCompleted: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "jobs_completed_total",
			Help: "Total jobs completed.",
		}, []string{"job_type", "worker_id"}),

		JobsFailed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "jobs_failed_total",
			Help: "Total jobs failed.",
		}, []string{"job_type", "worker_id"}),

		JobsRetried: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "jobs_retried_total",
			Help: "Total jobs scheduled for retry.",
		}, []string{"job_type", "worker_id"}),

		HandlerErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "handler_errors_total",
			Help: "Total handler execution errors.",
		}, []string{"job_type", "worker_id"}),

		WorkerPollErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "worker_poll_errors_total",
			Help: "Total queue polling errors.",
		}, []string{"worker_id"}),

		WorkerActiveJobs: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "worker_active_jobs",
			Help: "Current active jobs per worker.",
		}, []string{"worker_id"}),

		JobProcessing: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "job_processing_duration_seconds",
			Help:    "Time spent processing jobs.",
			Buckets: prometheus.DefBuckets,
		}, []string{"job_type", "worker_id"}),

		JobWait: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "job_wait_duration_seconds",
			Help:    "Time between job creation and worker start.",
			Buckets: prometheus.DefBuckets,
		}, []string{"job_type"}),
	}

	reg.MustRegister(
		m.JobsCreated,
		m.JobsStarted,
		m.JobsCompleted,
		m.JobsFailed,
	)
	return m
	}