package observability

import (
	"github.com/prometheus/client_golang/prometheus"
)

type Metrics struct {
	JobsCreated        *prometheus.CounterVec
	JobsStarted        *prometheus.CounterVec
	JobsCompleted      *prometheus.CounterVec
	JobsFailed         *prometheus.CounterVec
	JobsRetried        *prometheus.CounterVec
	QueueDepth         prometheus.Gauge
	WorkerActiveJobs   *prometheus.GaugeVec
	WorkerPollErrors   *prometheus.CounterVec
	HandlerErrors      *prometheus.CounterVec
	JobProcessingTime  *prometheus.HistogramVec
	JobWaitTime        *prometheus.HistogramVec
}

func NewMetrics(reg prometheus.Registerer) *Metrics {
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

		QueueDepth: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "queue_depth",
			Help: "Current queue depth.",
		}),

		WorkerActiveJobs: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "worker_active_jobs",
			Help: "Jobs currently being processed by worker.",
		}, []string{"worker_id"}),

		WorkerPollErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "worker_poll_errors_total",
			Help: "Total queue polling errors.",
		}, []string{"worker_id"}),

		HandlerErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "handler_errors_total",
			Help: "Total handler execution errors.",
		}, []string{"job_type", "worker_id"}),

		JobProcessingTime: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "job_processing_duration_seconds",
			Help:    "Time spent processing jobs.",
			Buckets: prometheus.DefBuckets,
		}, []string{"job_type", "worker_id"}),

		JobWaitTime: prometheus.NewHistogramVec(prometheus.HistogramOpts{
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
		m.JobsRetried,
		m.QueueDepth,
		m.WorkerActiveJobs,
		m.WorkerPollErrors,
		m.HandlerErrors,
		m.JobProcessingTime,
		m.JobWaitTime,
	)

	return m
}
