package observability

import (
	"time"
	"github.com/prometheus/client_golang/prometheus"
)

type Metrics struct {
	JobsCreated       prometheus.Counter
	JobsStarted       prometheus.Counter
	JobsCompleted     prometheus.Counter
	JobsFailed        prometheus.Counter
	JobsRetried       prometheus.Counter
	WorkerPollErrors  prometheus.Counter
	HandlerErrors     *prometheus.CounterVec
	WorkerActiveJobs  prometheus.Gauge
	QueueDepth        prometheus.Gauge
	JobProcessingTime prometheus.Observer
	JobWaitTime       prometheus.Observer
}

func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		JobsCreated: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "jobs_created_total",
			Help: "Total jobs accepted by the API.",
		}),
		JobsStarted: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "jobs_started_total",
			Help: "Total jobs started by workers.",
		}),
		JobsCompleted: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "jobs_completed_total",
			Help: "Total jobs completed successfully.",
		}),
		JobsFailed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "jobs_failed_total",
			Help: "Total jobs permanently failed.",
		}),
		JobsRetried: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "jobs_retried_total",
			Help: "Total jobs scheduled for retry.",
		}),
		WorkerPollErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "worker_poll_errors_total",
			Help: "Total worker queue polling errors.",
		}),
		HandlerErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "handler_errors_total",
			Help: "Total handler execution errors by job type.",
		}, []string{"job_type"}),
		WorkerActiveJobs: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "worker_active_jobs",
			Help: "Currently running worker jobs.",
		}),
		QueueDepth: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "queue_depth",
			Help: "Current queue depth.",
		}),
		JobProcessingTime: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "job_processing_duration_seconds",
			Help:    "Time spent processing a job.",
			Buckets: prometheus.DefBuckets,
		}),
		JobWaitTime: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "job_wait_duration_seconds",
			Help:    "Time between job creation and worker start.",
			Buckets: prometheus.DefBuckets,
		}),
	}

	reg.MustRegister{
		m.JobsCreated,
		m.JobsStarted,
		m.JobsCompleted,
		m.JobsFailed,
		m.JobsRetried,
		m.WorkerPollErrors,
		m.HandlerErrors,
		m.WorkerActiveJobs,
		m.QueueDepth,
		m.JobProcessingTime.(prometheus.Collector),
		m.JobWaitTime.(prometheus.Collector),
	)
	return m
}

func SecondsSince( t time.Time) float64 {
	if t.IsZero() {
		return 0
	}
	return time.Since(t).Seconds()
}
