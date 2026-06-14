package worker

import (
	"context"
	"errors"
	"log/slog"
	"runtime/debug"
	"sync"
	"time"

	"distributed-job-platform/internal/jobs/dispatcher"
	"distributed-job-platform/internal/jobs/executor"
	"distributed-job-platform/internal/jobs/metrics"
	"distributed-job-platform/internal/jobs/model"
	"distributed-job-platform/internal/jobs/repository"
)

type WorkerPool struct {
	dispatcher dispatcher.Dispatcher
	repo       repository.JobRepository
	registry   *executor.Registry
	backoff    BackoffStrategy
	metrics    metrics.Recorder
	wg         sync.WaitGroup
	numWorkers int
}

func NewWorkerPool(
	disp dispatcher.Dispatcher,
	repo repository.JobRepository,
	registry *executor.Registry,
	backoff BackoffStrategy,
	metrics metrics.Recorder,
	numWorkers int,
) *WorkerPool {
	return &WorkerPool{
		dispatcher: disp,
		repo:       repo,
		registry:   registry,
		backoff:    backoff,
		metrics:    metrics,
		numWorkers: numWorkers,
	}
}

func (wp *WorkerPool) Start(ctx context.Context) {
	slog.Info("Starting resilient worker pool", "num_workers", wp.numWorkers)
	for i := 0; i < wp.numWorkers; i++ {
		wp.wg.Add(1)
		go wp.run(ctx, i)
	}
}

func (wp *WorkerPool) Stop() {
	slog.Info("Waiting for workers to finish current jobs...")
	wp.wg.Wait()
	slog.Info("All workers stopped.")
}

func (wp *WorkerPool) run(ctx context.Context, workerID int) {
	defer wp.wg.Done()
	ch := wp.dispatcher.Jobs()

	for {
		select {
		case <-ctx.Done():
			slog.Info("Worker received shutdown signal", "worker_id", workerID)
			return
		case jobID, ok := <-ch:
			if !ok {
				return 
			}
			wp.processJob(ctx, workerID, jobID)
		}
	}
}

func (wp *WorkerPool) processJob(ctx context.Context, workerID int, jobID string) {
	// FIX F: Structured logging with progressive context binding
	logger := slog.With("worker_id", workerID, "job_id", jobID)

	// FIX A: Panic recovery with a BOUNDED cleanup context
	defer func() {
		if r := recover(); r != nil {
			logger.Error("PANIC recovered", "panic", r, "stack", string(debug.Stack()))
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = wp.repo.UpdateStatus(cleanupCtx, jobID, model.StatusFailed)
		}
	}()

	// 1. Fetch job
	job, err := wp.repo.GetByID(ctx, jobID)
	if err != nil {
		logger.Error("Failed to fetch job", "error", err)
		return
	}
	logger = logger.With("job_type", job.Type) // Bind job_type for all subsequent logs

	// Validate MaxAttempts with explicit assumption documentation
	// Assumption: MaxAttempts is immutable once created. If updated externally mid-run, this worker won't see it.
	if job.MaxAttempts <= 0 {
		job.MaxAttempts = 1
	}

	// Resolve Executor BEFORE marking as RUNNING
	exec, err := wp.registry.Get(job.Type)
	if err != nil {
		logger.Error("No executor registered for job type")
		_ = wp.repo.UpdateStatus(ctx, jobID, model.StatusFailed)
		return
	}

	if err := wp.repo.UpdateStatus(ctx, jobID, model.StatusRunning); err != nil {
		logger.Error("Failed to update status to RUNNING, aborting", "error", err)
		return
	}

	wp.safeRecordStarted(job.Type)
	startTime := time.Now()

	// Retry State Recovery
	startAttempt := job.Attempts + 1
	if startAttempt > job.MaxAttempts {
		logger.Error("Job already exhausted attempts", "attempts", job.Attempts, "max_attempts", job.MaxAttempts)
		wp.safeRecordFailed(job.Type, time.Since(startTime), false)
		_ = wp.repo.UpdateStatus(ctx, jobID, model.StatusFailed)
		return
	}

	// 2. Retry Loop
	for attempt := startAttempt; attempt <= job.MaxAttempts; attempt++ {
		if err := wp.repo.UpdateAttempts(ctx, jobID, attempt); err != nil {
			// FIX C: Explicitly log the drift risk instead of silently ignoring
			logger.Error("Failed to persist attempt count (local state may drift from DB on crash)", "error", err, "attempt", attempt)
		}
		job.Attempts = attempt 
		logger = logger.With("attempt", attempt) // Bind current attempt to logger

		// FIX B: Extract timeout logic to a helper to guarantee `cancel()` via `defer`
		err = wp.executeWithTimeout(ctx, exec, job.Payload)

		// SUCCESS
		if err == nil {
			logger.Info("Job completed successfully", "duration", time.Since(startTime))
			wp.safeRecordCompleted(job.Type, time.Since(startTime))
			if err := wp.repo.UpdateStatus(ctx, jobID, model.StatusCompleted); err != nil {
				logger.Error("Failed to mark job as COMPLETED", "error", err)
			}
			return
		}

		// PERMANENT FAILURE
		var permErr *executor.PermanentError
		if errors.As(err, &permErr) {
			logger.Error("Job failed permanently", "error", err)
			wp.safeRecordFailed(job.Type, time.Since(startTime), false)
			if err := wp.repo.UpdateStatus(ctx, jobID, model.StatusFailed); err != nil {
				logger.Error("Failed to mark job as FAILED (permanent)", "error", err)
			}
			return
		}

		// TRANSIENT FAILURE
		var transErr *executor.TransientError
		if errors.As(err, &transErr) {
			if attempt < job.MaxAttempts {
				delay := wp.backoff.Calculate(attempt)
				logger.Warn("Transient error, retrying", "error", err, "retry_delay", delay)
				
				select {
				case <-time.After(delay):
					continue 
				case <-ctx.Done():
					logger.Warn("Shutdown received during backoff, aborting retries")
					wp.safeRecordFailed(job.Type, time.Since(startTime), true)
					_ = wp.repo.UpdateStatus(ctx, jobID, model.StatusFailed)
					return
				}
			}
		} else {
			logger.Error("Job failed with unknown error", "error", err)
			wp.safeRecordFailed(job.Type, time.Since(startTime), false)
			if err := wp.repo.UpdateStatus(ctx, jobID, model.StatusFailed); err != nil {
				logger.Error("Failed to mark job as FAILED (unknown)", "error", err)
			}
			return
		}
	}

	logger.Error("Job exhausted all attempts", "max_attempts", job.MaxAttempts)
	wp.safeRecordFailed(job.Type, time.Since(startTime), true)
	if err := wp.repo.UpdateStatus(ctx, jobID, model.StatusFailed); err != nil {
		logger.Error("Failed to mark job as FAILED (exhausted)", "error", err)
	}
}

// preventing timer goroutine leaks if loop logic changes in the future.
func (wp *WorkerPool) executeWithTimeout(ctx context.Context, exec executor.Executor, payload []byte) error {
	execCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	return exec.Execute(execCtx, payload)
}

// --- Safe Metrics Wrappers ---
func (wp *WorkerPool) safeRecordStarted(jobType model.JobType) {
	defer func() { _ = recover() }()
	wp.metrics.RecordJobStarted(jobType)
}

func (wp *WorkerPool) safeRecordCompleted(jobType model.JobType, duration time.Duration) {
	defer func() { _ = recover() }()
	wp.metrics.RecordJobCompleted(jobType, duration)
}

func (wp *WorkerPool) safeRecordFailed(jobType model.JobType, duration time.Duration, isTransient bool) {
	defer func() { _ = recover() }()
	wp.metrics.RecordJobFailed(jobType, duration, isTransient)
}