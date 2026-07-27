papackage worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"sync"
	"time"

	appjob "distributed-job-platform/internal/application/job"
)

type Worker struct {
	id          string
	queue       QueueConsumer
	service     *appjob.Service
	handlers    HandlerRegistry
	logger      *slog.Logger
	concurrency int
	batchSize   int
}

func NewWorker(
	id string,
	queue QueueConsumer,
	service *appjob.Service,
	handlers HandlerRegistry,
	logger *slog.Logger,
	concurrency int,
	batchSize int,
) (*Worker, error) {
	if id == "" {
		return nil, errors.New("worker id is required")
	}
	if queue == nil {
		return nil, errors.New("queue consumer is required")
	}
	if service == nil {
		return nil, errors.New("job service is required")
	}
	if logger == nil {
		return nil, errors.New("logger is required")
	}

	// FIX: previously these defaults were applied silently. Silent
	// defaulting of tuning params is fine, but silent + unlogged means
	// a misconfigured deployment (e.g. concurrency=0 from a bad env var)
	// looks identical to an intentional one. Log it so it shows up in
	// startup logs and isn't a mystery during an incident.
	if concurrency <= 0 {
		logger.Warn("invalid concurrency, falling back to default", "given", concurrency, "default", 10)
		concurrency = 10
	}
	if batchSize <= 0 {
		logger.Warn("invalid batchSize, falling back to concurrency", "given", batchSize, "default", concurrency)
		batchSize = concurrency
	}

	return &Worker{
		id:          id,
		queue:       queue,
		service:     service,
		handlers:    handlers,
		logger:      logger,
		concurrency: concurrency,
		batchSize:   batchSize,
	}, nil
}

func (w *Worker) Run(ctx context.Context) error {
	jobs := make(chan Message, w.concurrency)
	var wg sync.WaitGroup

	// Fixed worker pool. This controls memory and protects downstream services.
	for i := 0; i < w.concurrency; i++ {
		wg.Add(1)
		go func(slot int) {
			defer wg.Done()
			w.processLoop(ctx, slot, jobs)
		}(i)
	}

	// Poll loop reads from Redis in batches and feeds the bounded channel.
	for {
		select {
		case <-ctx.Done():
			close(jobs)
			wg.Wait()
			return ctx.Err()
		default:
			messages, err := w.queue.Read(ctx, w.id, w.batchSize)
			if err != nil {
				w.logger.Error("queue read failed", "error", err)

				// FIX: time.Sleep(time.Second) was not interruptible.
				// If ctx was cancelled during the sleep, shutdown would
				// stall for up to 1s waiting on nothing. Race the sleep
				// against ctx.Done() so cancellation is immediate.
				select {
				case <-time.After(time.Second):
				case <-ctx.Done():
					close(jobs)
					wg.Wait()
					return ctx.Err()
				}
				continue
			}
			for _, msg := range messages {
				select {
				case jobs <- msg:
				case <-ctx.Done():
					close(jobs)
					wg.Wait()
					return ctx.Err()
				}
			}
		}
	}
}

func (w *Worker) processLoop(ctx context.Context, slot int, jobs <-chan Message) {
	for msg := range jobs {
		w.safeProcessMessage(ctx, slot, msg)
	}
}

// safeProcessMessage wraps processMessage with panic recovery.
//
// FIX: previously a panic inside handler.Handle (nil pointer, bad type
// assertion, index out of range - all common in third-party/plugin-style
// handler code) would propagate up through processLoop and crash the
// entire process, killing every other slot's in-flight work along with it.
// One bad job should never be able to take down the whole worker pool.
//
// We recover, log with a stack trace, and best-effort mark the job as
// failed so it doesn't get silently stuck in "running" forever. Marking
// failed here is deliberately NOT retried automatically - a panic usually
// means a bug in handler code, and blindly retrying a job that reliably
// crashes the process is how you turn one bad job into a crash-loop
// across your whole fleet.
func (w *Worker) safeProcessMessage(ctx context.Context, slot int, msg Message) {
	defer func() {
		if r := recover(); r != nil {
			w.logger.Error("panic recovered while processing job",
				"worker_id", w.id,
				"slot", slot,
				"message_id", msg.ID,
				"job_id", msg.JobID,
				"panic", fmt.Sprintf("%v", r),
				"stack", string(debug.Stack()),
			)
			if err := w.service.MarkFailed(ctx, msg.JobID, w.id, fmt.Sprintf("panic: %v", r)); err != nil {
				w.logger.Error("failed to mark job failed after panic", "error", err, "job_id", msg.JobID)
			}
			// Ack so the queue doesn't redeliver a message that we know
			// crashes the handler - that would just crash-loop the worker.
			if err := w.queue.Ack(ctx, msg.ID); err != nil {
				w.logger.Error("failed to ack after panic recovery", "error", err, "message_id", msg.ID)
			}
		}
	}()
	w.processMessage(ctx, slot, msg)
}

func (w *Worker) processMessage(ctx context.Context, slot int, msg Message) {
	start := time.Now()
	logger := w.logger.With(
		"worker_id", w.id,
		"slot", slot,
		"message_id", msg.ID,
		"job_id", msg.JobID,
		"job_type", msg.Type,
	)

	// Claim the job before executing.
	// This protects against two workers processing the same DB job state.
	if err := w.service.MarkRunning(ctx, msg.JobID, w.id); err != nil {
		// FIX: a claim failure has two very different causes that were
		// previously logged identically as errors:
		//   1. Transient DB issue (connection blip, timeout) - genuinely
		//      an error, should alert.
		//   2. The job was already claimed/completed by another worker,
		//      most likely because this exact message was redelivered
		//      (at-least-once queues do this - it's expected, not a bug).
		//
		// If your service layer exposes a sentinel for "already claimed"
		// (e.g. appjob.ErrAlreadyClaimed), branch on it with errors.Is so
		// case 2 doesn't spam error-level logs / paging alerts for
		// completely expected redelivery races. Falling through to the
		// generic branch is safe either way - we still just return
		// without acking, which is correct: if it truly was a duplicate
		// delivery of an already-completed job, this message can be
		// acked safely, so we do that here rather than leaving it to
		// redeliver indefinitely.
		if errors.Is(err, appjob.ErrAlreadyClaimed) {
			logger.Info("job already claimed elsewhere, treating as duplicate delivery")
			if ackErr := w.queue.Ack(ctx, msg.ID); ackErr != nil {
				logger.Error("failed to ack duplicate delivery", "error", ackErr)
			}
			return
		}
		logger.Error("failed to mark job running", "error", err)
		return
	}

	job, err := w.service.GetByID(ctx, msg.JobID)
	if err != nil {
		logger.Error("failed to load job", "error", err)
		return
	}

	handler, err := w.handlers.Get(job.Type)
	if err != nil {
		_ = w.service.MarkFailed(ctx, msg.JobID, w.id, err.Error())
		_ = w.queue.Ack(ctx, msg.ID)
		logger.Error("missing handler", "error", err)
		return
	}

	domainJob := job.ToDomain()

	// NOTE (contract, not a code fix): handler.Handle MUST be idempotent.
	// Ack happens only after MarkCompleted succeeds (see below), so a
	// crash between those two calls WILL cause this exact job to be
	// redelivered and re-executed. Any handler that isn't safe to run
	// twice (e.g. "charge a card" without a dedup/idempotency key) is a
	// latent bug in this system, not an edge case - at-least-once
	// delivery guarantees this will happen eventually at scale. Enforce
	// this at the Handler interface/documentation level, not here.
	if err := handler.Handle(ctx, domainJob); err != nil {
		// Handler failed, so we schedule retry through service rules.
		// ACK happens after retry state is safely recorded.
		_ = w.service.MarkRetrying(ctx, msg.JobID, w.id, err.Error())
		_ = w.queue.Ack(ctx, msg.ID)
		logger.Error(
			"job failed and retry was scheduled",
			"error", err,
			"duration_ms", time.Since(start).Milliseconds(),
		)
		return
	}

	if err := w.service.MarkCompleted(ctx, msg.JobID, w.id); err != nil {
		logger.Error("failed to mark job completed", "error", err)
		return
	}

	// ACK only after Postgres says the job is completed.
	// If the worker crashes before this, Redis can redeliver later -
	// which is exactly why handler.Handle must be idempotent (see above).
	if err := w.queue.Ack(ctx, msg.ID); err != nil {
		logger.Error("failed to ack message", "error", err)
		return
	}

	logger.Info("job completed", "duration_ms", time.Since(start).Milliseconds())
}
