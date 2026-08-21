# Distributed Job Platform: Current Analysis and Phased Build Plan

## Goal

Build a distributed job processing platform that:

- accepts jobs over HTTP
- stores job metadata durably
- executes jobs asynchronously
- retries failures safely
- scales to high job volume
- exposes strong observability through metrics, logs, and traces

The correct starting point is **requirements and responsibilities**, not Redis, Kafka, or worker count.

---

## Phase 1: Analyze the Current Codebase

### What is already implemented

The repository already has the beginnings of a clean backend structure:

- `cmd/api/` exists as the API entrypoint.
- `internal/domain/job/` defines the core job entity, statuses, repository interface, and domain errors.
- `internal/application/job/` contains a service layer for create/get job flows.
- `internal/interfaces/http/` contains HTTP request DTOs, handlers, and route registration.
- `internal/infrastructure/postgres/` contains a repository implementation attempt for job persistence.
- `migrations/` includes a jobs table migration file path.
- `internal/bootstrap/` contains app bootstrap, config, logger, health, and basic in-memory metrics scaffolding.
- `deployments/docker-compose.yml` exists, so containerized local infrastructure is intended.

### Basic setup that is conceptually present

These basics are present at the design level:

- API server structure in Go
- layered architecture: domain, application, interfaces, infrastructure
- job creation endpoint design
- job lookup endpoint design
- idempotency handling intent
- Postgres as durable job metadata storage
- health/metrics bootstrap placeholders
- status-driven job lifecycle model

### What is not fully implemented yet

The current repository is still at an early foundation stage. Several files are incomplete or broken enough that the app does not build cleanly yet.

Key gaps:

- module imports still use placeholder paths like `github.com/your-org/...`
- `go build ./...` currently fails
- `internal/infrastructure/postgres/job_repository.go` has multiple syntax and type errors
- `internal/bootstrap/config.go` is incomplete and does not parse YAML correctly
- `internal/bootstrap/app.go` has startup and shutdown issues
- `internal/infrastructure/observability/health.go` is empty
- migrations exist by filename, but the `.sql` content is currently empty
- Redis/queue integration is not implemented
- worker process does not exist yet
- retry scheduling does not exist yet
- monitoring is only a placeholder, not production observability

### Practical status summary

Right now, this project is best described as:

> "A partially scaffolded async job platform backend with domain modeling and API intent in place, but without a working queue, workers, retries, or production-grade observability yet."

That means Phase 1 should end with **stabilizing the foundation**, then the remaining work should move toward the real core of the project: **reliable asynchronous execution, correctness under failure, and operational visibility**.

---

## Main Part of the Project

The main part is **not** "add Redis" or "spin up workers."

The main part is the **critical logic**:

- what happens when a job is accepted
- how a job moves through states
- how duplicate work is prevented
- how failed work is retried safely
- how workers recover from crashes
- how the system proves no jobs are lost
- how operators can detect backlog, failure spikes, and worker problems quickly

This platform succeeds or fails on:

1. job lifecycle correctness
2. durable state transitions
3. safe asynchronous execution
4. retry and recovery behavior
5. observability and monitoring

---

## Requirements First

The API contract should be:

`POST /jobs`

Example request body:

```json
{
  "type": "email",
  "payload": {
    "to": "user@example.com"
  }
}
```

or:

```json
{
  "type": "pdf",
  "payload": {
    "template": "invoice"
  }
}
```

Response:

```json
{
  "jobId": "123"
}
```

with HTTP:

```text
202 Accepted
```

The API must:

- validate the request
- persist job metadata
- enqueue job execution
- return immediately

The API must not:

- send the email
- generate the PDF
- resize the image
- wait for job completion

---

## Recommended Build Phases

## Phase 2: Make the Current Foundation Actually Work

Objective:
turn the current scaffold into a clean, runnable baseline.

Deliverables:

- fix module/import paths
- make `go build ./...` pass
- complete config loading and validation
- implement health endpoint
- complete SQL migration for `jobs`
- fix Postgres repository implementation
- ensure `POST /jobs` and `GET /jobs/{id}` work end-to-end against Postgres

Required `jobs` table fields:

- `id`
- `type`
- `status`
- `priority`
- `payload`
- `idempotency_key`
- `retry_count`
- `max_retries`
- `error_message`
- `worker_id`
- `created_at`
- `started_at`
- `completed_at`

This phase is still foundation work. Do not add queue complexity before this is stable.

---

## Phase 3: Define the Job Lifecycle Explicitly

Objective:
make state transitions first-class and correct before introducing distributed execution.

Core statuses:

- `pending`
- `running`
- `completed`
- `failed`
- `retrying`
- `cancelled`

Rules to define now:

- `pending -> running`
- `running -> completed`
- `running -> failed`
- `running -> retrying`
- `retrying -> pending` or direct scheduled requeue flow
- `pending/running -> cancelled` only if business rules allow it

Required logic:

- idempotent job creation
- status transition validation
- started/completed timestamps set consistently
- worker ownership recorded when processing starts

This phase is where the platform’s correctness begins.

---

## Phase 4: Add the Queue Boundary

Objective:
separate job acceptance from job execution.

Responsibility split:

- API server: validate, store, enqueue, respond
- queue: buffer and distribute work
- worker: execute business logic

At this stage, the project can use Redis Streams or another queue, but the design should remain queue-agnostic in the application layer.

Queue requirements:

- durable enough for operational reliability expectations
- consumer group support or equivalent
- acknowledgement support
- redelivery/reclaim support
- backlog visibility

Critical outcome:

the API server must never execute the real job work directly.

---

## Phase 5: Build the Worker Runtime

Objective:
create a generic worker process that consumes queued jobs and dispatches by type.

Recommended handler interface:

```go
type JobHandler interface {
    Handle(ctx context.Context, job Job) error
}
```

Registry pattern:

```go
handlers := map[string]JobHandler{
    "email": emailHandler,
    "pdf": imageHandler,
    "resize_image": resizeHandler,
}
```

Worker responsibilities:

- receive job
- claim ownership
- set status to `running`
- execute handler
- ack on success
- update status on completion
- schedule retry on failure

Use bounded concurrency:

- fixed worker pool
- controlled goroutine count
- backpressure through channels or queue consumption limits

Do not use unbounded goroutine-per-job execution.

---

## Phase 6: Reliability and Failure Handling

Objective:
make the system safe under duplicate delivery, worker crashes, and transient dependency failures.

Critical logic here:

### Idempotency

- accept `Idempotency-Key` on create
- store it with a unique DB constraint
- return existing job when duplicate create arrives

### Crash recovery

- jobs claimed but not acknowledged must be recoverable
- stale in-flight jobs must be reclaimable by another worker

### Retry policy

- exponential backoff
- max retry count
- last error stored
- terminal failure state when retries exhausted

### Dead-letter handling

- permanently failed jobs must remain visible
- support re-drive or manual inspection later

This phase is the heart of distributed-job correctness.

---

## Phase 7: Monitoring, Metrics, and Operational Visibility

Objective:
make the system observable enough to operate under load and failure.

This is not optional. For this project, observability is part of the core logic.

### Minimum metrics

- `jobs_created_total`
- `jobs_started_total`
- `jobs_completed_total`
- `jobs_failed_total`
- `jobs_retried_total`
- `queue_depth`
- `job_processing_duration_seconds`
- `job_wait_duration_seconds`
- `worker_active_jobs`
- `worker_poll_errors_total`
- `handler_errors_total`

### Useful dimensions

- job type
- status
- worker id
- retry outcome

### Alerts to design early

- queue depth above threshold
- failure rate above threshold
- retry spike
- worker process unavailable
- no job completions during active intake
- long-running jobs stuck in `running`

### Logging

Every important log should include:

- `job_id`
- `worker_id`
- `job_type`
- `status`
- `retry_count`
- `duration_ms`
- `error`
- `trace_id`

### Tracing

Trace path should eventually cover:

- client request
- API create flow
- DB write
- queue publish
- worker consume
- handler execution
- external dependency call

Use OpenTelemetry once the core flows exist.

---

## Phase 8: Scale and Capacity Controls

Objective:
make the platform handle large throughput without breaking dependency limits or internal memory.

Required controls:

- bounded worker concurrency
- queue lag monitoring
- per-job-type throughput visibility
- rate limiting for external integrations
- priority handling if business needs it
- horizontal scaling of API and workers independently

Important point:

At high scale, the bottleneck is often external systems:

- SMTP provider
- PDF renderer
- image processing library
- webhook target
- object storage

So scaling is not only "more workers." It is also:

- smarter concurrency
- rate control
- dependency-aware scheduling

---

## Phase 9: Load Testing and Failure Injection

Objective:
validate the system under realistic volume and failure modes.

Test scenarios:

- sustained job creation load
- mixed job types
- worker crash during processing
- random handler failures
- queue reconnect/recovery
- slow downstream dependencies

Success criteria:

- API stays fast while jobs are accepted
- no lost jobs
- no duplicate execution for protected flows
- retries happen as designed
- queue drains after load stops
- metrics and logs clearly show system health

---

## Suggested Delivery Order for Faster Building

For quicker progress, use this order:

1. make the current code compile and run
2. complete DB schema and job CRUD path
3. lock down status transitions and idempotency
4. introduce queue publish/consume boundary
5. add worker runtime with bounded concurrency
6. add retries, reclaim, and terminal failure handling
7. add Prometheus metrics, structured logs, and traces
8. add scaling controls, priorities, and rate limiting
9. run load tests and failure injection

This order keeps the project moving while protecting correctness first.

---

## What You Need to Build Next

The next major work is not "the other part" in a vague sense. It is specifically:

### Immediate next build target

Create a working baseline where:

- API accepts jobs
- job is written to Postgres
- job can be fetched by ID
- idempotency works
- migration is real
- service/repository layers are corrected

### After that

Build the first real async path:

- enqueue persisted job
- create worker binary
- consume queued jobs
- dispatch handler by type
- update status transitions correctly
- instrument queue depth and job durations

That is the shortest path to the actual project core.

---

## Senior Backend Engineer View

Think in responsibilities:

- API layer: validate, persist, enqueue, return quickly
- storage layer: preserve job state durably
- queue layer: distribute work safely
- worker layer: execute with bounded concurrency
- reliability layer: retries, reclaim, idempotency, dedupe
- observability layer: metrics, logs, traces, alerts

If these responsibilities are implemented well, the exact technology choices can evolve later.

The real project is the correctness of asynchronous execution and the visibility of that execution under load and failure.

That is where the engineering value is.

-----

service.go in application these issues are arised and fixed

Here are all the comments exactly as they appeared in the code you provided, grouped by the function they belong to for easy reading:

### In `Create` function (Idempotency Check)
```go
// FIX (partial - flagged, not fully solved): this previously treated
// ANY error (transient DB failure, timeout, etc.) identically to
// "no existing record", silently falling through to insert with no
// log signal. If domainjob exposes a not-found sentinel, branch on
// it explicitly here with errors.Is so real transient errors return
// immediately instead of being swallowed. Left as a comment rather
// than inventing a sentinel that may not exist in your domain
// package - wire this up once you confirm what GetByIdempotencyKey
// actually returns on "not found" vs a real failure.
```

### In `Create` function (Before `queue.Enqueue`)
```go
// Why enqueue AFTER database insert?
// Workers must never receive a job that does not exist in Postgres.
// The safe order is: persist first, then publish the job ID.
//
// GAP (documented, not fixable inside this function): this is a
// classic dual-write problem. If repo.Create succeeds but
// queue.Enqueue fails (network blip, Redis down), the job now exists
// durably in Postgres as StatusPending but will NEVER be picked up by
// any worker, because no message was ever published for it. Returning
// an error here tells the caller something went wrong, but does not
// clean up or retry the enqueue - the job is orphaned.
//
// Mitigation (needs to live outside this file): a periodic
// reconciliation job that scans for jobs with status=Pending older
// than some threshold (e.g. 60s) with no corresponding queue message,
// and re-enqueues them. This is the standard fix for the dual-write
// problem short of a transactional outbox pattern (write the outbox
// row in the same DB transaction as job creation, then a separate
// relay process reads the outbox and publishes to the queue,
// guaranteeing at-least-once delivery without a distributed
// transaction). Do not skip this - orphaned Pending jobs are silent
// and will only be noticed when someone asks "why did this job never
// run?", potentially days later.
```

### Above `GetByID` function
```go
// GetByID returns the application-layer DTO view of a job (used by
// external-facing callers, e.g. an HTTP handler returning job status to a
// client). It deliberately does NOT include Payload - that's the actual
// job data, and there's no reason to round-trip it back out over an API
// that just wants status/progress info.
//
// FIX: renamed from the original "GetByIdD" typo. A typo in an exported
// method name isn't cosmetic - it's a compile-time break for every caller,
// and worse, it's the kind of thing that gets copy-pasted into new callers
// before anyone notices.
```

### Above `GetDomainJobByID` function
```go
// GetDomainJobByID returns the full domain object, Payload included. This
// exists specifically for internal callers like the Worker that need to
// actually execute the job - not just report its status. Keeping this
// separate from GetByID above means the DTO shape used by external APIs
// can evolve (e.g. hide new internal fields) without ever affecting what
// a handler receives to execute, and vice versa - no accidental data loss
// from routing execution through a response DTO that was never designed
// to carry full job state.
```

### In `MarkRetrying` function (Before `UpdateStatusIfCurrent`)
```go
// FIX: this CAS previously required the CURRENT status to already be
// StatusRetrying before allowing the transition to StatusPending. But
// nothing in this call path ever sets the job to Retrying first - the
// job arrives here still in StatusRunning (set by MarkRunning before
// the handler executed). That mismatch meant this update matched zero
// rows on every single call: RetryCount would increment correctly,
// but the job would NEVER actually transition back to Pending, so no
// worker would ever pick it up again. It would sit in Running forever
// with no error surfaced anywhere - the worst kind of bug, silent and
// invisible until someone notices a job stuck for hours.
//
// The fix: the allowed current-state here must match what MarkRunning
// actually set - StatusRunning - not a status this code path never
// produces.
```

These comments are excellent. They don't just describe *what* the code does; they document *why* certain architectural decisions were made, highlight known limitations (like the dual-write problem), and explain the history of bugs that were fixed (like the `GetByIdD` typo and the `MarkRetrying` state mismatch). This is exactly the kind of documentation that makes a codebase maintainable at scale.

---

The worker main.go file in cmd -

WORKER_CONCURRENCY/WORKER_BATCH_SIZE as env vars — I went with env vars rather than adding fields to your bootstrap.Config struct, because I can't see that struct and didn't want to guess at its shape and hand you code that doesn't compile. If you already have (or want) a cfg.Worker.Concurrency section, that's the better long-term home for it — swap envInt(...) for cfg.Worker.Concurrency and it's a one-line change.
The forced-exit goroutine has a small race — if the second SIGINT/SIGTERM arrives in the tiny window between <-ctx.Done() returning and signal.Notify(second, ...) registering, it'll be missed. Good enough as a safety net for a human hitting Ctrl+C twice; not airtight for automated tooling sending signals back-to-back. Worth knowing, not worth over-engineering for.
nil queue landmine (#5) — left untouched by design, per scope. Say the word if you want a guard clause added to Create in service.go.

above are the fixes done to the below changes:

1. err != context.Canceled is fragile. It happens to work today because worker.Run returns ctx.Err() unwrapped. But it's one fmt.Errorf("run failed: %w", err) away from breaking — a normal shutdown would then get logged as a fatal error. Should be errors.Is(err, context.Canceled).

2. Worker tuning is hardcoded (10, 10). Everything else here is config-driven — DSN, Redis address, env — except the two numbers that actually control throughput and memory footprint. To change concurrency you need a rebuild and redeploy, not a config/env change. That's a real operational gap for something meant to run as a scalable worker fleet.

3. Inconsistent logging on startup failure. Once logger (structured slog) exists, every failure after that point still goes through the plain-text stdlib log.Fatalf. In production, if you're shipping logs to something that parses JSON, these lines land as unparseable noise right when you need them most — during a crash-loop.

4. No hard shutdown path. SIGINT/SIGTERM trigger the graceful drain via context cancellation — good — but if a handler is ignoring context (a real possibility, flagged earlier in worker.go), the process just hangs with no way out short of an orchestrator's SIGKILL timeout. A second signal should force an immediate exit.

5. nil queue passed to NewService — correct today since the worker path never calls Create, but it's a landmine: nothing stops a future code path from calling jobService.Create here and nil-pointer panicking. Not fixing service.go again unprompted, just flagging it — happy to add a nil-check guard in Create if you want defense-in-depth there.