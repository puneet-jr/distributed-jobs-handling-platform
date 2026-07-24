# Distributed Job Platform Plan

## Goal

Build a distributed job processing platform that:

- accepts jobs through `POST /jobs`
- returns immediately with `202 Accepted`
- stores job metadata durably
- executes work asynchronously
- retries failures safely
- prevents duplicate execution where required
- scales API and workers independently
- exposes enough logs, metrics, and traces to operate the system

This plan assumes Week 1 has been attempted already and uses the **current repository state** as the baseline for Weeks 2 and 3.

## Requirements First

Before Redis, queues, or worker concurrency, the system must satisfy these product-level requirements:

1. The API must accept a job request and respond immediately.
2. The API must persist job metadata before acknowledging success.
3. The API must never execute the actual business job inline.
4. A worker process must pick up persisted jobs and execute them asynchronously.
5. Failures must be retried with bounded retry rules.
6. The platform must support observability from API to worker.
7. The design must tolerate worker crashes, duplicate delivery risk, and future scale.

## Target Outcome

At the end of Week 3, the project should support this flow:

1. Client calls `POST /jobs`.
2. API validates input.
3. API stores a `pending` job record in Postgres.
4. API enqueues the job ID into Redis Streams.
5. API returns `202 Accepted` with `jobId`.
6. Worker consumer group receives the job.
7. Worker dispatches by job type to a registered handler.
8. Worker updates job status to `running`, `completed`, `retrying`, or `failed`.
9. Metrics, logs, and traces expose platform health.

## Current Repository State

The repository is still in partial Week 1 state. Some domain files are present, but the application does not compile yet and the storage direction is inconsistent.

### Present and useful

- [cmd/api/main.go](C:/Users/punee/Desktop/MY-PROJECTS/Projects/Distributed%20Job%20Platform/distributed-job-platform/cmd/api/main.go)
- [internal/domain/job/status.go](C:/Users/punee/Desktop/MY-PROJECTS/Projects/Distributed%20Job%20Platform/distributed-job-platform/internal/domain/job/status.go)
- [internal/domain/job/entity.go](C:/Users/punee/Desktop/MY-PROJECTS/Projects/Distributed%20Job%20Platform/distributed-job-platform/internal/domain/job/entity.go)
- [internal/domain/job/repository.go](C:/Users/punee/Desktop/MY-PROJECTS/Projects/Distributed%20Job%20Platform/distributed-job-platform/internal/domain/job/repository.go)
- [internal/application/job/dto.go](C:/Users/punee/Desktop/MY-PROJECTS/Projects/Distributed%20Job%20Platform/distributed-job-platform/internal/application/job/dto.go)
- [internal/application/job/service.go](C:/Users/punee/Desktop/MY-PROJECTS/Projects/Distributed%20Job%20Platform/distributed-job-platform/internal/application/job/service.go)
- [internal/interfaces/http/request.go](C:/Users/punee/Desktop/MY-PROJECTS/Projects/Distributed%20Job%20Platform/distributed-job-platform/internal/interfaces/http/request.go)

### Incomplete or inconsistent

- [internal/bootstrap/app.go](C:/Users/punee/Desktop/MY-PROJECTS/Projects/Distributed%20Job%20Platform/distributed-job-platform/internal/bootstrap/app.go)
  - invalid import placement
  - invalid router construction
  - invalid server initialization
  - references `NewPostgresJobRepository` that does not exist
- [internal/interfaces/http/router.go](C:/Users/punee/Desktop/MY-PROJECTS/Projects/Distributed%20Job%20Platform/distributed-job-platform/internal/interfaces/http/router.go)
  - wrong package name
  - typo in `http.HandlerFunc`
  - typo in `HandleFunc`
  - wrong route variable usage
- [internal/interfaces/http/handler.go](C:/Users/punee/Desktop/MY-PROJECTS/Projects/Distributed%20Job%20Platform/distributed-job-platform/internal/interfaces/http/handler.go)
  - syntax errors
  - wrong field names
  - wrong request context variable
  - `GetJob` calls `List` instead of get-by-id flow
- [internal/bootstrap/config.go](C:/Users/punee/Desktop/MY-PROJECTS/Projects/Distributed%20Job%20Platform/distributed-job-platform/internal/bootstrap/config.go)
  - still models Mongo config instead of Postgres + Redis
- [internal/infrastructure/mongo/job_repository.go](C:/Users/punee/Desktop/MY-PROJECTS/Projects/Distributed%20Job%20Platform/distributed-job-platform/internal/infrastructure/mongo/job_repository.go)
  - does not compile
  - conflicts with target durable storage design
- [migrations/001_create_jobs_table.up.sql](C:/Users/punee/Desktop/MY-PROJECTS/Projects/Distributed%20Job%20Platform/distributed-job-platform/migrations/001_create_jobs_table.up.sql)
  - currently empty

## Architecture Direction

Keep the architecture responsibilities clear:

- API layer: validate, persist, enqueue, return.
- Postgres: source of truth for job metadata and recovery.
- Redis Streams: delivery mechanism for workers.
- Workers: bounded concurrency and handler dispatch.
- Observability: metrics, logs, traces, health.

Do **not** continue with Mongo for job metadata if the intended project outcome is Postgres + Redis.

## Delivery Plan

## Week 1 Review and Exit Criteria

Week 1 is the platform foundation. Based on the current repository, this week should be considered complete only if all of the following are true:

1. `go build ./...` passes.
2. `POST /jobs` returns `202 Accepted`.
3. `GET /jobs/{id}` returns persisted state.
4. Postgres is the active metadata store.
5. SQL migration for `jobs` table exists and runs.
6. Idempotency key support exists at API and storage levels.

### Week 1 code areas

- `cmd/api/main.go`
- `internal/bootstrap/app.go`
- `internal/bootstrap/config.go`
- `internal/domain/job/*`
- `internal/application/job/*`
- `internal/interfaces/http/*`
- `internal/shared/response/response.go`
- `internal/infrastructure/postgres/*` or equivalent new package
- `migrations/001_create_jobs_table.up.sql`
- `migrations/001_create_jobs_table.down.sql`
- `configs/local.yaml`
- `deployments/docker-compose.yml`

### Week 1 gaps still visible in the current code

- No working Postgres repository implementation exists.
- Config still points to Mongo, not Postgres and Redis.
- HTTP layer is not yet wired correctly.
- Migration is empty.
- App bootstrap does not compile.

These are important because Week 2 depends on a stable synchronous baseline.

## Week 2 Plan: Asynchronous Execution

### Week 2 objective

Introduce the queue and worker pipeline so the API persists and enqueues jobs, while a separate worker process consumes and executes them.

### Week 2 deliverables

1. A worker binary in `cmd/worker`.
2. A queue abstraction so the service is not tightly coupled to Redis Streams.
3. Redis Streams producer support in the API.
4. Redis Streams consumer group support in the worker.
5. A generic worker dispatcher with handler registry by job type.
6. Status transitions from `pending` to `running` to terminal states.

### Week 2 sequence

#### Stage 1: Introduce queue contracts

Files to add:

- `internal/queue/queue.go`

Purpose:

- define enqueue and consume contracts
- isolate Redis-specific details from application logic

Suggested code shape:

```go
package queue

import "context"

type Message struct {
	JobID string
}

type Queue interface {
	Enqueue(ctx context.Context, msg Message) error
}

type Consumer interface {
	Consume(ctx context.Context, consumer string, fn func(context.Context, Message) error) error
}
```

#### Stage 2: Implement Redis Streams integration

Files to add:

- `internal/infrastructure/queue/redis_streams.go`

Purpose:

- `XADD` new jobs to stream
- create and use consumer group
- read messages with `XREADGROUP`
- acknowledge successful processing

Responsibilities:

- one stream name per environment, for example `jobs`
- one consumer group, for example `job-workers`
- payload is at least the `jobId`

#### Stage 3: Update application service to persist then enqueue

Files to update:

- `internal/application/job/service.go`

New responsibilities:

1. validate request
2. check idempotency
3. insert job row with `pending`
4. enqueue job ID
5. return `202`

Important rule:

If database insert succeeds but enqueue fails, the API must not silently lose the job. In this project stage, mark this explicitly and decide one of these approaches:

- return an error and rely on transaction/outbox follow-up later
- or insert a `pending` job and add a recovery process later

For learning, document the limitation clearly in code comments.

#### Stage 4: Build worker process

Files to add:

- `cmd/worker/main.go`
- `internal/worker/worker.go`
- `internal/worker/registry.go`

Responsibilities:

- start a consumer
- fetch job metadata from Postgres using job ID
- update status to `running`
- dispatch to correct handler
- update status after completion or failure

Suggested interfaces:

```go
package worker

import (
	"context"

	domainjob "github.com/your-org/distributed-job-platform/internal/domain/job"
)

type Handler interface {
	Handle(ctx context.Context, job *domainjob.Job) error
}

type Registry map[string]Handler
```

#### Stage 5: Add initial job handlers

Files to add:

- `internal/worker/handlers/email.go`
- `internal/worker/handlers/pdf.go`
- `internal/worker/handlers/image.go`

Responsibilities:

- keep them simple and deterministic for now
- simulate business work before integrating real providers
- return standard errors so retry logic can classify them later

#### Stage 6: Add bounded concurrency

Files to add or update:

- `internal/worker/pool.go`
- `internal/worker/worker.go`

Responsibilities:

- fixed number of goroutines
- jobs fed through channel
- no unbounded goroutine creation

Suggested approach:

- a configurable worker count such as `10`, `20`, or `50`
- one consumer loop pushing messages into a channel
- worker goroutines consuming from that channel

### Week 2 acceptance criteria

1. API returns `202 Accepted` after storing and enqueueing.
2. Worker runs as a separate process.
3. At least one handler executes a job end to end.
4. Job status changes are persisted correctly.
5. Worker concurrency is fixed and configurable.
6. No job is processed inside the API server.

## Week 3 Plan: Reliability and Observability

### Week 3 objective

Make the async platform production-oriented: retries, dead-letter behavior, duplicate protection, metrics, logs, traces, and load testing.

### Week 3 deliverables

1. Retry policy with exponential backoff.
2. Dead-letter strategy after max retries.
3. Worker crash recovery using Redis Streams pending entries.
4. Idempotency and duplicate execution controls.
5. Prometheus metrics.
6. Structured logging with job context.
7. OpenTelemetry tracing.
8. k6-based load and failure testing.

### Week 3 sequence

#### Stage 1: Retry policy

Files to add:

- `internal/retry/policy.go`

Responsibilities:

- exponential backoff
- max retry count
- terminal failure after retry budget exhausted

Suggested code shape:

```go
package retry

import "time"

func Backoff(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}

	d := time.Second << (attempt - 1)
	if d > 30*time.Second {
		return 30 * time.Second
	}
	return d
}
```

#### Stage 2: Dead-letter handling

Files to add or update:

- `internal/worker/failure.go`
- `internal/queue/queue.go`
- `internal/infrastructure/queue/redis_streams.go`

Responsibilities:

- after max retries, stop reprocessing indefinitely
- push exhausted jobs to a dead-letter stream or mark them terminal in Postgres
- preserve failure reason for inspection

#### Stage 3: Pending entry recovery and duplicate prevention

Files to add or update:

- `internal/worker/recovery.go`
- `internal/infrastructure/queue/redis_streams.go`

Responsibilities:

- reclaim abandoned messages with `XAUTOCLAIM`
- avoid duplicate business side effects
- keep idempotency key protection at submission time
- optionally introduce resource locks for non-repeatable jobs

Important distinction:

- idempotency key protects duplicate job creation from clients
- worker-side lock or handler-side check protects duplicate execution of the same side effect

#### Stage 4: Metrics

Files to add:

- `internal/observability/metrics.go`
- `internal/bootstrap/metrics.go`

Metrics to expose:

- `jobs_created_total`
- `jobs_completed_total`
- `jobs_failed_total`
- `jobs_retried_total`
- `queue_depth`
- `job_processing_seconds`
- `worker_busy`
- `worker_idle`

#### Stage 5: Structured logging

Files to add or update:

- `internal/observability/logging.go`
- `internal/bootstrap/logger.go`
- worker and API entry points

Every log line around job processing should include:

- `job_id`
- `worker_id`
- `job_type`
- `status`
- `retry_count`
- `duration_ms`
- `error`
- `trace_id`

#### Stage 6: Tracing

Files to add:

- `internal/observability/tracing.go`

Responsibilities:

- trace request submission path
- propagate trace metadata to worker processing if possible
- trace external side-effect boundaries such as SMTP, PDF generation, or image work

#### Stage 7: Load and failure testing

Files to add:

- `tests/k6/jobs.js` or `load/k6/jobs.js`

Scenarios:

1. submit 100,000 jobs over a controlled interval
2. mix job types
3. kill a worker during processing
4. inject random handler failures
5. slow one handler path artificially

Success criteria:

- API p95 latency under target
- queue drain completes
- no lost jobs
- failure states visible
- retries work as expected

### Week 3 acceptance criteria

1. Failed jobs retry with exponential backoff.
2. Retry exhaustion is visible and not silent.
3. Worker crash recovery works from pending entries.
4. Metrics expose platform health.
5. Logs are structured and correlated to jobs.
6. Basic traces exist across API and worker boundaries.
7. Load testing validates functional behavior under stress.

## Recommended Folder Structure After Week 3

```text
cmd/
  api/
  worker/

internal/
  application/job/
  domain/job/
  interfaces/http/
  infrastructure/postgres/
  infrastructure/queue/
  observability/
  queue/
  retry/
  worker/

migrations/
configs/
deployments/
tests/k6/
```

## File-by-File Build Map

### Keep and finish

- `cmd/api/main.go`
- `internal/bootstrap/app.go`
- `internal/bootstrap/config.go`
- `internal/bootstrap/logger.go`
- `internal/bootstrap/metrics.go`
- `internal/application/job/dto.go`
- `internal/application/job/service.go`
- `internal/domain/job/entity.go`
- `internal/domain/job/repository.go`
- `internal/domain/job/status.go`
- `internal/interfaces/http/request.go`
- `internal/interfaces/http/handler.go`
- `internal/interfaces/http/router.go`
- `internal/shared/response/response.go`

### Replace or remove directionally

- `internal/infrastructure/mongo/job_repository.go`

Reason:

- current project outcome targets Postgres as durable store
- keeping Mongo here adds confusion and duplicate persistence paths

### Add in Week 2

- `cmd/worker/main.go`
- `internal/queue/queue.go`
- `internal/infrastructure/queue/redis_streams.go`
- `internal/worker/worker.go`
- `internal/worker/registry.go`
- `internal/worker/pool.go`
- `internal/worker/handlers/email.go`
- `internal/worker/handlers/pdf.go`
- `internal/worker/handlers/image.go`

### Add in Week 3

- `internal/retry/policy.go`
- `internal/worker/recovery.go`
- `internal/worker/failure.go`
- `internal/observability/metrics.go`
- `internal/observability/logging.go`
- `internal/observability/tracing.go`
- `tests/k6/jobs.js`

## Standards To Follow While Writing Code

- keep domain types free from transport and infrastructure concerns
- keep the API handler thin
- keep validation at API and service boundaries
- do not let worker handlers know Redis details
- prefer interfaces at boundaries, not everywhere
- use structured logs, not formatted print debugging
- use context propagation consistently
- treat Postgres as the source of truth
- treat Redis as delivery and coordination infrastructure, not durable metadata storage

## Risks To Avoid

- executing jobs in the API server
- mixing Mongo and Postgres for the same metadata path
- unbounded goroutine-per-job design
- retry loops without max attempts
- no dead-letter path
- no crash recovery for claimed jobs
- no idempotency key support
- weak observability until the end

## Practical Next Step

Before starting Week 2, first close the visible Week 1 gaps in the current codebase:

1. finish Postgres repository
2. correct bootstrap wiring
3. correct HTTP router and handlers
4. finish SQL migration
5. update config from Mongo to Postgres and Redis
6. confirm `go build ./...` and manual API testing

Only after that should the project move to Redis Streams and worker execution.
