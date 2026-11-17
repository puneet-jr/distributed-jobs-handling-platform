# Distributed Job Platform

An asynchronous job-processing platform written in Go. Clients submit jobs over HTTP; the API persists them in PostgreSQL and enqueues them on a Redis Stream; a separate worker process consumes and executes them with bounded concurrency, retries, and graceful shutdown.

The design priority is **job-lifecycle correctness**: durable state, guarded status transitions, idempotent submission, and safe execution outside the HTTP request path.

## Architecture

```text
                 POST /jobs                     XADD
   Client  ───────────────────►  API  ───────────────────►  Redis Stream ("jobs")
                                  │                               │
                         INSERT (PostgreSQL)                XREADGROUP / XAUTOCLAIM
                                  │                               │
                                  ▼                               ▼
                            PostgreSQL  ◄────── status updates ── Worker pool
                           (source of truth)                     (N goroutines)
```

| Component | Responsibility |
|---|---|
| `cmd/api` | HTTP server: create jobs, read job status, health, Prometheus metrics |
| `cmd/worker` | Consumes the stream, runs handlers, updates job state, exposes its own `/healthz` and `/metrics` |
| PostgreSQL | Durable job metadata and lifecycle state |
| Redis Streams | Queue with consumer groups, acknowledgements, and reclaim of stalled messages |

### Job lifecycle

```text
pending ──► running ──► completed
   │           │
   │           ├──► retrying ──► pending / running   (after exponential backoff)
   │           └──► failed                            (retries exhausted)
   └──► cancelled
```

Transitions are enforced in the domain layer (`internal/domain/job/status.go`) and applied in PostgreSQL as atomic compare-and-set updates (`UPDATE ... WHERE status IN (...)`), so two workers cannot claim the same job.

## Features

- **Idempotent job creation.** Clients send an `Idempotency-Key` header. A unique constraint on `idempotency_key` plus `ON CONFLICT DO NOTHING` ensures a repeated request returns the existing job instead of creating a duplicate.
- **Worker runtime.** Fixed-size worker pool (bounded concurrency), pluggable handler registry keyed by job type, batch polling.
- **Graceful shutdown.** `SIGINT`/`SIGTERM` cancel the root context, in-flight jobs drain, and a second signal forces exit.
- **Retries with exponential backoff.** 1s doubling per attempt, capped at 5 minutes, bounded by a per-job `max_retries`.
- **Crash recovery.** Pending stream messages idle for 2 minutes are reclaimed via `XAUTOCLAIM`, and stale `running` jobs are re-queued.
- **Observability.** Prometheus metrics on both the API and the worker (see below), plus health endpoints.
- **Load tests.** k6 scripts for smoke and constant-rate baseline testing.

## Project layout

```text
cmd/
  api/                  API entrypoint
  worker/               Worker entrypoint
internal/
  domain/job/           Entity, status machine, errors, repository interface
  application/job/      Service: create, claim, complete, retry/backoff
  interfaces/http/      Handlers, router, request types
  infrastructure/
    postgres/           Job repository (SQL)
    redisqueue/         Redis Streams producer/consumer
    observability/      Health checks
  workers/              Worker loop, handler registry, job handlers
  observability/        Prometheus metrics, queue-depth collector
  bootstrap/            Config, logger, wiring
migrations/             SQL migrations
configs/local.yaml      Local configuration
deployments/            docker-compose.yml (Postgres + Redis)
tests/k6/               Load-test scripts
```

## Getting started

### Prerequisites

- Go 1.25+
- Docker and Docker Compose
- [k6](https://k6.io/) (optional, for load tests)

### 1. Start PostgreSQL and Redis

```bash
docker compose -f deployments/docker-compose.yml up -d
```

### 2. Apply the migration

```bash
docker compose -f deployments/docker-compose.yml exec -T postgres \
  psql -U jobs -d jobs < migrations/001_create_jobs_table.up.sql
```

PowerShell:

```powershell
Get-Content .\migrations\001_create_jobs_table.up.sql |
  docker compose -f .\deployments\docker-compose.yml exec -T postgres psql -U jobs -d jobs
```

### 3. Run the API

```bash
CONFIG_PATH=./configs/local.yaml go run ./cmd/api
```

PowerShell:

```powershell
$env:CONFIG_PATH = ".\configs\local.yaml"
go run .\cmd\api
```

### 4. Run a worker

```bash
CONFIG_PATH=./configs/local.yaml \
WORKER_ID=worker-local-1 \
WORKER_CONCURRENCY=10 \
WORKER_BATCH_SIZE=10 \
WORKER_METRICS_ADDR=:9091 \
go run ./cmd/worker
```

PowerShell:

```powershell
$env:CONFIG_PATH = ".\configs\local.yaml"
$env:WORKER_ID = "worker-local-1"
$env:WORKER_CONCURRENCY = "10"
$env:WORKER_BATCH_SIZE = "10"
$env:WORKER_METRICS_ADDR = ":9091"
go run .\cmd\worker
```

More step-by-step commands (health checks, idempotency verification, k6) are in [commands.md](commands.md).

## Configuration

`configs/local.yaml`:

```yaml
app:      { env: local }
http:     { port: 8080 }
postgres: { host: localhost, port: 5432, user: jobs, password: jobs, database: jobs, sslmode: disable }
redis:    { host: localhost, port: 6379, password: "", db: 0 }
```

| Variable | Used by | Purpose |
|---|---|---|
| `CONFIG_PATH` | API, worker | Path to the YAML config |
| `WORKER_ID` | worker | Consumer name in the Redis consumer group |
| `WORKER_CONCURRENCY` | worker | Number of concurrent handler goroutines (default 10) |
| `WORKER_BATCH_SIZE` | worker | Messages fetched per poll (defaults to concurrency) |
| `WORKER_METRICS_ADDR` | worker | Address for the worker's `/healthz` and `/metrics` |

## API

| Method | Path | Description |
|---|---|---|
| `POST` | `/jobs` | Create a job. Returns `202` with the job ID and status |
| `GET` | `/jobs/{id}` | Fetch a job's status, retry count, and last error |
| `GET` | `/health` | API health check |
| `GET` | `/metrics` | Prometheus metrics |

### Create a job

```bash
curl -i -X POST http://localhost:8080/jobs \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: demo-001" \
  -d '{
        "type": "email",
        "payload": { "to": "test@example.com", "subject": "hello" },
        "priority": 0
      }'
```

Re-sending the same request with the same `Idempotency-Key` returns the existing job rather than creating a new one.

### Built-in job types

| Type | Required payload | Notes |
|---|---|---|
| `email` | `to` (string), optional `subject` | Logs the send; provider integration is a placeholder |
| `pdf` | `template` (string) | Simulates 500 ms of work |

Add a new type by implementing `workers.JobHandler` (`Handle(ctx, job) error`) and registering it in the handler registry.

## Metrics

Exposed in Prometheus format on the API (`:8080/metrics`) and the worker (`:9091/metrics`):

- `jobs_created_total`, `jobs_started_total`, `jobs_completed_total`, `jobs_failed_total`, `jobs_retried_total`
- `queue_depth`, `worker_active_jobs`
- `worker_poll_errors_total`, `handler_errors_total`
- `job_processing_duration_seconds`, `job_wait_duration_seconds` (histograms)

## Load testing

Scripts live in `tests/k6/`:

```bash
# Smoke test
k6 run tests/k6/jobs_smoke_vus.js

# 100 requests/second for 1 minute, unique idempotency key per request
k6 run tests/k6/jobs_baseline_100.js
```

The baseline test fails if more than 1% of requests error or fewer than 99% return `202`. Override the target with `-e BASE_URL=http://host:8080`.

## Known limitations

- **Dual write.** The API inserts into PostgreSQL and then enqueues to Redis as two separate steps. If the process dies between them, a job can exist in PostgreSQL without a queue message. The standard fix is a transactional outbox; this is not implemented yet.
- **No dead-letter queue.** Jobs that exhaust retries move to the terminal `failed` state in PostgreSQL; there is no separate dead-letter stream.
- **At-least-once delivery.** A message can be redelivered after a crash or reclaim, so job handlers must be idempotent.
- Docker Compose runs PostgreSQL and Redis only; the API and worker run on the host.

## Roadmap

- Transactional outbox to close the dual-write gap
- Dead-letter stream with inspection endpoint
- Job cancellation endpoint
- Dockerfiles and Compose services for the API and worker
