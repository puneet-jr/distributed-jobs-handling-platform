package postgres

import (
	"context"
	"database/sql"
	"errors"
	"time"

	domainjob "distributed-job-platform/internal/domain/job"
	"github.com/lib/pq"
)

type JobRepository struct {
	db *sql.DB
}

func NewJobRepository(db *sql.DB) (*JobRepository, error) {
	if db == nil {
		return nil, errors.New("database connection cannot be nil")
	}
	return &JobRepository{db: db}, nil
}

func (r *JobRepository) Create(ctx context.Context, job *domainjob.Job) error {
	query := `
		INSERT INTO jobs (
			id, type, status, priority, payload,
			idempotency_key, retry_count, max_retries, created_at, next_run_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		ON CONFLICT (idempotency_key) DO NOTHING
	`

	result, err := r.db.ExecContext(ctx, query,
		job.ID,
		job.Type,
		job.Status,
		job.Priority,
		job.Payload,
		nullString(job.IdempotencyKey),
		job.RetryCount,
		job.MaxRetries,
		job.CreatedAt,
		job.NextRunAt,
	)
	if err != nil {
		return err
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return domainjob.ErrDuplicateIdempotencyKey
	}

	return nil
}

func (r *JobRepository) GetByIdempotencyKey(ctx context.Context, key string) (*domainjob.Job, error) {
	query := `
		SELECT id, type, status, priority, payload,
		       idempotency_key, retry_count, max_retries,
		       error_message, worker_id, created_at, next_run_at, started_at, completed_at
		FROM jobs
		WHERE idempotency_key = $1
	`

	return r.scanOne(ctx, query, key)
}

func (r *JobRepository) GetByID(ctx context.Context, id string) (*domainjob.Job, error) {
	query := `
		SELECT id, type, status, priority, payload,
		       idempotency_key, retry_count, max_retries,
		       error_message, worker_id, created_at, next_run_at, started_at, completed_at
		FROM jobs
		WHERE id = $1
	`

	return r.scanOne(ctx, query, id)
}

func (r *JobRepository) UpdateStatus(
	ctx context.Context,
	id string,
	status domainjob.Status,
	workerID *string,
	errMsg *string,
) error {
	query := `
		UPDATE jobs
		SET status = $1,
		    worker_id = COALESCE($2, worker_id),
		    error_message = COALESCE($3, error_message),
		    next_run_at = CASE
		        WHEN $1 IN ('completed', 'failed', 'cancelled', 'running') THEN NULL
		        ELSE next_run_at
		    END,
		    started_at = CASE
		        WHEN $1 = 'running' AND started_at IS NULL THEN NOW()
		        ELSE started_at
		    END,
		    completed_at = CASE
		        WHEN $1 IN ('completed', 'failed', 'cancelled') AND completed_at IS NULL THEN NOW()
		        ELSE completed_at
		    END
		WHERE id = $4
	`

	result, err := r.db.ExecContext(ctx, query, status, workerID, errMsg, id)
	if err != nil {
		return err
	}

	return ensureUpdated(result)
}

func (r *JobRepository) UpdateStatusIfCurrent(
	ctx context.Context,
	id string,
	from []domainjob.Status,
	to domainjob.Status,
	workerID *string,
	errMsg *string,
) error {
	query := `
		UPDATE jobs
		SET status = $1,
		    worker_id = COALESCE($2, worker_id),
		    error_message = $3,
		    next_run_at = CASE
		        WHEN $1 IN ('completed', 'failed', 'cancelled', 'running') THEN NULL
		        ELSE next_run_at
		    END,
		    started_at = CASE
		        WHEN $1 = 'running' AND started_at IS NULL THEN NOW()
		        ELSE started_at
		    END,
		    completed_at = CASE
		        WHEN $1 IN ('completed', 'failed', 'cancelled') AND completed_at IS NULL THEN NOW()
		        ELSE completed_at
		    END
		WHERE id = $4
		  AND status = ANY($5)
	`

	result, err := r.db.ExecContext(
		ctx,
		query,
		to,
		workerID,
		errMsg,
		id,
		pq.Array(statusStrings(from)),
	)
	if err != nil {
		return err
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return domainjob.ErrInvalidStatusTransition
	}

	return nil
}

func statusStrings(statuses []domainjob.Status) []string {
	out := make([]string, 0, len(statuses))
	for _, status := range statuses {
		out = append(out, string(status))
	}
	return out
}

func (r *JobRepository) IncrementRetryCount(ctx context.Context, id string, errMsg string) error {
	query := `
		UPDATE jobs
		SET retry_count = retry_count + 1,
		    error_message = $2,
		    status = 'retrying'
		WHERE id = $1
	`

	result, err := r.db.ExecContext(ctx, query, id, errMsg)
	if err != nil {
		return err
	}

	return ensureUpdated(result)
}

func (r *JobRepository) ClaimRunnable(ctx context.Context, id string, workerID string) error {
	query := `
		UPDATE jobs
		SET status = 'running',
		    worker_id = $2,
		    error_message = NULL,
		    next_run_at = NULL,
		    started_at = NOW(),
		    completed_at = NULL
		WHERE id = $1
		  AND status IN ('pending', 'retrying')
		  AND (next_run_at IS NULL OR next_run_at <= NOW())
	`

	result, err := r.db.ExecContext(ctx, query, id, workerID)
	if err != nil {
		return err
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return domainjob.ErrInvalidStatusTransition
	}
	return nil
}

func (r *JobRepository) ScheduleRetry(
	ctx context.Context,
	id string,
	workerID string,
	errMsg string,
	nextRunAt time.Time,
) error {
	query := `
		UPDATE jobs
		SET status = 'retrying',
		    retry_count = retry_count + 1,
		    error_message = $3,
		    worker_id = $2,
		    next_run_at = $4,
		    completed_at = NULL
		WHERE id = $1
		  AND status = 'running'
		  AND worker_id = $2
	`

	result, err := r.db.ExecContext(ctx, query, id, workerID, errMsg, nextRunAt)
	if err != nil {
		return err
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return domainjob.ErrInvalidStatusTransition
	}
	return nil
}

func (r *JobRepository) ReclaimStaleRunning(
	ctx context.Context,
	staleBefore time.Time,
	limit int,
) ([]domainjob.Job, error) {
	query := `
		UPDATE jobs
		SET status = 'pending',
		    worker_id = NULL,
		    next_run_at = NULL,
		    error_message = COALESCE(error_message, 'reclaimed stale running job')
		WHERE id IN (
			SELECT id
			FROM jobs
			WHERE status = 'running'
			  AND started_at < $1
			ORDER BY started_at ASC
			LIMIT $2
		)
		RETURNING id, type, status, priority, payload,
		          idempotency_key, retry_count, max_retries,
		          error_message, worker_id, created_at, next_run_at, started_at, completed_at
	`

	rows, err := r.db.QueryContext(ctx, query, staleBefore, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return scanRows(rows)
}

func (r *JobRepository) ListRunnableRetries(ctx context.Context, limit int) ([]domainjob.Job, error) {
	query := `
		UPDATE jobs
		SET status = 'pending',
		    worker_id = NULL
		WHERE id IN (
			SELECT id
			FROM jobs
			WHERE status = 'retrying'
			  AND next_run_at <= NOW()
			ORDER BY next_run_at ASC
			LIMIT $1
		)
		RETURNING id, type, status, priority, payload,
		          idempotency_key, retry_count, max_retries,
		          error_message, worker_id, created_at, next_run_at, started_at, completed_at
	`

	rows, err := r.db.QueryContext(ctx, query, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return scanRows(rows)
}

func (r *JobRepository) List(ctx context.Context, limit, offset int) ([]domainjob.Job, error) {
	query := `
		SELECT id, type, status, priority, payload,
		       idempotency_key, retry_count, max_retries,
		       error_message, worker_id, created_at, next_run_at, started_at, completed_at
		FROM jobs
		ORDER BY created_at DESC
		LIMIT $1 OFFSET $2
	`

	rows, err := r.db.QueryContext(ctx, query, limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return scanRows(rows)
}

func (r *JobRepository) scanOne(ctx context.Context, query string, arg any) (*domainjob.Job, error) {
	row := r.db.QueryRowContext(ctx, query, arg)

	job, err := scanJob(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, domainjob.ErrJobNotFound
		}
		return nil, err
	}

	return job, nil
}

type scanner interface {
	Scan(dest ...any) error
}

func scanJob(s scanner) (*domainjob.Job, error) {
	job := &domainjob.Job{}
	var idempotencyKey sql.NullString

	err := s.Scan(
		&job.ID,
		&job.Type,
		&job.Status,
		&job.Priority,
		&job.Payload,
		&idempotencyKey,
		&job.RetryCount,
		&job.MaxRetries,
		&job.ErrorMessage,
		&job.WorkerID,
		&job.CreatedAt,
		&job.NextRunAt,
		&job.StartedAt,
		&job.CompletedAt,
	)
	if err != nil {
		return nil, err
	}

	if idempotencyKey.Valid {
		job.IdempotencyKey = idempotencyKey.String
	}

	return job, nil
}

func scanRows(rows *sql.Rows) ([]domainjob.Job, error) {
	jobs := make([]domainjob.Job, 0)

	for rows.Next() {
		job, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, *job)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return jobs, nil
}

func ensureUpdated(result sql.Result) error {
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return domainjob.ErrJobNotFound
	}
	return nil
}

func nullString(s string) sql.NullString {
	if s == "" {
		return sql.NullString{Valid: false}
	}
	return sql.NullString{String: s, Valid: true}
}
