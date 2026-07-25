package postgres

import (
	"context"
	"database/sql"
	"errors"

	domainjob "distributed-job-platform/internal/domain/job"
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
			idempotency_key, retry_count, max_retries, created_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
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
		       error_message, worker_id, created_at, started_at, completed_at
		FROM jobs
		WHERE idempotency_key = $1
	`

	return r.scanOne(ctx, query, key)
}

func (r *JobRepository) GetByID(ctx context.Context, id string) (*domainjob.Job, error) {
	query := `
		SELECT id, type, status, priority, payload,
		       idempotency_key, retry_count, max_retries,
		       error_message, worker_id, created_at, started_at, completed_at
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

func (r *JobRepository) List(ctx context.Context, limit, offset int) ([]domainjob.Job, error) {
	query := `
		SELECT id, type, status, priority, payload,
		       idempotency_key, retry_count, max_retries,
		       error_message, worker_id, created_at, started_at, completed_at
		FROM jobs
		ORDER BY created_at DESC
		LIMIT $1 OFFSET $2
	`

	rows, err := r.db.QueryContext(ctx, query, limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

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
