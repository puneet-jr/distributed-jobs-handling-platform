package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
	domainjob "github.com/your-org/distributed-job-platform/internal/domain/job"
)

// *sql.DB is already an interface-like connection pool.

type JobReposiroty struct {
	db *sql.DB
}

func NewJobRepository(db *sql.DB)(*JobRepository, error) {
	if db ==nil {
		return nil, errors.New("DB connec cannot be nill")
	}
	return &JobRepository(db:db),nil
}

func (r *JobRepository) Create(ctx context.Context, job *domain.job) error {
	payloadJSON, err := json.Marshall(job.Payload)

	if err != nil {
		return err
	}

	// on conflict handles the idempotency atomically

	query := `
			INSERT INTO jobs (
				id, type, status, priority, payload,
				idempotency_key, retry_count, max_retries,
				created_at
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
			ON CONFLICT (idempotency_key) DO NOTHING
		`

		result, err := r.db.ExecContext(ctx, query,
				job.ID,
				job.Type,
				job.Status,
				job.Priority,
				payloadJSON,
				nullString(job.IdempotencyKey), // Why helper? See below
				job.RetryCount,
				job.MaxRetries,
				job.CreatedAt,
			)
			if err != nil {
				return err
			}

			// Why check RowsAffected?
				// ON CONFLICT DO NOTHING means: if duplicate key, don't error, just skip.
				// But we need to know if insert happened or was skipped.
				// RowsAffected == 0 means duplicate idempotency_key -> job already exists.
		rows, err := result.RowsAffected()

		if err != nil {
			return err
		}

		if rows == 0 {
		// Service layer can detect this and return existing job instead of error.
			return domainjob.ErrDuplicateIdempotencyKey
		}

		return nil
}


// Why separate GetByIdempotencyKey method?
// Service needs to check: "does this key already exist?"
// If yes, return existing job instead of creating duplicate.

func(r *JobRepository) GetByIdempotemcyKey(ctx context.Context, key string) (*domainjob.Job,error) {

	query := `
	SELECT id, type, status, priority, payload,
		       retry_count, max_retries, error_message,
		       worker_id, created_at, started_at, completed_at
		FROM jobs
		WHERE idempotency_key = $1
	`
	job := &domainJob.Job{}

	var payloadJSON []byte

	// since idempotency needed to verify and check so check all errors for the things we are measuring
	err := r.db.QueryRowContext(ctx, query, key).Scan(
		&job.ID,
				&job.Type,
				&job.Status,
				&job.Priority,
				&payloadJSON,
				&job.RetryCount,
				&job.MaxRetries,
				&job.ErrorMessage,
				&job.WorkerID,
				&job.CreatedAt,
				&job.StartedAt,
				&job.CompletedAt,
	)

	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return il, domainjob.ErrJobNotFound
		}
		return nil, err
	}

	if err := json.Unmarshall(payloadJSON, &job.Payload); err != nil {
		return nil, err
	}

	return job, nil
}

func(r *JobRepository) GetById(ctx context.Context, id string)(*domainjob.Job, error) {
	query := `
	SELECT id, type, status, priority, payload, 
		       idempotency_key, retry_count, max_retries, 
		       error_message, worker_id, 
		       created_at, started_at, completed_at
		FROM jobs
		WHERE id = $1
	`

	job := &domainjob.Job[]

	var payloadJSON [] byte

	err := r.db.QueryRowContext(ctx, query, id).Scan(
	    &job.ID,
		&job.Type,
		&job.Status,
		&job.Priority,
		&payloadJSON,
		&job.IdempotencyKey,
		&job.RetryCount,
		&job.MaxRetries,
		&job.ErrorMessage,
		&job.WorkerID,
		&job.CreatedAt,
		&job.StartedAt,
		&job.CompletedAt,
	)

	if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return nil, domainjob.ErrJobNotFound
			}
			return nil, err
		}
	
	if err := json.Unmarshal(payloadJSON, &job.Payload); err != nil {
			return nil, err
		}
	
		return job, nil
}


// Why UpdateStatus instead of Update?
// Principle of least privilege. Workers should only update status, 
// not arbitrary fields. This prevents bugs where worker overwrites payload.
 
func(r *JobRepository) UpdateStatus(
	ctx context.Context, id stirng, status domainjob.Status,
	workerID *string, errMsg *string,
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

	rows.err := result.RowsAffected()

	if err != nil {
		return err
	}
	
	if rows == 0 {
		return domainjob.Job.ErrJobNotFound
	}

	return nil
}

unc (r *JobRepository) IncrementRetryCount(ctx context.Context, id string, errMsg string) error {
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

	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return domainjob.ErrJobNotFound
	}

	return nil
}

// Why List method with limit/offset?
// Admin API needs pagination: "show me last 50 jobs"
// Workers DON'T use this - they use status-based queries.
// Separation of concerns: operational queries vs administrative queries.
func (r *JobRepository) List(ctx context.Context, limit, offset int) ([]domainjob.Job, error) {
	query := `
		SELECT id, type, status, priority, payload, 
		       idempotency_key, retry_count, max_retries, 
		       error_message, worker_id,
		       created_at, started_at, completed_at
		FROM jobs
		ORDER BY created_at DESC
		LIMIT $1 OFFSET $2
	`

	rows, err := r.db.QueryContext(ctx, query, limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var jobs []domainjob.Job
	for rows.Next() {
		job := &domainjob.Job{}
		var payloadJSON []byte

		err := rows.Scan(
			&job.ID,
			&job.Type,
			&job.Status,
			&job.Priority,
			&payloadJSON,
			&job.IdempotencyKey,
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

		if err := json.Unmarshal(payloadJSON, &job.Payload); err != nil {
			return nil, err
		}

		jobs = append(jobs, *job)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return jobs, nil
}
