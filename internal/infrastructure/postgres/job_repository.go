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
