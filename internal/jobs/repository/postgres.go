package reposiroty

import (
	"context"
	"database/sql"
	"distributed-job-platform/internal/jobs/model"
    "fmt"
)

type JobRepositoryImpl struct {
	db *sql.DB
}

func NewJobRepository(db *sql.DB) JobRepositoryImpl {
	return &JobRepositoryImpl{db:db}
}

func (r *JobRepositoryImpl) Insert(ctx context.Context, job *model.Job) error {
    query := `INSERT INTO jobs (id, type, status, payload, created_at, updated_at) 
              VALUES ($1, $2, $3, $4, $5, $6)`
    _, err := r.db.ExecContext(ctx, query, job.ID, job.Type, job.Status, job.Payload, job.CreatedAt, job.UpdatedAt)
    return err
}

func (r *JobRepositoryImpl) GetByID(ctx context.Context, id string) (*model.Job, error) {
    query := `SELECT id, type, status, payload, created_at, updated_at FROM jobs WHERE id = $1`
    row := r.db.QueryRowContext(ctx, query, id)

    var j model.Job
    if err := row.Scan(&j.ID, &j.Type, &j.Status, &j.Payload, &j.CreatedAt, &j.UpdatedAt); err != nil {
        if err == sql.ErrNoRows {
            return nil, fmt.Errorf("job not found: %w", err)  // wrapped, not raw
        }
        return nil, fmt.Errorf("scan job: %w", err)  // wrap other DB errors too
    }
    return &j, nil
}

func (r *JobRepositoryImpl) List(ctx context.Context) ([]*model.Job, error) {
    query := `SELECT id, type, status, payload, created_at, updated_at FROM jobs ORDER BY created_at DESC`
    rows, err := r.db.QueryContext(ctx, query)
    if err != nil {
        return nil, err
    }
    defer rows.Close()

    var jobs []*model.Job
    for rows.Next() {
        var j model.Job
        if err := rows.Scan(&j.ID, &j.Type, &j.Status, &j.Payload, &j.CreatedAt, &j.UpdatedAt); err != nil {
            return nil, err
        }
        jobs = append(jobs, &j)
    }
    return jobs, nil
}

func (r *JobRepositoryImpl) UpdateStatus(ctx context.Context, id string, status model.JobStatus) error {
    query := `UPDATE jobs SET status = $1, updated_at = NOW() WHERE id = $2`
    _, err := r.db.ExecContext(ctx, query, status, id)
    return err
}