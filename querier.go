package liteaq

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

type Querier struct {
}

type dbexecutor interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

func NewQuerier() *Querier {
	return &Querier{}
}

func now() int64 {
	return time.Now().UnixMilli()
}

const jobColumns = "id, type, payload, created_at, dispatched_at, completed_at, retry, max_retries, failed_at, root_id, execute_at"

type rowScanner interface{ Scan(dest ...any) error } // *sql.Row and *sql.Rows

func millisPtr(n sql.NullInt64) *time.Time {
	if !n.Valid {
		return nil
	}
	t := time.UnixMilli(n.Int64).UTC()
	return &t
}

func intPtr(n sql.NullInt64) *int64 {
	if !n.Valid {
		return nil
	}
	return &n.Int64
}

func (t *JobDetails) status() Status {
	switch {
	case t.CompletedAt != nil:
		return StatusCompleted
	case t.DispatchedAt != nil:
		return StatusRunning
	default:
		return StatusPending
	}
}

func (j *JobDetails) scan(row rowScanner) error {
	// Make sure it's matching [jobColumns]

	var (
		createdAt, executeAt                        int64
		dispatchedAt, completedAt, failedAt, rootID sql.NullInt64
	)

	if err := row.Scan(
		&j.ID,
		&j.Type,
		&j.Payload,
		&createdAt,
		&dispatchedAt,
		&completedAt,
		&j.Retry,
		&j.MaxRetries,
		&failedAt,
		&rootID,
		&executeAt,
	); err != nil {
		return fmt.Errorf("scan row: %w", err)
	}

	j.CreatedAt = time.UnixMilli(createdAt).UTC()
	j.DispatchedAt = millisPtr(dispatchedAt)
	j.CompletedAt = millisPtr(completedAt)
	j.FailedAt = millisPtr(failedAt)
	j.RootId = intPtr(rootID)
	j.ExecuteAt = time.UnixMilli(executeAt).UTC()

	j.Status = j.status()
	return nil
}

func (q *Querier) Enqueue(ctx context.Context, exec dbexecutor, job *Job) (*JobDetails, error) {
	query := `
		INSERT INTO liteaq_jobs (type, payload, created_at, execute_at, max_retries)
		VALUES (?, ?, ?, ?, ?)
		RETURNING
	` + " " + jobColumns + ";"

	var details JobDetails
	n := now()
	row := exec.QueryRowContext(ctx, query, job.Type, job.Payload, n, n, job.MaxRetries)
	if err := details.scan(row); err != nil {
		return nil, fmt.Errorf("scan row: %w", err)
	}

	return &details, nil
}

func nextRootID(job *JobDetails) int64 {
	if job.RootId != nil {
		return *job.RootId
	}

	// first retry
	return job.ID
}

func (q *Querier) Retry(ctx context.Context, exec dbexecutor, job *JobDetails, delay time.Duration) (*JobDetails, error) {
	query := `
		INSERT INTO liteaq_jobs (type, payload, created_at, retry, max_retries, root_id, execute_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		RETURNING
	` + " " + jobColumns + ";"

	var details JobDetails

	n := time.Now()

	row := exec.QueryRowContext(ctx, query, job.Type, job.Payload, n.UnixMilli(), job.Retry+1, job.MaxRetries, nextRootID(job), n.Add(delay).UnixMilli())
	if err := details.scan(row); err != nil {
		return nil, fmt.Errorf("scan row: %w", err)
	}

	return &details, nil
}

func (q *Querier) Fail(ctx context.Context, exec dbexecutor, id int64, reason string) (*JobDetails, error) {
	query := `
		UPDATE liteaq_jobs
		SET failed_at = ?
		WHERE 
			id = ? 
			AND completed_at IS NULL 
			AND dispatched_at IS NOT NULL
			AND failed_at IS NULL
		RETURNING
	` + " " + jobColumns + ";"

	var details JobDetails
	row := exec.QueryRowContext(ctx, query, now(), id, reason)
	if err := details.scan(row); err != nil {
		return nil, fmt.Errorf("scan row: %w", err)
	}

	return &details, nil
}

func (q *Querier) Dequeue(ctx context.Context, exec dbexecutor) (*JobDetails, error) {
	query := `
		UPDATE liteaq_jobs
		SET dispatched_at = ?
		WHERE id = (
			SELECT id FROM liteaq_jobs
			WHERE 
				execute_at <= ? 
				AND completed_at IS NULL
				AND dispatched_at IS NULL
				AND failed_at IS NULL
			ORDER BY created_at, id
			LIMIT 1
		)
		RETURNING
	` + " " + jobColumns + ";"

	var details JobDetails
	n := now()
	row := exec.QueryRowContext(ctx, query, n, n)
	if err := details.scan(row); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("scan row: %w", err)
	}

	return &details, nil
}

func (q *Querier) Complete(ctx context.Context, exec dbexecutor, id int64) (*JobDetails, error) {
	query := `
		UPDATE liteaq_jobs
		SET completed_at = ?
		WHERE 
			id = ? 
			AND completed_at IS NULL 
			AND dispatched_at IS NOT NULL
			AND failed_at IS NULL
		RETURNING
	` + " " + jobColumns + ";"

	var details JobDetails
	row := exec.QueryRowContext(ctx, query, now(), id)
	if err := details.scan(row); err != nil {
		return nil, fmt.Errorf("scan row: %w", err)
	}

	return &details, nil
}

func Setup(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS liteaq_jobs (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			type TEXT NOT NULL,
			payload BLOB,
			created_at INTEGER NOT NULL,
			execute_at INTEGER NOT NULL,
			dispatched_at INTEGER,
			completed_at INTEGER,
			retry INTEGER NOT NULL DEFAULT 0,
			max_retries INTEGER NOT NULL DEFAULT 5,
			failed_at INTEGER,
			failed_reason TEXT,
			root_id INTEGER
		) STRICT;

		CREATE INDEX IF NOT EXISTS idx_liteaq_jobs_pending
		ON liteaq_jobs(created_at, id)
		WHERE dispatched_at IS NULL AND completed_at IS NULL;
	`)
	return err
}
