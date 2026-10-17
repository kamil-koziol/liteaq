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

const jobColumns = "id, type, payload, created_at, dispatched_at, completed_at"

type rowScanner interface{ Scan(dest ...any) error } // *sql.Row and *sql.Rows

func millisPtr(n sql.NullInt64) *time.Time {
	if !n.Valid {
		return nil
	}
	t := time.UnixMilli(n.Int64).UTC()
	return &t
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
		createdAt                 int64
		dispatchedAt, completedAt sql.NullInt64
	)

	if err := row.Scan(
		&j.ID,
		&j.Type,
		&j.Payload,
		&createdAt,
		&dispatchedAt,
		&completedAt,
	); err != nil {
		return fmt.Errorf("scan row: %w", err)
	}

	j.CreatedAt = time.UnixMilli(createdAt).UTC()
	j.DispatchedAt = millisPtr(dispatchedAt)
	j.CompletedAt = millisPtr(completedAt)

	j.Status = j.status()
	return nil
}

func (q *Querier) Enqueue(ctx context.Context, exec dbexecutor, task *Job) (*JobDetails, error) {
	query := `
		INSERT INTO liteaq_jobs (type, payload, created_at)
		VALUES (?, ?, ?)
		RETURNING
	` + " " + jobColumns + ";"

	var details JobDetails
	row := exec.QueryRowContext(ctx, query, task.Type, task.Payload, now())
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
				created_at <= current_timestamp 
				AND completed_at IS NULL
				AND dispatched_at IS NULL
			ORDER BY created_at, id
			LIMIT 1
		)
		RETURNING
	` + " " + jobColumns + ";"

	var details JobDetails
	row := exec.QueryRowContext(ctx, query, now())
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
			dispatched_at INTEGER,
			completed_at INTEGER
		) STRICT;

		CREATE INDEX IF NOT EXISTS idx_liteaq_jobs_pending
		ON liteaq_jobs(created_at, id)
		WHERE dispatched_at IS NULL AND completed_at IS NULL;
	`)
	return err
}
