package liteaq

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"
)

// Server is responsible for pulling jobs out of SQLite and running them.
type Server struct {
	db         *sql.DB
	querier    *Querier
	retryDelay RetryDelayFunc
}

type ServerOption func(*Server)

func WithRetryDelayFunc(fn RetryDelayFunc) ServerOption {
	return func(s *Server) {
		s.retryDelay = fn
	}
}

func NewServer(db *sql.DB, opts ...ServerOption) *Server {
	srv := &Server{
		db:         db,
		querier:    NewQuerier(),
		retryDelay: DefaultRetryDelay,
	}

	for _, opt := range opts {
		opt(srv)
	}

	return srv
}

var ErrSkipRetry = errors.New("liteaq: skip retry")

func (s *Server) executeJob(ctx context.Context, mux *ServeMux, job *JobDetails) error {
	fn, exists := mux.getHandler(job.Type)
	if !exists {
		return fmt.Errorf("no handler for job type %q", job.Type)
	}

	return fn(ctx, job)
}

// RetryDelayFunc returns how long to wait before retrying a failed job.
type RetryDelayFunc func(job *JobDetails, err error) time.Duration

// DefaultRetryDelay is exponential backoff capped at 1h with 10% jitter
func DefaultRetryDelay(job *JobDetails, _ error) time.Duration {
	d := min(time.Second<<min(job.Retry, 20), time.Hour)
	return d + rand.N(d/10+1)
}

func (s *Server) retry(ctx context.Context, job *JobDetails, reason error) (*JobDetails, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("start tx: %w", err)
	}
	defer tx.Rollback()

	newJob, err := s.querier.Retry(ctx, tx, job, s.retryDelay(job, reason))
	if err != nil {
		return nil, fmt.Errorf("retry job: %w", err)
	}

	// fail the previous one
	if _, err := s.querier.Fail(ctx, tx, job.ID, reason.Error()); err != nil {
		return nil, fmt.Errorf("fail job: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit: %w", err)
	}

	return newJob, nil
}

func (s *Server) Run(ctx context.Context, mux *ServeMux) {
	interval := 200 * time.Millisecond
	timer := time.NewTimer(interval)
	defer timer.Stop()

	var wg sync.WaitGroup
	defer wg.Wait()

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}

		job, err := s.querier.Dequeue(ctx, s.db)
		if err != nil {
			timer.Reset(interval)
			continue
		}

		if job == nil {
			timer.Reset(interval)
			continue
		}

		timer.Reset(0)

		wg.Add(1)
		go func(job *JobDetails) {
			defer wg.Done()

			jobCtx, cancel := context.WithCancel(ctx)
			defer cancel()

			log := slog.With("job_id", job.ID, "retry", job.Retry)
			if job.RootId != nil {
				log = slog.With("root_id", *job.RootId)
			}

			if jobErr := s.executeJob(jobCtx, mux, job); jobErr != nil {
				log.ErrorContext(jobCtx, "failed to execute job", "err", jobErr)

				if errors.Is(jobErr, ErrSkipRetry) {
					log.ErrorContext(jobCtx, "skip retry", "err", jobErr)
					return
				}

				finalRetry := job.Retry == job.MaxRetries
				if finalRetry {
					log.ErrorContext(jobCtx, "reached final retry", "max_retries", job.MaxRetries)
					return
				}

				newJob, err := s.retry(ctx, job, jobErr)
				if err != nil {
					log.ErrorContext(jobCtx, "unable to retry job", "err", err)
					return
				}

				log.InfoContext(jobCtx, "retried", "retried_id", newJob.ID, "execute_at", newJob.ExecuteAt)

				return
			}

			// Allow completion to finish even if the server context is canceled.
			completeCtx, completeCancel := context.WithTimeout(
				context.WithoutCancel(ctx),
				5*time.Second,
			)
			defer completeCancel()

			if _, err := s.querier.Complete(completeCtx, s.db, job.ID); err != nil {
				log.ErrorContext(completeCtx, "failed to complete the job", "err", err)
			}
		}(job)
	}
}
